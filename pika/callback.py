"""Callback management class, common area for keeping track of all callbacks in
the Pika stack.

"""
from __future__ import annotations

import functools
import logging
from typing import Any, Callable, Type, Union, cast

from pika import amqp_object, frame

_Prefix = Union[str, int]
_Caller = object
_Callback = Callable[..., Any]
# Custom type for values that can be converted to AMQP identifiers
AMQPValue = Union[Type[amqp_object.AMQPObject], amqp_object.AMQPObject, int,
                  str]

LOGGER = logging.getLogger(__name__)


def name_or_value(value: AMQPValue) -> str:
    """Will take Frame objects, classes, etc and attempt to return a valid
    string identifier for them.

    :param value: The value to sanitize
    :rtype: str

    """
    # Is it subclass of AMQPObject
    if isinstance(value, type) and issubclass(value, amqp_object.AMQPObject):
        return value.NAME

    # Is it a Pika frame object?
    if isinstance(value, frame.Method):
        return value.method.NAME

    # Is it a Pika frame object (go after Method since Method extends this)
    if isinstance(value, amqp_object.AMQPObject):
        return value.NAME

    # Cast the value to a string
    return str(value)


def sanitize_prefix(function: _Callback) -> _Callback:
    """Automatically call name_or_value on the prefix passed in.

    :param function: The function to wrap
    :rtype: _Callback
    """

    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        arg_list = list(args)
        offset = 1
        if 'prefix' in kwargs:
            kwargs['prefix'] = name_or_value(kwargs['prefix'])
        elif len(arg_list) - 1 >= offset:
            arg_list[offset] = name_or_value(arg_list[offset])
            offset += 1
        if 'key' in kwargs:
            kwargs['key'] = name_or_value(kwargs['key'])
        elif len(arg_list) - 1 >= offset:
            arg_list[offset] = name_or_value(arg_list[offset])

        return function(*arg_list, **kwargs)

    return cast(_Callback, wrapper)


def check_for_prefix_and_key(function: _Callback) -> _Callback:
    """Automatically return false if the key or prefix is not in the callbacks
    for the instance.

    :param function: Callback to validate
    :rtype: _Callback

    """

    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        offset = 1
        # Sanitize the prefix
        if 'prefix' in kwargs:
            prefix = name_or_value(kwargs['prefix'])
        else:
            prefix = name_or_value(args[offset])
            offset += 1

        # Make sure to sanitize the key as well
        if 'key' in kwargs:
            key = name_or_value(kwargs['key'])
        else:
            key = name_or_value(args[offset])

        # Make sure prefix and key are in the stack
        if prefix not in args[0]._stack or key not in args[0]._stack[prefix]:
            return False

        # Execute the method
        return function(*args, **kwargs)

    return cast(_Callback, wrapper)


class CallbackManager:
    """CallbackManager is a global callback system designed to be a single place
    where Pika can manage callbacks and process them. It should be referenced
    by the CallbackManager.instance() method instead of constructing new
    instances of it.

    """
    CALLS: str = 'calls'
    ARGUMENTS: str = 'arguments'
    DUPLICATE_WARNING: str = 'Duplicate callback found for "%s:%s"'
    CALLBACK: str = 'callback'
    ONE_SHOT: str = 'one_shot'
    ONLY_CALLER: str = 'only'

    def __init__(self) -> None:
        """Create an instance of the CallbackManager"""
        self._stack: dict[_Prefix, dict[AMQPValue, list[dict[str, Any]]]] = {}

    @sanitize_prefix
    def add(self,
            prefix: _Prefix,
            key: AMQPValue,
            callback: Callable[..., Any],
            one_shot: bool = True,
            only_caller: _Caller | None = None,
            arguments: dict[str, Any] | None = None) -> tuple[_Prefix, Any]:
        """Add a callback to the stack for the specified key. If the call is
        specified as one_shot, it will be removed after being fired

        The prefix is usually the channel number but the class is generic
        and prefix and key may be any value. If you pass in only_caller
        CallbackManager will restrict processing of the callback to only
        the calling function/object that you specify.

        :param prefix: Categorize the callback
        :param key: The key for the callback
        :param callback: The callback to call
        :param one_shot: Remove this callback after it is called
        :param only_caller: Only allow one_caller value to call the
                                   event that fires the callback.
        :param arguments: Arguments to validate when processing
        :rtype: tuple(prefix, key)

        """
        # Prep the stack
        if prefix not in self._stack:
            self._stack[prefix] = {}

        if key not in self._stack[prefix]:
            self._stack[prefix][key] = []

        # Check for a duplicate
        for callback_dict in self._stack[prefix][key]:
            if (callback_dict[self.CALLBACK] == callback and
                    callback_dict[self.ARGUMENTS] == arguments and
                    callback_dict[self.ONLY_CALLER] == only_caller):
                if callback_dict[self.ONE_SHOT] is True:
                    callback_dict[self.CALLS] += 1
                    LOGGER.debug('Incremented callback reference counter: %r',
                                 callback_dict)
                else:
                    LOGGER.warning(self.DUPLICATE_WARNING, prefix, key)
                return prefix, key

        # Create the callback dictionary
        callback_dict = self._callback_dict(callback, one_shot, only_caller,
                                            arguments)
        self._stack[prefix][key].append(callback_dict)
        LOGGER.debug('Added: %r', callback_dict)
        return prefix, key

    def clear(self) -> None:
        """Clear all the callbacks if there are any defined."""
        self._stack = {}
        LOGGER.debug('Callbacks cleared')

    @sanitize_prefix
    def cleanup(self, prefix: _Prefix) -> bool:
        """Remove all callbacks from the stack by a prefix. Returns True
        if keys were there to be removed

        :param prefix: The prefix for keeping track of callbacks with
        :rtype: bool

        """
        LOGGER.debug('Clearing out %r from the stack', prefix)
        if prefix not in self._stack or not self._stack[prefix]:
            return False
        del self._stack[prefix]
        return True

    @sanitize_prefix
    def pending(self, prefix: _Prefix, key: AMQPValue) -> int | None:
        """Return count of callbacks for a given prefix or key or None

        :param prefix: Categorize the callback
        :param key: The key for the callback
        :rtype: int | None

        """
        if prefix not in self._stack or key not in self._stack[prefix]:
            return None
        return len(self._stack[prefix][key])

    @sanitize_prefix
    @check_for_prefix_and_key
    def process(self, prefix: _Prefix, key: AMQPValue, caller: _Caller, *args:
                Any, **keywords: Any) -> bool:
        """Run through and process all the callbacks for the specified keys.
        Caller should be specified at all times so that callbacks which
        require a specific function to call CallbackManager.process will
        not be processed.

        :param prefix: Categorize the callback
        :param key: The key for the callback
        :param caller: Who is firing the event
        :param args: Any optional arguments
        :param keywords: Optional keyword arguments
        :rtype: bool

        """
        LOGGER.debug('Processing %s:%s', prefix, key)
        if prefix not in self._stack or key not in self._stack[prefix]:
            return False

        callbacks = []
        # Check each callback, append it to the list if it should be called
        for callback_dict in list(self._stack[prefix][key]):
            if self._should_process_callback(callback_dict, caller, list(args)):
                callbacks.append(callback_dict[self.CALLBACK])
                if callback_dict[self.ONE_SHOT]:
                    self._use_one_shot_callback(prefix, key, callback_dict)

        # Call each callback
        for callback in callbacks:
            LOGGER.debug('Calling %s for "%s:%s"', callback, prefix, key)
            try:
                callback(*args, **keywords)
            except Exception:
                LOGGER.exception('Calling %s for "%s:%s" failed', callback,
                                 prefix, key)
                raise
        return True

    @sanitize_prefix
    @check_for_prefix_and_key
    def remove(self,
               prefix: _Prefix,
               key: AMQPValue,
               callback_value: Callable[..., Any] | None = None,
               arguments: dict[str, Any] | None = None) -> bool:
        """Remove a callback from the stack by prefix, key and optionally
        the callback itself. If you only pass in prefix and key, all
        callbacks for that prefix and key will be removed.

        :param prefix: The prefix for keeping track of callbacks with
        :param key: The callback key
        :param callback_value: The method defined to call on callback
        :param arguments: Optional arguments to check
        :rtype: bool

        """
        if callback_value:
            offsets_to_remove = []
            for offset in range(len(self._stack[prefix][key]), 0, -1):
                callback_dict = self._stack[prefix][key][offset - 1]

                if (callback_dict[self.CALLBACK] == callback_value and
                        self._arguments_match(callback_dict, [arguments])):
                    offsets_to_remove.append(offset - 1)

            for offset in offsets_to_remove:
                try:
                    LOGGER.debug('Removing callback #%i: %r', offset,
                                 self._stack[prefix][key][offset])
                    del self._stack[prefix][key][offset]
                except KeyError:
                    pass

        self._cleanup_callback_dict(prefix, key)
        return True

    @sanitize_prefix
    @check_for_prefix_and_key
    def remove_all(self, prefix: _Prefix, key: AMQPValue) -> None:
        """Remove all callbacks for the specified prefix and key.

        :param prefix: The prefix for keeping track of callbacks with
        :param key: The callback key

        """
        del self._stack[prefix][key]
        self._cleanup_callback_dict(prefix, key)

    def _arguments_match(self, callback_dict: dict[str, Any],
                         args: list[Any]) -> bool:
        """Validate if the arguments passed in match the expected arguments in
        the callback_dict. We expect this to be a frame passed in to *args for
        process or passed in as a list from remove.

        :param callback_dict: The callback dictionary to evaluate against
        :param args: The arguments passed in as a list

        :rtype: bool
        """
        if callback_dict[self.ARGUMENTS] is None:
            return True
        if not args:
            return False
        if isinstance(args[0], dict):
            return self._dict_arguments_match(args[0],
                                              callback_dict[self.ARGUMENTS])
        return self._obj_arguments_match(
            args[0].method if hasattr(args[0], 'method') else args[0],
            callback_dict[self.ARGUMENTS])

    def _callback_dict(self, callback: Callable[..., Any], one_shot: bool,
                       only_caller: _Caller,
                       arguments: dict[str, Any] | None) -> dict[str, Any]:
        """Return the callback dictionary.

        :param callback: The callback to call
        :param one_shot: Remove this callback after it is called
        :param only_caller: Only allow one_caller value to call the
                                   event that fires the callback.
        :param arguments: arguments to attach to the callback dict
        :rtype: dict

        """
        value = {
            self.CALLBACK: callback,
            self.ONE_SHOT: one_shot,
            self.ONLY_CALLER: only_caller,
            self.ARGUMENTS: arguments
        }
        if one_shot:
            value[self.CALLS] = 1
        return value

    def _cleanup_callback_dict(self,
                               prefix: _Prefix,
                               key: AMQPValue | None = None) -> None:
        """Remove empty dict nodes in the callback stack.

        :param prefix: The prefix for keeping track of callbacks with
        :param key: The callback key

        """
        if key and key in self._stack[prefix] and not self._stack[prefix][key]:
            del self._stack[prefix][key]
        if prefix in self._stack and not self._stack[prefix]:
            del self._stack[prefix]

    @staticmethod
    def _dict_arguments_match(value: dict[str, Any],
                              expectation: dict[str, Any]) -> bool:
        """Checks an dict to see if it has attributes that meet the expectation.

        :param value: The dict to evaluate
        :param expectation: The values to check against
        :rtype: bool

        """
        LOGGER.debug('Comparing %r to %r', value, expectation)
        for key, expected in expectation.items():
            if value.get(key) != expected:
                LOGGER.debug('Values in dict do not match for %s', key)
                return False
        return True

    @staticmethod
    def _obj_arguments_match(value: object, expectation: dict[str,
                                                              Any]) -> bool:
        """Checks an object to see if it has attributes that meet the
        expectation.

        :param value: The object to evaluate
        :param expectation: The values to check against
        :rtype: bool

        """
        for key, expected in expectation.items():
            if not hasattr(value, key):
                LOGGER.debug('%r does not have required attribute: %s',
                             type(value), key)
                return False
            if getattr(value, key) != expected:
                LOGGER.debug('Values in %s do not match for %s', type(value),
                             key)
                return False
        return True

    def _should_process_callback(self, callback_dict: dict[str, Any],
                                 caller: _Caller, args: list[Any]) -> bool:
        """Returns True if the callback should be processed.

        :param callback_dict: The callback configuration
        :param caller: Who is firing the event
        :param args: Any optional arguments
        :rtype: bool

        """
        if not self._arguments_match(callback_dict, args):
            LOGGER.debug('Arguments do not match for %r, %r', callback_dict,
                         args)
            return False
        return (callback_dict[self.ONLY_CALLER] is None or
                (callback_dict[self.ONLY_CALLER] and
                 callback_dict[self.ONLY_CALLER] == caller))

    def _use_one_shot_callback(self, prefix: _Prefix, key: AMQPValue,
                               callback_dict: dict[str, Any]) -> None:
        """Process the one-shot callback, decrementing the use counter and
        removing it from the stack if it's now been fully used.

        :param prefix: The prefix for keeping track of callbacks with
        :param key: The callback key
        :param callback_dict: The callback dict to process

        """
        LOGGER.debug('Processing use of oneshot callback')
        callback_dict[self.CALLS] -= 1
        LOGGER.debug('%i registered uses left', callback_dict[self.CALLS])

        if callback_dict[self.CALLS] <= 0:
            self.remove(prefix, key, callback_dict[self.CALLBACK],
                        callback_dict[self.ARGUMENTS])
