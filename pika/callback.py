"""Callback management class, common area for keeping track of all callbacks in
the Pika stack.

"""
import functools
import logging

from pika import frame
from pika import amqp_object

LOGGER = logging.getLogger(__name__)


def _name_or_value(value):
    """Will take Frame objects, classes, etc and attempt to return a valid
    string identifier for them.

    :param value: The value to sanitize
    :type value:  pika.amqp_object.AMQPObject|pika.frame.Frame|int|unicode|str
    :rtype: str

    """
    # Is it subclass of AMQPObject
    try:
        if issubclass(value, amqp_object.AMQPObject):
            return value.NAME
    except TypeError:
        pass

    # Is it a Pika frame object?
    if isinstance(value, frame.Method):
        return value.method.NAME

    # Is it a Pika frame object (go after Method since Method extends this)
    if isinstance(value, amqp_object.AMQPObject):
        return value.NAME

    # Cast the value to a string, encoding it if it's unicode
    try:
        return str(value)
    except UnicodeEncodeError:
        return str(value.encode('utf-8'))


def sanitize_prefix(function):
    """Automatically call _name_or_value on the prefix passed in."""
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        LOGGER.debug('Args: %r, kwargs: %r', args, kwargs)
        args = list(args)
        offset = 1
        if 'prefix' in kwargs:
            kwargs['prefix'] = _name_or_value(kwargs['prefix'])
        elif len(args) - 1  >= offset:
            args[offset] = _name_or_value(args[offset])
            offset += 1
        if 'key' in kwargs:
            kwargs['key'] = _name_or_value(kwargs['key'])
        elif len(args) - 1 >= offset:
            args[offset] = _name_or_value(args[offset])

        return function(*tuple(args), **kwargs)
    return wrapper


def check_for_prefix_and_key(function):
    """Automatically return false if the key or prefix is not in the callbacks
    for the instance.

    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        offset = 1
        # Sanitize the prefix
        if 'prefix' in kwargs:
            prefix = _name_or_value(kwargs['prefix'])
        else:
            prefix = _name_or_value(args[offset])
            offset += 1

        # Make sure to sanitize the key as well
        if 'key' in kwargs:
            key = _name_or_value(kwargs['key'])
        else:
            key = _name_or_value(args[offset])

        # Make sure prefix and key are in the stack
        if (prefix not in args[0]._stack or
            key not in args[0]._stack[prefix]):
            return False

        # Execute the method
        return function(*args, **kwargs)
    return wrapper


class CallbackManager(object):
    """
    CallbackManager is a global callback system designed to be a single place
    where Pika can manage callbacks and process them. It should be referenced
    by the CallbackManager.instance() method instead of constructing new
    instances of it.
    """
    DUPLICATE_WARNING = 'Duplicate callback found for "%s:%s"'
    CALLBACK = 'callback'
    ONE_SHOT = 'one_shot'
    ONLY_CALLER = 'only'

    def __init__(self):
        """Create an instance of the CallbackManager"""
        self._stack = dict()

    @sanitize_prefix
    def add(self, prefix, key, callback, one_shot=True, only_caller=None):
        """Add a callback to the stack for the specified key. If the call is
        specified as one_shot, it will be removed after being fired

        The prefix is usually the channel number but the class is generic
        and prefix and key may be any value. If you pass in only_caller
        CallbackManager will restrict processing of the callback to only
        the calling function/object that you specify.

        :param str|int prefix: Categorize the callback
        :param object|str|dict key: The key for the callback
        :param method callback: The callback to call
        :param bool one_shot: Remove this callback after it is called
        :param object only_caller: Only allow one_caller value to call the
                                   event that fires the callback.
        :rtype: tuple(prefix, key)

        """
        # Prep the stack
        if prefix not in self._stack:
            self._stack[prefix] = dict()
        if key not in self._stack[prefix]:
            self._stack[prefix][key] = list()

        # Create the callback dictionary
        callback_dict = self._callback_dict(callback, one_shot, only_caller)

        # Check for a duplicate
        if callback_dict in self._stack[prefix][key]:
            LOGGER.warning(self.DUPLICATE_WARNING, prefix, key)
            return prefix, key

        # Append the callback to the stack
        self._stack[prefix][key].append(callback_dict)
        LOGGER.debug('Added "%s:%s" with callback: %r', prefix, key, callback)
        return prefix, key

    def clear(self):
        """Clear all the callbacks if there are any defined."""
        if self._stack:
            self._stack = dict()
            LOGGER.debug('Callbacks cleared')

    @sanitize_prefix
    def cleanup(self, prefix):
        """Remove all callbacks from the stack by a prefix. Returns True
        if keys were there to be removed

        :param str prefix: The prefix for keeping track of callbacks with
        :rtype: bool

        """
        if prefix not in self._stack or  not self._stack[prefix]:
            return False
        del self._stack[prefix]

    @sanitize_prefix
    def pending(self, prefix, key):
        """Return count of callbacks for a given prefix or key or None

        :param str|int prefix: Categorize the callback
        :param Object|str|dict key: The key for the callback
        :rtype: None or int

        """
        if not prefix in self._stack or not key in self._stack[prefix]:
            return None
        return len(self._stack[prefix][key])

    @sanitize_prefix
    @check_for_prefix_and_key
    def process(self, prefix, key, caller, *args, **keywords):
        """Run through and process all the callbacks for the specified keys.
        Caller should be specified at all times so that callbacks which
        require a specific function to call CallbackManager.process will
        not be processed.

        :param str|int prefix: Categorize the callback
        :param Object|str|dict key: The key for the callback
        :param Object caller: Who is firing the event
        :param list args: Any optional arguments
        :param dict keywords: Optional keyword arguments
        :rtype: bool


        """
        LOGGER.debug('Processing %s:%s', prefix, key)

        callbacks, removals = list(), list()
        # Check each callback, append it to the list if it should be called
        for callback_dict in self._stack[prefix][key]:
            if self._should_process_callback(callback_dict, caller):
                callbacks.append(callback_dict[self.CALLBACK])
                # Remove from the stack if it's a one shot callback
                if callback_dict[self.ONE_SHOT]:
                    removals.append([prefix, key, callback_dict[self.CALLBACK]])

        # Remove the one shot callbacks
        for prefix, key, callback in removals:
            self.remove(prefix, key, callback)

        # Call each callback
        for callback in callbacks:
            LOGGER.debug('Calling %s for "%s:%s"', callback, prefix, key)
            callback(*args, **keywords)
        return True


    @sanitize_prefix
    @check_for_prefix_and_key
    def remove(self, prefix, key, callback_value=None):
        """Remove a callback from the stack by prefix, key and optionally
        the callback itself. If you only pass in prefix and key, all
        callbacks for that prefix and key will be removed.

        :param str prefix: The prefix for keeping track of callbacks with
        :param str key: The callback key
        :param method callback_value: The method defined to call on callback
        :rtype: bool

        """
        if callback_value:
            offsets_to_remove = list()
            for offset in xrange(len(self._stack[prefix][key]), 0, -1):
                if (callback_value ==
                        self._stack[prefix][key][offset - 1][self.CALLBACK]):
                    offsets_to_remove.append(offset - 1)
            for offset in offsets_to_remove:
                del self._stack[prefix][key][offset]
            if not self._stack[prefix][key]:
                del self._stack[prefix][key]
        else:
            del self._stack[prefix][key]
        if not self._stack[prefix]:
            del self._stack[prefix]
        return True

    def _callback_dict(self, callback, one_shot, only_caller):
        """Return the callback dictionary.

        :param method callback: The callback to call
        :param bool one_shot: Remove this callback after it is called
        :param object only_caller: Only allow one_caller value to call the
                                   event that fires the callback.
        :rtype: dict

        """
        return {self.CALLBACK: callback,
                self.ONE_SHOT: one_shot,
                self.ONLY_CALLER: only_caller}

    def _should_process_callback(self, callback_dict, caller):
        """Returns True if the callback should be processed.

        :param dict callback_dict: The callback configuration
        :param Object caller: Who is firing the event
        :rtype: bool

        """
        return (callback_dict[self.ONLY_CALLER] is None or
                (callback_dict[self.ONLY_CALLER] and
                 callback_dict[self.ONLY_CALLER] == caller))
