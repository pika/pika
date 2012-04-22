# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Callback management class, common area for keeping track of all callbacks in
the Pika stack.

"""
import logging
from warnings import warn

class CallbackWrapper(object):
    __slots__ = ['handle', 'one_shot', 'only']

    def __init__(self, handle, one_shot, only_caller):
        self.handle   = handle
        self.only     = only_caller
        self.one_shot = one_shot

    def __eq__(self, other):
        return self.handle   == other.handle \
           and self.only     == other.only \
           and self.one_shot == other.one_shot

    def __contains__(self, callback):
        return self.handle == callback


def _name_or_value(key):
    """Will take Frame objects, classes, etc and attempt to return a valid
    string identifier for them.

    :param [Object,str,dict] key: The key to sanitize
    :returns: str

    """
    # Is it a Pika AMQP Method class with a NAME attribute?
    if hasattr(key, 'method') and hasattr(key.method, 'NAME'):
        return key.method.NAME

    # Is it a Pika object with a name attribute?
    if hasattr(key, 'NAME'):
        return key.NAME

    # Is it a dictionary with a name key
    if hasattr(key, '__dict__') and 'NAME' in key.__dict__:
        return key.__dict__['NAME']

    # Return the item as a string
    return str(key)


class CallbackManager(object):
    """
    CallbackManager is a global callback system designed to be a single place
    where Pika can manage callbacks and process them. It should be referenced
    by the CallbackManager.instance() method instead of constructing new
    instances of it.
    """

    CALLBACK_CLASS = CallbackWrapper

    def __init__(self):
        """Create an instance of the CallbackManager"""
        self._logger = logging.getLogger('pika.callback.CallbackManager')
        self._callbacks = dict()

    def add(self, prefix, key, callback, one_shot=True, only_caller=None):
        """Add a callback to the stack for the specified key. If the call is
        specified as one_shot, it will be removed after being fired

        The prefix is usually the channel number but the class is generic
        and prefix and key may be any value. If you pass in only_caller
        CallbackManager will restrict processing of the callback to only
        the calling function/object that you specify.

        :param [str,int] prefix: Categorize the callback
        :param [Object,str,dict] key: The key for the callback
        :param method callback: The callback to call
        :param bool one_shot: Remove this callback after it is called
        :param Object only_caller: Only allow one_caller value to call the
            event that fires the callback.
        :returns: tuple(prefix, key)

        """
        # Lets not use objects, since we could have object/class issues
        key = _name_or_value(key)

        callback_slot    = self._callbacks.setdefault(prefix, dict()).setdefault(key, list())

        wrapped_callback = self.CALLBACK_CLASS(callback, one_shot, only_caller)

        if any(wc == wrapped_callback for wc in callback_slot):
            warn('%s.add: Duplicate callback found for "%s:%s"' %\
                 (self.__class__.__name__, prefix, key))
            return

        callback_slot.append(wrapped_callback)
        self._logger.debug('Added "%s:%s" with callback: %s',
                           prefix, key, callback)
        return prefix, key

    def clear(self):
        """Clear all the callbacks if there are any defined."""
        if self._callbacks:
            self._callbacks = dict()

    def pending(self, prefix, key):
        """Return count of callbacks for a given prefix or key or None

        :param [str,int] prefix: Categorize the callback
        :param [Object,str,dict] key: The key for the callback
        :returns: None or int

        """
        # Lets not use objects, since we could have module class/obj
        key = _name_or_value(key)

        if not prefix in self._callbacks or not key in self._callbacks[prefix]:
            return None

        return len(self._callbacks[prefix][key])

    def process(self, prefix, key, caller, *args, **keywords):
        """Run through and process all the callbacks for the specified keys.
        Caller should be specified at all times so that callbacks which
        require a specific function to call CallbackManager.process will
        not be processed.

        :param [str,int] prefix: Categorize the callback
        :param [Object,str,dict] key: The key for the callback
        :param Object caller: Who is firing the event
        :param list args: Any optional arguments
        :param dict keywords: Optional keyword arguments
        :returns: bool


        """
        # Lets not use objects, since we could have module class/obj
        key = _name_or_value(key)

        # Make sure we have a callback for this event
        if prefix not in self._callbacks or key not in self._callbacks[prefix]:
            return False

        callbacks = list()
        one_shot_remove = list()

        # Loop through callbacks that want all prefixes and what we asked for
        for callback in self._callbacks[prefix][key]:
            if not callback.only or callback.only == caller.__class__:
                callbacks.append(callback.handle)

                # If it's a one shot callback, add it to a list for removal
                if callback.one_shot:
                    one_shot_remove.append([prefix, key, callback])

        # Remove the one shot callbacks that were called
        for prefix, key, callback in one_shot_remove:
            self.remove(prefix, key, callback)

        # Prevent recursion
        for callback in callbacks:
            self._logger.debug('Calling %s for "%s:%s"', callback, prefix, key)
            callback(*args, **keywords)

        # Indicate success
        return True

    def remove(self, prefix, key=None, callback=None):
        """Remove a callback from the stack by prefix, key and optionally
        the callback itself. If you only pass in prefix and key, all
        callbacks for that prefix and key will be removed.

        :param str prefix: The prefix for keeping track of callbacks with
        :param str key: The callback key
        :param method callback: The method defined to call on callback
        :returns: bool

        """
        if not prefix in self._callbacks:
            # TODO: log error
            return

        if key is None:
            keys = self._callbacks[prefix].keys()
        else:
            # Lets not use objects, since we could have module class/obj
            keys = [_name_or_value(key)]

        for key in keys:
            if key in self._callbacks[prefix]:

                if callback:
                    # Remove the callback from the _callbacks dict
                    # callback is just a handler
                    if callable(callback):
                        callbacks = self._callbacks[prefix][key]
                        for i in xrange(len(callbacks) - 1, -1, -1):
                            if callback in callbacks[i]:
                                del(callbacks[i])
                                self._logger.debug('Removed %s for "%s:%s"',
                                                   callback, prefix, key)
                    # callback is a CallbackWrapper instance
                    elif callback in self._callbacks[prefix][key]:
                        self._callbacks[prefix][key].remove(callback)
                        self._logger.debug('Removed %s for "%s:%s"',
                                           callback, prefix, key)

                    # Remove the list from the dict if it's empty
                    if not self._callbacks[prefix][key]:
                        del(self._callbacks[prefix][key])
                        self._logger.debug('Removed empty key "%s:%s"',
                                           prefix, key)

                    # Remove the prefix if it's empty
                    if not self._callbacks[prefix]:
                        del(self._callbacks[prefix])
                        self._logger.debug('Removed empty prefix "%s"', prefix)
                    return True
                else:
                    # Remove the list from the dict if it's empty
                    del(self._callbacks[prefix][key])
                    self._logger.debug('Removed key "%s:%s"', prefix, key)

                    # Remove the prefix if it's empty
                    if not self._callbacks[prefix]:
                        del(self._callbacks[prefix])
                        self._logger.debug('Removed empty prefix "%s"', prefix)

        # Prefix, Key or Callback could not be found
        return False

    def cleanup(self, prefix):
        """Remove all callbacks from the stack by a prefix.

        :param str prefix: The prefix for keeping track of callbacks with

        """
        keys = self._callbacks.get(prefix, {}).keys()
        for key in keys:
            self.remove(prefix, key)
