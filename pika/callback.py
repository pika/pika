# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

from warnings import warn
import pika.log


class CallbackManager(object):
    """
    CallbackManager is a global callback system designed to be a single place
    where Pika can manage callbacks and process them. It should be referenced
    by the CallbackManager.instance() method instead of constructing new
    instances of it.
    """

    def __init__(self):
        # Callback stack for our instance
        self._callbacks = dict()

    def sanitize(self, key):
        """
        Will take Frame objects, classes, etc and attempt to return a valid
        string identifier for them.
        """
        if hasattr(key, 'method') and hasattr(key.method, 'NAME'):
            return key.method.NAME

        if hasattr(key, 'NAME'):
            return key.NAME

        if hasattr(key, '__dict__') and 'NAME' in key.__dict__:
            return key.__dict__['NAME']

        return str(key)

    def add(self, prefix, key, callback, one_shot=True, only_caller=None):
        """
        Add a callback to the stack for the specified key. If the call is
        specified as one_shot, it will be removed after being fired

        The prefix is usually the channel number but the class is generic
        and prefix and key may be any value. If you pass in only_caller
        CallbackManager will restrict processing of the callback to only
        the calling function/object that you specify.
        """
        # Lets not use objects, since we could have object/class issues
        key = self.sanitize(key)

        # Make sure we've seen the prefix before
        if prefix not in self._callbacks:
            self._callbacks[prefix] = dict()

        # If we don't have the key in our callbacks, add it
        if key not in self._callbacks[prefix]:
            self._callbacks[prefix][key] = list()

        # Our callback info we need elsewhere in the class
        callback_dict = {'handle': callback, 'one_shot': one_shot}
        if only_caller:
            callback_dict['only'] = only_caller

        # If we passed in that we do not want duplicates, check and keep us
        # from adding it a second time
        if callback_dict in self._callbacks[prefix][key]:
            warn('%s.add: Duplicate callback found for "%s:%s"' %\
                 (self.__class__.__name__, prefix, key))
            return

        # Append the callback to our key list
        self._callbacks[prefix][key].append(callback_dict)
        pika.log.debug('%s: Added "%s:%s" with callback: %s',
                        self.__class__.__name__, prefix, key, callback)
        return prefix, key

    def pending(self, prefix, key):
        """
        Return count of callbacks for a given prefix or key or None
        """
        # Lets not use objects, since we could have module class/obj
        key = self.sanitize(key)

        if not prefix in self._callbacks or not key in self._callbacks[prefix]:
            return None

        return len(self._callbacks[prefix][key])

    def process(self, prefix, key, caller, *args, **keywords):
        """
        Run through and process all the callbacks for the specified keys.
        Caller should be specified at all times so that callbacks which
        require a specific function to call CallbackManager.process will
        not be processed.
        """
        # Lets not use objects, since we could have module class/obj
        key = self.sanitize(key)

        # Make sure we have a callback for this event
        if prefix not in self._callbacks or key not in self._callbacks[prefix]:
            return None

        callbacks = list()
        one_shot_remove = list()

        # Loop through callbacks that want all prefixes and what we asked for
        for callback in self._callbacks[prefix][key]:
            if 'only' not in callback or callback['only'] == caller.__class__:
                callbacks.append(callback['handle'])

                # If it's a one shot callback, add it to a list for removal
                if callback['one_shot']:
                    one_shot_remove.append([prefix, key, callback])

        # Remove the one shot callbacks that were called
        for prefix, key, callback in one_shot_remove:
            self.remove(prefix, key, callback)

        # Prevent recursion
        for callback in callbacks:
            pika.log.debug('CallbackManager: Calling %s for "%s:%s"' % \
                           (callback, prefix, key))
            callback(*args, **keywords)

    def remove(self, prefix, key, callback=None):
        """
        Remove a callback from the stack by prefix, key and optionally
        the callback itself. If you only pass in prefix and key, all
        callbacks for that prefix and key will be removed.
        """
        # Cast our key to a string so we don't get any weirdness
        # Lets not use objects, since we could have module class/obj
        key = self.sanitize(key)

        if prefix in self._callbacks and key in self._callbacks[prefix]:

            if callback:
                # Remove the callback from the _callbacks dict
                if callback in self._callbacks[prefix][key]:
                    self._callbacks[prefix][key].remove(callback)
                    pika.log.debug('%s: Removed %s for "%s:%s"',
                                    self.__class__.__name__, callback,
                                    prefix, key)

                # Remove the list from the dict if it's empty
                if not self._callbacks[prefix][key]:
                    del(self._callbacks[prefix][key])
                    pika.log.debug('%s: Removed empty key "%s:%s"',
                                    self.__class__.__name__, prefix, key)

                # Remove the prefix if it's empty
                if not self._callbacks[prefix]:
                    del(self._callbacks[prefix])
                    pika.log.debug('%s: Removed empty prefix "%s"',
                                    self.__class__.__name__, prefix)
                return True
            else:
                # Remove the list from the dict if it's empty
                del(self._callbacks[prefix][key])
                pika.log.debug('%s: Removed key "%s:%s"',
                                self.__class__.__name__, prefix, key)

                # Remove the prefix if it's empty
                if not self._callbacks[prefix]:
                    del(self._callbacks[prefix])
                    pika.log.debug('%s: Removed empty prefix "%s"',
                                    self.__class__.__name__, prefix)

        else:
            # If we just passed in a prefix for a key
            if prefix in self._callbacks and key in self._callbacks[prefix]:
                del(self._callbacks[prefix][key])
                pika.log.debug('%s: Removed all callbacks for "%s:%s"',
                               self.__class__.__name__, prefix, key)
            return True

        # Prefix, Key or Callback could not be found
        return False
