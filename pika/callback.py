# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
#
# ***** END LICENSE BLOCK *****

import logging


class CallbackManager(object):

    def __init__(self):
        # Callback stack for our instance
        self._callbacks = dict()

    @classmethod
    def instance(class_):
        """
        Returns a handle to the already created object or creates a new object
        """
        if not hasattr(class_, "_instance"):
            class_._instance = class_()
        return class_._instance

    def add(self, key, callback, one_shot=True):
        """
        Add a callback to the stack for the specified key. If the call is
        specified as one_shot, it will be removed after being fired
        """
        # If we don't have the key in our callbacks, add it
        if key not in self._callbacks:
            self._callbacks[key] = list()

        # Append the callback to our key list
        self._callbacks[key].append({'handle': callback,
                                     'one_shot': one_shot})

        logging.debug('CallbackManager: Added "%s" with callback: %s' % \
                      callback)

    def remove(self, key):
        """
        Remove a callback from the stack by key
        """
        if key in self._callbacks:
            del(self._callbacks[key])
            logging.debug('CallbackManager: Remove all callbacks for "%s"' % \
                          key)
            return True

        # Key could not be found
        return False

    def process(self, key, *args, **keywords):
        """
        Run through and process all the callbacks for the specified keys
        """
        one_shot_remove = list()
        for callback in self._callbacks[key]:
            logging.debug('CallbackManager: Calling %s for "%s"' % \
                          (callback['handle'], key))
            callback['handle'](*args, **keywords)
            if callback['one_shot']:
                one_shot_remove.append([key, callback])

        # Remove the callback from the key
        for (key, callback) in one_shot_remove:
            self._callbacks[key].remove(callback)

            # Remove the list from the dict if it's empty
            if not self._callbacks[key]:
                del(self._callbacks[key])
