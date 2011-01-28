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

import pika.log as log
import random


class ReconnectionStrategy(object):

    can_reconnect = False

    def on_connect_attempt(self, conn):
        pass

    def on_connect_attempt_failure(self, conn):
        pass

    def on_transport_connected(self, conn):
        pass

    def on_transport_disconnected(self, conn):
        pass

    def on_connection_open(self, conn):
        pass

    def on_connection_closed(self, conn):
        pass


class NullReconnectionStrategy(ReconnectionStrategy):
    pass


class SimpleReconnectionStrategy(ReconnectionStrategy):

    can_reconnect = True

    def __init__(self, initial_retry_delay=1.0, multiplier=2.0,
                 max_delay=30.0, jitter=0.5):

        self.initial_retry_delay = initial_retry_delay
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter
        self._reset()

    def _reset(self):

        log.debug("%s._reset Called" % self.__class__.__name__)
        self.current_delay = self.initial_retry_delay
        self.attempts_since_last_success = 0

    def on_connect_attempt(self, conn):
        log.debug("%s.on_connect_attempt: %r",  self.__class__.__name__, conn)
        self.attempts_since_last_success += 1

    def on_connect_attempt_failure(self, conn):
        log.error("%s.on_connect_attempt_failure: %r",
                  self.__class__.__name__, conn)

    def on_transport_connected(self, conn):
        log.debug("%s.on_transport_connected: %r",
                  self.__class__.__name__, conn)

    def on_transport_disconnected(self, conn):

        log.debug("%s.on_transport_disconnected: %r",
                  self.__class__.__name__, conn)

    def on_connection_open(self, conn):
        log.debug("%s.on_connection_open: %r",
                  self.__class__.__name__, conn)
        self._reset()

    def on_connection_closed(self, conn):
        t = self.current_delay * ((random.random() * self.jitter) + 1)

        log.info("%s retrying %r in %r seconds (%r attempts)",
                 self.__class__.__name__, conn.parameters, t,
                 self.attempts_since_last_success)
        self.current_delay = min(self.max_delay,
                                 self.current_delay * self.multiplier)
        conn.add_timeout(t, conn.reconnect)
