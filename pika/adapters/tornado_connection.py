#!/usr/bin/env python
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
import tornado.ioloop

from pika.adapters.base_connection import BaseConnection

# Redefine our constants with Tornado's
ERROR = tornado.ioloop.IOLoop.ERROR
READ = tornado.ioloop.IOLoop.READ
WRITE = tornado.ioloop.IOLoop.WRITE


class TornadoConnection(BaseConnection):

    def _adapter_connect(self, host, port):
        """
        Connect to the given host and port
        """
        BaseConnection._adapter_connect(self, host, port)

        # Setup our ioloop
        self.ioloop = tornado.ioloop.IOLoop.instance()

        # Add the ioloop handler for the event state
        self.ioloop.add_handler(self.socket.fileno(),
                                self._handle_events,
                                self.event_state)

        # Let everyone know we're connected
        self._on_connected()

    def _adapter_disconnect(self):
        log.debug('%s.disconnect', self.__class__.__name__)

        # Remove from the IOLoop
        self.ioloop.remove_handler(self.socket.fileno())

        # Close our socket since the Connection class told us to do so
        self.socket.close()

        msg = "Tornado IOLoop may be running but Pika has shutdown."
        log.warning(msg)
