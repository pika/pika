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

import socket
import pika.connection

class BlockingConnection(pika.connection.Connection):
    def connect(self, host, port):
        self.socket = socket.socket()
        self.socket.connect((host, port))
        self.on_connected()

    def disconnect_transport(self):
        self.socket.close()
        self.on_disconnected()

    def _recv(self, bufsize, timeout=None):
        prev_timeout = self.socket.gettimeout()
        self.socket.settimeout(timeout)
        try:
            return self.socket.recv(bufsize)
        finally:
            self.socket.settimeout(prev_timeout)

    def mainloop(self):
        while self.is_alive():
            self.drain_events()

    def drain_events(self, timeout=None):
        while self.outbound_buffer:
            fragment = self.outbound_buffer.read()
            r = self.socket.send(fragment)
            self.outbound_buffer.consume(r)

        try:
            buf = self._recv(self.suggested_buffer_size(), timeout)
        except socket.timeout:
            # subclass of socket.error catched below, so re-raise.
            raise
        except socket.error, exn:
            if exn.errno == EAGAIN:
                # Weird, but happens very occasionally.
                return
            else:
                self.disconnect_transport()
                return

        if not buf:
            self.disconnect_transport()
            return

        self.on_data_available(buf)
