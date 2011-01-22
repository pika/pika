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


import errno
import logging
import socket
import time
import tornado.ioloop

from pika.adapter import BaseConnection


class TornadoConnection(BaseConnection):

    def add_timeout(self, delay_sec, callback):

        deadline = time.time() + delay_sec
        self.io_loop.add_timeout(deadline, callback)

    def connect(self, host, port):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.sock.connect((host, port))
        self.sock.setblocking(0)
        self.io_loop = tornado.ioloop.IOLoop.instance()

        # Append our handler to tornado's ioloop for our socket
        events = tornado.ioloop.IOLoop.READ | tornado.ioloop.IOLoop.ERROR \
               | tornado.ioloop.IOLoop.WRITE
        self.io_loop.add_handler(self.sock.fileno(),
                                 self._handle_events,
                                 events)

        # Suggested Buffer Size
        self.buffer_size = self.suggested_buffer_size()

        # Let everyone know we're connected
        self.on_connected()

    def disconnect(self):

        # Remove from the IOLoop
        self.io_loop.remove_handler(self.sock.fileno())

        # Close our socket since the Conneciton class told us to do so
        self.sock.close()

        # Let everyone know we're done
        self.on_disconnected()

    def _disconnect(self, error):

        # Remove from the IOLoop
        self.io_loop.remove_handler(self.sock.fileno())

        # Close our socket since the Conneciton class told us to do so
        self.sock.close()

        self.on_disconnected(error)

    def flush_outbound(self):

        # Make sure we've written out our buffer but dont make a timer
        #self._handle_write(False)
        pass

    def _handle_events(self, fd, events):

        # Incoming events from IOLoop, make sure we have our socket
        if not self.sock:
            logging.warning("Got events for closed stream %d", fd)
            return

        if events & tornado.ioloop.IOLoop.READ:
            self._handle_read()

        if events & tornado.ioloop.IOLoop.ERROR:
            self.sock.close()

        if events * tornado.ioloop.IOLoop.WRITE:
            self._handle_write()

    def _handle_error(self, error):

        if error[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
            return
        elif e[0] == errno.EBADF:
            logging.error("%s: Write to a closed socket" %
                          self.__class__.__NAME__)
        else:
            logging.error("%s: Write error on %d: %s" %
                          (self.__class__.__NAME__,
                           self.sock.fileno(), error))
        self._disconnect(str(error))

    def _handle_read(self):

        try:
            self.on_data_available(self.sock.recv(self.buffer_size))
        except socket.error, e:
            self._handle_error(e)


    def _handle_write(self):

        if len(self.outbound_buffer):
            fragment = self.outbound_buffer.read(self.buffer_size)
            try:
                r = self.sock.send(fragment)
            except socket.error, e:
                self._handle_error(e)

            # Remove the content we used from our buffer
            self.outbound_buffer.consume(r)

