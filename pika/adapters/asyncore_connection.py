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

import asyncore
import errno
import logging
import socket
import time

from pika.adapters.base_connection import BaseConnection


class AsyncoreDispatcher(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.connect((host, port))

        self.buffer_size = 8192
        self.connecting = True
        self.connection = None
        self.timeouts = dict()
        self.writable_ = False

    def handle_connect(self):
        logging.debug("%s.handle_connect" % self.__class__.__name__)
        self.connecting = False
        self.connection._on_connected()

    def handle_close(self):
        logging.debug("%s.handle_close" % self.__class__.__name__)
        # If we're not already closing or closed, disconnect the Connection
        if not self.connection.closing and not self.connection.closed:
            self.connection.disconnect()

    def handle_error_(self, error):
        logging.debug("%s.handle_error" % self.__class__.__name__)
        if error[0] in (errno.EWOULDBLOCK, errno.EAGAIN, error.EINTR):
            return
        elif error[0] == errno.EBADF:
            logging.error("%s: Socket is closed" %
                          self.__class__.__name__)
            self.handle_close()
        else:
            logging.error("%s: Socket Error on %d: %s" %
                          (self.__class__.__name__,
                           self._fileno, error[0]))
        self.connection._on_connection_closed(None, True)
        self.close()

    def handle_read(self):
        logging.debug("%s.handle_read" % self.__class__.__name__)
        try:
            data_in = self.recv(self.buffer_size)
        except socket.error, e:
            return self.handle_error(e)

        if data_in:
            self.connection.on_data_available(data_in)

    def handle_write(self):

        if self.connection.outbound_buffer.size:
            try:
                data = self.connection.outbound_buffer.read(self.buffer_size)
                sent = self.send(data)
            except socket.error, e:
                return self.handle_error(e)

            # If we sent data, remove it from the buffer
            if sent:
                # Remove the length written from the buffer
                self.connection.outbound_buffer.consume(sent)

                # If our buffer is empty, turn off writing
                if not self.connection.outbound_buffer.size:
                    self.writable_ = False

    def writable(self):
        if not self.connected:
            return True
        return self.writable_

    # IOLoop Compatibility

    @classmethod
    def instance(class_):
        """
        Returns a handle to the already created object or creates a new object
        """
        if not hasattr(class_, "_instance"):
            class_._instance = class_()
        return class_._instance

    def add_timeout(self, delay_sec, handler):
        logging.debug("%s.add_timeout" % self.__class__.__name__)

        # Calculate our deadline for running the callback
        deadline = time.time() + delay_sec
        logging.debug('%s.add_timeout: In %.4f seconds call %s' % \
                      (self.__class__.__name__,
                       deadline - time.time(),
                       handler))
        self.timeouts[deadline] = handler

    def process_timeouts(self):
        logging.debug("%s.process_timeouts" % self.__class__.__name__)
        # Process our timeout events
        deadlines = self.timeouts.keys()

        start_time = time.time()
        for deadline in deadlines:
            if deadline <= start_time:
                logging.debug("%s: Timeout calling %s" %\
                              (self.__class__.__name__,
                               self.timeouts[deadline]))
                self.timeouts[deadline]()
                del(self.timeouts[deadline])

    def start(self):
        logging.debug("%s.start" % self.__class__.__name__)
        while self.connected or self.connecting:
            asyncore.loop(timeout=1, count=1)
            self.process_timeouts()

    def stop(self):
        logging.debug("%s.stop" % self.__class__.__name__)
        if self.connected:
            self.close()


class AsyncoreConnection(BaseConnection):

    def add_timeout(self, delay_sec, callback):
        self.ioloop.add_timeout(delay_sec, callback)

    def connect(self, host, port):
        logging.debug("%s.connect" % self.__class__.__name__)
        self.ioloop = AsyncoreDispatcher(host, port)
        self.ioloop.buffer_size = self.suggested_buffer_size()
        self.ioloop.connection = self

    def disconnect(self):
        logging.debug("%s.disconnect" % self.__class__.__name__)
        self.ioloop.stop()
        self.ioloop.close()
        del self.ioloop

    def flush_outbound(self):
        logging.debug("%s.flush_outbound" % self.__class__.__name__)
        if self.outbound_buffer.size:
            self.ioloop.writable_ = True
