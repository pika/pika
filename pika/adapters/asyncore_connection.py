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
import sys
import socket
import asyncore
import time
import traceback

from pika.adapters.base_connection import BaseConnection


class RabbitDispatcher(asyncore.dispatcher):

    def __init__(self, connection):
        logging.debug("%s.__init__" % self.__class__.__name__)
        asyncore.dispatcher.__init__(self)
        self.connection = connection
        self.buffer_size = self.connection.suggested_buffer_size()

    def create_socket(self, socket_domain, socket_type):
        logging.debug("%s.create_socket" % self.__class__.__name__)
        asyncore.dispatcher.create_socket(self, socket_domain, socket_type)
        # Disable TCP nagling for improved latency.
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def handle_connect(self):
        logging.debug("%s.handle_connect" % self.__class__.__name__)
        self.connection._on_connected()

    def handle_close(self):
        logging.debug("%s.handle_close" % self.__class__.__name__)

    def _handle_error(self, error):
        logging.debug("%s.handle_error" % self.__class__.__name__)
        if error[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
            return
        elif error[0] == errno.EBADF:
            logging.error("%s: Write to a closed socket" %
                          self.__class__.__name__)
        else:
            logging.error("%s: Write error on %d: %s" %
                          (self.__class__.__name__,
                           self.fileno(), error[0]))
        self.close()
        self.connection._on_connection_closed(None, True)


    def handle_read(self):
        logging.debug("%s.handle_read" % self.__class__.__name__)
        try:
            buf = self.recv(self.buffer_size)
        except socket.error, exn:
            self._handle_error(exn)
            return

        if not buf:
            self.close()
        else:
            self.connection.on_data_available(buf)

    def writable(self):
        return bool(self.connection.outbound_buffer)

    def handle_write(self):
        logging.debug("%s.handle_write" % self.__class__.__name__)

        fragment = self.connection.outbound_buffer.read(self.buffer_size)
        try:
            r = self.send(fragment)
        except socket.error, exn:
            self._handle_error(exn)
            return

        self.connection.outbound_buffer.consume(r)

    def handle_error(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback)


class AsyncoreConnection(BaseConnection):

    def add_timeout(self, delay_sec, callback):
        self.ioloop.add_timeout(delay_sec, callback)

    def connect(self, host, port):
        logging.debug("%s.connect" % self.__class__.__name__)
        self.dispatcher = RabbitDispatcher(self)
        self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dispatcher.connect((host, port))
        self.ioloop = IOLoop().instance()

    def disconnect(self):
        logging.debug("%s.disconnect" % self.__class__.__name__)
        self.dispatcher.close()
        del self.dispatcher
        self.ioloop.stop()



    def flush_outbound(self):
        logging.debug("%s.flush_outbound" % self.__class__.__name__)
        while self.outbound_buffer:
            self.ioloop.loop(1)


class IOLoop(object):
    """
    IOLoop Emulation layer to make adapters behave the same way
    """

    def __init__(self):

        # Define if we should be looping or not
        self.running = True

        # Timer related functionality
        self.timeouts = dict()

        # Use poll in asyncore
        import select
        if hasattr(select, 'poll'):
            self.use_poll = True
        else:
            self.use_poll = False

    @classmethod
    def instance(class_):
        """
        Returns a handle to the already created object or creates a new object
        """
        if not hasattr(class_, "_instance"):
            class_._instance = class_()
        return class_._instance


    def add_timeout(self, delay_sec, handler):
        # Calculate our deadline for running the callback
        deadline = time.time() + delay_sec
        logging.debug('%s.add_timeout: In %.4f seconds call %s' % \
                      (self.__class__.__name__,
                       deadline - time.time(),
                       handler))
        self.timeouts[deadline] = handler

    def process_timeouts(self):

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

        # Loop while we are connected
        while self.running:

            # Loop in asycore for one second
            asyncore.loop(timeout=1, use_poll=self.use_poll, count=1)

            # Process our timeouts
            self.process_timeouts()

    def stop(self):

        # Stop the IOLoop from running by exiting the while in IOLoop.stop
        self.running = False

    def loop(self, count):

        # Force a loop while ignoring timer
        asyncore.loop(timeout=1, use_poll=self.use_poll, count=1)
