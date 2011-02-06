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
import pika.log as log
import select
import socket
import time

from pika.adapters.base_connection import BaseConnection


class AsyncoreDispatcher(asyncore.dispatcher):
    """
    We extend asyncore.dispatcher here and throw in everything we need to
    handle both asyncore's needs and pika's. In the async adapter structure
    we expect a ioloop behavior which includes timeouts and a start and stop
    function.
    """

    def __init__(self, host, port):
        """
        Initialize the dispatcher, socket and our defaults. We turn of nageling
        in the socket to allow for faster throughput.
        """
        asyncore.dispatcher.__init__(self)

        # Create the socket, turn off nageling and connect
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.connect((host, port))

        # Setup defaults
        self.connecting = True
        self.connection = None
        self._timeouts = dict()
        self.writable_ = False
        self.map = None

    def handle_connect(self):
        """
        asyncore required method. Is called on connection.
        """
        log.debug("%s.handle_connect", self.__class__.__name__)
        self.connecting = False
        self.connection._on_connected()

        # Make our own map to pass in places
        self.map = dict({self.socket.fileno(): self})

    def handle_close(self):
        """
        asyncore required method. Is called on close.
        """
        log.debug("%s.handle_close", self.__class__.__name__)
        # If we're not already closing or closed, disconnect the Connection
        if not self.connection.closing and not self.connection.closed:
            self.connection._adapter_disconnect()

    def handle_read(self):
        """
        Read from the socket and call our on_data_available with the data
        """
        try:
            data = self.recv(self.suggested_buffer_size)
        except socket.timeout:
            raise
        except socket.error, error:
            return self._handle_error(error)

        # We received no data, so disconnect
        if not data:
            return self.connection._adapter_disconnect()

        # Pass the data into our top level frame dispatching method
        self.connection._on_data_available(data)

    def handle_write(self):
        """
        asyncore required function, is called when we can write to the socket
        """
        data = self.connection.outbound_buffer.read(self.suggested_buffer_size)
        try:
            bytes_written = self.send(data)
        except socket.timeout:
            raise
        except socket.error, error:
            return self._handle_error(error)

        # Remove the content we used from our buffer
        if not bytes_written:
            return self.connection._adapter_disconnect()

        # Remove what we wrote from the outbound buffer
        self.connection.outbound_buffer.consume(bytes_written)

        # If our buffer is empty, turn off writing
        if not self.connection.outbound_buffer.size:
            self.writable_ = False

    def writable(self):
        """
        asyncore required function, used to toggle the write bit on the
        select poller. For some reason, if we return false while connecting
        asyncore hangs, so we check for that explicitly and tell it that
        it can write while it's connecting.
        """
        if not self.connected:
            return True

        # Flag maintained by AsyncoreConneciton.flush_outbound and
        # self.handle_write
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

    def add_timeout(self, deadline, handler):
        """
        Add a timeout to the stack by deadline
        """
        log.debug('%s.add_timeout(deadline=%.4f,handler=%s)',
                  self.__class__.__name__, deadline, handler)
        timeout_id = 'id%.8f' % time.time()
        self._timeouts[timeout_id] = {'deadline': deadline,
                                      'handler': handler}
        return timeout_id

    def remove_timeout(self, timeout_id):
        """
        Remove a timeout from the stack
        """
        if timeout_id in self._timeouts:
            del self._timeouts[timeout_id]

    def _process_timeouts(self):
        """
        Process our self._timeouts event stack
        """
        log.debug("%s.process_timeouts", self.__class__.__name__)
        # Process our timeout events
        keys = self._timeouts.keys()
        start_time = time.time()
        for timeout_id in keys:
            if timeout_id in self._timeouts and \
               self._timeouts[timeout_id]['deadline'] <= start_time:
                log.debug('%s: Timeout calling %s',
                          self.__class__.__name__,
                          self._timeouts[timeout_id]['handler'])
                self._timeouts[timeout_id]['handler']()
                del(self._timeouts[timeout_id])

    def start(self):
        """
        Pika Adapter IOLoop start function. This blocks until we are no longer
        connected.
        """
        log.debug("%s.start", self.__class__.__name__)
        while self.connected or self.connecting:
            try:
                # Use our socket map if we've made it, makes things less buggy
                if self.map:
                    asyncore.loop(timeout=1, map=self.map, count=1)
                else:
                    asyncore.loop(timeout=1, count=1)
            except select.error, e:
                if e[0] == 9:
                    break
            self._process_timeouts()

    def stop(self):
        """
        Pika Adapter IOLoop stop function. When called, it will close an open
        connection, exiting us out of the IOLoop running in start.
        """
        log.debug("%s.stop", self.__class__.__name__)
        self.close()


class AsyncoreConnection(BaseConnection):

    def _adapter_connect(self, host, port):
        """
        Connect to our RabbitMQ boker using AsyncoreDispatcher, then setting
        Pika's suggested buffer size for socket reading and writing. We pass
        the handle to self so that the AsyncoreDispatcher object can call back
        into our various state methods.
        """
        log.debug("%s.connect", self.__class__.__name__)
        self.ioloop = AsyncoreDispatcher(host, port)

        # Map some core values for compatibility
        self.ioloop._handle_error = self._handle_error
        self.ioloop.connection = self
        self.ioloop.suggested_buffer_size = self._suggested_buffer_size
        self.socket = self.ioloop.socket

    def _flush_outbound(self):
        """
        We really can't flush the socket in asyncore, so instead just use this
        to toggle a flag that lets it know we want to write to the socket.
        """
        log.debug("%s.flush_outbound", self.__class__.__name__)
        if self.outbound_buffer.size:
            self.ioloop.writable_ = True
