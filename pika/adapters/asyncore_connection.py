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
        self.buffer_size = 8192
        self.connecting = True
        self.connection = None
        self.timeouts = dict()
        self.writable_ = False

    def handle_connect(self):
        """
        asyncore required method. Is called on connection.
        """
        logging.debug("%s.handle_connect" % self.__class__.__name__)
        self.connecting = False
        self.connection._on_connected()

    def handle_close(self):
        """
        asyncore required method. Is called on close.
        """
        logging.debug("%s.handle_close" % self.__class__.__name__)
        # If we're not already closing or closed, disconnect the Connection
        if not self.connection.closing and not self.connection.closed:
            self.connection.disconnect()

    def handle_error_(self, error):
        """
        Internal error handling method. Here we expect a socket.error coming in
        and will handle different socket errors differently.
        """
        logging.debug("%s.handle_error" % self.__class__.__name__)
        # Ok errors, just continue what we were doing before
        if error[0] in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EINTR):
            return
        # Socket is closed, so lets just go to our handle_close method
        elif error[0] == errno.EBADF:
            logging.error("%s: Socket is closed" %
                          self.__class__.__name__)
            self.handle_close()
        # Haven't run into this one yet, log it.
        logging.error("%s: Socket Error on %d: %s" %
                      (self.__class__.__name__, self._fileno, error[0]))

        # It's a bad enough error that we'll just want to close things out
        self.connection._on_connection_closed(None, True)
        self.close()

    def handle_read(self):
        """
        asyncore required function, is called when there is data on the socket
        to read.
        """
        logging.debug("%s.handle_read" % self.__class__.__name__)
        try:
            data_in = self.recv(self.buffer_size)
        except socket.error as e:
            return self.handle_error(e)

        if data_in:
            return self.connection.on_data_available(data_in)

        self.close()
        # There is a bug in asyncore that keeps it stuck in poll after close
        # Use this to remove it from that loop
        asyncore.loop(0.1, map=[])

    def handle_write(self):
        """
        asyncore required function, is called when we can write to the socket
        """
        # Did we get here without anything to write?
        if self.connection.outbound_buffer.size:
            try:
                data = self.connection.outbound_buffer.read(self.buffer_size)
                sent = self.send(data)
            except socket.error as e:
                return self.handle_error(e)

            # If we sent data, remove it from the buffer
            if sent:
                # Remove the length written from the buffer
                self.connection.outbound_buffer.consume(sent)

                # If our buffer is empty, turn off writing
                if not self.connection.outbound_buffer.size:
                    self.writable_ = False

    def writable(self):
        """
        asyncore required function, is used to toggle the write bit on the
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

    def add_timeout(self, delay_sec, handler):
        """
        Add a timeout to our stack for delay_sec. When processed, handler
        will be called without any arguments.
        """
        # Calculate our deadline for running the callback
        deadline = time.time() + delay_sec
        logging.debug('%s.add_timeout: In %.4f seconds call %s' % \
                      (self.__class__.__name__,
                       deadline - time.time(),
                       handler))
        self.timeouts[deadline] = handler


    def cancel_timeout(self, handler):
        """
        Pass through a deadline and handler to the active poller
        """
        for key in self.timeouts.keys():
            if self.timeouts[key] == handler:
                del self.timeouts[key]
                break

    def _process_timeouts(self):
        """
        Only called by our IOLoop after each timeout of asyncore.loop. It will
        look through the all of the keys in our dictionary looking for items
        which are less than or equal to our current time. When that condition
        is met, it will call the handler assigned to that key in the dictionary
        and remove the key from the dictionary.
        """
        logging.debug("%s.process_timeouts" % self.__class__.__name__)

        # Get our list of keys to iterate trhough
        deadlines = self.timeouts.keys()

        # Process our timeout events
        for deadline in deadlines:
            if deadline <= time.time():
                logging.debug("%s: Timeout calling %s" %\
                              (self.__class__.__name__,
                               self.timeouts[deadline]))
                self.timeouts[deadline]()
                del(self.timeouts[deadline])

    def start(self):
        """
        Pika Adapter IOLoop start function. This blocks until we are no longer
        connected.
        """
        logging.debug("%s.start" % self.__class__.__name__)
        while self.connected or self.connecting:
            asyncore.loop(timeout=1, count=1)
            self._process_timeouts()

    def stop(self):
        """
        Pika Adapter IOLoop stop function. When called, it will close an open
        connection, exiting us out of the IOLoop running in start.
        """
        logging.debug("%s.stop" % self.__class__.__name__)
        self.close()
        # There is a bug in asyncore that keeps it stuck in poll after close
        # Use this to remove it from that loop
        asyncore.loop(0.1, map=[])


class AsyncoreConnection(BaseConnection):

    def add_timeout(self, delay_sec, callback):
        """
        Add a timeout to our IOLoop's stack for delay_sec. When processed,
        handler will be called without any arguments.
        """
        self.ioloop.add_timeout(delay_sec, callback)


    def cancel_timeout(self, callback):
        """
        Cancels a timeout that we've already added to the timeout stack
        """
        self.ioloop.cancel_timeout(callback)

    def connect(self, host, port):
        """
        Connect to our RabbitMQ boker using AsyncoreDispatcher, then setting
        Pika's suggested buffer size for socket reading and writing. We pass
        the handle to self so that the AsyncoreDispatcher object can call back
        into our various state methods.
        """
        logging.debug("%s.connect" % self.__class__.__name__)
        self.ioloop = AsyncoreDispatcher(host, port)
        self.ioloop.buffer_size = self.suggested_buffer_size()
        self.ioloop.connection = self

    def disconnect(self):
        """
        Called from within our Connection class to disconnect our socket and
        adapter. This could be because of remote disconnect, protocol errors,
        etc.
        """
        logging.debug("%s.disconnect" % self.__class__.__name__)
        self.ioloop.stop()
        self.ioloop.close()
        # There is a bug in asyncore that keeps it stuck in poll after close
        # Use this to remove it from that loop
        asyncore.loop(0.1, map=[])

    def flush_outbound(self):
        """
        We really can't flush the socket in asyncore, so instead just use this
        to toggle a flag that lets it know we want to write to the socket.
        """
        logging.debug("%s.flush_outbound" % self.__class__.__name__)
        if self.outbound_buffer.size:
            self.ioloop.writable_ = True
