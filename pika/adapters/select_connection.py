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
import select
import socket
import time


from pika.adapters.base_connection import BaseConnection


class SelectConnection(BaseConnection):

    def add_timeout(self, delay_sec, callback):

        deadline = time.time() + delay_sec
        self.ioloop.add_timeout(deadline, callback)

    def connect(self, host, port):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((host, port))
        self.sock.setblocking(0)

        # Setup our IOLoop
        self.ioloop = IOLoop.instance()

        # Append our handler to ioloop for our socket
        self.events = Poller.READ | Poller.ERROR

        self.ioloop.start_poller(self.sock.fileno(),
                                 self._handle_events,
                                 self.events)

        # Suggested Buffer Size
        self.buffer_size = self.suggested_buffer_size()

        # Let everyone know we're connected
        self.on_connected()

        # Add a state change handler
        self.add_state_change_handler(self.on_state_change)

    def disconnect(self):

        # Remove from the IOLoop
        self.ioloop.stop()

        # Close our socket since the Connection class told us to do so
        self.sock.close()

        # Let everyone know we're done
        self.on_disconnected()

    def flush_outbound(self):

        # Make sure we've written out our buffer
        if bool(self.outbound_buffer):
            events = self.events
            events |= Poller.WRITE
            self.ioloop.set_events(events)

    def on_state_change(self, connection, is_open):
        logging.debug("%s.on_state_change" % self.__class__.__name__)
        if not is_open:
            self.disconnect()

    def _handle_events(self, fd, events):
        logging.debug("%s._handle_events" % self.__class__.__name__)
        # Incoming events from IOLoop, make sure we have our socket
        if not self.sock:
            logging.warning("Got events for closed stream %d", fd)
            return

        if events & Poller.READ:
            self._handle_read()

        if events & Poller.ERROR:
            self.sock.close()

        if events & Poller.WRITE:
            self._handle_write()

    def _handle_error(self, error):

        if error[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
            return
        elif e[0] == errno.EBADF:
            logging.error("%s: Write to a closed socket" %
                          self.__class__.__name__)
        else:
            logging.error("%s: Write error on %d: %s" %
                          (self.__class__.__name__,
                           self.sock.fileno(), error))
        self.disconnect()

    def _handle_read(self):

        try:
            self.on_data_available(self.sock.recv(self.buffer_size))
        except socket.error, e:
            self._handle_error(e)

    def _handle_write(self):

        # Loop while we have data to write
        while bool(self.outbound_buffer):

            # Get data to send based upon Pika's suggested buffer size
            fragment = self.outbound_buffer.read(self.buffer_size)
            try:
                r = self.sock.send(fragment)
            except socket.error, e:
                self._handle_error(e)

            # Remove the content we used from our buffer
            self.outbound_buffer.consume(r)

        # Remove the write event
        self.ioloop.set_events(self.events)


class IOLoop(object):

    def __init__(self):

        # Decide what poller to use and set it up as appropriate
        if hasattr(select, 'epoll'):
            self.poller_type = 'epoll'
        #elif hasattr(select, 'kqueue'):
        #    self.poller_type = 'kqueue'
        elif hasattr(select, 'poll'):
            self.poller_type = 'poll'
        else:
            self.poller_type = 'select'

    @classmethod
    def instance(class_):
        if not hasattr(class_, "_instance"):
            class_._instance = class_()
        return class_._instance

    def add_timeout(self, deadline, handler):

        self.poller.add_timeout(deadline, handler)

    def start_poller(self, fd, handler, events):
        if self.poller_type == 'epoll':
            self.poller = EPollPoller(fd, handler, events)
        if self.poller_type == 'kqueue':
            self.poller = KQueuePoller(fd, handler, events)
        elif self.poller_type == 'poll':
            self.poller = PollPoller(fd, handler, events)
        elif self.poller_type == 'select':
            self.poller = Poller(fd, handler, events)

    def set_events(self, events):
        """
        Pass in the events we want to process
        """
        self.poller.set_events(events)

    def start(self):
        """
        Wait until we have a poller
        """
        while not self.poller:
            time.sleep(.5)

        # Loop on the poller
        self.poller.start()

    def stop(self):
        self.poller.set_events(0)
        self.poller.open = False


class Poller(object):

    READ = 0x0001
    WRITE = 0x0004
    ERROR = 0x0008

    TIMEOUT = 1

    def __init__(self, fd, handler, events):
        logging.debug('%s Poller Starting' % self.__class__.__name__)

        self.fd = fd
        self.handler = handler
        self.events = events
        self.open = True
        self.timeouts = dict()

    def set_events(self, events):
        logging.debug('%s.set_events(%i)' % (self.__class__.__name__, events))

        self.events = events

    def add_timeout(self, deadline, handler):
        logging.debug('%s.add_timeout: In %.4f seconds call %s' % \
                      (self.__class__.__name__,
                       deadline - time.time(),
                       handler))

        self.timeouts[deadline] = handler

    def process_timeouts(self):

        # Process our timeout events
        deadlines = self.timeouts.keys()
        for deadline in deadlines:
            if deadline >= time.time():
                logging.debug("%s: Timeout calling %s" %\
                              (self.__class__.__name__,
                               self.timeouts[deadline]))
                self.timeouts[deadline]()
                del(self.timeouts[deadline])

    def start(self):

        while self.open:

            # Build our values to pass into select
            if self.events & self.READ:
                input_fd = [self.fd]
            else:
                input_fd = []

            if self.events & self.WRITE:
                output_fd = [self.fd]
            else:
                output_fd = []

            if self.events & self.ERROR:
                error_fd = [self.fd]
            else:
                error_fd = []

            # Wait on select to let us know what's up
            read, write, error = select.select(input_fd,
                                               output_fd,
                                               error_fd,
                                               self.TIMEOUT)
            # Build our events value
            events = 0
            if read:
                events |= self.READ
            if write:
                events |= self.WRITE
            if error:
                events |= self.ERROR

            if events:
                self.handler(self.fd, events)

            # Process our timeouts
            self.process_timeouts()


class EPollPoller(Poller):

    def __init__(self, fd, handler, events):

        # Get the benefit of our Poller object without having to dupe code
        super(EPollPoller, self).__init__(fd, handler, events)

        self.epoll = select.epoll()

        # Used EPoll's event values so we don't need to do anything special
        self.events = events

        # Register with epoll
        self.epoll.register(self.fd, self.events)

    def set_events(self, events):
        logging.debug('%s.set_events(%i)' % (self.__class__.__name__, events))

        # Used EPoll's event values so we don't need to do anything special
        self.events = events
        self.epoll.modify(self.fd, events)

    def start(self):

        while self.open:

            fd, event = self.epoll.poll(self.TIMEOUT)
            if event:
                self.handler(fd, event)

            # Process our timeouts
            self.process_timeouts()


class KQueuePoller(Poller):

    def __init__(self, fd, handler, events):

        # Get the benefit of our Poller object without having to dupe code
        super(KQueuePoller, self).__init__(fd, handler, events)

        self._kqueue = select.kqueue()

        # KQueue needs us to register each event individually
        self.set_events(events)

    def set_events(self, events):
        logging.debug('%s.set_events(%i)' % (self.__class__.__name__, events))

        # Keep a list of the events we want to pass into _kqueue.control
        kevents = []

        if events & Poller.WRITE:

            # Add write
            kevents.append(select.kevent(self.fd,
                                         filter=select.KQ_FILTER_WRITE,
                                         flags=select.KQ_EV_ADD))
        elif self.events & Poller.WRITE:

            # We had write, remove it
            kevents.append(select.kevent(self.fd,
                                         filter=select.KQ_FILTER_WRITE,
                                         flags=select.KQ_EV_DELETE))

        if events & Poller.READ:

            # Add write
            kevents.append(select.kevent(self.fd,
                                         filter=select.KQ_FILTER_READ,
                                         flags=select.KQ_EV_ADD))
        elif self.events & Poller.READ:

            # We had write, remove it
            kevents.append(select.kevent(self.fd,
                                         filter=select.KQ_FILTER_READ,
                                         flags=select.KQ_EV_DELETE))

        for event in kevents:
            self._kqueue.control([event], 0)

        self.events = events

    def start(self):

        while self.open:

            events = 0
            kevents = self._kqueue.control(None, 1000, self.TIMEOUT)

            for event in kevents:
                if event.filter == select.KQ_FILTER_READ and \
                   self.READ & self.events and event.data:
                    print "Read Event"
                    events |= self.READ

                if event.filter == select.KQ_FILTER_WRITE and \
                   self.WRITE & self.events:
                    print "Write event"
                    events |= self.WRITE

                if event.flags & select.KQ_EV_ERROR and \
                    self.ERROR & self.events:
                    print "Error event"
                    events |= self.ERROR

                # Call our event handler if we have events in our stack
                if events:
                    logging.debug("%s: Calling %s" % (self.__class__.__name__,
                                                      self.handler))
                    self.handler(event.ident, events)

            # Process our timeouts
            self.process_timeouts()


class PollPoller(Poller):

    def __init__(self, fd, handler, events):

        # Get the benefit of our Poller object without having to dupe code
        super(PollPoller, self).__init__(fd, handler, events)

        self.poll = select.poll()

        # Poll needs us to register each event individually
        self.set_events(events)

    def set_events(self, events):
        logging.debug('%s.set_events(%i)' % (self.__class__.__name__, events))

        if events & Poller.WRITE:
            # Add write
            self.poll.register(self.fd, select.POLLOUT)
        elif self.events & Poller.WRITE:
            # We had write, remove it
            self.poll.unregister(self.fd, select.POLLOUT)

        if events & Poller.READ:
            # Add write
            self.poll.register(self.fd, select.POLLIN)
        elif self.events & Poller.READ:
            # We had write, remove it
            self.poll.unregister(self.fd, select.POLLIN)

        if events & Poller.ERROR:
            # Add write
            self.poll.register(self.fd, select.POLLERR)
        elif self.events & Poller.ERROR:
            # We had write, remove it
            self.poll.unregister(self.fd, select.POLLERR)

        # Set our current level of events
        self.events = events

    def start(self):

        while self.open:

            fd, event = self.poll.poll(self.TIMEOUT)
            if event:
                self.handler(fd, event)

            # Process our timeouts
            self.process_timeouts()
