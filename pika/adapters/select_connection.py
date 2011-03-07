# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import select
import time

from pika.adapters.base_connection import BaseConnection
import pika.log

# One of select, epoll, kqueue or poll
SELECT_TYPE = None

# Use epoll's constants to keep life easy
READ = 0x0001
WRITE = 0x0004
ERROR = 0x0008


class SelectConnection(BaseConnection):

    def __init__(self, parameters=None, on_open_callback=None,
                 reconnection_strategy=None):
        # Run our base connection init
        BaseConnection.__init__(self, parameters, on_open_callback,
                                reconnection_strategy)

    def _adapter_connect(self, host, port):
        """
        Connect to the given host and port
        """
        BaseConnection._adapter_connect(self, host, port)

        # Setup the IOLoop
        self.ioloop = IOLoop()

        # Setup our and start our IOLoop and Poller
        self.ioloop.fileno = self.socket.fileno()
        self.ioloop.start_poller(self._handle_events, self.event_state)

        # Let everyone know we're connected
        self._on_connected()

    def _flush_outbound(self):
        """
        Call the state manager who will figure out that we need to write then
        call the poller's poll function to force it to process events.
        """
        self._manage_event_state()

        # Force our poller to come up for air
        self.ioloop.poller.poll()


class IOLoop(object):
    """
    Singlton wrapper that decides which type of poller to use, creates an
    instance of it in start_poller and keeps the invoking application in a
    blocking state by calling the pollers start method. Poller should keep
    looping until IOLoop.instance().stop() is called or there is a socket
    error.

    Also provides a convenient pass-through for add_timeout and set_events
    """
    def __init__(self):
        self.fileno = None

    def add_timeout(self, deadline, handler):
        """
        Pass through a deadline and handler to the active poller
        """
        return self.poller.add_timeout(deadline, handler)

    def remove_timeout(self, timeout_id):
        """
        Remove a timeout if it's still in the timeout stack of our poller
        """
        self.poller.remove_timeout(timeout_id)

    @property
    def poller_type(self):
        return self.poller.__class__.__name__

    def start_poller(self, handler, events):
        """
        Start the Poller, once started will take over for IOLoop.start()
        """
        # By default we don't have a poller type
        self.poller = None

        # Decide what poller to use and set it up as appropriate
        if hasattr(select, 'poll'):
            if not SELECT_TYPE or SELECT_TYPE == 'poll':
                self.poller = PollPoller(self.fileno, handler, events)

        if not self.poller and hasattr(select, 'epoll'):
            if not SELECT_TYPE or SELECT_TYPE == 'epoll':
                self.poller = EPollPoller(self.fileno, handler, events)

        if not self.poller and hasattr(select, 'kqueue'):
            if not SELECT_TYPE or SELECT_TYPE == 'kqueue':
                self.poller = KQueuePoller(self.fileno, handler, events)

        # We couldn't satisfy epoll, kqueue or poll
        if not self.poller:
            self.poller = SelectPoller(self.fileno, handler, events)

    def update_handler(self, fileno, events):
        """
        Pass in the events we want to process
        """
        self.poller.update_handler(fileno, events)

    def start(self):
        """
        Wait until we have a poller
        """
        while not self.poller:
            time.sleep(SelectPoller.TIMEOUT)

        # Loop on the poller
        self.poller.start()

    def stop(self):
        """
        Stop the poller's event loop
        """
        self.poller.update_handler(self.fileno, 0)
        self.poller.open = False


class SelectPoller(object):
    """
    Default behavior is to use Select since it's the widest supported and has
    all of the methods we need for child classes as well. One should only need
    to override the update_handler and start methods for additional types.
    """
    # How many seconds to wait until we try and process timeouts
    TIMEOUT = 1

    def __init__(self, fileno, handler, events):
        self.fileno = fileno
        self.events = events
        self.open = True
        self._handler = handler
        self._timeouts = dict()

    def update_handler(self, fileno, events):
        """
        Set our events to our current events
        """
        self.events = events

    def add_timeout(self, deadline, handler):
        """
        Add a timeout to the stack by deadline
        """
        timeout_id = '%.8f' % time.time()
        self._timeouts[timeout_id] = {'deadline': deadline,
                                      'handler': handler}
        return timeout_id

    def remove_timeout(self, timeout_id):
        """
        Remove a timeout from the stack
        """
        if timeout_id in self._timeouts:
            del self._timeouts[timeout_id]

    def process_timeouts(self):
        """
        Process our self._timeouts event stack
        """
        # Process our timeout events
        keys = self._timeouts.keys()

        start_time = time.time()
        for timeout_id in keys:
            if timeout_id in self._timeouts and \
               self._timeouts[timeout_id]['deadline'] <= start_time:
                pika.log.debug('%s: Timeout calling %s',
                               self.__class__.__name__,
                               self._timeouts[timeout_id]['handler'])
                self._timeouts[timeout_id]['handler']()
                del(self._timeouts[timeout_id])

    def start(self):
        """
        Start the main poller loop. It will loop here until self.closed
        """
        while self.open:

            # Call our poller
            self.poll()

            # Process our timeouts
            self.process_timeouts()

    def poll(self):
        # Build our values to pass into select
        input_fileno, output_fileno, error_fileno = [], [], []

        if self.events & READ:
            input_fileno = [self.fileno]
        if self.events & WRITE:
            output_fileno = [self.fileno]
        if self.events & ERROR:
            error_fileno = [self.fileno]

        # Wait on select to let us know what's up
        try:
            read, write, error = select.select(input_fileno,
                                               output_fileno,
                                               error_fileno,
                                               SelectPoller.TIMEOUT)
        except select.error, error:
            return self._handler(self.fileno, ERROR, error)

        # Build our events bit mask
        events = 0
        if read:
            events |= READ
        if write:
            events |= WRITE
        if error:
            events |= ERROR

        if events:
            pika.log.debug("%s: Calling %s", self.__class__.__name__,
                           self._handler)
            self._handler(self.fileno, events)


class KQueuePoller(SelectPoller):

    def __init__(self, fileno, handler, events):
        SelectPoller.__init__(self, fileno, handler, events)
        # Make our events 0 by default for first run of update_handler
        self.events = 0
        # Create our KQueue object
        self._kqueue = select.kqueue()
        # KQueue needs us to register each event individually
        self.update_handler(fileno, events)

    def update_handler(self, fileno, events):
        # No need to update if our events are the same
        if self.events == events:
            return

        # Keep a list of the events we want to pass into _kqueue.control
        kevents = list()

        # Build our list of kevents based upon if we have to add or remove
        # events and each event gets its own operation

        # We don't want READ
        if not events & READ:

            # We did have a read last time
            if self.events & READ:

                # Remove READ
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_DELETE))
        # We do want READ
        else:

            # We did not have a read last time
            if not self.events & READ:

                # Add READ
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_ADD))

        # We don't want write events
        if not events & WRITE:

            # We had a write last time
            if self.events & WRITE:

                # Remove it
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_DELETE))
        # We do want write events
        else:

            # We didn't have a WRITE last time
            if not self.events & WRITE:

                # Add write
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_ADD))

        # Send our event changes to kqueue control
        for event in kevents:
            self._kqueue.control([event], 0)

        # Carry the state we just sent
        self.events = events

    def start(self):
        """
        Start the main poller loop. It will loop here until self.closed
        """
        while self.open:

            # Call our poll function
            self.poll()

            # Process our timeouts
            self.process_timeouts()

    def poll(self):

        # We'll build a bitmask of events that happened in kqueue
        events = 0

        # Get up to a max of 1000 events or wait until timeout
        try:
            kevents = self._kqueue.control(None, 1000,
                                           SelectPoller.TIMEOUT)
        except OSError, error:
            return self._handler(self.fileno, ERROR, error)

        # Loop through the events returned to us and build a bitmask
        for event in kevents:

            # We had a read event, data and we're listening for them
            if event.filter == select.KQ_FILTER_READ and \
               READ & self.events:
                events |= READ

            # We're clear to write so get that done
            if event.filter == select.KQ_FILTER_WRITE and \
               WRITE & self.events:
                events |= WRITE

            # Look for errors, no event registration needed
            if event.flags & select.KQ_EV_ERROR and \
                ERROR & self.events:
                events |= ERROR

        # Call our event handler if we have events in our stack
        if events:
            pika.log.debug("%s: Calling %s(%i)", self.__class__.__name__,
                           self._handler, events)
            self._handler(self.fileno, events)


class PollPoller(SelectPoller):

    def __init__(self, fileno, handler, events):
        SelectPoller.__init__(self, fileno, handler, events)
        self._poll = select.poll()
        self._poll.register(fileno, self.events)

    def update_handler(self, fileno, events):
        self.events = events
        self._poll.modify(fileno, self.events)

    def start(self):
        """
        Start the main poller loop. It will loop here until self.closed
        """
        while self.open:

            # Poll our poller
            self.poll()

            # Process our timeouts
            self.process_timeouts()

    def poll(self):

        # Poll until TIMEOUT waiting for an event
        events = self._poll.poll(int(SelectPoller.TIMEOUT * 1000))

        # If we didn't timeout pass the event to the handler
        if events:
            pika.log.debug("%s: Calling %s", self.__class__.__name__,
                           self._handler)
            self._handler(events[0][0], events[0][1])


class EPollPoller(PollPoller):
    """
    EPoll and Poll function signatures match.
    """
    def __init__(self, fileno, handler, events):
        SelectPoller.__init__(self, fileno, handler, events)
        self._poll = select.epoll()
        self._poll.register(fileno, self.events)

    def poll(self):

        # Poll until TIMEOUT waiting for an event
        events = self._poll.poll(SelectPoller.TIMEOUT)

        # If we didn't timeout pass the event to the handler
        if events:
            pika.log.debug("%s: Calling %s", self.__class__.__name__,
                           self._handler)
            self._handler(events[0][0], events[0][1])
