"""A connection adapter that tries to use the best polling method for the
platform pika is running on.

"""
import logging
import select
import time

from pika.adapters.base_connection import BaseConnection

LOGGER = logging.getLogger(__name__)

# One of select, epoll, kqueue or poll
SELECT_TYPE = None

# Use epoll's constants to keep life easy
READ = 0x0001
WRITE = 0x0004
ERROR = 0x0008


class SelectConnection(BaseConnection):
    """An asynchronous connection adapter that attempts to use the fastest
    event loop adapter for the given platform.

    """

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=True):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param on_open_error_callback: Method to call if the connection cant
                                       be opened
        :type on_open_error_callback: method
        :param method on_close_callback: Method to call on connection close
        :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
        :raises: RuntimeError

        """
        ioloop = IOLoop(self._manage_event_state)
        super(SelectConnection, self).__init__(parameters, on_open_callback,
                                               on_open_error_callback,
                                               on_close_callback,
                                               ioloop,
                                               stop_ioloop_on_close)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker, returning True on success, False
        on failure.

        :rtype: bool

        """
        if super(SelectConnection, self)._adapter_connect():
            self.ioloop.start_poller(self._handle_events,
                                     self.event_state,
                                     self.socket.fileno())
            return True
        return False

    def _flush_outbound(self):
        """Call the state manager who will figure out that we need to write then
        call the poller's poll function to force it to process events.

        """
        self.ioloop.poller._manage_event_state()
        # Force our poller to come up for air, but in write only mode
        # write only mode prevents messages from coming in and kicking off
        # events through the consumer
        self.ioloop.poller.poll(write_only=True)


class IOLoop(object):
    """Singlton wrapper that decides which type of poller to use, creates an
    instance of it in start_poller and keeps the invoking application in a
    blocking state by calling the pollers start method. Poller should keep
    looping until IOLoop.instance().stop() is called or there is a socket
    error.

    Also provides a convenient pass-through for add_timeout and set_events

    """
    def __init__(self, state_manager):
        """Create an instance of the IOLoop object.

        :param method state_manager: The method to manage state

        """
        self.poller = None
        self._manage_event_state = state_manager

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        if not self.poller:
            time.sleep(deadline)
            return callback_method()
        return self.poller.add_timeout(deadline, callback_method)

    @property
    def poller_type(self):
        """Return the type of poller.

        :rtype: str

        """
        return self.poller.__class__.__name__

    def remove_timeout(self, timeout_id):
        """Remove a timeout if it's still in the timeout stack of the poller

        :param str timeout_id: The timeout id to remove

        """
        self.poller.remove_timeout(timeout_id)

    def start(self):
        """Start the IOLoop, waiting for a Poller to take over."""
        LOGGER.debug('Starting IOLoop')
        while not self.poller:
            time.sleep(SelectPoller.TIMEOUT)
        self.poller.start()
        self.poller.flush_pending_timeouts()

    def start_poller(self, handler, events, fileno):
        """Start the Poller, once started will take over for IOLoop.start()

        :param method handler: The method to call to handle events
        :param int events: The events to handle
        :param int fileno: The file descriptor to poll for

        """
        LOGGER.debug('Starting the Poller')
        self.poller = None
        if hasattr(select, 'poll') and hasattr(select.poll, 'modify'):
            if not SELECT_TYPE or SELECT_TYPE == 'poll':
                LOGGER.debug('Using PollPoller')
                self.poller = PollPoller(fileno, handler, events,
                                         self._manage_event_state)
        if not self.poller and hasattr(select, 'epoll'):
            if not SELECT_TYPE or SELECT_TYPE == 'epoll':
                LOGGER.debug('Using EPollPoller')
                self.poller = EPollPoller(fileno, handler, events,
                                          self._manage_event_state)
        if not self.poller and hasattr(select, 'kqueue'):
            if not SELECT_TYPE or SELECT_TYPE == 'kqueue':
                LOGGER.debug('Using KQueuePoller')
                self.poller = KQueuePoller(fileno, handler, events,
                                           self._manage_event_state)
        if not self.poller:
            LOGGER.debug('Using SelectPoller')
            self.poller = SelectPoller(fileno, handler, events,
                                       self._manage_event_state)

    def stop(self):
        """Stop the poller's event loop"""
        LOGGER.debug('Stopping the poller event loop')
        self.poller.open = False

    def update_handler(self, fileno, events):
        """Pass in the events to process for the given file descriptor.

        :param int fileno: The file descriptor to poll for
        :param int events: The events to handle

        """
        self.poller.update_handler(fileno, events)


class SelectPoller(object):
    """Default behavior is to use Select since it's the widest supported and has
    all of the methods we need for child classes as well. One should only need
    to override the update_handler and start methods for additional types.

    """
    TIMEOUT = 1

    def __init__(self, fileno, handler, events, state_manager):
        """Create an instance of the SelectPoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for
        :param method state_manager: The method to manage state

        """
        self.fileno = fileno
        self.events = events
        self.open = True
        self._handler = handler
        self._timeouts = dict()
        self._manage_event_state = state_manager

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        value = {'deadline': time.time() + deadline,
                 'callback': callback_method}
        timeout_id = hash(frozenset(value.items()))
        self._timeouts[timeout_id] = value
        return timeout_id

    def flush_pending_timeouts(self):
        """
        """
        if len(self._timeouts) > 0:
            time.sleep(SelectPoller.TIMEOUT)
        self.process_timeouts()

    def poll(self, write_only=False):
        """Check to see if the events that are cared about have fired.

        :param bool write_only: Don't look at self.events, just look to see if
            the adapter can write.

        """
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
            self._handler(self.fileno, events, write_only=write_only)

    def process_timeouts(self):
        """Process the self._timeouts event stack"""
        start_time = time.time()
        for timeout_id in self._timeouts.keys():
            if timeout_id not in self._timeouts:
                continue
            if self._timeouts[timeout_id]['deadline'] <= start_time:
                callback = self._timeouts[timeout_id]['callback']
                del self._timeouts[timeout_id]
                callback()

    def remove_timeout(self, timeout_id):
        """Remove a timeout if it's still in the timeout stack

        :param str timeout_id: The timeout id to remove

        """
        if timeout_id in self._timeouts:
            del self._timeouts[timeout_id]

    def start(self):
        """Start the main poller loop. It will loop here until self.closed"""
        while self.open:
            self.poll()
            self.process_timeouts()
            self._manage_event_state()

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """
        self.events = events


class KQueuePoller(SelectPoller):
    """KQueuePoller works on BSD based systems and is faster than select"""
    def __init__(self, fileno, handler, events, state_manager):
        """Create an instance of the KQueuePoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for
        :param method state_manager: The method to manage state

        """
        super(KQueuePoller, self).__init__(fileno, handler, events,
                                           state_manager)
        self.events = 0
        self._kqueue = select.kqueue()
        self.update_handler(fileno, events)
        self._manage_event_state = state_manager

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """
        # No need to update if our events are the same
        if self.events == events:
            return

        kevents = list()
        if not events & READ:
            if self.events & READ:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_DELETE))
        else:
            if not self.events & READ:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_ADD))
        if not events & WRITE:
            if self.events & WRITE:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_DELETE))
        else:
            if not self.events & WRITE:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_ADD))
        for event in kevents:
            self._kqueue.control([event], 0)
        self.events = events

    def start(self):
        """Start the main poller loop. It will loop here until self.closed"""
        while self.open:
            self.poll()
            self.process_timeouts()
            self._manage_event_state()

    def poll(self, write_only=False):
        """Check to see if the events that are cared about have fired.

        :param bool write_only: Don't look at self.events, just look to see if
            the adapter can write.

        """
        events = 0
        try:
            kevents = self._kqueue.control(None, 1000, SelectPoller.TIMEOUT)
        except OSError, error:
            return self._handler(self.fileno, ERROR, error)
        for event in kevents:
            if event.filter == select.KQ_FILTER_READ and READ & self.events:
                events |= READ
            if event.filter == select.KQ_FILTER_WRITE and WRITE & self.events:
                events |= WRITE
            if event.flags & select.KQ_EV_ERROR and ERROR & self.events:
                events |= ERROR
        if events:
            LOGGER.debug("Calling %s(%i)", self._handler, events)
            self._handler(self.fileno, events, write_only=write_only)


class PollPoller(SelectPoller):
    """Poll works on Linux and can have better performance than EPoll in
    certain scenarios.  Both are faster than select.

    """
    def __init__(self, fileno, handler, events, state_manager):
        """Create an instance of the KQueuePoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for
        :param method state_manager: The method to manage state

        """
        super(PollPoller, self).__init__(fileno, handler, events, state_manager)
        self._poll = select.poll()
        self._poll.register(fileno, self.events)

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """
        self.events = events
        self._poll.modify(fileno, self.events)

    def start(self):
        """Start the main poller loop. It will loop here until self.closed"""
        was_open = self.open
        while self.open:
            self.poll()
            self.process_timeouts()
            self._manage_event_state()
        if not was_open:
            return
        try:
            LOGGER.info("Unregistering poller on fd %d" % self.fileno)
            self.update_handler(self.fileno, 0)
            self._poll.unregister(self.fileno)
        except IOError, err:
            LOGGER.debug("Got IOError while shutting down poller: %s", err)

    def poll(self, write_only=False):
        """Poll until TIMEOUT waiting for an event

        :param bool write_only: Only process write events

        """
        events = self._poll.poll(int(SelectPoller.TIMEOUT * 1000))
        if events:
            LOGGER.debug("Calling %s with %d events",
                         self._handler, len(events))
            for fileno, event in events:
                self._handler(fileno, event, write_only=write_only)


class EPollPoller(PollPoller):
    """EPoll works on Linux and can have better performance than Poll in
    certain scenarios. Both are faster than select.

    """
    def __init__(self, fileno, handler, events, state_manager):
        """Create an instance of the EPollPoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for
        :param method state_manager: The method to manage state

        """
        super(EPollPoller, self).__init__(fileno, handler, events,
                                          state_manager)
        self._poll = select.epoll()
        self._poll.register(fileno, self.events)

    def poll(self, write_only=False):
        """Poll until TIMEOUT waiting for an event

        :param bool write_only: Only process write events

        """
        events = self._poll.poll(SelectPoller.TIMEOUT)
        if events:
            LOGGER.debug("Calling %s", self._handler)
            for fileno, event in events:
                self._handler(fileno, event, write_only=write_only)
