"""A connection adapter that tries to use the best polling method for the
platform pika is running on.

"""
import os
import logging
import select
import errno
import time
from operator import itemgetter

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
        super(SelectConnection, self).__init__(parameters,
                                               on_open_callback,
                                               on_open_error_callback,
                                               on_close_callback,
                                               ioloop,
                                               stop_ioloop_on_close)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker, returning True on success, False
        on failure.

        :rtype: bool

        """
        error = super(SelectConnection, self)._adapter_connect()
        if not error:
            self.ioloop.add_handler(self.socket.fileno(),
                                    self._handle_events,
                                     self.event_state)
        return error

    def _adapter_disconnect(self):
        """Disconnect from the RabbitMQ broker"""
        if self.socket:
            self.ioloop.remove_handler(self.socket.fileno())
        super(SelectConnection, self)._adapter_disconnect()

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
        self._manage_event_state = state_manager
        self.setup_poller()

    def __getattr__(self, attr):
        """Call through to the poller for timeout and ioloop.add/update/remove
        methods.
        """
        return getattr(self.poller, attr)

    @property
    def poller_type(self):
        """Return the type of poller.

        :rtype: str

        """
        return self.poller.__class__.__name__

    def start(self):
        """Start the IOLoop, waiting for a Poller to take over."""
        self.poller.start()

    def setup_poller(self):
        """Start the Poller, once started will take over for IOLoop.start()

        :param method handler: The method to call to handle events
        :param int events: The events to handle
        :param int fileno: The file descriptor to poll for

        """
        LOGGER.debug('Starting the Poller')
        self.poller = None
        if hasattr(select, 'epoll'):
            if not SELECT_TYPE or SELECT_TYPE == 'epoll':
                LOGGER.debug('Using EPollPoller')
                self.poller = EPollPoller(self._manage_event_state)
        if not self.poller and hasattr(select, 'kqueue'):
            if not SELECT_TYPE or SELECT_TYPE == 'kqueue':
                LOGGER.debug('Using KQueuePoller')
                self.poller = KQueuePoller(self._manage_event_state)
        if not self.poller and hasattr(select, 'poll') and hasattr(select.poll(), 'modify'):
            if not SELECT_TYPE or SELECT_TYPE == 'poll':
                LOGGER.debug('Using PollPoller')
                self.poller = PollPoller(self._manage_event_state)
        if not self.poller:
            LOGGER.debug('Using SelectPoller')
            self.poller = SelectPoller(self._manage_event_state)

    def stop(self):
        """Stop the poller's event loop"""
        LOGGER.debug('Stopping the poller event loop')
        self.poller.stop()


class SelectPoller(object):
    """Default behavior is to use Select since it's the widest supported and has
    all of the methods we need for child classes as well. One should only need
    to override the update_handler and start methods for additional types.

    """
    TIMEOUT = 5
    # if the poller used MS specify 1000
    TIMEOUT_MULT = 1

    def __init__(self, state_manager):
        """Create an instance of the SelectPoller

        :param method state_manager: The method to manage state

        """
        self.filenos = dict()
        self.r_fds = set()
        self.w_fds = set()
        self.e_fds = set()
        self.stopping = False
        self._timeouts = {}
        self._next_timeout = None
        r_interupt, self.w_interupt = os.pipe()
        self.add_handler(r_interupt, self.read_interupt, READ)
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
        timeout_at = time.time() + deadline
        value = {'deadline': timeout_at,
                 'callback': callback_method}
        timeout_id = hash(frozenset(value.items()))
        self._timeouts[timeout_id] = value
        
        if not self._next_timeout or timeout_at < self._next_timeout:
            self._next_timeout = timeout_at

        return timeout_id

    def flush_pending_timeouts(self):
        """
        """
        if len(self._timeouts) > 0:
            time.sleep(SelectPoller.TIMEOUT)
        self.process_timeouts()


    def get_next_timeout(self):
        """Get the interval to the next timeout event, or a default interval
            
        """
        if self._next_timeout:
            timeout = max((self._next_timeout - time.time(), 0))
        elif self._timeouts:
            deadlines = [t['deadline'] for t in self._timeouts.values()]
            self._next_timeout = min(deadlines) 
            timeout = max((self._next_timeout - time.time(), 0))
        else:
            timeout = SelectPoller.TIMEOUT
        return timeout * SelectPoller.TIMEOUT_MULT

    def poll(self, write_only=False):
        """Check to see if the events that are cared about have fired.

        :param bool write_only: Don't look at self.events, just look to see if
            the adapter can write.

        """

        # Wait on select to let us know what's up
        try:
            read, write, error = select.select(
                self.r_fds, self.w_fds, self.e_fds, self.get_next_timeout())
        except select.error as error:
            if error.errno != errno.EINTR:
                raise

        # Build our events bit mask

        for fileno in self.filenos:
            events = 0
            if fileno in read:
                events |= READ
            if fileno in write:
                events |= WRITE
            if fileno in error:
                events |= ERROR
           
            if events:
                handler = self.filenos[fileno]
                handler(fileno, events, write_only=write_only)

    def process_timeouts(self):
        """Process the self._timeouts event stack"""
        now = time.time()
        to_run = filter(lambda t: t['deadline']<=now, self._timeouts.values())
        for t in sorted(to_run, key=itemgetter('deadline')):
            t['callback']()
            del self._timeouts[hash(frozenset(t.items()))]
            self._next_timeout = None

    def remove_timeout(self, timeout_id):
        """Remove a timeout if it's still in the timeout stack

        :param str timeout_id: The timeout id to remove

        """
        try:
            timeout = self._timeouts.pop(timeout_id)
            if timeout['deadline'] == self._next_timeout:
                self._next_timeout = None
        except KeyError:
            pass

    def start(self):
        """Start the main poller loop. It will loop here until self.stopping"""
        LOGGER.debug('Starting IOLoop')
        self.stopping = False
        while not self.stopping:
            self.poll()
            self.process_timeouts()

    def stop(self):
        LOGGER.debug('Stopping IOLoop')
        self.stopping = True
        # Sent byte to interupt the poll loop
        os.write(self.w_interupt, 'X')

    def read_interupt(self, c, e, write_only):
        # read the interrupt byte
        os.read(c, 512)
        
    def add_handler(self, fileno, handler, events):
       """Add a new fileno to the set

       :param int fileno: The file descriptor
       :param method handler: What is called when an event happens
       :param int events: The event mask

       """
       self.filenos[fileno] = handler
       self.update_handler(fileno, events)
        
    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """

        for fd_set, ev in zip(
            [self.r_fds, self.w_fds, self.e_fds]
            [READ, WRITE, ERROR]):
            
            if events & ev:
                fd_set.add(fileno)
            else:
                fd_set.discard(fileno)

    def remove_handler(self, fileno):
        """Remove a fileno to the set
        
        :param int fileno: The file descriptor

        """
        del self.filenos[fileno]


class KQueuePoller(SelectPoller):
    """KQueuePoller works on BSD based systems and is faster than select"""
    def __init__(self, state_manager):
        """Create an instance of the KQueuePoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for
        :param method state_manager: The method to manage state

        """
        super(KQueuePoller, self).__init__(state_manager)
        self._kqueue = select.kqueue()
        self._manage_event_state = state_manager

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """

        kevents = list()
        if not events & READ:
            if fileno in self.r_fds:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_DELETE))
        else:
            if fileno not in self.r_fds:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_ADD))
        if not events & WRITE:
            if fileno in self.w_fds:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_DELETE))
        else:
            if fileno not in self.w_fds:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_ADD))
        for event in kevents:
            self._kqueue.control([event], 0)
        super(KQueuePoller, self).update_handler(fileno, events)

    def start(self):
        """Start the main poller loop. It will loop here until self.closed"""
        while self.open:
            self.poll()
            self.process_timeouts()
            self._manage_event_state()
    
    def _map_event(self, kevent):
        """return the event type associated with a kevent object

        :param kevent kevent: a kevent object as returned by kqueue.control()

        """
        if kevent.filter == select.KQ_FILTER_READ:
            return READ
        elif kevent.filter == select.KQ_FILTER_WRITE:
            return WRITE
        elif kevent.flags & KQ_EV_ERROR:
            return ERROR

    def poll(self, write_only=False):
        """Check to see if the events that are cared about have fired.

        :param bool write_only: Don't look at self.events, just look to see if
            the adapter can write.

        """
        events = 0
        try:
            kevents = self._kqueue.control(None, 1000, self.get_next_timeout())
        except OSError as error:
            if error.errno != errno.EINTR:
                return
            raise
        
        for event in kevents:
            fileno = event.ident
            handler = self.filenos[fileno]
            event = self._map_event(event.filter)
            handler(fileno, event, write_only)


class PollPoller(SelectPoller):
    """Poll works on Linux and can have better performance than EPoll in
    certain scenarios.  Both are faster than select.

    """
    TIMEOUT_MULT=1000
    def __init__(self, state_manager):
        """Create an instance of the KQueuePoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for
        :param method state_manager: The method to manage state

        """
        self._poll = self.create_poller()
        super(PollPoller, self).__init__(state_manager)

    def create_poller(self):
        return select.poll()
   
    def add_handler(self, fileno, handler, events):
        """Add a file descriptor to the poll set

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for

        """
        LOGGER.info("registering file %s", fileno)
        self._poll.register(fileno, events)
        super(PollPoller, self).add_handler(fileno, handler, events)

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """
        self._poll.modify(fileno, events)
    
    def remove_handler(self, fileno):
        """Remove a fileno to the set
       
        :param int fileno: The file descriptor
        
        """
        self.update_handler(fileno, 0)
        self._poll.unregister(fileno)
        super(PollPoller, self).remove_handler(fileno)
    

    def poll(self, write_only=False):
        """Poll until TIMEOUT waiting for an event

        :param bool write_only: Only process write events

        """
        try:
            events = self._poll.poll(self.get_next_timeout())
        except (IOError, select.error) as error:
            if error.errno == errno.EINTR:
                return
            raise
        
        for fileno, event in events:
            handler = self.filenos[fileno]
            handler(fileno, event, write_only=write_only)


class EPollPoller(PollPoller):
    """EPoll works on Linux and can have better performance than Poll in
    certain scenarios. Both are faster than select.

    """
    TIMEOUT_MULT=1
    def create_poller(self):
        return select.epoll()
