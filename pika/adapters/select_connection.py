"""A connection adapter that tries to use the best polling method for the
platform pika is running on.

"""
import os
import logging
import socket
import select
import errno
import time
from collections import defaultdict
import threading

import pika.compat
from pika.compat import dictkeys

from pika.adapters.base_connection import BaseConnection

LOGGER = logging.getLogger(__name__)

# One of select, epoll, kqueue or poll
SELECT_TYPE = None

# Use epoll's constants to keep life easy
READ = 0x0001
WRITE = 0x0004
ERROR = 0x0008


if pika.compat.PY2:
    _SELECT_ERROR = select.error
else:
    # select.error was deprecated and replaced by OSError in python 3.3
    _SELECT_ERROR = OSError


def _get_select_errno(error):
    if pika.compat.PY2:
        assert isinstance(error, select.error), repr(error)
        return error.args[0]
    else:
        assert isinstance(error, OSError), repr(error)
        return error.errno


class SelectConnection(BaseConnection):
    """An asynchronous connection adapter that attempts to use the fastest
    event loop adapter for the given platform.

    """

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=True,
                 custom_ioloop=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param on_open_error_callback: Method to call if the connection cant
                                       be opened
        :type on_open_error_callback: method
        :param method on_close_callback: Method to call on connection close
        :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
        :param custom_ioloop: Override using the global IOLoop in Tornado
        :raises: RuntimeError

        """
        ioloop = custom_ioloop or IOLoop()
        super(SelectConnection, self).__init__(parameters, on_open_callback,
                                               on_open_error_callback,
                                               on_close_callback, ioloop,
                                               stop_ioloop_on_close)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker, returning True on success, False
        on failure.

        :rtype: bool

        """
        error = super(SelectConnection, self)._adapter_connect()
        if not error:
            self.ioloop.add_handler(self.socket.fileno(), self._handle_events,
                                    self.event_state)
        return error

    def _adapter_disconnect(self):
        """Disconnect from the RabbitMQ broker"""
        if self.socket:
            self.ioloop.remove_handler(self.socket.fileno())
        super(SelectConnection, self)._adapter_disconnect()


class IOLoop(object):
    """Singlton wrapper that decides which type of poller to use, creates an
    instance of it in start_poller and keeps the invoking application in a
    blocking state by calling the pollers start method. Poller should keep
    looping until IOLoop.instance().stop() is called or there is a socket
    error.

    Passes through all operations to the loaded poller object.

    """

    def __init__(self):
        self._poller = self._get_poller()

    def __getattr__(self, attr):
        return getattr(self._poller, attr)

    def _get_poller(self):
        """Determine the best poller to use for this enviroment."""

        poller = None

        if hasattr(select, 'epoll'):
            if not SELECT_TYPE or SELECT_TYPE == 'epoll':
                LOGGER.debug('Using EPollPoller')
                poller = EPollPoller()

        if not poller and hasattr(select, 'kqueue'):
            if not SELECT_TYPE or SELECT_TYPE == 'kqueue':
                LOGGER.debug('Using KQueuePoller')
                poller = KQueuePoller()

        if (not poller and hasattr(select, 'poll') and
                hasattr(select.poll(), 'modify')):  # pylint: disable=E1101
            if not SELECT_TYPE or SELECT_TYPE == 'poll':
                LOGGER.debug('Using PollPoller')
                poller = PollPoller()

        if not poller:
            LOGGER.debug('Using SelectPoller')
            poller = SelectPoller()

        return poller


class SelectPoller(object):
    """Default behavior is to use Select since it's the widest supported and has
    all of the methods we need for child classes as well. One should only need
    to override the update_handler and start methods for additional types.

    """
    # Drop out of the poll loop every POLL_TIMEOUT secs as a worst case, this
    # is only a backstop value. We will run timeouts when they are scheduled.
    POLL_TIMEOUT = 5
    # if the poller uses MS specify 1000
    POLL_TIMEOUT_MULT = 1

    def __init__(self):
        """Create an instance of the SelectPoller

        """
        # fd-to-handler function mappings
        self._fd_handlers = dict()

        # event-to-fdset mappings
        self._fd_events = {READ: set(), WRITE: set(), ERROR: set()}

        self._stopping = False
        self._timeouts = {}
        self._next_timeout = None
        self._processing_fd_event_map = {}

        # Mutex for controlling critical sections where ioloop-interrupt sockets
        # are created, used, and destroyed. Needed in case `stop()` is called
        # from a thread.
        self._mutex = threading.Lock()

        # ioloop-interrupt socket pair; initialized in start()
        self._r_interrupt = None
        self._w_interrupt = None

    def get_interrupt_pair(self):
        """ Use a socketpair to be able to interrupt the ioloop if called
            from another thread. Socketpair() is not supported on some OS (Win)
            so use a pair of simple UDP sockets instead. The sockets will be
            closed and garbage collected by python when the ioloop itself is.
        """
        try:
            read_sock, write_sock = socket.socketpair()

        except AttributeError:
            LOGGER.debug("Using custom socketpair for interrupt")
            read_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            read_sock.bind(('localhost', 0))
            write_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            write_sock.connect(read_sock.getsockname())

        read_sock.setblocking(0)
        write_sock.setblocking(0)
        return read_sock, write_sock

    def read_interrupt(self, interrupt_sock,
                       events, write_only):  # pylint: disable=W0613
        """ Read the interrupt byte(s). We ignore the event mask and write_only
        flag as we can ony get here if there's data to be read on our fd.

        :param int interrupt_sock: The file descriptor to read from
        :param int events: (unused) The events generated for this fd
        :param bool write_only: (unused) True if poll was called to trigger a
            write
        """
        try:
            os.read(interrupt_sock, 512)
        except OSError as err:
            if err.errno != errno.EAGAIN:
                raise

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
        value = {'deadline': timeout_at, 'callback': callback_method}
        timeout_id = hash(frozenset(value.items()))
        self._timeouts[timeout_id] = value

        if not self._next_timeout or timeout_at < self._next_timeout:
            self._next_timeout = timeout_at

        return timeout_id

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

    def get_next_deadline(self):
        """Get the interval to the next timeout event, or a default interval
        """
        if self._next_timeout:
            timeout = max((self._next_timeout - time.time(), 0))

        elif self._timeouts:
            deadlines = [t['deadline'] for t in self._timeouts.values()]
            self._next_timeout = min(deadlines)
            timeout = max((self._next_timeout - time.time(), 0))

        else:
            timeout = SelectPoller.POLL_TIMEOUT

        timeout = min((timeout, SelectPoller.POLL_TIMEOUT))
        return timeout * SelectPoller.POLL_TIMEOUT_MULT


    def process_timeouts(self):
        """Process the self._timeouts event stack"""

        now = time.time()
        # Run the timeouts in order of deadlines. Although this shouldn't
        # be strictly necessary it preserves old behaviour when timeouts
        # were only run periodically.
        to_run = sorted([(k, timer) for (k, timer) in self._timeouts.items()
                         if timer['deadline'] <= now],
                        key=lambda item: item[1]['deadline'])

        for k, timer in to_run:
            if k not in self._timeouts:
                # Previous invocation(s) should have deleted the timer.
                continue
            try:
                timer['callback']()
            finally:
                # Don't do 'del self._timeout[k]' as the key might
                # have been deleted just now.
                if self._timeouts.pop(k, None) is not None:
                    self._next_timeout = None


    def add_handler(self, fileno, handler, events):
        """Add a new fileno to the set to be monitored

       :param int fileno: The file descriptor
       :param method handler: What is called when an event happens
       :param int events: The event mask

       """
        self._fd_handlers[fileno] = handler
        self.update_handler(fileno, events)

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """

        for ev in (READ, WRITE, ERROR):
            if events & ev:
                self._fd_events[ev].add(fileno)
            else:
                self._fd_events[ev].discard(fileno)


    def remove_handler(self, fileno):
        """Remove a file descriptor from the set

        :param int fileno: The file descriptor

        """
        try:
            del self._processing_fd_event_map[fileno]
        except KeyError:
            pass

        self.update_handler(fileno, 0)
        del self._fd_handlers[fileno]

    def start(self):
        """Start the main poller loop. It will loop here until self._stopping"""

        LOGGER.debug('Starting IOLoop')
        self._stopping = False

        with self._mutex:
            # Watch out for reentry
            if self._r_interrupt is None:
                # Create ioloop-interrupt socket pair and register read handler.
                # NOTE: we defer their creation because some users (e.g.,
                # BlockingConnection adapter) don't use the event loop and these
                # sockets would get reported as leaks
                self._r_interrupt, self._w_interrupt = self.get_interrupt_pair()
                self.add_handler(self._r_interrupt.fileno(),
                                 self.read_interrupt,
                                 READ)
                interrupt_sockets_created = True
            else:
                interrupt_sockets_created = False
        try:
            # Run event loop
            while not self._stopping:
                self.poll()
                self.process_timeouts()
        finally:
            # Unregister and close ioloop-interrupt socket pair
            if interrupt_sockets_created:
                with self._mutex:
                    self.remove_handler(self._r_interrupt.fileno())
                    self._r_interrupt.close()
                    self._r_interrupt = None
                    self._w_interrupt.close()
                    self._w_interrupt = None

    def stop(self):
        """Request exit from the ioloop."""

        LOGGER.debug('Stopping IOLoop')
        self._stopping = True

        with self._mutex:
            if self._w_interrupt is None:
                return

            try:
                # Send byte to interrupt the poll loop, use write() for
                # consitency.
                os.write(self._w_interrupt.fileno(), b'X')
            except OSError as err:
                if err.errno != errno.EWOULDBLOCK:
                    raise
            except Exception as err:
                # There's nothing sensible to do here, we'll exit the interrupt
                # loop after POLL_TIMEOUT secs in worst case anyway.
                LOGGER.warning("Failed to send ioloop interrupt: %s", err)
                raise

    def poll(self, write_only=False):
        """Wait for events on interested filedescriptors.

        :param bool write_only: Passed through to the hadnlers to indicate
            that they should only process write events.
        """
        while True:
            try:
                read, write, error = select.select(self._fd_events[READ],
                                                   self._fd_events[WRITE],
                                                   self._fd_events[ERROR],
                                                   self.get_next_deadline())
                break
            except _SELECT_ERROR as error:
                if _get_select_errno(error) == errno.EINTR:
                    continue
                else:
                    raise

        # Build an event bit mask for each fileno we've recieved an event for

        fd_event_map = defaultdict(int)
        for fd_set, ev in zip((read, write, error), (READ, WRITE, ERROR)):
            for fileno in fd_set:
                fd_event_map[fileno] |= ev

        self._process_fd_events(fd_event_map, write_only)

    def _process_fd_events(self, fd_event_map, write_only):
        """ Processes the callbacks for each fileno we've recieved events.
            Before doing so we re-calculate the event mask based on what is
            currently set in case it has been changed under our feet by a
            previous callback. We also take a store a refernce to the
            fd_event_map in the class so that we can detect removal of an
            fileno during processing of another callback and not generate
            spurious callbacks on it.

            :param dict fd_event_map: Map of fds to events recieved on them.
        """

        self._processing_fd_event_map = fd_event_map

        for fileno in dictkeys(fd_event_map):
            if fileno not in fd_event_map:
                # the fileno has been removed from the map under our feet.
                continue

            events = fd_event_map[fileno]
            for ev in [READ, WRITE, ERROR]:
                if fileno not in self._fd_events[ev]:
                    events &= ~ev

            if events:
                handler = self._fd_handlers[fileno]
                handler(fileno, events, write_only=write_only)

class KQueuePoller(SelectPoller):
    """KQueuePoller works on BSD based systems and is faster than select"""

    def __init__(self):
        """Create an instance of the KQueuePoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for

        """
        self._kqueue = select.kqueue()
        super(KQueuePoller, self).__init__()

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """

        kevents = list()
        if not events & READ:
            if fileno in self._fd_events[READ]:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_DELETE))
        else:
            if fileno not in self._fd_events[READ]:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_READ,
                                             flags=select.KQ_EV_ADD))
        if not events & WRITE:
            if fileno in self._fd_events[WRITE]:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_DELETE))
        else:
            if fileno not in self._fd_events[WRITE]:
                kevents.append(select.kevent(fileno,
                                             filter=select.KQ_FILTER_WRITE,
                                             flags=select.KQ_EV_ADD))
        for event in kevents:
            self._kqueue.control([event], 0)
        super(KQueuePoller, self).update_handler(fileno, events)

    def _map_event(self, kevent):
        """return the event type associated with a kevent object

        :param kevent kevent: a kevent object as returned by kqueue.control()

        """
        if kevent.filter == select.KQ_FILTER_READ:
            return READ
        elif kevent.filter == select.KQ_FILTER_WRITE:
            return WRITE
        elif kevent.flags & select.KQ_EV_ERROR:
            return ERROR

    def poll(self, write_only=False):
        """Check to see if the events that are cared about have fired.

        :param bool write_only: Don't look at self.events, just look to see if
            the adapter can write.

        """
        while True:
            try:
                kevents = self._kqueue.control(None, 1000,
                                               self.get_next_deadline())
                break
            except _SELECT_ERROR as error:
                if _get_select_errno(error) == errno.EINTR:
                    continue
                else:
                    raise

        fd_event_map = defaultdict(int)
        for event in kevents:
            fileno = event.ident
            fd_event_map[fileno] |= self._map_event(event)

        self._process_fd_events(fd_event_map, write_only)


class PollPoller(SelectPoller):
    """Poll works on Linux and can have better performance than EPoll in
    certain scenarios.  Both are faster than select.

    """
    POLL_TIMEOUT_MULT = 1000

    def __init__(self):
        """Create an instance of the KQueuePoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for

        """
        self._poll = self.create_poller()
        super(PollPoller, self).__init__()

    def create_poller(self):
        return select.poll()  # pylint: disable=E1101

    def add_handler(self, fileno, handler, events):
        """Add a file descriptor to the poll set

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for

        """
        self._poll.register(fileno, events)
        super(PollPoller, self).add_handler(fileno, handler, events)

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """
        super(PollPoller, self).update_handler(fileno, events)
        self._poll.modify(fileno, events)

    def remove_handler(self, fileno):
        """Remove a fileno to the set

        :param int fileno: The file descriptor

        """
        super(PollPoller, self).remove_handler(fileno)
        self._poll.unregister(fileno)

    def poll(self, write_only=False):
        """Poll until the next timeout waiting for an event

        :param bool write_only: Only process write events

        """
        while True:
            try:
                events = self._poll.poll(self.get_next_deadline())
                break
            except _SELECT_ERROR as error:
                if _get_select_errno(error) == errno.EINTR:
                    continue
                else:
                    raise

        fd_event_map = defaultdict(int)
        for fileno, event in events:
            fd_event_map[fileno] |= event

        self._process_fd_events(fd_event_map, write_only)


class EPollPoller(PollPoller):
    """EPoll works on Linux and can have better performance than Poll in
    certain scenarios. Both are faster than select.

    """
    POLL_TIMEOUT_MULT = 1

    def create_poller(self):
        return select.epoll()  # pylint: disable=E1101
