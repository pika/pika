"""A connection adapter that tries to use the best polling method for the
platform pika is running on.

"""
import abc
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


# Reason for this unconventional dict initialization is the fact that on some
# platforms select.error is an aliases for OSError. We don't want the lambda
# for select.error to win over one for OSError.
_SELECT_ERROR_CHECKERS = {}
if pika.compat.PY3:
    #InterruptedError is undefined in PY2
    #pylint: disable=E0602
    _SELECT_ERROR_CHECKERS[InterruptedError] = lambda e: True
_SELECT_ERROR_CHECKERS[select.error] = lambda e: e.args[0] == errno.EINTR
_SELECT_ERROR_CHECKERS[IOError] = lambda e: e.errno == errno.EINTR
_SELECT_ERROR_CHECKERS[OSError] = lambda e: e.errno == errno.EINTR


# We can reduce the number of elements in the list by looking at super-sub
# class relationship because only the most generic ones needs to be caught.
# For now the optimization is left out.
# Following is better but still incomplete.
#_SELECT_ERRORS = tuple(filter(lambda e: not isinstance(e, OSError),
#                              _SELECT_ERROR_CHECKERS.keys())
#                       + [OSError])
_SELECT_ERRORS = tuple(_SELECT_ERROR_CHECKERS.keys())

def _is_resumable(exc):
    ''' Check if caught exception represents EINTR error.
    :param exc: exception; must be one of classes in _SELECT_ERRORS '''
    checker = _SELECT_ERROR_CHECKERS.get(exc.__class__, None)
    if checker is not None:
        return checker(exc)
    else:
        return False

class SelectConnection(BaseConnection):
    """An asynchronous connection adapter that attempts to use the fastest
    event loop adapter for the given platform.

    """

    def __init__(self,  # pylint: disable=R0913
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=True,
                 custom_ioloop=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
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
    """Singleton wrapper that decides which type of poller to use, creates an
    instance of it in start_poller and keeps the invoking application in a
    blocking state by calling the pollers start method. Poller should keep
    looping until IOLoop.instance().stop() is called or there is a socket
    error.

    Passes through all operations to the loaded poller object.

    """

    def __init__(self):
        self._poller = self._get_poller()

    @staticmethod
    def _get_poller():
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

    def add_timeout(self, deadline, callback_method):
        """[API] Add the callback_method to the IOLoop timer to fire after
        deadline seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        return self._poller.add_timeout(deadline, callback_method)

    def remove_timeout(self, timeout_id):
        """[API] Remove a timeout

        :param str timeout_id: The timeout id to remove

        """
        self._poller.remove_timeout(timeout_id)

    def add_handler(self, fileno, handler, events):
        """[API] Add a new fileno to the set to be monitored

        :param int fileno: The file descriptor
        :param method handler: What is called when an event happens
        :param int events: The event mask using READ, WRITE, ERROR

        """
        self._poller.add_handler(fileno, handler, events)

    def update_handler(self, fileno, events):
        """[API] Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask using READ, WRITE, ERROR

        """
        self._poller.update_handler(fileno, events)

    def remove_handler(self, fileno):
        """[API] Remove a file descriptor from the set

        :param int fileno: The file descriptor

        """
        self._poller.remove_handler(fileno)

    def start(self):
        """[API] Start the main poller loop. It will loop until requested to
        exit. See `IOLoop.stop`.

        """
        self._poller.start()

    def stop(self):
        """[API] Request exit from the ioloop. The loop is NOT guaranteed to
        stop before this method returns. This is the only method that may be
        called from another thread.

        """
        self._poller.stop()

    def process_timeouts(self):
        """[Extension] Process pending timeouts, invoking callbacks for those
        whose time has come

        """
        self._poller.process_timeouts()

    def activate_poller(self):
        """[Extension] Activate the poller

        """
        self._poller.activate_poller()

    def deactivate_poller(self):
        """[Extension] Deactivate the poller

        """
        self._poller.deactivate_poller()

    def poll(self):
        """[Extension] Wait for events of interest on registered file
        descriptors until an event of interest occurs or next timer deadline or
        `_PollerBase._MAX_POLL_TIMEOUT`, whichever is sooner, and dispatch the
        corresponding event handlers.

        """
        self._poller.poll()


_AbstractBase = abc.ABCMeta('_AbstractBase', (object,), {})


class _PollerBase(_AbstractBase):  # pylint: disable=R0902
    """Base class for select-based IOLoop implementations"""

    # Drop out of the poll loop every _MAX_POLL_TIMEOUT secs as a worst case;
    # this is only a backstop value; we will run timeouts when they are
    # scheduled.
    _MAX_POLL_TIMEOUT = 5

    # if the poller uses MS override with 1000
    POLL_TIMEOUT_MULT = 1


    def __init__(self):
        # fd-to-handler function mappings
        self._fd_handlers = dict()

        # event-to-fdset mappings
        self._fd_events = {READ: set(), WRITE: set(), ERROR: set()}

        self._processing_fd_event_map = {}

        # Reentrancy tracker of the `start` method
        self._start_nesting_levels = 0

        self._timeouts = {}
        self._next_timeout = None

        self._stopping = False

        # Mutex for controlling critical sections where ioloop-interrupt sockets
        # are created, used, and destroyed. Needed in case `stop()` is called
        # from a thread.
        self._mutex = threading.Lock()

        # ioloop-interrupt socket pair; initialized in start()
        self._r_interrupt = None
        self._w_interrupt = None

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
        # TODO when timer resolution is low (e.g., windows), we get id collision
        # when retrying failing connection with tiny (e.g., 0) retry interval
        timeout_id = hash(frozenset(value.items()))
        self._timeouts[timeout_id] = value

        if not self._next_timeout or timeout_at < self._next_timeout:
            self._next_timeout = timeout_at

        LOGGER.debug('add_timeout: added timeout %s; deadline=%s at %s',
                     timeout_id, deadline, timeout_at)
        return timeout_id

    def remove_timeout(self, timeout_id):
        """Remove a timeout if it's still in the timeout stack

        :param str timeout_id: The timeout id to remove

        """
        try:
            timeout = self._timeouts.pop(timeout_id)
        except KeyError:
            LOGGER.warning('remove_timeout: %s not found', timeout_id)
        else:
            if timeout['deadline'] == self._next_timeout:
                self._next_timeout = None

            LOGGER.debug('remove_timeout: removed %s', timeout_id)

    def _get_next_deadline(self):
        """Get the interval to the next timeout event, or a default interval

        """
        if self._next_timeout:
            timeout = max(self._next_timeout - time.time(), 0)

        elif self._timeouts:
            deadlines = [t['deadline'] for t in self._timeouts.values()]
            self._next_timeout = min(deadlines)
            timeout = max((self._next_timeout - time.time(), 0))

        else:
            timeout = self._MAX_POLL_TIMEOUT

        timeout = min(timeout, self._MAX_POLL_TIMEOUT)
        return timeout * self.POLL_TIMEOUT_MULT

    def process_timeouts(self):
        """Process pending timeouts, invoking callbacks for those whose time has
        come

        """
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
        :param int events: The event mask using READ, WRITE, ERROR

        """
        self._fd_handlers[fileno] = handler
        self._set_handler_events(fileno, events)

        # Inform the derived class
        self._register_fd(fileno, events)

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask using READ, WRITE, ERROR

        """
        # Record the change
        events_cleared, events_set = self._set_handler_events(fileno, events)

        # Inform the derived class
        self._modify_fd_events(fileno,
                               events=events,
                               events_to_clear=events_cleared,
                               events_to_set=events_set)

    def remove_handler(self, fileno):
        """Remove a file descriptor from the set

        :param int fileno: The file descriptor

        """
        try:
            del self._processing_fd_event_map[fileno]
        except KeyError:
            pass

        events_cleared, _ = self._set_handler_events(fileno, 0)
        del self._fd_handlers[fileno]

        # Inform the derived class
        self._unregister_fd(fileno, events_to_clear=events_cleared)

    def _set_handler_events(self, fileno, events):
        """Set the handler's events to the given events; internal to
        `_PollerBase`.

        :param int fileno: The file descriptor
        :param int events: The event mask (READ, WRITE, ERROR)

        :returns: a 2-tuple (events_cleared, events_set)
        """
        events_cleared = 0
        events_set = 0

        for evt in (READ, WRITE, ERROR):
            if events & evt:
                if fileno not in self._fd_events[evt]:
                    self._fd_events[evt].add(fileno)
                    events_set |= evt
            else:
                if fileno in self._fd_events[evt]:
                    self._fd_events[evt].discard(fileno)
                    events_cleared |= evt

        return events_cleared, events_set

    def activate_poller(self):
        """Activate the poller

        """
        # Activate the underlying poller and register current events
        self._init_poller()
        fd_to_events = defaultdict(int)
        for event, file_descriptors in self._fd_events.items():
            for fileno in file_descriptors:
                fd_to_events[fileno] |= event

        for fileno, events in fd_to_events.items():
            self._register_fd(fileno, events)

    def deactivate_poller(self):
        """Deactivate the poller

        """
        self._uninit_poller()

    def start(self):
        """Start the main poller loop. It will loop until requested to exit

        """
        self._start_nesting_levels += 1

        if self._start_nesting_levels == 1:
            LOGGER.debug('Entering IOLoop')
            self._stopping = False

            # Activate the underlying poller and register current events
            self.activate_poller()

            # Create ioloop-interrupt socket pair and register read handler.
            # NOTE: we defer their creation because some users (e.g.,
            # BlockingConnection adapter) don't use the event loop and these
            # sockets would get reported as leaks
            with self._mutex:
                assert self._r_interrupt is None
                self._r_interrupt, self._w_interrupt = self._get_interrupt_pair()
                self.add_handler(self._r_interrupt.fileno(),
                                 self._read_interrupt,
                                 READ)

        else:
            LOGGER.debug('Reentering IOLoop at nesting level=%s',
                         self._start_nesting_levels)

        try:
            # Run event loop
            while not self._stopping:
                self.poll()
                self.process_timeouts()

        finally:
            self._start_nesting_levels -= 1

            if self._start_nesting_levels == 0:
                LOGGER.debug('Cleaning up IOLoop')
                # Unregister and close ioloop-interrupt socket pair
                with self._mutex:
                    self.remove_handler(self._r_interrupt.fileno())
                    self._r_interrupt.close()
                    self._r_interrupt = None
                    self._w_interrupt.close()
                    self._w_interrupt = None

                # Deactivate the underlying poller
                self.deactivate_poller()
            else:
                LOGGER.debug('Leaving IOLoop with %s nesting levels remaining',
                             self._start_nesting_levels)

    def stop(self):
        """Request exit from the ioloop. The loop is NOT guaranteed to stop
        before this method returns. This is the only method that may be called
        from another thread.

        """
        LOGGER.debug('Stopping IOLoop')
        self._stopping = True

        with self._mutex:
            if self._w_interrupt is None:
                return

            try:
                # Send byte to interrupt the poll loop, use send() instead of
                # os.write for Windows compatibility
                self._w_interrupt.send(b'X')
            except OSError as err:
                if err.errno != errno.EWOULDBLOCK:
                    raise
            except Exception as err:
                # There's nothing sensible to do here, we'll exit the interrupt
                # loop after POLL_TIMEOUT secs in worst case anyway.
                LOGGER.warning("Failed to send ioloop interrupt: %s", err)
                raise

    @abc.abstractmethod
    def poll(self):
        """Wait for events on interested filedescriptors.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _init_poller(self):
        """Notify the implementation to allocate the poller resource"""
        raise NotImplementedError

    @abc.abstractmethod
    def _uninit_poller(self):
        """Notify the implementation to release the poller resource"""
        raise NotImplementedError

    @abc.abstractmethod
    def _register_fd(self, fileno, events):
        """The base class invokes this method to notify the implementation to
        register the file descriptor with the polling object. The request must
        be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: The event mask (READ, WRITE, ERROR)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _modify_fd_events(self, fileno, events, events_to_clear, events_to_set):
        """The base class invoikes this method to notify the implementation to
        modify an already registered file descriptor. The request must be
        ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: absolute events (READ, WRITE, ERROR)
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        :param int events_to_set: The events to set (READ, WRITE, ERROR)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _unregister_fd(self, fileno, events_to_clear):
        """The base class invokes this method to notify the implementation to
        unregister the file descriptor being tracked by the polling object. The
        request must be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        """
        raise NotImplementedError

    def _dispatch_fd_events(self, fd_event_map):
        """ Helper to dispatch callbacks for file descriptors that received
        events.

        Before doing so we re-calculate the event mask based on what is
        currently set in case it has been changed under our feet by a
        previous callback. We also take a store a refernce to the
        fd_event_map so that we can detect removal of an
        fileno during processing of another callback and not generate
        spurious callbacks on it.

        :param dict fd_event_map: Map of fds to events received on them.
        """
        # Reset the prior map; if the call is nested, this will suppress the
        # remaining dispatch in the earlier call.
        self._processing_fd_event_map.clear()

        self._processing_fd_event_map = fd_event_map

        for fileno in dictkeys(fd_event_map):
            if fileno not in fd_event_map:
                # the fileno has been removed from the map under our feet.
                continue

            events = fd_event_map[fileno]
            for evt in [READ, WRITE, ERROR]:
                if fileno not in self._fd_events[evt]:
                    events &= ~evt

            if events:
                handler = self._fd_handlers[fileno]
                handler(fileno, events)

    @staticmethod
    def _get_interrupt_pair():
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

    def _read_interrupt(self, interrupt_fd, events):  # pylint: disable=W0613
        """ Read the interrupt byte(s). We ignore the event mask as we can ony
        get here if there's data to be read on our fd.

        :param int interrupt_fd: The file descriptor to read from
        :param int events: (unused) The events generated for this fd
        """
        try:
            # NOTE Use recv instead of os.read for windows compatibility
            # TODO _r_interrupt is a DGRAM sock, so attempted reading of 512
            # bytes will not have the desired effect in case stop was called
            # multiple times
            self._r_interrupt.recv(512)
        except OSError as err:
            if err.errno != errno.EAGAIN:
                raise


class SelectPoller(_PollerBase):
    """Default behavior is to use Select since it's the widest supported and has
    all of the methods we need for child classes as well. One should only need
    to override the update_handler and start methods for additional types.

    """
    # if the poller uses MS specify 1000
    POLL_TIMEOUT_MULT = 1

    def __init__(self):
        """Create an instance of the SelectPoller

        """
        super(SelectPoller, self).__init__()


    def poll(self):
        """Wait for events of interest on registered file descriptors until an
        event of interest occurs or next timer deadline or _MAX_POLL_TIMEOUT,
        whichever is sooner, and dispatch the corresponding event handlers.

        """
        while True:
            try:
                if (self._fd_events[READ] or self._fd_events[WRITE] or
                      self._fd_events[ERROR]):
                    read, write, error = select.select(
                        self._fd_events[READ],
                        self._fd_events[WRITE],
                        self._fd_events[ERROR],
                        self._get_next_deadline())
                else:
                    # NOTE When called without any FDs, select fails on
                    # Windows with error 10022, 'An invalid argument was
                    # supplied'.
                    time.sleep(self._get_next_deadline())
                    read, write, error = [], [], []

                break
            except _SELECT_ERRORS as error:
                if _is_resumable(error):
                    continue
                else:
                    raise

        # Build an event bit mask for each fileno we've received an event for

        fd_event_map = defaultdict(int)
        for fd_set, evt in zip((read, write, error), (READ, WRITE, ERROR)):
            for fileno in fd_set:
                fd_event_map[fileno] |= evt

        self._dispatch_fd_events(fd_event_map)

    def _init_poller(self):
        """Notify the implementation to allocate the poller resource"""
        # It's a no op in SelectPoller
        pass

    def _uninit_poller(self):
        """Notify the implementation to release the poller resource"""
        # It's a no op in SelectPoller
        pass

    def _register_fd(self, fileno, events):
        """The base class invokes this method to notify the implementation to
        register the file descriptor with the polling object. The request must
        be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: The event mask using READ, WRITE, ERROR
        """
        # It's a no op in SelectPoller
        pass

    def _modify_fd_events(self, fileno, events, events_to_clear, events_to_set):
        """The base class invoikes this method to notify the implementation to
        modify an already registered file descriptor. The request must be
        ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: absolute events (READ, WRITE, ERROR)
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        :param int events_to_set: The events to set (READ, WRITE, ERROR)
        """
        # It's a no op in SelectPoller
        pass

    def _unregister_fd(self, fileno, events_to_clear):
        """The base class invokes this method to notify the implementation to
        unregister the file descriptor being tracked by the polling object. The
        request must be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        """
        # It's a no op in SelectPoller
        pass


class KQueuePoller(_PollerBase):
    """KQueuePoller works on BSD based systems and is faster than select"""

    def __init__(self):
        """Create an instance of the KQueuePoller

        :param int fileno: The file descriptor to check events for
        :param method handler: What is called when an event happens
        :param int events: The events to look for

        """
        super(KQueuePoller, self).__init__()

        self._kqueue = None

    @staticmethod
    def _map_event(kevent):
        """return the event type associated with a kevent object

        :param kevent kevent: a kevent object as returned by kqueue.control()

        """
        if kevent.filter == select.KQ_FILTER_READ:
            return READ
        elif kevent.filter == select.KQ_FILTER_WRITE:
            return WRITE
        elif kevent.flags & select.KQ_EV_ERROR:
            return ERROR

    def poll(self):
        """Wait for events of interest on registered file descriptors until an
        event of interest occurs or next timer deadline or _MAX_POLL_TIMEOUT,
        whichever is sooner, and dispatch the corresponding event handlers.

        """
        while True:
            try:
                kevents = self._kqueue.control(None, 1000,
                                               self._get_next_deadline())
                break
            except _SELECT_ERRORS as error:
                if _is_resumable(error):
                    continue
                else:
                    raise

        fd_event_map = defaultdict(int)
        for event in kevents:
            fd_event_map[event.ident] |= self._map_event(event)

        self._dispatch_fd_events(fd_event_map)

    def _init_poller(self):
        """Notify the implementation to allocate the poller resource"""
        assert self._kqueue is None

        self._kqueue = select.kqueue()

    def _uninit_poller(self):
        """Notify the implementation to release the poller resource"""
        self._kqueue.close()
        self._kqueue = None

    def _register_fd(self, fileno, events):
        """The base class invokes this method to notify the implementation to
        register the file descriptor with the polling object. The request must
        be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: The event mask using READ, WRITE, ERROR
        """
        self._modify_fd_events(fileno,
                               events=events,
                               events_to_clear=0,
                               events_to_set=events)

    def _modify_fd_events(self, fileno, events, events_to_clear, events_to_set):
        """The base class invoikes this method to notify the implementation to
        modify an already registered file descriptor. The request must be
        ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: absolute events (READ, WRITE, ERROR)
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        :param int events_to_set: The events to set (READ, WRITE, ERROR)
        """
        if self._kqueue is None:
            return

        kevents = list()

        if events_to_clear & READ:
            kevents.append(select.kevent(fileno,
                                         filter=select.KQ_FILTER_READ,
                                         flags=select.KQ_EV_DELETE))
        if events_to_set & READ:
            kevents.append(select.kevent(fileno,
                                         filter=select.KQ_FILTER_READ,
                                         flags=select.KQ_EV_ADD))
        if events_to_clear & WRITE:
            kevents.append(select.kevent(fileno,
                                         filter=select.KQ_FILTER_WRITE,
                                         flags=select.KQ_EV_DELETE))
        if events_to_set & WRITE:
            kevents.append(select.kevent(fileno,
                                         filter=select.KQ_FILTER_WRITE,
                                         flags=select.KQ_EV_ADD))

        self._kqueue.control(kevents, 0)

    def _unregister_fd(self, fileno, events_to_clear):
        """The base class invokes this method to notify the implementation to
        unregister the file descriptor being tracked by the polling object. The
        request must be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        """
        self._modify_fd_events(fileno,
                               events=0,
                               events_to_clear=events_to_clear,
                               events_to_set=0)


class PollPoller(_PollerBase):
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
        self._poll = None
        super(PollPoller, self).__init__()

    @staticmethod
    def _create_poller():
        """
        :rtype: `select.poll`
        """
        return select.poll()  # pylint: disable=E1101

    def poll(self):
        """Wait for events of interest on registered file descriptors until an
        event of interest occurs or next timer deadline or _MAX_POLL_TIMEOUT,
        whichever is sooner, and dispatch the corresponding event handlers.

        """
        while True:
            try:
                events = self._poll.poll(self._get_next_deadline())
                break
            except _SELECT_ERRORS as error:
                if _is_resumable(error):
                    continue
                else:
                    raise

        fd_event_map = defaultdict(int)
        for fileno, event in events:
            fd_event_map[fileno] |= event

        self._dispatch_fd_events(fd_event_map)

    def _init_poller(self):
        """Notify the implementation to allocate the poller resource"""
        assert self._poll is None

        self._poll = self._create_poller()

    def _uninit_poller(self):
        """Notify the implementation to release the poller resource"""
        if hasattr(self._poll, "close"):
            self._poll.close()

        self._poll = None

    def _register_fd(self, fileno, events):
        """The base class invokes this method to notify the implementation to
        register the file descriptor with the polling object. The request must
        be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: The event mask using READ, WRITE, ERROR
        """
        if self._poll is not None:
            self._poll.register(fileno, events)

    def _modify_fd_events(self, fileno, events, events_to_clear, events_to_set):
        """The base class invoikes this method to notify the implementation to
        modify an already registered file descriptor. The request must be
        ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: absolute events (READ, WRITE, ERROR)
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        :param int events_to_set: The events to set (READ, WRITE, ERROR)
        """
        if self._poll is not None:
            self._poll.modify(fileno, events)

    def _unregister_fd(self, fileno, events_to_clear):
        """The base class invokes this method to notify the implementation to
        unregister the file descriptor being tracked by the polling object. The
        request must be ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        """
        if self._poll is not None:
            self._poll.unregister(fileno)


class EPollPoller(PollPoller):
    """EPoll works on Linux and can have better performance than Poll in
    certain scenarios. Both are faster than select.

    """
    POLL_TIMEOUT_MULT = 1

    @staticmethod
    def _create_poller():
        """
        :rtype: `select.poll`
        """
        return select.epoll()  # pylint: disable=E1101
