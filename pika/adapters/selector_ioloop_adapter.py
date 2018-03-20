"""
Implementation of `async_interface.AbstractAsyncServices` on top of a
selector-based I/O loop, such as tornado's and our home-grown
select_connection's I/O loops.

"""
import abc
import logging
import socket
import threading

from pika.adapters import async_interface
from pika.adapters import async_service_utils
from pika.adapters.async_service_utils import check_callback_arg, check_fd_arg


LOGGER = logging.getLogger(__name__)


class AbstractSelectorIOLoop(object):
    """Selector-based I/O loop interface expected by
    `selector_ioloop_adapter.SelectorAsyncServicesAdapter`

    NOTE: this interface follows the corresponding methods and attributes
     of `tornado.ioloop.IOLoop` in order to avoid additional adapter layering
     when wrapping tornado's IOLoop.
    """

    @property
    @abc.abstractmethod
    def READ(self):  # pylint: disable=C0103
        """The value of the I/O loop's READ flag; READ/WRITE/ERROR may be used
        with bitwise operators as expected.

        Implementation note: the implementations can simply replace these
        READ/WRITE/ERROR properties with class-level attributes

        """
        pass

    @property
    @abc.abstractmethod
    def WRITE(self):  # pylint: disable=C0103
        """The value of the I/O loop's WRITE flag; READ/WRITE/ERROR may be used
        with bitwise operators as expected

        """
        pass

    @property
    @abc.abstractmethod
    def ERROR(self):  # pylint: disable=C0103
        """The value of the I/O loop's ERROR flag; READ/WRITE/ERROR may be used
        with bitwise operators as expected

        """
        pass

    @abc.abstractmethod
    def close(self):
        """Release IOLoop's resources.

        the `close()` method is intended to be called by the application or test
        code only after `start()` returns. After calling `close()`, no other
        interaction with the closed instance of `IOLoop` should be performed.

        """
        pass

    @abc.abstractmethod
    def start(self):
        """Run the I/O loop. It will loop until requested to exit. See `stop()`.

        """
        pass

    @abc.abstractmethod
    def stop(self):
        """Request exit from the ioloop. The loop is NOT guaranteed to
        stop before this method returns.

        To invoke `stop()` safely from a thread other than this IOLoop's thread,
        call it via `add_callback_threadsafe`; e.g.,

            `ioloop.add_callback(ioloop.stop)`

        """
        pass

    @abc.abstractmethod
    def call_later(self, delay, callback):
        """Add the callback to the IOLoop timer to be called after delay seconds
        from the time of call on best-effort basis. Returns a handle to the
        timeout.

        :param int delay: The number of seconds to wait to call callback
        :param method callback: The callback method
        :returns: handle to the created timeout that may be passed to
            `remove_timeout()`
        :rtype: opaque

        """
        pass

    @abc.abstractmethod
    def remove_timeout(self, timeout_handle):
        """Remove a timeout

        :param timeout_handle: Handle of timeout to remove

        """
        pass

    @abc.abstractmethod
    def add_callback(self, callback):
        """Requests a call to the given function as soon as possible in the
        context of this IOLoop's thread.

        NOTE: This is the only thread-safe method in IOLoop. All other
        manipulations of IOLoop must be performed from the IOLoop's thread.

        For example, a thread may request a call to the `stop` method of an
        ioloop that is running in a different thread via
        `ioloop.add_callback_threadsafe(ioloop.stop)`

        :param method callback: The callback method

        """
        pass

    @abc.abstractmethod
    def add_handler(self, fd, handler, events):
        """Start watching the given file descriptor for events

        :param int fd: The file descriptor
        :param method handler: When requested event(s) occur,
            `handler(fd, events)` will be called.
        :param int events: The event mask using READ, WRITE, ERROR.

        """
        pass

    @abc.abstractmethod
    def update_handler(self, fd, events):
        """Changes the events we watch for

        :param int fd: The file descriptor
        :param int events: The event mask using READ, WRITE, ERROR

        """
        pass

    @abc.abstractmethod
    def remove_handler(self, fd):
        """Stop watching the given file descriptor for events

        :param int fd: The file descriptor

        """
        pass


class SelectorAsyncServicesAdapter(
        async_service_utils.AsyncSocketConnectionMixin,
        async_service_utils.AsyncStreamingConnectionMixin,
        async_interface.AbstractAsyncServices):
    """Implementation of ioloop exposing the
    `async_interface.AbstractAsyncServices` interface and making use of
    selector-style native loop having the `AbstractSelectorIOLoop` interface.

    For use by `pika.adapters.BaseConnection` class.

    """
    def __init__(self, native_loop):
        """
        :param AbstractSelectorIOLoop native_loop: An instance compatible with
            the `AbstractSelectorIOLoop` interface, but not necessarily derived
            from it.
        """
        self._loop = native_loop

        # Active watchers: maps file descriptors to `_FileDescriptorCallbacks`
        self._watchers = dict()

        # Native loop-specific event masks of interest
        self._readable_mask = self._loop.READ
        # NOTE: tying ERROR to WRITE is particularly handy for Windows, whose
        # `select.select()` differs from Posix by reporting
        # connection-establishment failure only through exceptfds (ERROR event),
        # while the typical application workflow is to wait for the socket to
        # become writable when waiting for socket connection to be established.
        self._writable_mask = self._loop.WRITE | self._loop.ERROR

    def get_native_ioloop(self):
        """Returns the native I/O loop instance, such as Twisted reactor,
        asyncio's or tornado's event loop

        """
        return self._loop

    def close(self):
        """Release IOLoop's resources.

        the `close()` method is intended to be called by Pika's own test
        code only after `start()` returns. After calling `close()`, no other
        interaction with the closed instance of `IOLoop` should be performed.

        NOTE: This method is provided for Pika's own test scripts that need to
        be able to run I/O loops generically to test multiple Connection Adapter
        implementations. Pika users should use the native I/O loop's API
        instead.

        """
        self._loop.close()

    def run(self):
        """Run the I/O loop. It will loop until requested to exit. See `stop()`.

        NOTE: This method is provided for Pika's own test scripts that need to
        be able to run I/O loops generically to test multiple Connection Adapter
        implementations (not all of the supported I/O Loop frameworks have
        methods named start/stop). Pika users should use the native I/O loop's
        API instead.

        """
        self._loop.start()

    def stop(self):
        """Request exit from the ioloop. The loop is NOT guaranteed to
        stop before this method returns.

        NOTE: This method is provided for Pika's own test scripts that need to
        be able to run I/O loops generically to test multiple Connection Adapter
        implementations (not all of the supported I/O Loop frameworks have
        methods named start/stop). Pika users should use the native I/O loop's
        API instead.

        To invoke `stop()` safely from a thread other than this IOLoop's thread,
        call it via `add_callback_threadsafe`; e.g.,

            `ioloop.add_callback_threadsafe(ioloop.stop)`

        """
        self._loop.stop()

    def add_callback_threadsafe(self, callback):
        """Requests a call to the given function as soon as possible. It will be
        called from this IOLoop's thread.

        NOTE: This is the only thread-safe method offered by the IOLoop adapter.
              All other manipulations of the IOLoop adapter and objects governed
              by it must be performed from the IOLoop's thread.

        :param method callback: The callback method; must be callable.
        """
        self._loop.add_callback(callback)

    def call_later(self, delay, callback):
        """Add the callback to the IOLoop timer to be called after delay seconds
        from the time of call on best-effort basis. Returns a handle to the
        timeout.

        :param int delay: The number of seconds to wait to call callback
        :param method callback: The callback method
        :returns: timer handle that may be used to cancel the callback.
        :rtype: _TimerHandle

        """
        return _TimerHandle(self._loop.call_later(delay, callback), self._loop)

    def set_reader(self, fd, on_readable):
        """Call the given callback whenever the file descriptor is readable.
        Replace prior reader callback, if any, for the given file descriptor.

        :param fd: file descriptor
        :param callable on_readable: a callback taking no args to be notified
            when fd becomes readable.

        """
        check_fd_arg(fd)
        check_callback_arg(on_readable, 'on_readable')

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            self._loop.add_handler(fd,
                                   self._on_reader_writer_fd_events,
                                   self._readable_mask)
            self._watchers[fd] = _FileDescriptorCallbacks(reader=on_readable)
            LOGGER.debug('set_reader(%s, _) added handler Rd', fd)
        else:
            if callbacks.reader is None:
                assert callbacks.writer is not None
                self._loop.update_handler(
                    fd,
                    self._readable_mask | self._writable_mask)
                LOGGER.debug('set_reader(%s, _) updated handler RdWr', fd)
            else:
                LOGGER.debug('set_reader(%s, _) replacing reader', fd)

            callbacks.reader = on_readable

    def remove_reader(self, fd):
        """Stop watching the given file descriptor for readability

        :param fd: file descriptor
        :returns: True if reader was removed; False if none was registered.

        """
        check_fd_arg(fd)

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            LOGGER.debug('remove_reader(%s) neither was set', fd)
            return False

        if callbacks.reader is None:
            assert callbacks.writer is not None
            LOGGER.debug('remove_reader(%s) reader wasn\'t set Wr', fd)
            return False

        callbacks.reader = None

        if callbacks.writer is None:
            del self._watchers[fd]
            self._loop.remove_handler(fd)
            LOGGER.debug('remove_reader(%s) removed handler', fd)
        else:
            self._loop.update_handler(fd, self._writable_mask)
            LOGGER.debug('remove_reader(%s) updated handler Wr', fd)

        return True

    def set_writer(self, fd, on_writable):
        """Call the given callback whenever the file descriptor is writable.
        Replace prior writer callback, if any, for the given file descriptor.

        :param fd: file descriptor
        :param callable on_writable: a callback taking no args to be notified
            when fd becomes writable.

        """
        check_fd_arg(fd)
        check_callback_arg(on_writable, 'on_writable')

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            self._loop.add_handler(fd,
                                   self._on_reader_writer_fd_events,
                                   self._writable_mask)
            self._watchers[fd] = _FileDescriptorCallbacks(writer=on_writable)
            LOGGER.debug('set_writer(%s, _) added handler Wr', fd)
        else:
            if callbacks.writer is None:
                assert callbacks.reader is not None
                self._loop.update_handler(
                    fd,
                    self._readable_mask | self._writable_mask)
                LOGGER.debug('set_writer(%s, _) updated handler RdWr', fd)
            else:
                LOGGER.debug('set_writer(%s, _) replacing writer', fd)

            callbacks.writer = on_writable

    def remove_writer(self, fd):
        """Stop watching the given file descriptor for writability

        :param fd: file descriptor
        :returns: True if reader was removed; False if none was registered.

        """
        check_fd_arg(fd)

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            LOGGER.debug('remove_writer(%s) neither was set.', fd)
            return False

        if callbacks.writer is None:
            assert callbacks.reader is not None
            LOGGER.debug('remove_writer(%s) writer wasn\'t set Rd', fd)
            return False

        callbacks.writer = None

        if callbacks.reader is None:
            del self._watchers[fd]
            self._loop.remove_handler(fd)
            LOGGER.debug('remove_writer(%s) removed handler', fd)
        else:
            self._loop.update_handler(fd, self._readable_mask)
            LOGGER.debug('remove_writer(%s) updated handler Rd', fd)

        return True

    def getaddrinfo(self, host, port, on_done, family=0, socktype=0, proto=0,
                    flags=0):
        """Perform the equivalent of `socket.getaddrinfo()` asynchronously.

        See `socket.getaddrinfo()` for the standard args.

        :param callable on_done: user callback that takes the return value of
            `socket.getaddrinfo()` upon successful completion or exception upon
            failure (check for `BaseException`) as its only arg. It will not be
            called if the operation was cancelled.
        :rtype: AbstractAsyncReference
        """
        return _AddressResolver(native_loop=self._loop,
                                host=host,
                                port=port,
                                family=family,
                                socktype=socktype,
                                proto=proto,
                                flags=flags,
                                on_done=on_done).start()

    def _on_reader_writer_fd_events(self, fd, events):
        """Handle indicated file descriptor events requested via `set_reader()`
        and `set_writer()`.

        :param fd: file descriptor
        :param events: event mask using native loop's READ/WRITE/ERROR. NOTE:
            depending on the underlying poller mechanism, ERROR may be indicated
            upon certain file description state even though we don't request it.
            We ignore ERROR here since `set_reader()`/`set_writer()` don't
            request for it.
        """
        callbacks = self._watchers[fd]

        if events & self._readable_mask and callbacks.reader is None:
            # NOTE: we check for consistency here ahead of the writer callback
            # because the writer callback, if any, can change the events being
            # watched
            LOGGER.warning(
                'READ indicated on fd=%s, but reader callback is None; '
                'events=%s', fd, bin(events))


        if events & self._writable_mask:
            if callbacks.writer is not None:
                callbacks.writer()
            else:
                LOGGER.warning(
                    'WRITE indicated on fd=%s, but writer callback is None; '
                    'events=%s', fd, bin(events))

        if events & self._readable_mask:
            if callbacks.reader is not None:
                callbacks.reader()
            else:
                # Reader callback might have been removed in the scope of writer
                # callback.
                pass


class _FileDescriptorCallbacks(object):
    """Holds reader and writer callbacks for a file descriptor"""

    __slots__ = ('reader', 'writer')

    def __init__(self, reader=None, writer=None):

        self.reader = reader
        self.writer = writer


class _TimerHandle(async_interface.AbstractTimerReference):
    """This module's adaptation of `async_interface.AbstractTimerReference`.

    """

    def __init__(self, handle, loop):
        """

        :param opaque handle: timer handle from the underlying loop
            implementation that may be passed to its `remove_timeout()` method
        :param AbstractSelectorIOLoop loop: the I/O loop instance that created
            the timeout.
        """
        self._handle = handle
        self._loop = loop

    def cancel(self):
        if self._loop is not None:
            self._loop.remove_timeout(self._handle)
            self._handle = None
            self._loop = None


class _SelectorIOLoopAsyncHandle(async_interface.AbstractAsyncReference):
    """This module's adaptation of `async_interface.AbstractAsyncReference`

    """

    def __init__(self, subject):
        """
        :param subject: subject of the reference containing a `cancel()` method

        """
        self._cancel = subject.cancel

    def cancel(self):
        """Cancel pending operation

        :returns: False if was already done or cancelled; True otherwise

        """
        return self._cancel()


class _AddressResolver(object):
    """Performs getaddrinfo asynchronously using a thread, then reports result
    via callback from the given I/O loop.

    NOTE: at this stage, we're using a thread per request, which may prove
    inefficient and even prohibitive if the app performs many of these
    operations concurrently.
    """
    NOT_STARTED = 0
    ACTIVE = 1
    CANCELED = 2
    COMPLETED = 3

    def __init__(self,
                 native_loop,
                 host,
                 port,
                 family,
                 socktype,
                 proto,
                 flags,
                 on_done):
        """

        :param AbstractSelectorIOLoop native_loop:
        :param host: `see socket.getaddrinfo()`
        :param port: `see socket.getaddrinfo()`
        :param family: `see socket.getaddrinfo()`
        :param socktype: `see socket.getaddrinfo()`
        :param proto: `see socket.getaddrinfo()`
        :param flags: `see socket.getaddrinfo()`
        :param on_done: on_done(records|BaseException) callback for reporting
            result from the given I/O loop. The single arg will be either an
            exception object (check for `BaseException`) in case of failure or
            the result returned by `socket.getaddrinfo()`.
        """
        check_callback_arg(on_done, 'on_done')

        self._state = self.NOT_STARTED
        self._result = None
        self._loop = native_loop
        self._host = host
        self._port = port
        self._family = family
        self._socktype = socktype
        self._proto = proto
        self._flags = flags
        self._on_done = on_done

        self._mutex = threading.Lock()
        self._threading_timer = None

    def _cleanup(self):
        """Release resources

        """
        self._loop = None
        self._threading_timer = None
        self._on_done = None

    def start(self):
        """Start asynchronous DNS lookup.

        :rtype: async_interface.AbstractAsyncReference

        """
        assert self._state == self.NOT_STARTED, self._state

        self._state = self.ACTIVE
        self._threading_timer = threading.Timer(0, self._resolve)
        self._threading_timer.start()

        return _SelectorIOLoopAsyncHandle(self)

    def cancel(self):
        """Cancel the pending resolver

        :returns: False if was already done or cancelled; True otherwise
        :rtype: bool

        """
        # Try to cancel, but no guarantees
        with self._mutex:
            if self._state == self.ACTIVE:
                LOGGER.debug('Canceling resolver for %s:%s', self._host,
                             self._port)
                self._state = self.CANCELED

                # Attempt to cancel, but not guaranteed
                self._threading_timer.cancel()

                self._cleanup()

                return True
            else:
                LOGGER.debug(
                    'Ignoring _AddressResolver cancel request when not ACTIVE; '
                    '(%s:%s); state=%s', self._host, self._port, self._state)
                return False

    def _resolve(self):
        """Call `socket.getaddrinfo()` and return result via user's callback
        function on the given I/O loop

        """
        try:
            # NOTE: on python 2.x, can't pass keyword args to getaddrinfo()
            result = socket.getaddrinfo(self._host,
                                        self._port,
                                        self._family,
                                        self._socktype,
                                        self._proto,
                                        self._flags)
        except Exception as result:  # pylint: disable=W0703
            LOGGER.error('Address resolution failed: %r', result)

        self._result = result

        # Schedule result to be returned to user via user's event loop
        with self._mutex:
            if self._state == self.ACTIVE:
                self._loop.add_callback(self._dispatch_result)
            else:
                LOGGER.debug('Asynchronous getaddrinfo cancellation detected; '
                             'in thread; host=%r', self._host)

    def _dispatch_result(self):
        """This is called from the user's I/O loop to pass the result to the
         user via the user's on_done callback

        """
        if self._state == self.ACTIVE:
            self._state = self.COMPLETED
            try:
                self._on_done(self._result)
            finally:
                self._cleanup()
        else:
            LOGGER.debug('Asynchronous getaddrinfo cancellation detected; '
                         'in I/O loop context; host=%r', self._host)
