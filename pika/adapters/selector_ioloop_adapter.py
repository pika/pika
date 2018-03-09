"""
Implementation of `ioloop_interface.AbstractAsyncServices` on top of a
selector-based I/O loop, such as tornado's and our home-grown
select_connection's I/O loops.

"""
import abc
import errno
import logging
import numbers
import os
import socket
import ssl
import threading

from pika.adapters import ioloop_interface, adapter_transport
import pika.compat


LOGGER = logging.getLogger(__name__)


class AbstractSelectorIOLoop(object):
    """Selector-based I/O loop interface expected by
    `selector_ioloop_adapter.SelectorAsyncServicesAdapter`

    NOTE: this interface follows the corresponding methods and attributes
     of `tornado.ioloop.IOLoop` in order to avoid additional adapter layering
     when wrapping tornado's IOLoop.
    """

    @abc.abstractmethod
    @property
    def READ(self):  # pylint: disable=C0103
        """The value of the I/O loop's READ flag; READ/WRITE/ERROR may be used
        with bitwise operators as expected.

        Implementation note: the implementations can simply replace these
        READ/WRITE/ERROR properties with class-level attributes

        """
        pass

    @abc.abstractmethod
    @property
    def WRITE(self):  # pylint: disable=C0103
        """The value of the I/O loop's WRITE flag; READ/WRITE/ERROR may be used
        with bitwise operators as expected

        """
        pass

    @abc.abstractmethod
    @property
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


class SelectorAsyncServicesAdapter(ioloop_interface.AbstractAsyncServices):
    """Implementation of ioloop exposing the
    `ioloop_interface.AbstractAsyncServices` interface and making use of
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

    def close(self):
        """Release IOLoop's resources.

        the `close()` method is intended to be called by the application or test
        code only after `start()` returns. After calling `close()`, no other
        interaction with the closed instance of `IOLoop` should be performed.

        """
        self._loop.close()

    def get_native_ioloop(self):
        """Returns the native I/O loop instance, such as Twisted reactor,
        asyncio's or tornado's event loop

        """
        return self._loop

    def start(self):
        """Run the I/O loop. It will loop until requested to exit. See `stop()`.

        """
        self._loop.start()

    def stop(self):
        """Request exit from the ioloop. The loop is NOT guaranteed to
        stop before this method returns.

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
        :rtype: handle to the created timeout that may be passed to
            `remove_timeout()`

        """
        self._loop.call_later(delay, callback)

    def remove_timeout(self, timeout_handle):
        """Remove a timeout

        :param timeout_handle: Handle of timeout to remove

        """
        self._loop.remove_timeout(timeout_handle)

    def set_reader(self, fd, on_readable):
        """Call the given callback whenever the file descriptor is readable.
        Replace prior reader callback, if any, for the given file descriptor.

        :param fd: file descriptor
        :param callable on_readable: a callback taking no args to be notified
            when fd becomes readable.

        """
        _check_fd_arg(fd)
        _check_callback_arg(on_readable, 'on_readable')

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            self._loop.add_handler(fd,
                                   self._on_reader_writer_fd_events,
                                   self._loop.READ)
            self._watchers[fd] = _FileDescriptorCallbacks(reader=on_readable)
        else:
            if callbacks.reader is None:
                assert callbacks.writer is not None
                self._loop.update_handler(fd,
                                          self._loop.READ | self._loop.WRITE)

            callbacks.reader = on_readable

    def remove_reader(self, fd):
        """Stop watching the given file descriptor for readability

        :param fd: file descriptor
        :returns: True if reader was removed; False if none was registered.

        """
        _check_fd_arg(fd)

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            return False

        if callbacks.reader is None:
            return False

        callbacks.reader = None

        if callbacks.writer is None:
            del self._watchers[fd]
            self._loop.remove_handler(fd)
        else:
            self._loop.update_handler(fd, self._loop.WRITE)

        return True

    def set_writer(self, fd, on_writable):
        """Call the given callback whenever the file descriptor is writable.
        Replace prior writer callback, if any, for the given file descriptor.

        :param fd: file descriptor
        :param callable on_writable: a callback taking no args to be notified
            when fd becomes writable.

        """
        _check_fd_arg(fd)
        _check_callback_arg(on_writable, 'on_writable')

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            self._loop.add_handler(fd,
                                   self._on_reader_writer_fd_events,
                                   self._loop.WRITE)
            self._watchers[fd] = _FileDescriptorCallbacks(writer=on_writable)
        else:
            if callbacks.writer is None:
                assert callbacks.reader is not None
                self._loop.update_handler(fd,
                                          self._loop.READ | self._loop.WRITE)

            callbacks.writer = on_writable

    def remove_writer(self, fd):
        """Stop watching the given file descriptor for writability

        :param fd: file descriptor
        :returns: True if reader was removed; False if none was registered.

        """
        _check_fd_arg(fd)

        try:
            callbacks = self._watchers[fd]
        except KeyError:
            return False

        if callbacks.writer is None:
            return False

        callbacks.writer = None

        if callbacks.reader is None:
            del self._watchers[fd]
            self._loop.remove_handler(fd)
        else:
            self._loop.update_handler(fd, self._loop.READ)

        return True

    def getaddrinfo(self, host, port, on_done, family=0, socktype=0, proto=0,
                    flags=0):
        """Perform the equivalent of `socket.getaddrinfo()` asynchronously.

        See `socket.getaddrinfo()` for the standard args.

        :param callable on_done: user callback that takes the return value of
            `socket.getaddrinfo()` upon successful completion or exception upon
            failure as its only arg. It will not be called if the operation
            was successfully cancelled.
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


    def connect_socket(self, sock, resolved_addr, on_done):
        """Perform the equivalent of `socket.connect()` on a previously-resolved
        address asynchronously.

        IMPLEMENTATION NOTE: Pika's connection logic resolves the addresses
            prior to making socket connections, so we don't need to burden the
            implementations of this method with the extra logic of asynchronous
            DNS resolution. Implementations can use `socket.inet_pton()` to
            verify the address.

        :param socket.socket sock: non-blocking socket that needs to be
            connected as per `socket.socket.connect()`
        :param tuple resolved_addr: resolved destination address/port two-tuple
            as per `socket.socket.connect()`, except that the first element must
            be an actual IP address that's consistent with the given socket's
            address family.
        :param callable on_done: user callback that takes None upon successful
            completion or Exception-based exception upon error as its only arg.
            It will not be called if the operation was successfully cancelled.

        :rtype: AbstractAsyncReference
        :raises ValueError: if host portion of `resolved_addr` is not an IP
            address or is inconsistent with the socket's address family as
            validated via `socket.inet_pton()`

        """
        return _SocketConnector(native_loop=self._loop,
                                sock=sock,
                                resolved_addr=resolved_addr,
                                on_done=on_done).start()

    def create_streaming_connection(self,
                                    protocol_factory,
                                    sock,
                                    on_done,
                                    ssl_context=None,
                                    server_hostname=None):
        """Perform SSL session establishment, if requested, on the already-
        connected socket and link the streaming transport/protocol pair.

        NOTE: This method takes ownership of the socket.

        :param callable protocol_factory: returns an instance with the
            `AbstractStreamProtocol` interface. The protocol's
            `connection_made(transport)` method will be called to link it to
            the transport after remaining connection activity (e.g., SSL session
            establishment), if any, is completed successfully.
        :param socket.socket sock: Already-connected, non-blocking
            `socket.SOCK_STREAM` socket to be used by the transport. We take
            ownership of this socket.
        :param callable on_done: User callback
            `on_done(Exception | (transport, protocol))` to be notified when the
            asynchronous operation completes. Exception-based arg indicates
            failure; otherwise the two-tuple will contain the linked
            transport/protocol pair, with the AbstractStreamTransport and
            AbstractStreamProtocol respectively.
        :param None | ssl.SSLContext ssl_context: if None, this will proceed as
            a plaintext connection; otherwise, if not None, SSL session
            establishment will be performed prior to linking the transport and
            protocol.
        :param str | None server_hostname: For use during SSL session
            establishment to match against the target server's certificate. The
            value `None` disables this check (which is a huge security risk)
        :rtype: AbstractAsyncReference
        """
        try:
            return _StreamConnector(ioloop=self,
                                    protocol_factory=protocol_factory,
                                    sock=sock,
                                    ssl_context=ssl_context,
                                    server_hostname=server_hostname,
                                    on_done=on_done).start()
        except Exception as error:
            LOGGER.error('create_streaming_connection(%s) failed: %r',
                         sock, error)
            # Close the socket since this function takes ownership
            sock.close()

            raise

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

        if events & self._loop.READ and callbacks.reader is None:
            LOGGER.warning(
                'READ indicated on fd=%s, but reader callback is None', fd)


        if events & self._loop.WRITE:
            if callbacks.writer is not None:
                callbacks.writer()
            else:
                LOGGER.warning(
                    'WRITE indicated on fd=%s, but writer callback is None', fd)

        if events & self._loop.READ:
            if callbacks.reader is not None:
                callbacks.reader()
            else:
                # Reader callback might have been removed in the scope of writer
                # callback.
                pass


def _check_callback_arg(callback, name):
    """Raise TypeError if callback is not callable

    :param callback: callback to check
    :param name: Name to include in exception text
    :raises TypeError:

    """
    if not callable(callback):
        raise TypeError(
            '{} must be callable, but got {!r}'.format(name, callback))

def _check_fd_arg(fd):
    """Raise TypeError if file descriptor is not an integer

    :param fd: file descriptor
    :raises TypeError:

    """
    if not isinstance(fd, numbers.Integral):
        raise TypeError(
            'Paramter must be a file descriptor, but got {!r}'.format(fd))


class _FileDescriptorCallbacks(object):
    """Holds reader and writer callbacks for a file descriptor"""

    __slots__ = ('reader', 'writer')

    def __init__(self, reader=None, writer=None):

        self.reader = reader
        self.writer = writer


class _SelectorIOLoopAsyncHandle(ioloop_interface.AbstractAsyncReference):
    """`ioloop_interface.AbstractAsyncReference`-based handle representing
    the asynchronous resolution request.
    """
    def __init__(self, subject):
        """
        :param subject: subject of the reference containing a `cancel()` method

        """
        self._cancel = subject.cancel

    def cancel(self):
        self._cancel()


class _StreamConnector(object):
    """Performs asynchronous SSL session establishment, if requested, on the
    already-connected socket and links the streaming transport to protocol.

    """
    NOT_STARTED = 0
    ACTIVE = 1
    CANCELED = 2
    COMPLETED = 3

    def __init__(self, ioloop,
                 protocol_factory,
                 sock,
                 ssl_context,
                 server_hostname,
                 on_done):
        """
        NOTE: We take ownership of the given socket

        See `AbstractAsyncServices.create_streaming_connection()` for detailed
        documentation of the corresponding args.

        :param SelectorAsyncServicesAdapter ioloop:
        :param callable protocol_factory:
        :param socket.socket sock:
        :param ssl.SSLContext | None ssl_context:
        :param str | None server_hostname:
        :param callable on_done:

        """
        _check_callback_arg(protocol_factory, 'protocol_factory')
        _check_callback_arg(on_done, 'on_done')

        if not isinstance(ssl_context, (type(None), ssl.SSLContext)):
            raise ValueError('Expected ssl_context=None | ssl.SSLContext, but '
                             'got {!r}'.format(ssl_context))

        if server_hostname is not None and ssl_context is None:
            raise ValueError('Non-None server_hostname must not be passed '
                             'without ssl context')

        # Check that the socket connection establishment had completed in order
        # to avoid stalling while waiting for the socket to become readable
        # and/or writable.
        #
        # Also on Windows, failed connection establishment is reflected by
        # `select()` only through `exceptfds`, so a socket with pending
        # connection that eventually fails will never indicate readable/writable
        # in contrast with posix.
        try:
            self._sock.getpeername()
        except Exception as error:
            raise ValueError(
                'Expected connected socket, but it wasn\'t connected: '
                '{}; error={!r}'.format(self._sock, error))

        self._ioloop = ioloop
        self._protocol_factory = protocol_factory
        self._sock = sock
        self._ssl_context = ssl_context
        self._server_hostname = server_hostname
        self._on_done = on_done

        self._state = self.NOT_STARTED

    def _cleanup(self):
        """Cancel pending async operations, if any

        """
        self._ioloop.remove_reader(self._sock.fileno())
        self._ioloop.remove_writer(self._sock.fileno())

    def start(self):
        """Kick off the workflow

        :rtype: ioloop_interface.AbstractAsyncReference
        """
        assert self._state == self.NOT_STARTED, self._state

        self._state = self.ACTIVE

        # Request callback from I/O loop to start processing so that we don't
        # end up making callbacks from the caller's scope
        self._ioloop.add_callback_threadsafe(self._start_safe)

        return _SelectorIOLoopAsyncHandle(self)

    def cancel(self):
        """Cancel pending connection request without calling user's completion
        callback.

        """
        if self._state == self.ACTIVE:
            try:
                self._state = self.CANCELED
                LOGGER.debug('Canceling streaming linkup for %s', self._sock)
                self._cleanup()
            finally:
                # Close the socket, since we took ownership
                self._sock.close()
        else:
            LOGGER.debug('Ignoring _StreamConnector cancel requested when not '
                         'ACTIVE; state=%s; %s', self._state, self._sock)

    def _report_completion(self, result):
        """Advance to COMPLETED state, cancel async operation(s), and invoke
        user's completion callback.

        :param Exception | tuple result: value to pass in user's callback

        """
        assert isinstance(result, (Exception, tuple)), result
        assert self._state == self.ACTIVE, self._state

        self._state = self.COMPLETED
        self._cleanup()
        if isinstance(result, Exception):
            # Close the socket on error, since we took ownership of it
            try:
                self._sock.close()
            except Exception as error:  # pylint: disable=W0703
                LOGGER.error('_sock.close() failed: error=%r; %s',
                             error, self._sock)

        self._on_done(result)

    def _start_safe(self):
        """Called as callback from I/O loop to kick-start the workflow, so it's
        safe to call user's completion callback from here, if needed

        """
        if self._state != self.ACTIVE:
            LOGGER.debug('Abandoning streaming linkup due to inactive state '
                         'transition; state=%s; %s; .',
                         self._state, self._sock)
            return

        # Link up protocol and transport if this is a plaintext linkup;
        # otherwise kick-off SSL workflow first
        if self._ssl_context is None:
            self._linkup()
        else:
            LOGGER.debug('Starting SSL handshake on %s', self._sock)

            # Wrap our plain socket in ssl socket
            try:
                self._sock = self._ssl_context.wrap_socket(
                    self._sock,
                    server_side=False,
                    do_handshake_on_connect=False,
                    suppress_ragged_eofs=True,
                    server_hostname=self._server_hostname)
            except Exception as error:  # pylint: disable=W0703
                LOGGER.error('SSL wrap_socket(%s) failed: %r', self._sock,
                             error)
                self._report_completion(error)
                return

            self._do_ssl_handshake()

    def _linkup(self):
        """Connection is ready: instantiate and link up transport and protocol,
        and invoke user's completion callback.

        """
        transport = None

        try:
            # Create the protocol
            try:
                protocol = self._protocol_factory()
            except Exception as error:
                LOGGER.error('protocol_factory() failed: error=%r; %s',
                             error, self._sock)
                raise

            if self._ssl_context is None:
                # Create plaintext streaming transport
                try:
                    transport = adapter_transport.PlainTransport(self._sock,
                                                                 protocol)
                except Exception as error:
                    LOGGER.error('PlainTransport() failed: error=%r; %s',
                                 error, self._sock)
                    raise
            else:
                # Create SSL streaming transport
                try:
                    transport = adapter_transport.SSLTransport(self._sock,
                                                               protocol)
                except Exception as error:
                    LOGGER.error('SSLTransport() failed: error=%r; %s',
                                 error, self._sock)
                    raise

            # Acquaint protocol with its transport
            try:
                protocol.connection_made(transport)
            except Exception as error:
                LOGGER.error('protocol.connection_made(%r) failed: error=%r; %s',
                             transport, error, self._sock)
                raise

        except Exception as error:  # pylint: disable=W0703
            result = error

            if transport is not None:
                try:
                    transport.abort()
                except Exception as error:  # pylint: disable=W0703
                    # Report and suppress the exception, since we need to pass
                    # the original error with user's completion callback
                    LOGGER.error('transport.abort() failed: error=%r; %s',
                                 error, self._sock)
        else:
            result = (transport, protocol)

        self._report_completion(result)

    def _do_ssl_handshake(self):
        """Perform asynchronous SSL handshake on the already wrapped socket

        """
        if self._state != self.ACTIVE:
            LOGGER.debug('Abandoning streaming linkup due to inactive state '
                         'transition; state=%s; %s; .',
                         self._state, self._sock)
            return

        try:
            self._sock.do_handshake()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                LOGGER.debug('SSL handshake wants read; %s.', self._sock)
                self._ioloop.set_reader(self._sock.fileno(),
                                        self._do_ssl_handshake())
                self._ioloop.remove_writer(self._sock.fileno())
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                LOGGER.debug('SSL handshake wants write. %s', self._sock)
                self._ioloop.set_writer(self._sock.fileno(),
                                        self._do_ssl_handshake())
                self._ioloop.remove_reader(self._sock.fileno())
            else:
                LOGGER.error('SSL do_handshake failed: error=%r; %s',
                             error, self._sock)
                self._report_completion(error)
                return
        except Exception as error:  # pylint: disable=W0703
            LOGGER.error('SSL do_handshake failed: error=%r; %s',
                         error, self._sock)
            self._report_completion(error)
            return

        else:
            LOGGER.info('SSL handshake completed successfully: %s', self._sock)

            # Suspend I/O and link up transport with protocol
            self._ioloop.remove_reader(self._sock.fileno())
            self._ioloop.remove_writer(self._sock.fileno())

            self._linkup()

class _SocketConnector(object):
    """Connects the given non-blocking socket asynchronously suing the given
    I/O loop
    """

    NOT_STARTED = 0
    ACTIVE = 1
    CANCELED = 2
    COMPLETED = 3

    def __init__(self, native_loop, sock, resolved_addr, on_done):
        """
        :param AbstractSelectorIOLoop native_loop:
        :param socket.socket sock: non-blocking socket that needs to be
            connected as per `socket.socket.connect()`
        :param tuple resolved_addr: resolved destination address/port two-tuple
        :param callable on_done: user callback that takes None upon successful
            completion or Exception-based exception upon error as its only arg.
            It will not be called if the operation was successfully cancelled.
        :raises ValueError: if host portion of `resolved_addr` is not an IP
            address or is inconsistent with the socket's address family as
            validated via `socket.inet_pton()`
        """
        _check_callback_arg(on_done, 'on_done')

        try:
            socket.inet_pton(sock.family, resolved_addr[0])
        except Exception as error:
            msg = ('Invalid or unresolved IP address '
                   '{!r} for socket {}: {!r}').format(resolved_addr, sock, error)
            LOGGER.error(msg)
            raise ValueError(msg)

        self._loop = native_loop
        self._sock = sock
        self._addr = resolved_addr
        self._on_done = on_done
        self._state = self.NOT_STARTED
        self._watching_socket_events = False

    def _cleanup(self):
        """Remove socket watcher, if any

        """
        if self._watching_socket_events:
            self._loop.remove_handler(self._sock.fileno())

    def start(self):
        """Start asynchronous connection establishment.

        :rtype: ioloop_interface.AbstractAsyncReference
        """
        assert self._state == self.NOT_STARTED, self._state

        self._state = self.ACTIVE

        # Continue the rest of the operation on the I/O loop to avoid calling
        # user's completion callback from the scope of user's call
        self._loop.add_callback(self._start_safe)

        return _SelectorIOLoopAsyncHandle(self)

    def cancel(self):
        """Cancel pending connection request without calling user's completion
        callback.

        """
        if self._state == self.ACTIVE:
            self._state = self.CANCELED
            LOGGER.debug('Canceling connection request for %s to %s',
                         self._sock, self._addr)
            self._cleanup()
        else:
            LOGGER.debug('Ignoring _SocketConnector cancel requested when not '
                         'ACTIVE; state=%s; %s', self._state, self._sock)

    def _report_completion(self, result):
        """Advance to COMPLETED state, remove socket watcher, and invoke user's
        completion callback.

        :param Exception | None result: value to pass in user's callback

        """
        assert isinstance(result, (Exception, type(None))), result
        assert self._state == self.ACTIVE, self._state

        self._state = self.COMPLETED
        self._cleanup()

        self._on_done(result)

    def _start_safe(self):
        """Called as callback from I/O loop to kick-start the workflow, so it's
        safe to call user's completion callback from here, if needed

        """
        if self._state != self.ACTIVE:
            LOGGER.debug('Abandoning sock=%s connection establishment to %s '
                         'due to inactive state=%s',
                         self._sock, self._addr, self._state)
            return

        try:
            self._sock.connect(self._addr)
        except pika.compat.SOCKET_ERROR as error:
            # EINPROGRESS for posix and EWOULDBLOCK for Windows
            if error.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK,):
                LOGGER.error('%s.connect(%s) failed: %r',
                             self._sock, self._addr, error)
                self._report_completion(error)
                return
        except Exception as error:  # pylint: disable=W0703
            LOGGER.error('%s.connect(%s) failed: %r',
                         self._sock, self._addr, error)
            self._report_completion(error)
            return

        # Start watching socket for WRITE and ERROR (on Windows,
        # connection-establishment error is indicated only through ERROR, while
        # WRITE is not indicated in that case)
        try:
            self._loop.add_handler(self._sock.fileno(),
                                   self._handle_socket_events,
                                   self._loop.WRITE | self._loop.ERROR)
        except Exception as error:  # pylint: disable=W0703
            LOGGER.error('loop.add_handler(%s, ..., WRITE + ERROR) failed: %r',
                         self._sock, error)
            self._report_completion(error)
            return
        else:
            self._watching_socket_events = True
            LOGGER.debug('Connection-establishment is in progress for %s.',
                         self._sock)

    def _handle_socket_events(self, _fd, events):
        """Called when socket connects or fails to. Check for predicament and
        invoke user's completion callback.

        :param int fd: File descriptor of the socket we're watching.
        :param int events: bitmask of indicated events (self._loop.WRITE/ERROR).

        """
        if self._state != self.ACTIVE:
            LOGGER.warning(
                'Socket connection-establishment event watcher '
                'called in inactive state (ignoring): %s; events=%s; state=%s',
                self._sock, bin(events), self._state)
            return

        error_code = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if not error_code:
            assert not (events & self._loop.ERROR), bin(events)
            LOGGER.info('Socket connected: %s', self._sock)
            result = None

        else:
            error_msg = os.strerror(error_code)
            LOGGER.error(
                'Socket failed to connect: %s; error=%s (%s)',
                self._sock, error_code, error_msg)
            result = pika.compat.SOCKET_ERROR(error_code, error_msg)

        self._report_completion(result)


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
        :param on_done: on_done(records|Exception) callback for reporting result
            from the given I/O loop. The single arg will be either an
            `Exception`-based exception object in case of failure or the result
            returned by `socket.getaddrinfo()`.
        """
        _check_callback_arg(on_done, 'on_done')

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

        :rtype: ioloop_interface.AbstractAsyncReference

        """
        assert self._state == self.NOT_STARTED, self._state

        self._state = self.ACTIVE
        self._threading_timer = threading.Timer(0, self._resolve)
        self._threading_timer.start()

        return _SelectorIOLoopAsyncHandle(self)

    def cancel(self):
        """Cancel the pending resolver

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
            else:
                LOGGER.debug(
                    'Ignoring _AddressResolver cancel request when not ACTIVE; '
                    '(%s:%s); state=%s', self._host, self._port, self._state)

    def _resolve(self):
        """Call `socket.getaddrinfo()` and return result via user's callback
        function on the given I/O loop

        """
        try:
            result = socket.getaddrinfo(host=self._host,
                                        port=self._port,
                                        family=self._family,
                                        type=self._socktype,
                                        proto=self._proto,
                                        flags=self._flags)
        except Exception as result:  # pylint: disable=W0703
            LOGGER.error('Address resoultion failed: %r', result)

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
