"""Utilities for implementing `async_interface.AbstractAsyncServices` for
pika connection adapters.

"""

import collections
import errno
import functools
import logging
import numbers
import os
import socket
import ssl

from pika.adapters import async_interface
import pika.compat


# "Try again" error codes for non-blocking socket I/O - send()/recv().
# NOTE: POSIX.1 allows either error to be returned for this case and doesn't require
# them to have the same value.
_TRY_IO_AGAIN_SOCK_ERROR_CODES = (errno.EAGAIN, errno.EWOULDBLOCK,)

# "Connection establishment pending" error codes for non-blocking socket
# connect() call.
# NOTE: EINPROGRESS for Posix and EWOULDBLOCK for Windows
_CONNECTION_IN_PROGRESS_SOCK_ERROR_CODES = (errno.EINPROGRESS,
                                            errno.EWOULDBLOCK,)

_LOGGER = logging.getLogger(__name__)


def check_callback_arg(callback, name):
    """Raise TypeError if callback is not callable

    :param callback: callback to check
    :param name: Name to include in exception text
    :raises TypeError:

    """
    if not callable(callback):
        raise TypeError(
            '{} must be callable, but got {!r}'.format(name, callback))


def check_fd_arg(fd):
    """Raise TypeError if file descriptor is not an integer

    :param fd: file descriptor
    :raises TypeError:

    """
    if not isinstance(fd, numbers.Integral):
        raise TypeError(
            'Paramter must be a file descriptor, but got {!r}'.format(fd))


def _retry_on_sigint(func):
    """Function decorator for retrying on SIGINT.

    """
    @functools.wraps(func)
    def retry_sigint_wrap(*args, **kwargs):
        """Wrapper for decorated function"""
        while True:
            try:
                return func(*args, **kwargs)
            except pika.compat.SOCKET_ERROR as error:
                if error.errno == errno.EINTR:
                    continue
                else:
                    raise

    return retry_sigint_wrap


class AsyncSocketConnectionMixin(object):
    """Implements
    `async_interface.AbstractAsyncServices.connect_socket()` on top of the
    basic `async_interface.AbstractAsyncServices` services.

    """

    def connect_socket(self, sock, resolved_addr, on_done):
        """Perform the equivalent of `socket.connect()` on a previously-resolved
        address asynchronously.

        IMPLEMENTATION NOTE: Pika's connection logic resolves the addresses
            prior to making socket connections, so we don't need to burden the
            implementations of this method with the extra logic of asynchronous
            DNS resolution. Implementations can use `socket.inet_pton()` to
            verify the address.

        :param async_interface.AbstractAsyncServices self:
        :param socket.socket sock: non-blocking socket that needs to be
            connected via `socket.socket.connect()`
        :param tuple resolved_addr: resolved destination address/port two-tuple
            as per `socket.socket.connect()`, except that the first element must
            be an actual IP address that's consistent with the given socket's
            address family.
        :param callable on_done: user callback that takes None upon successful
            completion or exception upon failure (check for `BaseException`) as
            its only arg. It will not be called if the operation was cancelled.

        :rtype: AbstractAsyncReference
        :raises ValueError: if host portion of `resolved_addr` is not an IP
            address or is inconsistent with the socket's address family as
            validated via `socket.inet_pton()`

        """
        return _AsyncSocketConnector(async_services=self,
                                     sock=sock,
                                     resolved_addr=resolved_addr,
                                     on_done=on_done).start()


class AsyncStreamingConnectionMixin(object):
    """Implements
    `async_interface.AbstractAsyncServices.create_streaming_connection()` on
    top of the basic `async_interface.AbstractAsyncServices` services.

    """
    def create_streaming_connection(self,
                                    protocol_factory,
                                    sock,
                                    on_done,
                                    ssl_context=None,
                                    server_hostname=None):
        """Perform SSL session establishment, if requested, on the already-
        connected socket and link the streaming transport/protocol pair.

        NOTE: This method takes ownership of the socket.

        :param async_interface.AbstractAsyncServices self:
        :param callable protocol_factory: returns an instance with the
            `async_interface.AbstractStreamProtocol` interface. The protocol's
            `connection_made(transport)` method will be called to link it to
            the transport after remaining connection activity (e.g., SSL session
            establishment), if any, is completed successfully.
        :param socket.socket sock: Already-connected, non-blocking
            `socket.SOCK_STREAM` socket to be used by the transport. We take
            ownership of this socket.
        :param callable on_done: User callback
            `on_done(BaseException | (transport, protocol))` to be notified when
            the asynchronous operation completes. An exception arg indicates
            failure; otherwise the two-tuple will contain the linked
            transport/protocol pair, with the
            `async_interface.AbstractStreamTransport` and
            `async_interface.AbstractStreamProtocol` respectively.
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
            return _AsyncStreamConnector(
                async_services=self,
                protocol_factory=protocol_factory,
                sock=sock,
                ssl_context=ssl_context,
                server_hostname=server_hostname,
                on_done=on_done).start()
        except Exception as error:
            _LOGGER.error('create_streaming_connection(%s) failed: %r',
                          sock, error)
            # Close the socket since this function takes ownership
            try:
                sock.close()
            except Exception as error:  # pylint: disable=W0703
                # We log and suppress the exception from sock.close() so that
                # the original error from _AsyncStreamConnector constructor will
                # percolate
                _LOGGER.error('%s.close() failed: %r', sock, error)

            raise


class _AsyncServiceAsyncHandle(async_interface.AbstractAsyncReference):
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


class _AsyncSocketConnector(object):
    """Connects the given non-blocking socket asynchronously using the given
    `async_interface.AbstractAsyncServices` instance. Used for implementing
    `async_interface.AbstractAsyncServices.connect_socket()`.
    """

    _STATE_NOT_STARTED = 0  # start() not called yet
    _STATE_ACTIVE = 1  # workflow started
    _STATE_CANCELED = 2  # workflow aborted by user's cancel() call
    _STATE_COMPLETED = 3  # workflow completed: succeeded or failed

    def __init__(self, async_services, sock, resolved_addr, on_done):
        """
        :param async_interface.AbstractAsyncServices async_services:
        :param socket.socket sock: non-blocking socket that needs to be
            connected via `socket.socket.connect()`
        :param tuple resolved_addr: resolved destination address/port two-tuple
            which is compatible with the given's socket's address family
        :param callable on_done: user callback that takes None upon successful
            completion or exception upon error (check for `BaseException`) as
            its only arg. It will not be called if the operation was cancelled.
        :raises ValueError: if host portion of `resolved_addr` is not an IP
            address or is inconsistent with the socket's address family as
            validated via `socket.inet_pton()`
        """
        check_callback_arg(on_done, 'on_done')

        try:
            socket.inet_pton(sock.family, resolved_addr[0])
        except Exception as error:
            msg = ('Invalid or unresolved IP address '
                   '{!r} for socket {}: {!r}').format(resolved_addr, sock, error)
            _LOGGER.error(msg)
            raise ValueError(msg)

        self._async = async_services
        self._sock = sock
        self._addr = resolved_addr
        self._on_done = on_done
        self._state = self._STATE_NOT_STARTED
        self._watching_socket_events = False

    def _cleanup(self):
        """Remove socket watcher, if any

        """
        if self._watching_socket_events:
            self._async.remove_writer(self._sock.fileno())

    def start(self):
        """Start asynchronous connection establishment.

        :rtype: async_interface.AbstractAsyncReference
        """
        assert self._state == self._STATE_NOT_STARTED, self._state

        self._state = self._STATE_ACTIVE

        # Continue the rest of the operation on the I/O loop to avoid calling
        # user's completion callback from the scope of user's call
        self._async.add_callback_threadsafe(self._start_safe)

        return _AsyncServiceAsyncHandle(self)

    def cancel(self):
        """Cancel pending connection request without calling user's completion
        callback.

        :returns: False if was already done or cancelled; True otherwise
        :rtype: bool

        """
        if self._state == self._STATE_ACTIVE:
            self._state = self._STATE_CANCELED
            _LOGGER.debug('User canceled connection request for %s to %s',
                          self._sock, self._addr)
            self._cleanup()
            return True

        _LOGGER.debug('_AsyncSocketConnector cancel requested when not ACTIVE: '
                      'state=%s; %s', self._state, self._sock)
        return False

    def _report_completion(self, result):
        """Advance to COMPLETED state, remove socket watcher, and invoke user's
        completion callback.

        :param BaseException | None result: value to pass in user's callback

        """
        assert isinstance(result, (BaseException, type(None))), result
        assert self._state == self._STATE_ACTIVE, self._state

        self._state = self._STATE_COMPLETED
        self._cleanup()

        self._on_done(result)

    def _start_safe(self):
        """Called as callback from I/O loop to kick-start the workflow, so it's
        safe to call user's completion callback from here, if needed

        """
        if self._state != self._STATE_ACTIVE:
            # Must have been canceled by user before we were called
            _LOGGER.debug('Abandoning sock=%s connection establishment to %s '
                          'due to inactive state=%s',
                          self._sock, self._addr, self._state)
            return

        try:
            self._sock.connect(self._addr)
        except (Exception, pika.compat.SOCKET_ERROR) as error:  # pylint: disable=W0703
            if (isinstance(error, pika.compat.SOCKET_ERROR) and
                    error.errno in _CONNECTION_IN_PROGRESS_SOCK_ERROR_CODES):
                # Connection establishment is pending
                pass
            else:
                _LOGGER.error('%s.connect(%s) failed: %r',
                              self._sock, self._addr, error)
                self._report_completion(error)
                return

        # Get notified when the socket becomes writable
        try:
            self._async.set_writer(self._sock.fileno(), self._on_writable)
        except Exception as error:  # pylint: disable=W0703
            _LOGGER.error('async.set_writer(%s) failed: %r', self._sock, error)
            self._report_completion(error)
            return
        else:
            self._watching_socket_events = True
            _LOGGER.debug('Connection-establishment is in progress for %s.',
                          self._sock)

    def _on_writable(self):
        """Called when socket connects or fails to. Check for predicament and
        invoke user's completion callback.

        """
        if self._state != self._STATE_ACTIVE:
            # This should never happen since we remove the watcher upon
            # `cancel()`
            _LOGGER.error(
                'Socket connection-establishment event watcher '
                'called in inactive state (ignoring): %s; state=%s',
                self._sock, self._state)
            return

        # The moment of truth...
        error_code = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if not error_code:
            _LOGGER.info('Socket connected: %s', self._sock)
            result = None
        else:
            error_msg = os.strerror(error_code)
            _LOGGER.error('Socket failed to connect: %s; error=%s (%s)',
                          self._sock, error_code, error_msg)
            result = pika.compat.SOCKET_ERROR(error_code, error_msg)

        self._report_completion(result)


class _AsyncStreamConnector(object):
    """Performs asynchronous SSL session establishment, if requested, on the
    already-connected socket and links the streaming transport to protocol.
    Used for implementing
    `async_interface.AbstractAsyncServices.create_streaming_connection()`.

    """
    _STATE_NOT_STARTED = 0  # start() not called yet
    _STATE_ACTIVE = 1  # start() called and kicked off the workflow
    _STATE_CANCELED = 2  # workflow terminated by cancel() request
    _STATE_COMPLETED = 3 # workflow terminated by success or failure

    def __init__(self, async_services,
                 protocol_factory,
                 sock,
                 ssl_context,
                 server_hostname,
                 on_done):
        """
        NOTE: We take ownership of the given socket upon successful completion
        of the constructor.

        See `AbstractAsyncServices.create_streaming_connection()` for detailed
        documentation of the corresponding args.

        :param async_interface.AbstractAsyncServices async_services:
        :param callable protocol_factory:
        :param socket.socket sock:
        :param ssl.SSLContext | None ssl_context:
        :param str | None server_hostname:
        :param callable on_done:

        """
        check_callback_arg(protocol_factory, 'protocol_factory')
        check_callback_arg(on_done, 'on_done')

        if not isinstance(ssl_context, (type(None), ssl.SSLContext)):
            raise ValueError('Expected ssl_context=None | ssl.SSLContext, but '
                             'got {!r}'.format(ssl_context))

        if server_hostname is not None and ssl_context is None:
            raise ValueError('Non-None server_hostname must not be passed '
                             'without ssl context')

        # Check that the socket connection establishment had completed in order
        # to avoid stalling while waiting for the socket to become readable
        # and/or writable.
        try:
            sock.getpeername()
        except Exception as error:
            raise ValueError(
                'Expected connected socket, but `getpeername()` failed: '
                'error={!r}; {}; '.format(error, sock))

        self._async = async_services
        self._protocol_factory = protocol_factory
        self._sock = sock
        self._ssl_context = ssl_context
        self._server_hostname = server_hostname
        self._on_done = on_done

        self._state = self._STATE_NOT_STARTED
        self._watching_socket = False

    def _cleanup(self, close):
        """Cancel pending async operations, if any

        :param bool close: close the socket if true
        """
        if self._watching_socket:
            self._async.remove_reader(self._sock.fileno())
            self._async.remove_writer(self._sock.fileno())

        try:
            if close:
                try:
                    self._sock.close()
                except Exception as error:  # pylint: disable=W0703
                    _LOGGER.error('_sock.close() failed: error=%r; %s',
                                  error, self._sock)
                    raise
        finally:
            self._sock = None
            self._async = None
            self._protocol_factory = None
            self._ssl_context = None
            self._server_hostname = None
            self._on_done = None

    def start(self):
        """Kick off the workflow

        :rtype: async_interface.AbstractAsyncReference
        """
        assert self._state == self._STATE_NOT_STARTED, self._state

        self._state = self._STATE_ACTIVE

        # Request callback from I/O loop to start processing so that we don't
        # end up making callbacks from the caller's scope
        self._async.add_callback_threadsafe(self._start_safe)

        return _AsyncServiceAsyncHandle(self)

    def cancel(self):
        """Cancel pending connection request without calling user's completion
        callback.

        :returns: False if was already done or cancelled; True otherwise
        :rtype: bool

        """
        if self._state == self._STATE_ACTIVE:
            self._state = self._STATE_CANCELED
            _LOGGER.debug('User canceled streaming linkup for %s', self._sock)
            # Close the socket, since we took ownership
            self._cleanup(close=True)
            return True

        _LOGGER.debug('_AsyncStreamConnector cancel requested when not ACTIVE: '
                      'state=%s; %s', self._state, self._sock)
        return False

    def _report_completion(self, result):
        """Advance to COMPLETED state, cancel async operation(s), and invoke
        user's completion callback.

        :param BaseException | tuple result: value to pass in user's callback.
            `tuple(transport, protocol)` on success, exception on error

        """
        assert isinstance(result, (BaseException, tuple)), result
        assert self._state == self._STATE_ACTIVE, self._state

        self._state = self._STATE_COMPLETED

        # Notify user
        try:
            self._on_done(result)
        finally:
            # NOTE: Close the socket on error, since we took ownership of it
            self._cleanup(close=isinstance(result, BaseException))

    def _start_safe(self):
        """Called as callback from I/O loop to kick-start the workflow, so it's
        safe to call user's completion callback from here if needed

        """
        if self._state != self._STATE_ACTIVE:
            # Must have been canceled by user before we were called
            _LOGGER.debug('Abandoning streaming linkup due to inactive state '
                          'transition; state=%s; %s; .',
                          self._state, self._sock)
            return

        # Link up protocol and transport if this is a plaintext linkup;
        # otherwise kick-off SSL workflow first
        if self._ssl_context is None:
            self._linkup()
        else:
            _LOGGER.debug('Starting SSL handshake on %s', self._sock)

            # Wrap our plain socket in ssl socket
            try:
                self._sock = self._ssl_context.wrap_socket(
                    self._sock,
                    server_side=False,
                    do_handshake_on_connect=False,
                    suppress_ragged_eofs=False,  # False = error on incoming EOF
                    server_hostname=self._server_hostname)
            except Exception as error:  # pylint: disable=W0703
                _LOGGER.error('SSL wrap_socket(%s) failed: %r', self._sock,
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
                _LOGGER.error('protocol_factory() failed: error=%r; %s',
                              error, self._sock)
                raise

            if self._ssl_context is None:
                # Create plaintext streaming transport
                try:
                    transport = _AsyncPlaintextTransport(self._sock,
                                                         protocol,
                                                         self._async)
                except Exception as error:
                    _LOGGER.error('PlainTransport() failed: error=%r; %s',
                                  error, self._sock)
                    raise
            else:
                # Create SSL streaming transport
                try:
                    transport = _AsyncSSLTransport(self._sock,
                                                   protocol,
                                                   self._async)
                except Exception as error:
                    _LOGGER.error('SSLTransport() failed: error=%r; %s',
                                  error, self._sock)
                    raise

            # Acquaint protocol with its transport
            try:
                protocol.connection_made(transport)
            except Exception as error:
                _LOGGER.error(
                    'protocol.connection_made(%r) failed: error=%r; %s',
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
                    _LOGGER.error('transport.abort() failed: error=%r; %s',
                                  error, self._sock)
        else:
            result = (transport, protocol)

        self._report_completion(result)

    def _do_ssl_handshake(self):
        """Perform asynchronous SSL handshake on the already wrapped socket

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Abandoning streaming linkup due to inactive state '
                          'transition; state=%s; %s; .',
                          self._state, self._sock)
            return

        try:
            try:
                self._sock.do_handshake()
            except ssl.SSLError as error:
                if error.errno == ssl.SSL_ERROR_WANT_READ:
                    _LOGGER.debug('SSL handshake wants read; %s.', self._sock)
                    self._watching_socket = True
                    self._async.set_reader(self._sock.fileno(),
                                           self._do_ssl_handshake())
                    self._async.remove_writer(self._sock.fileno())
                elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                    _LOGGER.debug('SSL handshake wants write. %s', self._sock)
                    self._watching_socket = True
                    self._async.set_writer(self._sock.fileno(),
                                           self._do_ssl_handshake())
                    self._async.remove_reader(self._sock.fileno())
                else:
                    raise
            else:
                _LOGGER.info('SSL handshake completed successfully: %s',
                             self._sock)

                # Suspend I/O and link up transport with protocol
                self._async.remove_reader(self._sock.fileno())
                self._async.remove_writer(self._sock.fileno())
                # So that our `_cleanup()` won't interfere with the transport's
                # socket watcher configuration.
                self._watching_socket = False

                self._linkup()
        except Exception as error:  # pylint: disable=W0703
            _LOGGER.error('SSL do_handshake failed: error=%r; %s',
                          error, self._sock)
            self._report_completion(error)
            return


class _AsyncTransportBase(  # pylint: disable=W0223
        async_interface.AbstractStreamTransport):
    """Base class for `_AsyncPlaintextTransport` and `_AsyncSSLTransport`.

    """

    _STATE_ACTIVE = 1
    _STATE_FAILED = 2  # connection failed
    _STATE_ABORTED_BY_USER = 3  # cancel() called
    _STATE_COMPLETED = 4  # done with connection

    _MAX_RECV_BYTES = 4096  # per socket.recv() documentation recommendation

    # Max per consume call to prevent event starvation
    _MAX_CONSUME_BYTES = 1024 * 100

    class RxEndOfFile(OSError):
        """We raise this when EOF (empty read) is detected on input

        """
        def __init__(self):
            super(_AsyncTransportBase.RxEndOfFile, self).__init__(
                -1, 'End of input stream (EOF)')

    def __init__(self, sock, protocol, async_services):
        """

        :param socket.socket | ssl.SSLSocket sock: connected socket
        :param async_interface.AbstractStreamProtocol protocol: corresponding
            protocol in this transport/protocol pairing; the protocol already
            had its `connection_made()` method called.
        :param async_interface.AbstractAsyncServices async_services:

        """
        self._sock = sock
        self._protocol = protocol
        self._async = async_services

        self._state = self._STATE_ACTIVE
        self._tx_buffers = collections.deque()
        self._tx_buffered_byte_count = 0

    def drop(self):
        """Close connection abruptly and synchronously without flushing pending
        data and without invoking the corresponding protocol's
        `connection_lost()` method.

        NOTE: This differs from asyncio transport's `abort()` and `close()`
        methods which close the stream asynchronously and eventually call the
        protocol's `connection_lost()` method. The abrupt synchronous behavior
        of the `drop()` method suits Pika's current connection-management logic
        better.

        :raises Exception: Exception-based exception on error
        """
        _LOGGER.info('Dropping transport connection immediately: state=%s; %s',
                     self._state, self._sock)

        self._deactivate()
        self._close_and_finalize()

        self._state = self._STATE_COMPLETED

    def get_protocol(self):
        """Return the protocol linked to this transport.

        :rtype: async_interface.AbstractStreamProtocol
        """
        return self._protocol

    def get_write_buffer_size(self):
        """
        :return: Current size of output data buffered by the transport
        """
        return self._tx_buffered_byte_count

    def _buffer_tx_data(self, data):
        """Buffer the given data until it can be sent asynchronously.

        :param bytes data:
        :raises ValueError: if called with empty data

        """
        if not data:
            _LOGGER.error('write() called with empty data: state=%s; %s',
                          self._state, self._sock)
            raise ValueError('write() called with empty data {!r}'.format(data))

        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Ignoring write() called during inactive state: '
                          'state=%s; %s', self._state, self._sock)
            return

        self._tx_buffers.append(data)
        self._tx_buffered_byte_count += len(data)

    def _consume(self):
        """Utility method for use by subclasses to ingest data from socket and
        dispatch it to protocol's `data_received()` method socket-specific
        "try again" exception, per-event data consumption limit is reached,
        transport becomes inactive, or a fatal failure.

        Consumes up to `self._MAX_CONSUME_BYTES` to prevent event starvation or
        until state becomes inactive (e.g., `protocol.data_received()` callback
        aborts the transport)

        :raises: Whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR
        :raises: Whatever the `protocol.data_received()` callback raises
        :raises _AsyncTransportBase.RxEndOfFile: upon shutdown of input stream

        """
        bytes_consumed = 0

        while (self._state == self._STATE_ACTIVE and
               bytes_consumed < self._MAX_CONSUME_BYTES):
            data = self._sigint_safe_recv(self._sock, self._MAX_RECV_BYTES)
            bytes_consumed += len(data)

            # Empty data, should disconnect
            if not data:
                _LOGGER.error('Socket EOF; %s', self._sock)
                raise self.RxEndOfFile()

            # Pass the data to the protocol
            try:
                self._protocol.data_received(data)
            except Exception as error:
                _LOGGER.error('protocol.data_received() failed: error=%r; %s',
                              error, self._sock)
                raise

    def _produce(self):
        """Utility method for use by subclasses to emit data from tx_buffers.
        This method sends chunks from `tx_buffers` until all chunks are
        exhausted or sending is interrupted by an exception. Maintains integrity
        of `self.tx_buffers`.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR

        """
        while self._tx_buffers:
            num_bytes_sent = self._sigint_safe_send(self._sock,
                                                    self._tx_buffers[0])

            chunk = self._tx_buffers.popleft()
            if num_bytes_sent < len(chunk):
                _LOGGER.debug('Partial send, requeing remaining data; %s of %s',
                              num_bytes_sent, len(chunk))
                self._tx_buffers.appendleft(chunk[num_bytes_sent:])

            self._tx_buffered_byte_count -= num_bytes_sent
            assert self._tx_buffered_byte_count >= 0, \
                self._tx_buffered_byte_count

    @staticmethod
    @_retry_on_sigint
    def _sigint_safe_recv(sock, max_bytes):
        """Receive data from socket, retrying on SIGINT.

        :param sock: stream or SSL socket
        :param max_bytes: maximum number of bytes to receive
        :return: received data or empty bytes uppon end of file
        :raises: whatever the corresponding `sock.recv()` raises except socket
                 error with errno.EINTR

        """
        return sock.recv(max_bytes)

    @staticmethod
    @_retry_on_sigint
    def _sigint_safe_send(sock, data):
        """Send data to socket, retrying on SIGINT.

        :param sock: stream or SSL socket
        :param data: data bytes to send
        :return: number of bytes actually sent
        :raises: whatever the corresponding `sock.send()` raises except socket
                 error with errno.EINTR

        """
        return sock.send(data)

    def _deactivate(self):
        """Unregister the transport from I/O events

        """
        if self._state == self._STATE_ACTIVE:
            _LOGGER.info('Deactivating transport: state=%s; %s',
                         self._state, self._sock)
            self._async.remove_reader(self._sock.fileno())
            self._async.remove_writer(self._sock.fileno())
            self._tx_buffers.clear()

    def _close_and_finalize(self):
        """Close the transport's socket and unlink the transport it from
        references to other assets (protocol, etc.)

        """
        if self._state != self._STATE_COMPLETED:
            _LOGGER.info('Closing transport socket and unlinking: state=%s; %s',
                         self._state, self._sock)
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except pika.compat.SOCKET_ERROR:
                pass
            self._sock.close()
            self._sock = None
            self._protocol = None
            self._async = None
            self._state = self._STATE_COMPLETED

    def _initiate_abort(self, error):
        """Initiate asynchronous abort of the transport that concludes with a
        call to the protocol's `connection_lost()` method. No flushing of
        output buffers will take place.

        :param BaseException | None error: None if being canceled by user,
            including via falsie return value from protocol.eof_received;
            otherwise the exception corresponding to the the failed connection.
        """
        _LOGGER.info(
            'Initiating abrupt asynchronous transport shutdown: state=%s; '
            'error=%r; %s', self._state, error, self._sock)

        assert self._state != self._STATE_COMPLETED, self._state

        if self._state == self._STATE_COMPLETED:
            return

        self._deactivate()

        # Update state
        if error is None:
            # Being aborted by user

            if self._state == self._STATE_ABORTED_BY_USER:
                # Abort by user already pending
                return

            # Notification priority is given to user-initiated abort over
            # failed connection
            self._state = self._STATE_ABORTED_BY_USER
        else:
            # Connection failed

            if self._state != self._STATE_ACTIVE:
                assert self._state == self._STATE_ABORTED_BY_USER, self._state
                return

            self._state = self._STATE_FAILED

        # Schedule callback from I/O loop to avoid potential reentry into user
        # code
        self._async.add_callback_threadsafe(
            functools.partial(
                self._connection_lost_notify_safe,
                error))

    def _connection_lost_notify_safe(self, error):
        """Handle aborting of transport either due to socket error or user-
        initiated `abort()` call. Must be called from an I/O loop callback owned
        by us in order to avoid reentry into user code from user's API call into
        the transport.

        :param BaseException | None error: None if being canceled by user;
            otherwise the exception corresponding to the the failed connection.
        """
        _LOGGER.debug('Concluding transport shutdown: state=%s; error=%r',
                      self._state, errno)

        if self._state == self._STATE_COMPLETED:
            return

        if error is not None and self._state != self._STATE_FAILED:
            # Priority is given to user-initiated abort notification
            assert self._state == self._STATE_ABORTED_BY_USER, self._state
            return

        # Inform protocol
        try:
            self._protocol.connection_lost(error)
        except Exception as exc:  # pylint: disable=W0703
            _LOGGER.error('protocol.connection_lost(%r) failed: exc=%r; %s',
                          error, exc, self._sock)
        finally:
            self._close_and_finalize()


class _AsyncPlaintextTransport(_AsyncTransportBase):
    """Implementation of `async_interface.AbstractStreamTransport` for a
    plaintext connection.

    """

    def __init__(self, sock, protocol, async_services):
        """

        :param socket.socket sock: non-blocking connected socket
        :param async_interface.AbstractStreamProtocol protocol: corresponding
            protocol in this transport/protocol pairing; the protocol already
            had its `connection_made()` method called.
        :param async_interface.AbstractAsyncServices async_services:

        """
        super(_AsyncPlaintextTransport, self).__init__(sock,
                                                       protocol,
                                                       async_services)

        # Request to be notified of incoming data; we'll watch for writability
        # only when our write buffer is non-empty
        self._async.set_reader(self._sock.fileno(), self._on_socket_readable)

    def write(self, data):
        """Buffer the given data until it can be sent asynchronously.

        :param bytes data:
        :raises ValueError: if called with empty data

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Ignoring write() called during inactive state: '
                          'state=%s; %s', self._state, self._sock)
            return

        if not self.get_write_buffer_size():
            self._async.set_writer(self._sock.fileno(),
                                   self._on_socket_writable)
            _LOGGER.debug('Turned on writability watcher: %s', self._sock)

        self._buffer_tx_data(data)

    def _on_socket_readable(self):
        """Ingest data from socket and dispatch it to protocol until exception
        occurs (typically EAGAIN or EWOULDBLOCK), per-event data consumption
        limit is reached, transport becomes inactive, or failure.

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Ignoring readability notification due to inactive '
                          'state: state=%s; %s', self._state, self._sock)
            return

        try:
            self._consume()
        except self.RxEndOfFile:
            try:
                keep_open = self._protocol.eof_received()
            except Exception as error:  # pylint: disable=W0703
                _LOGGER.error('protocol.eof_received() failed: error=%r; %s',
                              error, self._sock)
                self._initiate_abort(error)
            else:
                if keep_open:
                    _LOGGER.info(
                        'protocol.eof_received() elected to keep open: %s',
                        self._sock)
                    self._async.remove_reader(self._sock.fileno())
                else:
                    _LOGGER.info('protocol.eof_received() elected to close: %s',
                                 self._sock)
                    self._initiate_abort(None)
        except (Exception, pika.compat.SOCKET_ERROR) as error:  # pylint: disable=W0703
            if (isinstance(error, pika.compat.SOCKET_ERROR) and
                    error.errno in _TRY_IO_AGAIN_SOCK_ERROR_CODES):
                _LOGGER.debug('Recv would block on %s', self._sock)
            else:
                _LOGGER.error('Consume failed, aborting connection: %r; %s',
                              error, self._sock)
                self._initiate_abort(error)
        else:
            if self._state != self._STATE_ACTIVE:
                # Most likely our protocol's `data_received()` aborted the
                # transport
                _LOGGER.debug('Leaving Plaintext consumer due to inactive '
                              'state: state=%s; %s',
                              self._state, self._sock)
            else:
                #
                pass

    def _on_socket_writable(self):
        """Handle writable socket notification

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Ignoring writability notification due to inactive '
                          'state: state=%s; %s', self._state, self._sock)
            return

        # We shouldn't be getting called with empty tx buffers
        assert self._tx_buffers

        try:
            # Transmit buffered data to remote socket
            self._produce()
        except (Exception, pika.compat.SOCKET_ERROR) as error:  # pylint: disable=W0703
            if (isinstance(error, pika.compat.SOCKET_ERROR) and
                    error.errno in _TRY_IO_AGAIN_SOCK_ERROR_CODES):
                _LOGGER.debug('Send would block on %s', self._sock)
            else:
                _LOGGER.error('Consume failed, aborting connection: %r; %s',
                              error, self._sock)
                self._initiate_abort(error)
        else:
            if not self._tx_buffers:
                self._async.remove_writer(self._sock.fileno())
                _LOGGER.debug('Turned off writability watcher: %s', self._sock)


class _AsyncSSLTransport(_AsyncTransportBase):
    """Implementation of `async_interface.AbstractStreamTransport` for an SSL
    connection.

    """

    def __init__(self, sock, protocol, async_services):
        """

        :param ssl.SSLSocket sock: non-blocking connected socket
        :param async_interface.AbstractStreamProtocol protocol: corresponding
            protocol in this transport/protocol pairing; the protocol already
            had its `connection_made()` method called.
        :param async_interface.AbstractAsyncServices async_services:

        """
        super(_AsyncSSLTransport, self).__init__(sock, protocol, async_services)

        self._ssl_readable_action = self._consume
        self._ssl_writable_action = None

        # Bootstrap consumer; we'll take care of producer once data is buffered
        self._async.set_reader(self._sock.fileno(), self._on_socket_readable)
        # Try reading asap just in case read-ahead caused some
        self._async.add_callback_threadsafe(self._on_socket_readable)

    def write(self, data):
        """Buffer the given data until it can be sent asynchronously.

        :param bytes data:
        :raises ValueError: if called with empty data

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Ignoring write() called during inactive state: '
                          'state=%s; %s', self._state, self._sock)
            return

        tx_buffer_was_empty = self.get_write_buffer_size() == 0

        self._buffer_tx_data(data)

        if tx_buffer_was_empty and self._ssl_writable_action is None:
            self._ssl_writable_action = self._produce
            self._async.set_writer(self._sock.fileno(),
                                   self._on_socket_writable)
            _LOGGER.debug('Turned on writability watcher: %s', self._sock)

    def _on_socket_readable(self):
        """Handle readable socket indication

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Ignoring readability notification due to inactive '
                          'state: state=%s; %s', self._state, self._sock)
            return

        if self._ssl_readable_action:
            try:
                self._ssl_readable_action()
            except Exception as error:  # pylint: disable=W0703
                self._initiate_abort(error)
        else:
            _LOGGER.debug('SSL readable action was suppressed: '
                          'ssl_writable_action=%r; %s',
                          self._ssl_writable_action, self._sock)

    def _on_socket_writable(self):
        """Handle writable socket notification

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug('Ignoring writability notification due to inactive '
                          'state: state=%s; %s', self._state, self._sock)
            return

        if self._ssl_writable_action:
            try:
                self._ssl_writable_action()
            except Exception as error:  # pylint: disable=W0703
                self._initiate_abort(error)
        else:
            _LOGGER.debug('SSL writable action was suppressed: '
                          'ssl_readable_action=%r; %s',
                          self._ssl_readable_action, self._sock)

    def _consume(self):
        """[override] Ingest data from socket and dispatch it to protocol until
        exception occurs (typically ssl.SSLError with
        SSL_ERROR_WANT_READ/WRITE), per-event data consumption limit is reached,
        transport becomes inactive, or failure.

        Update consumer/producer registration.

        :raises Exception: error that signals that connection needs to be
            aborted
        """
        next_consume_on_readable = True

        try:
            super(_AsyncSSLTransport, self)._consume()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                _LOGGER.debug('SSL ingester wants read: %s', self._sock)
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                # Looks like SSL re-negotiation
                _LOGGER.debug('SSL ingester wants write: %s', self._sock)
                next_consume_on_readable = False
            else:
                _LOGGER.error('SSL consumer failed: %r; %s', error, self._sock)
                raise  # let outer catch block abort the transport
        else:
            if self._state != self._STATE_ACTIVE:
                # Most likely our protocol's `data_received()` aborted the
                # transport
                _LOGGER.debug('Leaving SSL consumer due to inactive '
                              'state: state=%s; %s',
                              self._state, self._sock)
                return

            # Consumer exited without exception; there may still be more,
            # possibly unprocessed, data records in SSL input buffers that
            # can be read without waiting for socket to become readable.

            # In case buffered input SSL data records still remain
            self._async.add_callback_threadsafe(self._on_socket_readable)

        # Update consumer registration
        if next_consume_on_readable:
            if not self._ssl_readable_action:
                self._async.set_reader(self._sock.fileno(),
                                       self._on_socket_readable)
            self._ssl_readable_action = self._consume

            if self._ssl_writable_action is self._consume:
                self._async.remove_writer(self._sock.fileno())
                self._ssl_writable_action = None
        else:
            # WANT_WRITE
            if not self._ssl_writable_action:
                self._async.set_writer(self._sock.fileno(),
                                       self._on_socket_writable)
            self._ssl_writable_action = self._consume

            if self._ssl_readable_action:
                self._async.remove_reader(self._sock.fileno())
                self._ssl_readable_action = None

        # Update producer registration
        if self._tx_buffers and not self._ssl_writable_action:
            self._ssl_writable_action = self._produce
            self._async.set_writer(self._sock.fileno(),
                                   self._on_socket_writable)

    def _produce(self):
        """[override] Emit data from tx_buffers all chunks are exhausted or
        sending is interrupted by an exception (typically ssl.SSLError with
        SSL_ERROR_WANT_READ/WRITE).

        Update consumer/producer registration.

        :raises Exception: error that signals that connection needs to be
            aborted

        """
        next_produce_on_writable = None  # None means no need to produce

        try:
            super(_AsyncSSLTransport, self)._produce()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                # Looks like SSL re-negotiation
                _LOGGER.debug('SSL emitter wants read: %s', self._sock)
                next_produce_on_writable = False
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                _LOGGER.debug('SSL emitter wants write: %s', self._sock)
                next_produce_on_writable = True
            else:
                _LOGGER.error('SSL producer failed: %r; %s', error, self._sock)
                raise  # let outer catch block abort the transport
        else:
            # No exception, so everything must have been written to the socket
            assert not self._tx_buffers, len(self._tx_buffers)

        # Update producer registration
        if self._tx_buffers:
            assert next_produce_on_writable is not None

            if next_produce_on_writable:
                if not self._ssl_writable_action:
                    self._async.set_writer(self._sock.fileno(),
                                           self._on_socket_writable)
                self._ssl_writable_action = self._produce

                if self._ssl_readable_action is self._produce:
                    self._async.remove_reader(self._sock.fileno())
                    self._ssl_readable_action = None
            else:
                # WANT_READ
                if not self._ssl_readable_action:
                    self._async.set_reader(self._sock.fileno(),
                                           self._on_socket_readable)
                self._ssl_readable_action = self._produce

                if self._ssl_writable_action:
                    self._async.remove_writer(self._sock.fileno())
                    self._ssl_writable_action = None
        else:
            if self._ssl_readable_action is self._produce:
                self._async.remove_reader(self._sock.fileno())
                self._ssl_readable_action = None
                assert self._ssl_writable_action is not self._produce
            else:
                assert self._ssl_writable_action is self._produce
                self._ssl_writable_action = None
                self._async.remove_writer(self._sock.fileno())

        # Update consumer registration
        if not self._ssl_readable_action:
            self._ssl_readable_action = self._consume
            self._async.set_reader(self._sock.fileno(),
                                   self._on_socket_readable)
            # In case input SSL data records have been buffered
            self._async.add_callback_threadsafe(self._on_socket_readable)
        elif self._sock.pending():
            self._async.add_callback_threadsafe(self._on_socket_readable)
