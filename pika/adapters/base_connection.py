"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
import abc
import errno
import functools
import logging
import os
import socket
import ssl

import pika.compat
import pika.tcp_socket_opts

from pika import connection
from pika.compat import SOCKET_ERROR
from pika.compat import SOL_TCP

LOGGER = logging.getLogger(__name__)


def _retry_on_sigint(func):
    """Function decorator for retrying on SIGINT.

    """

    @functools.wraps(func)
    def retry_sigint_wrap(*args, **kwargs):
        """Wrapper for decorated function"""
        while True:
            try:
                return func(*args, **kwargs)
            except SOCKET_ERROR as error:
                if error.errno == errno.EINTR:
                    continue
                else:
                    raise

    return retry_sigint_wrap


class _TransportABC(pika.compat.AbstractBase):
    """Abstract Base Class for plain and SSL socket transports"""

    TRY_AGAIN_SOCK_ERROR_CODES = (errno.EAGAIN, errno.EWOULDBLOCK,)

    class EndOfInput(EnvironmentError):
        """Exception to raise when `socket.recv()` returns empty data

        """
        def __init__(self):
            super(_TransportABC.EndOfInput, self).__init__(
                -1, 'End of input stream')


    def __init__(self, sock, on_connected=None):
        """

        :param sock: Non-blocking socket on which connection establishment has
                     been initiated.
        :param callable|None on_connected: if callable, will be called upon
            detection of successful connection establishment with the transport
            instance as its only arg and the transport will idle until
            kick-started via `begin_transporting()`. If None, the transport will
            idle until kick-started via `begin_transporting()`

        """
        if not (callable(on_connected) or on_connected is None):
            raise TypeError('on_connected must be callable or None.')

        self.sock = sock
        self.on_connected_callback = on_connected
        self.tx_buffers = None
        self.sink = None
        self.max_rx_bytes = None

    @abc.abstractmethod
    def poll_which_events(self):
        """Return socket events of interest for the next IOLoop poll.

        :return: a bitmask of PollEvents

        """
        pass

    def handle_events(self, events):
        """Handle the indicated IOLoop events on the socket by invoking the
        corresponding `_on_writable` and `_on_readable` pure virtual methods.

        :param int events: Indicated bitmask of socket events from PollEvents
        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions
        :raises _TransportABC.EndOfInput: upon shutdown of input stream
        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions

        """
        if events & PollEvents.WRITE:
            self._on_writable()

        if events & PollEvents.READ:
            self._on_readable()

    def begin_transporting(self, tx_buffers, rx_sink, max_rx_bytes):
        """Begin transporting data; if called before connection establishment
        completes, will facilitate connection establishment in due course

        :param deque tx_buffers: container for ougoing frame buffers
        :param callable rx_sink: a method that takes incoming data bytes as its
                                 only arg.
        :param int max_rx_bytes: byte limit for `sock.recv()` calls

        """
        self.on_connected_callback = None
        self.tx_buffers = tx_buffers
        self.sink = rx_sink
        self.max_rx_bytes = max_rx_bytes

    @abc.abstractmethod
    def _on_readable(self):
        """Called by `handle_events()` when readable socket is indicated.

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions
        :raises _TransportABC.EndOfInput: upon shutdown of input stream

        """
        pass

    @abc.abstractmethod
    def _on_writable(self):
        """Called by `handle_events()` when writable socket is indicated.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions

        """
        pass

    def _ingest(self):
        """Utility method for use by subclasses to ingest data from socket and
        dispatch it to sink.

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR
        :raises _TransportABC.EndOfInput: upon shutdown of input stream

        """
        data = self._sigint_safe_recv(self.sock, self.max_rx_bytes)

        # Empty data, should disconnect
        if not data:
            LOGGER.error('Socket EOF')
            raise self.EndOfInput()

        # Pass the data into our top level frame dispatching method
        self.sink(data)

    def _emit(self):
        """Utility method for use by subclasses to emit data from tx_buffers.
        This method sends frames from `tx_buffers` until all frames are
        exhausted or sending is interrupted by an exception. Maintains integrity
        of `self.tx_buffers`.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR

        """
        while self.tx_buffers:
            num_bytes_sent = self._sigint_safe_send(self.sock,
                                                    self.tx_buffers[0])

            frame = self.tx_buffers.popleft()
            if num_bytes_sent < len(frame):
                LOGGER.debug('Partial send, requeing remaining data.')
                self.tx_buffers.appendleft(frame[num_bytes_sent:])

    @staticmethod
    @_retry_on_sigint
    def _sigint_safe_recv(sock, max_bytes):
        """Receive data from socket, retrying on SIGINT.

        :param sock: stream or SSL socket
        :param max_bytes: maximum number of bytes to receive
        :return: received data or empty bytes on end of file
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
        :returns: number of bytes actually sent
        :raises: whatever the corresponding `sock.send()` raises except socket
                 error with errno.EINTR

        """
        return sock.send(data)


class _PlainTransport(_TransportABC):
    """Implementation of plaintext transport"""

    def poll_which_events(self):
        """Return socket events of interest for the next IOLoop poll.

        :return: a bitmask of PollEvents

        """
        if self.on_connected_callback is not None:
            events = PollEvents.WRITE
        elif self.tx_buffers is not None:
            if self.tx_buffers:
                events = PollEvents.READ | PollEvents.WRITE
            else:
                events = PollEvents.READ
        else:
            events = 0

        return events

    def _on_readable(self):
        """Ingest data from socket and dispatch it to sink.

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the "try again" exception
        :raises _TransportABC.EndOfInput: upon shutdown of input stream

        """
        assert self.on_connected_callback is None

        try:
            self._ingest()
        except SOCKET_ERROR as error:
            if error.errno in self.TRY_AGAIN_SOCK_ERROR_CODES:
                LOGGER.debug('Recv would block.')
                return
            else:
                raise

    def _on_writable(self):
        """Emit data from tx_buffers.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the "try again" exception
        :raises SOCKET_ERROR: if connection establishment failed

        """
        if self.on_connected_callback is not None:
            try:
                self._conclude_connection_establishment()
            finally:
                self.on_connected_callback = None
        else:
            try:
                self._emit()
            except SOCKET_ERROR as error:
                if error.errno in self.TRY_AGAIN_SOCK_ERROR_CODES:
                    LOGGER.debug('Send would block.')
                else:
                    raise

    def _conclude_connection_establishment(self):
        """Called once when `on_connected_callback` is available and socket
        becomes writable. If socket is not in error state, invokes
        `on_connected_callback()`, otherwise raises `SOCKET_ERROR`.

        :raises SOCKET_ERROR: if connection establishment failed
        """
        error_code = self.sock.getsockopt(socket.SOL_SOCKET,
                                          socket.SO_ERROR)
        if not error_code:
            LOGGER.debug(
                'Plaintext socket connection established; fileno=%r',
                self.sock.fileno())
            self.on_connected_callback(self)
        else:
            error_msg = os.strerror(error_code)
            LOGGER.error(
                'Plaintext socket connection failed: errno=%r; msg=%r; '
                'fileno=%r', error_code, error_msg, self.sock.fileno())
            raise SOCKET_ERROR(error_code, error_msg)


class _SSLTransport(_TransportABC):
    """Implementation of SSL transport."""

    def __init__(self, sock, on_connected=None):
        """
        :param sock: Non-blocking SSL socket on which plaintext connection
                     establishment has been initiated or already established.
        :param callable|None on_connected: if callable, will be called upon
            successful completion of SSL handshake with the transport instance
            as its only arg after which the transport will idle until
            kick-started via `begin_transporting()`. If None, the transport will
            idle until kick-started via `begin_transporting()`

        """
        super(_SSLTransport, self).__init__(sock, on_connected)

        # Bootstrap SSL handshake; completion of TCP connection handshake is
        # indicated by socket becoming writable
        self._readable_action = None
        self._writable_action = (None if on_connected is None
                                 else self._do_handshake)

        self._do_handshake_initiated = False

    def begin_transporting(self, tx_buffers, rx_sink, max_rx_bytes):
        """Begin transporting data; if called before connection establishment
        completes, will facilitate connection establishment in due course

        :param deque tx_buffers: container for ougoing frame buffers
        :param callable rx_sink: a method that takes incoming data bytes as its
                                 only arg.
        :param int max_rx_bytes: byte limit for `sock.recv()` calls

        """
        super(_SSLTransport, self).begin_transporting(tx_buffers,
                                                      rx_sink,
                                                      max_rx_bytes)
        if not self._do_handshake_initiated:
            self._readable_action = None
            self._writable_action = self._do_handshake
        else:
            # Bootstrap ingester in case some incoming data ended up in SSL
            # rx buffer; it will also configure our event action state
            self._ingest()

    def poll_which_events(self):
        """Return socket events of interest for the next IOLoop poll.

        :return: a bitmask of PollEvents

        """
        events = 0
        if self._readable_action is not None:
            events |= PollEvents.READ
        if self._writable_action is not None:
            events |= PollEvents.WRITE

        # The transport would stall without events
        assert events or self.tx_buffers is None

        return events

    def _on_readable(self):
        """Handles readability indication.

        :raises: whatever the corresponding `sock.recv()` raises except
                 the socket error with errno.EINTR and the ssl.SSLError
                 exception with SSL_ERROR_WANT_READ/WRITE
        :raises _TransportABC.EndOfInput: upon shutdown of input stream

        """
        if self._readable_action is not None:
            self._readable_action()
        else:
            LOGGER.debug('SSL readable handler is suppressed.')

    def _on_writable(self):
        """Handles writability indication.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the ssl.SSLError exception
                 with SSL_ERROR_WANT_READ/WRITE

        """
        if self._writable_action is not None:
            self._writable_action()
        else:
            LOGGER.debug('SSL writable handler is suppressed.')

    def _do_handshake(self):
        """Perform SSL handshake

        :raises ssl.SSLError: whatever the corresponding `sock.do_handshake()`
                              raises except the ssl.SSLError exception with
                              SSL_ERROR_WANT_READ/WRITE.

        """
        self._do_handshake_initiated = True
        try:
            self.sock.do_handshake()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                LOGGER.debug('SSL handshake wants read; fileno=%r.',
                             self.sock.fileno())
                self._readable_action = self._do_handshake
                self._writable_action = None
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                LOGGER.debug('SSL handshake wants write. fileno=%r',
                             self.sock.fileno())
                self._readable_action = None
                self._writable_action = self._do_handshake
            else:
                LOGGER.error('SSL handshake failed. fileno=%r',
                             self.sock.fileno())
                raise
        else:
            LOGGER.info('SSL handshake completed successfully. fileno=%r',
                        self.sock.fileno())
            if self.on_connected_callback is None:
                # Bootstrap ingester in case some incoming data ended up in SSL
                # rx buffer; it will also configure our event action state
                self._ingest()
            else:
                try:
                    self.on_connected_callback(self)
                finally:
                    self.on_connected_callback = None
                    self._readable_action = None
                    self._writable_action = None

    def _ingest(self):
        """Ingest data from socket and dispatch it to sink until an exception
        occurs (typically ssl.SSLError with SSL_ERROR_WANT_READ/WRITE).

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the ssl.SSLError exception
                 with SSL_ERROR_WANT_READ/WRITE
        :raises _TransportABC.EndOfInput: upon shutdown of input stream

        """
        try:
            # Empty out SSL rx buffer
            while True:
                super(_SSLTransport, self)._ingest()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                LOGGER.debug('SSL ingester wants read.')
                self._readable_action = self._ingest
                self._writable_action = self._emit if self.tx_buffers else None
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                LOGGER.debug('SSL ingester wants write.')
                self._readable_action = None
                self._writable_action = self._ingest
            else:
                raise

    def _emit(self):
        """Emit data from tx_buffers.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the ssl.SSLError exception
                 with SSL_ERROR_WANT_READ/WRITE

        """
        try:
            super(_SSLTransport, self)._emit()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                LOGGER.debug('SSL emitter wants read.')
                self._readable_action = self._emit
                self._writable_action = None
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                LOGGER.debug('SSL emitter wants write.')
                # TODO Do we need to re-boostrap ingester as in _do_handshake?
                self._readable_action = self._ingest
                self._writable_action = self._emit
            else:
                raise
        else:
            # TODO Do we need to re-boostrap ingester as in _do_handshake?
            self._readable_action = self._ingest
            self._writable_action = self._emit if self.tx_buffers else None


class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters"""

    # Use epoll's constants to keep life easy
    READ = 0x0001
    WRITE = 0x0004
    ERROR = 0x0008

    ERRORS_TO_IGNORE = [errno.EWOULDBLOCK, errno.EAGAIN, errno.EINTR]

    ERRORS_TO_ABORT = [
        errno.EBADF, errno.ECONNABORTED, errno.EPIPE, errno.ETIMEDOUT
    ]

    DO_HANDSHAKE = True

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 ioloop=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param object ioloop: IOLoop object to use
        :raises: RuntimeError
        :raises: ValueError

        """
        if parameters and not isinstance(parameters, connection.Parameters):
            raise ValueError(
                'Expected instance of Parameters, not %r' % parameters)

        self.base_events = self.READ | self.ERROR
        self.event_state = self.base_events
        self.ioloop = ioloop
        self.socket = None
        self.write_buffer = None
        super(BaseConnection,
              self).__init__(parameters, on_open_callback,
                             on_open_error_callback, on_close_callback)

    def __repr__(self):

        def get_socket_repr(sock):
            """Return socket info suitable for use in repr"""
            if sock is None:
                return None

            sockname = None
            peername = None
            try:
                sockname = sock.getsockname()
            except SOCKET_ERROR:
                # closed?
                pass
            else:
                try:
                    peername = sock.getpeername()
                except SOCKET_ERROR:
                    # not connected?
                    pass

            return '%s->%s' % (sockname, peername)

        return ('<%s %s socket=%s params=%s>' %
                (self.__class__.__name__,
                 self._STATE_NAMES[self.connection_state],
                 get_socket_repr(self.socket), self.params))

    def add_timeout(self, deadline, callback):
        """Add the callback to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout

        :param int deadline: The number of seconds to wait to call callback
        :param method callback: The callback method
        :rtype: str

        """
        return self.ioloop.add_timeout(deadline, callback)

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        super(BaseConnection, self).close(reply_code, reply_text)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :rtype: str

        """
        self.ioloop.remove_timeout(timeout_id)

    def add_callback_threadsafe(self, callback):
        """Requests a call to the given function as soon as possible in the
        context of this connection's IOLoop thread.

        NOTE: This is the only thread-safe method offered by the connection. All
         other manipulations of the connection must be performed from the
         connection's thread.

        For example, a thread may request a call to the
        `channel.basic_ack` method of a connection that is running in a
        different thread via

        ```
        connection.add_callback_threadsafe(
            functools.partial(channel.basic_ack, delivery_tag=...))
        ```

        :param method callback: The callback method; must be callable.

        """
        if not callable(callback):
            raise TypeError(
                'callback must be a callable, but got %r' % (callback,))

        self.ioloop.add_callback_threadsafe(callback)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker, returning True if connected.

        :returns: error string or exception instance on error; None on success

        """
        # Get the addresses for the socket, supporting IPv4 & IPv6
        while True:
            try:
                addresses = self._getaddrinfo(
                    self.params.host, self.params.port, 0, socket.SOCK_STREAM,
                    socket.IPPROTO_TCP)
                break
            except SOCKET_ERROR as error:
                if error.errno == errno.EINTR:
                    continue

                LOGGER.critical('Could not get addresses to use: %s (%s)',
                                error, self.params.host)
                return error

        # If the socket is created and connected, continue on
        error = "No socket addresses available"
        for sock_addr in addresses:
            error = self._create_and_connect_to_socket(sock_addr)
            if not error:
                # Make the socket non-blocking after the connect
                self.socket.setblocking(0)
                return None
            self._cleanup_socket()

        # Failed to connect
        return error

    def _adapter_disconnect(self):
        """Invoked if the connection is being told to disconnect"""
        self._cleanup_socket()

    def _cleanup_socket(self):
        """Close the socket cleanly"""
        if self.socket:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except SOCKET_ERROR:
                pass
            self.socket.close()
            self.socket = None

    def _create_and_connect_to_socket(self, sock_addr_tuple):
        """Create socket and connect to it, using SSL if enabled.

        :returns: error string on failure; None on success
        """
        self.socket = self._create_tcp_connection_socket(
            sock_addr_tuple[0], sock_addr_tuple[1], sock_addr_tuple[2])
        self.socket.setsockopt(SOL_TCP, socket.TCP_NODELAY, 1)
        self.socket.settimeout(self.params.socket_timeout)
        pika.tcp_socket_opts.set_sock_opts(self.params.tcp_options, self.socket)

        # Wrap socket if using SSL
        if self.params.ssl_options is not None:
            self.socket = self._wrap_socket(self.socket)
            ssl_text = " with SSL"
        else:
            ssl_text = ""

        LOGGER.info('Connecting to %s:%s%s', sock_addr_tuple[4][0],
                    sock_addr_tuple[4][1], ssl_text)

        # Connect to the socket
        try:
            self.socket.connect(sock_addr_tuple[4])
        except socket.timeout:
            error = 'Connection to %s:%s failed: timeout' % (
                sock_addr_tuple[4][0], sock_addr_tuple[4][1])
            LOGGER.error(error)
            return error
        except SOCKET_ERROR as error:
            error = 'Connection to %s:%s failed: %s' % (sock_addr_tuple[4][0],
                                                        sock_addr_tuple[4][1],
                                                        error)
            LOGGER.error(error)
            return error

        # Handle SSL Connection Negotiation
        if self.params.ssl_options is not None and self.DO_HANDSHAKE:
            try:
                self._do_ssl_handshake()
            except ssl.SSLError as error:
                error = 'SSL connection to %s:%s failed: %s' % (
                    sock_addr_tuple[4][0], sock_addr_tuple[4][1], error)
                LOGGER.error(error)
                return error
        # Made it this far
        return None

    @staticmethod
    def _create_tcp_connection_socket(sock_family, sock_type, sock_proto):
        """ Create TCP/IP stream socket for AMQP connection

        :param int sock_family: socket family
        :param int sock_type: socket type
        :param int sock_proto: socket protocol number

        NOTE We break this out to make it easier to patch in mock tests
        """
        return socket.socket(sock_family, sock_type, sock_proto)

    def _do_ssl_handshake(self):
        """Perform SSL handshaking, copied from python stdlib test_ssl.py.

        """
        if not self.DO_HANDSHAKE:
            return
        while True:
            try:
                self.socket.do_handshake()
                break
            # TODO should be using SSLWantReadError, etc. directly
            except ssl.SSLError as err:
                # TODO these exc are for non-blocking sockets, but ours isn't
                # at this stage, so it's not clear why we have this.
                if err.errno == ssl.SSL_ERROR_WANT_READ:
                    self.event_state = self.READ
                elif err.errno == ssl.SSL_ERROR_WANT_WRITE:
                    self.event_state = self.WRITE
                else:
                    raise
                self._manage_event_state()

    @staticmethod
    def _getaddrinfo(host, port, family, socktype, proto):
        """Wrap `socket.getaddrinfo` to make it easier to patch for unit tests
        """
        return socket.getaddrinfo(host, port, family, socktype, proto)

    def _flush_outbound(self):
        """Have the state manager schedule the necessary I/O.
        """
        # NOTE: We don't call _handle_write() from this context, because pika
        # code was not designed to be writing to (or reading from) the socket
        # from any methods, except from ioloop handler callbacks. Many methods
        # in pika core and adapters do not deal gracefully with connection
        # errors occurring in their context; e.g., Connection.channel (pika
        # issue #659), Connection._on_connection_tune (if connection loss is
        # detected in _send_connection_tune_ok, before _send_connection_open is
        # called), etc., etc., etc.
        self._manage_event_state()

    def _handle_connection_socket_error(self, exception):
        """Internal error handling method. Here we expect a socket error
        coming in and will handle different socket errors differently.

        :param exception: An exception object with `errno` attribute.

        """
        # Assertion failure here would indicate internal logic error
        if not exception.errno:
            raise ValueError("No socket error indicated: %r" % (exception,))
        if exception.errno in self.ERRORS_TO_IGNORE:
            raise ValueError("Soft error not handled by I/O logic: %r"
                             % (exception,))

        # Socket is no longer connected, abort
        if exception.errno in self.ERRORS_TO_ABORT:
            LOGGER.error("Fatal Socket Error: %r", exception)

        elif isinstance(exception, ssl.SSLError):

            if exception.errno == ssl.SSL_ERROR_WANT_READ:
                # TODO doesn't seem right: this logic updates event state, but
                # the logic at the bottom unconditionaly disconnects anyway.
                # Clearly, we're not prepared to handle re-handshaking. It
                # should have been handled gracefully without ending up in this
                # error handler; ditto for SSL_ERROR_WANT_WRITE below.
                self.event_state = self.READ
            elif exception.errno == ssl.SSL_ERROR_WANT_WRITE:
                self.event_state = self.WRITE
            else:
                LOGGER.error("SSL Socket error: %r", exception)

        else:
            # Haven't run into this one yet, log it.
            LOGGER.error("Unexpected socket Error: %r", exception)

        # Disconnect from our IOLoop and let Connection know what's up
        self._on_terminate(connection.InternalCloseReasons.SOCKET_ERROR,
                           repr(exception))

    def _handle_timeout(self):
        """Handle a socket timeout in read or write.
        We don't do anything in the non-blocking handlers because we
        only have the socket in a blocking state during connect."""
        LOGGER.warning("Unexpected socket timeout")

    def _handle_connection_socket_events(self, fd, events):
        """Handle IO/Event loop events, processing them.

        :param int fd: The file descriptor for the events
        :param int events: Events from the IO/Event loop

        """
        if not self.socket:
            LOGGER.error('Received events on closed socket: %r', fd)
            return

        if self.socket and (events & self.WRITE):
            self._handle_write()
            self._manage_event_state()

        if self.socket and (events & self.READ):
            self._handle_read()


        if self.socket and (events & self.ERROR):
            # No need to handle the ERROR event separately from I/O. If there is
            # a stream error, READ and/or WRITE will also be inidicated and the
            # corresponding _handle_write/_handle_read will get tripped up with
            # the approrpriate error and terminate the connection. So, we'll
            # just debug-log the information in case it helps with diagnostics.
            error = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            LOGGER.debug('Sock error events %r; sock error code %r', events,
                         error)

    def _handle_read(self):
        """Read from the socket and call our on_data_available with the data."""
        try:
            while True:
                try:
                    if self.params.ssl_options is not None:
                        # TODO Why using read instead of recv on ssl socket?
                        data = self.socket.read(self._buffer_size)
                    else:
                        data = self.socket.recv(self._buffer_size)

                    break
                except SOCKET_ERROR as error:
                    if error.errno == errno.EINTR:
                        continue
                    else:
                        raise

        except socket.timeout:
            self._handle_timeout()
            return 0

        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                # ssl wants more data but there is nothing currently
                # available in the socket, wait for it to become readable.
                return 0
            return self._handle_connection_socket_error(error)

        except SOCKET_ERROR as error:
            if error.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return 0
            return self._handle_connection_socket_error(error)

        if not data:
            # Remote peer closed or shut down our input stream - disconnect
            LOGGER.error('Read empty data, calling disconnect')
            return self._on_terminate(
                connection.InternalCloseReasons.SOCKET_ERROR, "EOF")

        # Pass the data into our top level frame dispatching method
        self._on_data_available(data)
        return len(data)

    def _handle_write(self):
        """Try and write as much as we can, if we get blocked requeue
        what's left"""
        total_bytes_sent = 0
        try:
            while self.outbound_buffer:
                frame = self.outbound_buffer.popleft()
                while True:
                    try:
                        num_bytes_sent = self.socket.send(frame)
                        break
                    except SOCKET_ERROR as error:
                        if error.errno == errno.EINTR:
                            continue
                        else:
                            raise

                total_bytes_sent += num_bytes_sent
                if num_bytes_sent < len(frame):
                    LOGGER.debug("Partial write, requeing remaining data")
                    self.outbound_buffer.appendleft(frame[num_bytes_sent:])
                    break

        except socket.timeout:
            # Will only come here if the socket is blocking
            LOGGER.debug("socket timeout, requeuing frame")
            self.outbound_buffer.appendleft(frame)
            self._handle_timeout()

        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_WRITE:
                # In Python 3.5+, SSLSocket.send raises this if the socket is
                # not currently able to write. Handle this just like an
                # EWOULDBLOCK socket error.
                LOGGER.debug("Would block, requeuing frame")
                self.outbound_buffer.appendleft(frame)
            else:
                return self._handle_connection_socket_error(error)

        except SOCKET_ERROR as error:
            if error.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                LOGGER.debug("Would block, requeuing frame")
                self.outbound_buffer.appendleft(frame)
            else:
                return self._handle_connection_socket_error(error)

        return total_bytes_sent

    def _init_connection_state(self):
        """Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped.

        """
        super(BaseConnection, self)._init_connection_state()
        self.base_events = self.READ | self.ERROR
        self.event_state = self.base_events
        self.socket = None

    def _manage_event_state(self):
        """Manage the bitmask for reading/writing/error which is used by the
        io/event handler to specify when there is an event such as a read or
        write.

        """
        if self.outbound_buffer:
            if not self.event_state & self.WRITE:
                self.event_state |= self.WRITE
                self.ioloop.update_handler(self.socket.fileno(),
                                           self.event_state)
        elif self.event_state & self.WRITE:
            self.event_state = self.base_events
            self.ioloop.update_handler(self.socket.fileno(), self.event_state)

    def _wrap_socket(self, sock):
        """Wrap the socket for connecting over SSL. This allows the user to use
        a dict for the usual SSL options or an SSLOptions object for more
        advanced control.

        :rtype: ssl.SSLSocket

        """
        ssl_options = self.params.ssl_options

        return ssl_options.context.wrap_socket(
            sock,
            server_side=False,
            do_handshake_on_connect=self.DO_HANDSHAKE,
            suppress_ragged_eofs=True,
            server_hostname=ssl_options.server_hostname)
