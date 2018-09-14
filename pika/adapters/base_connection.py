"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
import errno
import logging
import socket
import ssl

import pika.compat
import pika.tcp_socket_opts

from pika import __version__
from pika import connection
from pika.compat import SOCKET_ERROR
from pika.compat import SOL_TCP

LOGGER = logging.getLogger(__name__)


class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters"""

    # Use epoll's constants to keep life easy
    READ = 0x0001
    WRITE = 0x0004
    ERROR = 0x0008

    ERRORS_TO_ABORT = [
        errno.EBADF, errno.ECONNABORTED, errno.EPIPE, errno.ETIMEDOUT
    ]
    ERRORS_TO_IGNORE = [errno.EWOULDBLOCK, errno.EAGAIN, errno.EINTR]
    DO_HANDSHAKE = True
    WARN_ABOUT_IOLOOP = False

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 ioloop=None,
                 stop_ioloop_on_close=True):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param object ioloop: IOLoop object to use
        :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
        :raises: RuntimeError
        :raises: ValueError

        """
        if parameters and not isinstance(parameters, connection.Parameters):
            raise ValueError(
                'Expected instance of Parameters, not %r' % (parameters,))

        # Let the developer know we could not import SSL
        if parameters and parameters.ssl and not ssl:
            raise RuntimeError("SSL specified but it is not available")
        self.base_events = self.READ | self.ERROR
        self.event_state = self.base_events
        self.ioloop = ioloop
        self.socket = None
        self.stop_ioloop_on_close = stop_ioloop_on_close
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

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        return self.ioloop.add_timeout(deadline, callback_method)

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        try:
            super(BaseConnection, self).close(reply_code, reply_text)
        finally:
            if self.is_closed:
                self._handle_ioloop_stop()

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
        try:
            self._cleanup_socket()
        finally:
            self._handle_ioloop_stop()

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
        if self.params.ssl:
            self.socket = self._wrap_socket(self.socket)
            ssl_text = " with SSL"
        else:
            ssl_text = ""

        LOGGER.info('Pika version %s connecting to %s:%s%s',
                    __version__,
                    sock_addr_tuple[4][0],
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
        if self.params.ssl and self.DO_HANDSHAKE:
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
                if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                    self.event_state = self.READ
                elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                    self.event_state = self.WRITE
                else:
                    raise
                self._manage_event_state()

    @staticmethod
    def _getaddrinfo(host, port, family, socktype, proto):
        """Wrap `socket.getaddrinfo` to make it easier to patch for unit tests
        """
        return socket.getaddrinfo(host, port, family, socktype, proto)

    @staticmethod
    def _get_error_code(error_value):
        """Get the error code from the error_value accounting for Python
        version differences.

        :rtype: int

        """
        if not error_value:
            return None

        return error_value.errno

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

    def _handle_ioloop_stop(self):
        """Invoked when the connection is closed to determine if the IOLoop
        should be stopped or not.

        """
        if self.stop_ioloop_on_close and self.ioloop:
            self.ioloop.stop()
        elif self.WARN_ABOUT_IOLOOP:
            LOGGER.warning('Connection is closed but not stopping IOLoop')

    def _handle_error(self, error_value):
        """Internal error handling method. Here we expect a socket error
        coming in and will handle different socket errors differently.

        :param int|object error_value: The inbound error

        """
        # TODO doesn't seem right: docstring defines error_value as int|object,
        # but _get_error_code expects a falsie or an exception-like object
        error_code = self._get_error_code(error_value)

        if not error_code:
            LOGGER.critical("Tried to handle an error where no error existed")
            return

        # Ok errors, just continue what we were doing before
        if error_code in self.ERRORS_TO_IGNORE:
            LOGGER.debug("Ignoring %s", error_code)
            return

        # Socket is no longer connected, abort
        elif error_code in self.ERRORS_TO_ABORT:
            LOGGER.error("Fatal Socket Error: %r", error_value)

        elif self.params.ssl and isinstance(error_value, ssl.SSLError):

            if error_value.args[0] == ssl.SSL_ERROR_WANT_READ:
                # TODO doesn't seem right: this logic updates event state, but
                # the logic at the bottom unconditionaly disconnects anyway.
                self.event_state = self.READ
            elif error_value.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                self.event_state = self.WRITE
            else:
                LOGGER.error("SSL Socket error: %r", error_value)

        else:
            # Haven't run into this one yet, log it.
            LOGGER.error("Socket Error: %s", error_code)

        # Disconnect from our IOLoop and let Connection know what's up
        self._on_terminate(connection.InternalCloseReasons.SOCKET_ERROR,
                           repr(error_value))

    def _handle_timeout(self):
        """Handle a socket timeout in read or write.
        We don't do anything in the non-blocking handlers because we
        only have the socket in a blocking state during connect."""
        LOGGER.warning("Unexpected socket timeout")

    def _handle_events(self, fd, events, error=None, write_only=False):
        """Handle IO/Event loop events, processing them.

        :param int fd: The file descriptor for the events
        :param int events: Events from the IO/Event loop
        :param int error: Was an error specified; TODO none of the current
          adapters appear to be able to pass the `error` arg - is it needed?
        :param bool write_only: Only handle write events

        """
        if not self.socket:
            LOGGER.error('Received events on closed socket: %r', fd)
            return

        if self.socket and (events & self.WRITE):
            self._handle_write()
            self._manage_event_state()

        if self.socket and not write_only and (events & self.READ):
            self._handle_read()

        if (self.socket and write_only and (events & self.READ) and
            (events & self.ERROR)):
            error_msg = ('BAD libc:  Write-Only but Read+Error. '
                         'Assume socket disconnected.')
            LOGGER.error(error_msg)
            self._on_terminate(connection.InternalCloseReasons.SOCKET_ERROR,
                               error_msg)

        if self.socket and (events & self.ERROR):
            LOGGER.error('Error event %r, %r', events, error)
            self._handle_error(error)

    def _handle_read(self):
        """Read from the socket and call our on_data_available with the data."""
        try:
            while True:
                try:
                    if self.params.ssl:
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
            if error.args[0] == ssl.SSL_ERROR_WANT_READ:
                # ssl wants more data but there is nothing currently
                # available in the socket, wait for it to become readable.
                return 0
            return self._handle_error(error)

        except SOCKET_ERROR as error:
            if error.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return 0
            return self._handle_error(error)

        # Empty data, should disconnect
        if not data or data == 0:
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
            if error.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                # In Python 3.5+, SSLSocket.send raises this if the socket is
                # not currently able to write. Handle this just like an
                # EWOULDBLOCK socket error.
                LOGGER.debug("Would block, requeuing frame")
                self.outbound_buffer.appendleft(frame)
            else:
                return self._handle_error(error)

        except SOCKET_ERROR as error:
            if error.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                LOGGER.debug("Would block, requeuing frame")
                self.outbound_buffer.appendleft(frame)
            else:
                return self._handle_error(error)

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
        ssl_options = self.params.ssl_options or {}
        # our wrapped return sock
        ssl_sock = None

        if isinstance(ssl_options, connection.SSLOptions):
            context = ssl.SSLContext(ssl_options.ssl_version)
            context.verify_mode = ssl_options.verify_mode
            if ssl_options.certfile is not None:
                context.load_cert_chain(
                    certfile=ssl_options.certfile,
                    keyfile=ssl_options.keyfile,
                    password=ssl_options.key_password)

            # only one of either cafile or capath have to defined
            if ssl_options.cafile is not None or ssl_options.capath is not None:
                context.load_verify_locations(
                    cafile=ssl_options.cafile,
                    capath=ssl_options.capath,
                    cadata=ssl_options.cadata)

            if ssl_options.ciphers is not None:
                context.set_ciphers(ssl_options.ciphers)

            ssl_sock = context.wrap_socket(
                sock,
                server_side=ssl_options.server_side,
                do_handshake_on_connect=ssl_options.do_handshake_on_connect,
                suppress_ragged_eofs=ssl_options.suppress_ragged_eofs,
                server_hostname=ssl_options.server_hostname)
        else:
            ssl_sock = ssl.wrap_socket(
                sock, do_handshake_on_connect=self.DO_HANDSHAKE, **ssl_options)

        return ssl_sock
