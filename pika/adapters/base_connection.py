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

        :param float deadline: The number of seconds to wait to call callback
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
