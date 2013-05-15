"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
import errno
import logging
import socket
import ssl

from pika import connection
from pika import exceptions

LOGGER = logging.getLogger(__name__)


class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters"""

    # Use epoll's constants to keep life easy
    READ = 0x0001
    WRITE = 0x0004
    ERROR = 0x0008

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
        :param on_open_error_callback: Method to call if the connection cant
                                       be opened
        :type on_open_error_callback: method
        :param method on_close_callback: Method to call on connection close
        :param object ioloop: IOLoop object to use
        :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
        :raises: RuntimeError

        """
        # Let the developer know we could not import SSL
        if parameters and parameters.ssl and not ssl:
            raise RuntimeError("SSL specified but it is not available")
        self.base_events = self.READ | self.ERROR
        self.event_state = self.base_events
        self.fd = None
        self.ioloop = ioloop
        self.socket = None
        self.stop_ioloop_on_close = stop_ioloop_on_close
        self.write_buffer = None
        super(BaseConnection, self).__init__(parameters,
                                             on_open_callback,
                                             on_open_error_callback,
                                             on_close_callback)

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
        super(BaseConnection, self).close(reply_code, reply_text)
        self._handle_ioloop_stop()

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :rtype: str

        """
        self.ioloop.remove_timeout(timeout_id)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker, returning True if connected

        :rtype: bool

        """
        # Get the addresses for the socket, supporting IPv4 & IPv6
        sock_addrs = socket.getaddrinfo(self.params.host, self.params.port,
                            0, 0, socket.getprotobyname("tcp"))

        # Iterate through each addr tuple trying to connect
        for sock_addr in sock_addrs:

            # If the socket is created and connected, continue on
            if self._create_and_connect_to_socket(sock_addr):
                return True

        # Failed to connect
        return False

    def _adapter_disconnect(self):
        """Invoked if the connection is being told to disconnect"""
        if self.socket:
            self.socket.close()
        self.socket = None
        self._check_state_on_disconnect()
        self._handle_ioloop_stop()
        self._init_connection_state()

    def _check_state_on_disconnect(self):
        """Checks to see if we were in opening a connection with RabbitMQ when
        we were disconnected and raises exceptions for the anticipated
        exception types.

        """
        if self.connection_state == self.CONNECTION_PROTOCOL:
            LOGGER.error('Incompatible Protocol Versions')
            raise exceptions.IncompatibleProtocolError
        elif self.connection_state == self.CONNECTION_START:
            LOGGER.error("Socket closed while authenticating indicating a "
                         "probable authentication error")
            raise exceptions.ProbableAuthenticationError
        elif self.connection_state == self.CONNECTION_TUNE:
            LOGGER.error("Socket closed while tuning the connection indicating "
                         "a probable permission error when accessing a virtual "
                         "host")
            raise exceptions.ProbableAccessDeniedError
        elif self.is_open:
            LOGGER.warning("Socket closed when connection was open")
        elif not self.is_closing:
            LOGGER.warning('Unknown state on disconnect: %i',
                           self.connection_state)

    def _create_and_connect_to_socket(self, sock_addr_tuple):
        """Create socket and connect to it, using SSL if enabled."""
        self.socket = socket.socket(sock_addr_tuple[0], socket.SOCK_STREAM, 0)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.socket.settimeout(self.params.socket_timeout)

        # Wrap socket if using SSL
        if self.params.ssl:
            self.socket = self._wrap_socket(self.socket)
            ssl_text = " with SSL"
        else:
            ssl_text = ""

        LOGGER.info('Connecting to %s:%s%s',
                    sock_addr_tuple[4][0], sock_addr_tuple[4][1], ssl_text)

        # Connect to the socket
        try:
            self.socket.connect(sock_addr_tuple[4])
        except socket.timeout:
            LOGGER.error('Connection to %s:%s failed: timeout',
                         sock_addr_tuple[4][0],sock_addr_tuple[4][1])
            return False
        except socket.error as error:
            LOGGER.warning('Connection to %s:%s failed: %s',
                           sock_addr_tuple[4][0],sock_addr_tuple[4][1], error)
            return False

        # Handle SSL Connection Negotiation
        if self.params.ssl and self.DO_HANDSHAKE:
            try:
                self._do_ssl_handshake()
            except ssl.SSLError as error:
                LOGGER.error('SSL connection to %s:%s failed: %s',
                             sock_addr_tuple[4][0],sock_addr_tuple[4][1], error)
                return False

        # Made it this far
        return True

    def _do_ssl_handshake(self):
        """Perform SSL handshaking, copied from python stdlib test_ssl.py.

        """
        if not self.DO_HANDSHAKE:
            return
        while True:
            try:
                self.socket.do_handshake()
                break
            except ssl.SSLError, err:
                if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                    self.event_state = self.READ
                elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                    self.event_state = self.WRITE
                else:
                    raise
                self._manage_event_state()

    def _get_error_code(self, error_value):
        """Get the error code from the error_value accounting for Python
        version differences.

        :rtype: int

        """
        if not error_value:
            return None
        if hasattr(error_value, 'errno'):  # Python >= 2.6
            return error_value.errno
        elif error_value is not None:
            return error_value[0]  # Python <= 2.5
        return None

    def _flush_outbound(self):
        """Call the state manager who will figure out that we need to write."""
        self._manage_event_state()

    def _handle_disconnect(self):
        """Called internally when the socket is disconnected already
        """
        self._adapter_disconnect()
        self._on_connection_closed(None, True)

    def _handle_ioloop_stop(self):
        """Invoked when the connection is closed to determine if the IOLoop
        should be stopped or not.

        """
        if self.stop_ioloop_on_close and self.ioloop:
            self.ioloop.stop()
        elif self.WARN_ABOUT_IOLOOP:
            LOGGER.warning('Connection is closed but not stopping IOLoop')

    def _handle_error(self, error_value):
        """Internal error handling method. Here we expect a socket.error
        coming in and will handle different socket errors differently.

        :param int|object error_value: The inbound error

        """
        if 'timed out' in str(error_value):
            raise socket.timeout
        error_code = self._get_error_code(error_value)
        if not error_code:
            LOGGER.critical("Tried to handle an error where no error existed")
            return

        # Ok errors, just continue what we were doing before
        if error_code in self.ERRORS_TO_IGNORE:
            LOGGER.debug("Ignoring %s", error_code)
            return

        # Socket is closed, so lets just go to our handle_close method
        elif error_code in (errno.EBADF, errno.ECONNABORTED):
            LOGGER.error("Socket is closed")

        elif self.params.ssl and isinstance(error_value, ssl.SSLError):

            if error_value.args[0] == ssl.SSL_ERROR_WANT_READ:
                self.event_state = self.READ
            elif error_value.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                self.event_state = self.WRITE
            else:
                LOGGER.error("SSL Socket error on fd %d: %r",
                             self.socket.fileno(), error_value)
        else:
            # Haven't run into this one yet, log it.
            LOGGER.error("Socket Error on fd %d: %s",
                         self.socket.fileno(), error_code)

        # Disconnect from our IOLoop and let Connection know what's up
        self._handle_disconnect()

    def _handle_events(self, fd, events, error=None, write_only=False):
        """Handle IO/Event loop events, processing them.

        :param int fd: The file descriptor for the events
        :param int events: Events from the IO/Event loop
        :param int error: Was an error specified
        :param bool write_only: Only handle write events

        """
        if not fd:
            LOGGER.error('Received events on closed socket: %d', fd)
            return

        if events & self.WRITE:
            self._handle_write()
            self._manage_event_state()

        if not write_only and (events & self.READ):
            self._handle_read()

        if events & self.ERROR:
            LOGGER.error('Error event %r, %r', events, error)
            self._handle_error(error)

    def _handle_read(self):
        """Read from the socket and call our on_data_available with the data."""
        try:
            if self.params.ssl:
                data = self.socket.read(self._buffer_size)
            else:
                data = self.socket.recv(self._buffer_size)
        except socket.timeout:
            raise
        except socket.error, error:
            return self._handle_error(error)

        # Empty data, should disconnect
        if not data or data == 0:
            LOGGER.error('Read empty data, calling disconnect')
            return self._handle_disconnect()

        # Pass the data into our top level frame dispatching method
        self._on_data_available(data)
        return len(data)

    def _handle_write(self):
        """Handle any outbound buffer writes that need to take place."""
        total_written = 0
        if self.outbound_buffer:
            try:
                bytes_written = self.socket.send(self.outbound_buffer.popleft())
            except socket.timeout:
                raise
            except socket.error, error:
                return self._handle_error(error)
            total_written += bytes_written
        return total_written

    def _init_connection_state(self):
        """Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped.

        """
        super(BaseConnection, self)._init_connection_state()
        self.fd = None
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
        """Wrap the socket for connecting over SSL.

        :rtype: ssl.SSLSocket

        """
        return ssl.wrap_socket(sock,
                               do_handshake_on_connect=self.DO_HANDSHAKE,
                               **self.params.ssl_options)
