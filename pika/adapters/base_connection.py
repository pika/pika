# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
import errno
import logging
import socket
import sys
import time

# Workaround for 2.5 support
try:
    socket.SOL_TCP
except AttributeError:
    socket.SOL_TCP = socket.IPPROTO_TCP
try:
    import ssl
except ImportError:
    ssl = None

from pika import connection
from pika import exceptions

LOGGER = logging.getLogger(__name__)


class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters"""

    # Use epoll's constants to keep life easy
    READ = 0x0001
    WRITE = 0x0004
    ERROR = 0x0008

    SOCKET_TIMEOUT = 2
    ERRORS_TO_IGNORE = [errno.EWOULDBLOCK, errno.EAGAIN, errno.EINTR]
    HANDSHAKE = 'do_handshake_on_connect'

    def __init__(self, parameters=None,
                       on_open_callback=None,
                       reconnection_strategy=None):
        """Create a new instance of the Connection object.

        :param parameters: Connection parameters
        :type parameters: pika.connection.ConnectionParameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method
        :param reconnection_strategy: A reconnection strategy object
        :type reconnection_strategy: pika.reconnection_strategies.ReconnectionStrategy
        :raises: RuntimeError

        """
        # Let the developer know we could not import SSL
        if parameters and parameters.ssl and ssl:
            raise RuntimeError("SSL specified but it is not available")
        self.fd = None
        self.ioloop = None
        self.base_events = self.READ | self.ERROR
        self.event_state = self.base_events
        self.socket = None
        self.write_buffer = None
        self._ssl_connecting = False
        self._ssl_handshake = False
        super(BaseConnection, self).__init__(parameters,
                                             on_open_callback,
                                             reconnection_strategy)

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: int

        """
        return self.ioloop.add_timeout(deadline, callback_method)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :rtype: int

        """
        self.ioloop.remove_timeout(timeout_id)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker"""
        LOGGER.debug('Connecting the adapter to the remote host')
        remaining_attempts = self.params.connection_attempts or sys.maxint
        reason = 'Unknown'

        # Loop while we have remaining attempts
        while remaining_attempts:
            remaining_attempts -= 1
            try:
                self._socket_connect()
                return
            except socket.timeout, timeout:
                reason = "timeout"
            except socket.error, err:
                reason = err[-1]
                self.socket.close()
            retry = ''
            if remaining_attempts:
                retry = "Retrying in %i seconds with %i retry(s) left" %\
                        (self.params.retry_delay, remaining_attempts)
            LOGGER.warning("Could not connect: %s. %s", reason, retry)
            if remaining_attempts:
                time.sleep(self.params.retry_delay)
        LOGGER.error("Could not connect: %s", reason)
        raise exceptions.AMQPConnectionError(reason)

    def _adapter_disconnect(self):
        """Invoked if the connection is being told to disconnect"""
        self.ioloop.stop()
        self.socket.shutdown(socket.SHUT_RDWR)
        self._check_state_on_disconnect()

    def _check_state_on_disconnect(self):
        """
        Checks to see if we were in opening a connection with RabbitMQ when
        we were disconnected and raises exceptions for the anticipated
        exception types.
        """
        if self.connection_state == self.CONNECTION_PROTOCOL:
            LOGGER.error("Incompatible Protocol Versions")
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

    def _do_ssl_handshake(self):
        """Perform SSL handshaking, copied from python stdlib test_ssl.py.

        """
        LOGGER.debug('_do_ssl_handshake')
        try:
            self.socket.do_handshake()
        except ssl.SSLError, err:
            if err.args[0] in (ssl.SSL_ERROR_WANT_READ,
                               ssl.SSL_ERROR_WANT_WRITE):
                return
            elif err.args[0] == ssl.SSL_ERROR_EOF:
                return self._handle_disconnect()
            raise
        except socket.error, err:
            if err.args[0] == errno.ECONNABORTED:
                return self._handle_disconnect()
        else:
            self._ssl_connecting = False

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
        self.ioloop.stop()
        self._on_connection_closed(None, True)

    def _handle_error(self, error_value):
        """Internal error handling method. Here we expect a socket.error
        coming in and will handle different socket errors differently.

        :param int|object error_value: The inbound error

        """
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
            # SSL socket operation needs to be retried
            if error_code in (ssl.SSL_ERROR_WANT_READ,
                              ssl.SSL_ERROR_WANT_WRITE):
                return
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
        if not self.socket:
            LOGGER.error('Received events on closed socket: %d',
                         self.socket.fileno())
            return

        if not write_only and (events & self.READ):
            self._handle_read()

        if events & self.ERROR:
            self._handle_error(error)

        if events & self.WRITE:
            self._handle_write()
            self._manage_event_state()

    def _handle_read(self):
        """Read from the socket and call our on_data_available with the data."""
        if self.params.ssl and self._ssl_connecting:
            return self._do_ssl_handshake()
        try:
            if self.params.ssl and self.socket.pending():
                data = self.socket.read(self._buffer_size)
            else:
                data = self.socket.recv(self._buffer_size)
        except socket.timeout:
            raise
        except socket.error, error:
            return self._handle_error(error)

        # Empty data, should disconnect
        if not data:
            LOGGER.error('Read empty data, calling disconnect')
            return self._adapter_disconnect()

        # Pass the data into our top level frame dispatching method
        self._on_data_available(data)
        return len(data)

    def _handle_write(self):
        """Handle any outbound buffer writes that need to take place."""
        if self.params.ssl and self._ssl_connecting:
            return self._do_ssl_handshake()

        if not self.write_buffer:
            self.write_buffer = self.outbound_buffer.read(self._buffer_size)
        try:
            bytes_written = self.socket.send(self.write_buffer)
        except socket.timeout:
            raise
        except socket.error, error:
            return self._handle_error(error)

        # Remove the content from our output buffer
        self.outbound_buffer.consume(bytes_written)
        if self.outbound_buffer.size:
            LOGGER.debug("Outbound buffer size: %i", self.outbound_buffer.size)

        if bytes_written:
            self.write_buffer = None
            return bytes_written

    def _init_connection_state(self):
        """Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped.

        """
        super(BaseConnection, self)._init_connection_state()
        self.fd = None
        self.ioloop = None
        self.base_events = self.READ | self.ERROR
        self.event_state = self.base_events
        self.socket = None
        self.write_buffer = None
        self._ssl_connecting = False
        self._ssl_handshake = False

    def _manage_event_state(self):
        """Manage the bitmask for reading/writing/error which is used by the
        io/event handler to specify when there is an event such as a read or
        write.

        """
        if self.outbound_buffer.size:
            if not self.event_state & self.WRITE:
                self.event_state |= self.WRITE
                self.ioloop.update_handler(self.socket.fileno(),
                                           self.event_state)
        elif self.event_state & self.WRITE:
            self.event_state = self.base_events
            self.ioloop.update_handler(self.socket.fileno(), self.event_state)

    def _socket_connect(self):
        """Create socket and connect to it, using SSL if enabled."""
        LOGGER.debug('Creating the socket')
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        ssl_text = ""
        if self.params.ssl:
            self.socket = self._wrap_socket(self.socket)
            ssl_text = " with SSL"
        LOGGER.info("Connecting fd %d to %s:%i%s",
                    self.socket.fileno(), self.params.host,
                    self.params.port, ssl_text)
        self.socket.settimeout(self.SOCKET_TIMEOUT)
        self.socket.connect((self.params.host, self.params.port))
        self.socket.setblocking(0)
        
    def _wrap_socket(self, sock):
        """Wrap the socket for connecting over SSL.

        :rtype: ssl.SSLSocket

        """
        if self.params.ssl_options:
            self.params.ssl_options[self.HANDSHAKE] = self._ssl_handshake
            sock = ssl.wrap_socket(sock, **self.params.ssl_options)
        else:
            sock = ssl.wrap_socket(sock,
                                   do_handshake_on_connect=self._ssl_handshake)
        self._ssl_connecting = True
        return sock
