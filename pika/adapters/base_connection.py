# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Pika provides multiple adapters to connect to RabbitMQ:

- adapters.select_connection.SelectConnection: A native event based connection
  adapter that implements select, kqueue, poll and epoll.
- adapters.asyncore_connection.AsyncoreConnection: Legacy adapter kept for
  convenience of previous Pika users. It is recommended to use the
  SelectConnection instead of AsyncoreConnection.
- adapters.tornado_connection.TornadoConnection: Connection adapter for use
  with the Tornado web framework.
- adapters.blocking_connection.BlockingConnection: Enables blocking,
  synchronous operation on top of library for simple uses. This is not
  recommended and is included for legacy reasons only.
"""

import errno
import socket
import time

# See if we have SSL support
try:
    import ssl
    SSL = True
except ImportError:
    SSL = False

from pika.connection import Connection, CONNECTION_PROTOCOL, CONNECTION_START,\
    CONNECTION_TUNE
from pika.exceptions import AMQPConnectionError, IncompatibleProtocolError, \
    ProbableAuthenticationError, ProbableAccessDeniedError
import pika.log as log

# Use epoll's constants to keep life easy
READ = 0x0001
WRITE = 0x0004
ERROR = 0x0008

# Connection timeout (2 seconds to open socket)
CONNECTION_TIMEOUT = 2
ERRORS_TO_IGNORE = [errno.EWOULDBLOCK, errno.EAGAIN, errno.EINTR]


class BaseConnection(Connection):

    def __init__(self, parameters=None,
                       on_open_callback=None,
                       reconnection_strategy=None):

        # Let the developer know we could not import SSL
        if parameters.ssl and not SSL:
            raise Exception("SSL specified but it is not available")

        # Call our parent's __init__
        Connection.__init__(self, parameters, on_open_callback,
                            reconnection_strategy)

    def _init_connection_state(self):
        Connection._init_connection_state(self)

        # Set our defaults
        self.fd = None
        self.ioloop = None

        # Event states (base and current)
        self.base_events = READ | ERROR
        self.event_state = self.base_events
        self.socket = None
        self.write_buffer = None
        self._ssl_connecting = False
        self._ssl_handshake = False

    def _socket_connect(self):
        """Create socket and connect to it, using SSL if enabled."""
        # Create our socket and set our socket options
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

        # Wrap the SSL socket if we SSL turned on
        ssl_text = ""
        if self.parameters.ssl:
            ssl_text = " with SSL"
            if self.parameters.ssl_options:
                # Always overwrite this value
                self.parameters.ssl_options['do_handshake_on_connect'] = \
                    self._ssl_handshake
                self.socket = ssl.wrap_socket(self.socket,
                                              **self.parameters.ssl_options)
            else:
                self.socket = ssl.wrap_socket(self.socket,
                                              do_handshake_on_connect= \
                                                  self._ssl_handshake)

            # Flags for SSL handshake negotiation
            self._ssl_connecting = True

        # Try and connect
        log.info("Connecting fd %d to %s:%i%s", self.socket.fileno(),
                 self.parameters.host,
                 self.parameters.port, ssl_text)
        self.socket.settimeout(CONNECTION_TIMEOUT)
        self.socket.connect((self.parameters.host,
                             self.parameters.port))

        # Set the socket to non-blocking
        self.socket.setblocking(0)

    def _adapter_connect(self):
        """
        Base connection function to be extended as needed.
        """

        # Set our remaining attempts to the initial value
        from sys import maxint
        remaining_attempts = self.parameters.connection_attempts or maxint

        # Loop while we have remaining attempts
        while remaining_attempts:
            remaining_attempts -= 1
            try:
                return self._socket_connect()
            except socket.timeout, timeout:
                reason = "timeout"
            except socket.error, err:
                reason = err[-1]
                self.socket.close()

            retry = ''
            if remaining_attempts:
                retry = "Retrying in %i seconds with %i retry(s) left" % \
                        (self.parameters.retry_delay, remaining_attempts)

            log.warning("Could not connect: %s. %s", reason, retry)

            if remaining_attempts:
                time.sleep(self.parameters.retry_delay)

        # Log the errors and raise the  exception
        log.error("Could not connect: %s", reason)
        raise AMQPConnectionError(reason)

    def add_timeout(self, deadline, callback):
        return self.ioloop.add_timeout(deadline, callback)

    def remove_timeout(self, timeout_id):
        self.ioloop.remove_timeout(timeout_id)

    def _erase_credentials(self):
        pass

    def _flush_outbound(self):
        """
        Call the state manager who will figure out that we need to write.
        """
        self._manage_event_state()

    def _adapter_disconnect(self):
        """
        Called if we are forced to disconnect for some reason from Connection.
        """
        # Remove from the IOLoop
        self.ioloop.stop()

        # Close our socket
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

        # Check our state on disconnect
        self._check_state_on_disconnect()

    def _check_state_on_disconnect(self):
        """
        Checks to see if we were in opening a connection with RabbitMQ when
        we were disconnected and raises exceptions for the anticipated
        exception types.
        """
        if self.connection_state == CONNECTION_PROTOCOL:
            log.error("Incompatible Protocol Versions")
            raise IncompatibleProtocolError
        elif self.connection_state == CONNECTION_START:
            log.error("Socket closed while authenticating indicating a \
probable authentication error")
            raise ProbableAuthenticationError
        elif self.connection_state == CONNECTION_TUNE:
            log.error("Socket closed while tuning the connection indicating a \
probable permission error when accessing a virtual host")
            raise ProbableAccessDeniedError

    def _handle_disconnect(self):
        """
        Called internally when we know our socket is disconnected already
        """
        # Remove from the IOLoop
        self.ioloop.stop()

        # Close up our Connection state
        self._on_connection_closed(None, True)

    def _handle_error(self, error):
        """
        Internal error handling method. Here we expect a socket.error coming in
        and will handle different socket errors differently.
        """
        # Handle version differences in Python
        if hasattr(error, 'errno'):  # Python >= 2.6
            error_code = error.errno
        elif error is not None:
            error_code = error[0]  # Python <= 2.5
        else:
            # This shouldn't happen, but log it in case it does
            log.error("%s: Tried to handle an error where no error existed",
                      self.__class__.__name__)

        # Ok errors, just continue what we were doing before
        if error_code in ERRORS_TO_IGNORE:
            log.debug("Ignoring %s", error_code)
            return None

        # Socket is closed, so lets just go to our handle_close method
        elif error_code in (errno.EBADF, errno.ECONNABORTED):
            log.error("%s: Socket is closed",
                    self.__class__.__name__)

        elif self.parameters.ssl and isinstance(error, ssl.SSLError):
            # SSL socket operation needs to be retried
            if error_code in (ssl.SSL_ERROR_WANT_READ,
                               ssl.SSL_ERROR_WANT_WRITE):
                return None
            else:
                log.error("%s: SSL Socket error on fd %d: %s",
                      self.__class__.__name__,
                      self.socket.fileno(),
                      repr(error))
        else:
            # Haven't run into this one yet, log it.
            log.error("%s: Socket Error on fd %d: %s",
                      self.__class__.__name__,
                      self.socket.fileno(),
                      error_code)

        # Disconnect from our IOLoop and let Connection know what's up
        self._handle_disconnect()
        return None

    def _do_ssl_handshake(self):
        """
        Copied from python stdlib test_ssl.py.

        """
        log.debug("_do_ssl_handshake")
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

    def _handle_events(self, fd, events, error=None):
        """
        Our IO/Event loop have called us with events, so process them
        """
        if not self.socket:
            log.error("%s: Got events for closed stream %d",
                      self.__class__.__name__, self.socket.fileno())
            return

        if events & READ:
            self._handle_read()

        if events & ERROR:
            self._handle_error(error)

        if events & WRITE:
            self._handle_write()

            # Call our event state manager who will decide if we reset our
            # event state due to having an empty outbound buffer
            self._manage_event_state()

    def _handle_read(self):
        """
        Read from the socket and call our on_data_available with the data.
        """
        if self.parameters.ssl and self._ssl_connecting:
            return self._do_ssl_handshake()
        try:
            if self.parameters.ssl and self.socket.pending():
                data = self.socket.read(self._suggested_buffer_size)
            else:
                data = self.socket.recv(self._suggested_buffer_size)
        except socket.timeout:
            raise
        except socket.error, error:
            return self._handle_error(error)

        # We received no data, so disconnect
        if not data:
            log.debug('Calling disconnect')
            return self._adapter_disconnect()

        # Pass the data into our top level frame dispatching method
        self._on_data_available(data)

    def _handle_write(self):
        """
        We only get here when we have data to write, so try and send
        Pika's suggested buffer size of data (be nice to Windows).
        """
        if self.parameters.ssl and self._ssl_connecting:
            return self._do_ssl_handshake()

        if not self.write_buffer:
            self.write_buffer = \
                self.outbound_buffer.read(self._suggested_buffer_size)
        try:
            bytes_written = self.socket.send(self.write_buffer)
        except socket.timeout:
            raise
        except socket.error, error:
            return self._handle_error(error)

        if bytes_written:
            self.write_buffer = None

        # Remove the content from our output buffer
        self.outbound_buffer.consume(bytes_written)
        if self.outbound_buffer.size:
            log.debug("Outbound buffer size: %i", self.outbound_buffer.size)

    def _manage_event_state(self):
        """
        We use this to manage the bitmask for reading/writing/error which
        we want to use to have our io/event handler tell us when we can
        read/write, etc.
        """
        # Do we have data pending in the outbound buffer?
        if self.outbound_buffer.size:

            # If we don't already have write in our event state append it
            # otherwise do nothing
            if not self.event_state & WRITE:

                # We can assume that we're in our base_event state
                self.event_state |= WRITE

                # Update the IOLoop
                self.ioloop.update_handler(self.socket.fileno(),
                                           self.event_state)

        # We don't have data in the outbound buffer
        elif self.event_state & WRITE:

            # Set our event state to the base events
            self.event_state = self.base_events

            # Update the IOLoop
            self.ioloop.update_handler(self.socket.fileno(), self.event_state)
