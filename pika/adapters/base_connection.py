"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
import errno
import logging
import socket
import ssl

import pika.compat
import pika.exceptions
import pika.tcp_socket_opts

from pika import connection


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
            except pika.compat.SOCKET_ERROR:
                # closed?
                pass
            else:
                try:
                    peername = sock.getpeername()
                except pika.compat.SOCKET_ERROR:
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
        """Perform one round of connection establishment asynchronously. Upon
        completion of the round will invoke
        `Connection._adapter_connect_done(None|Exception)`, where the arg value
        of None signals success, while an Exception-based instance signals
         failure of the round.

        :returns: error string or exception instance on error; None on success

        """
        # Get the addresses for the socket, supporting IPv4 & IPv6
        while True:
            try:
                addresses = self._getaddrinfo(
                    self.params.host, self.params.port, 0, socket.SOCK_STREAM,
                    socket.IPPROTO_TCP)
                break
            except pika.compat.SOCKET_ERROR as error:
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
            except pika.compat.SOCKET_ERROR:
                pass
            self.socket.close()
            self.socket = None

    def _create_and_connect_to_socket(self, sock_addr_tuple):
        """Create socket and connect to it, using SSL if enabled.

        :returns: error string on failure; None on success
        """
        self.socket = self._create_tcp_connection_socket(
            sock_addr_tuple[0], sock_addr_tuple[1], sock_addr_tuple[2])
        self.socket.setsockopt(pika.compat.SOL_TCP, socket.TCP_NODELAY, 1)
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
        except pika.compat.SOCKET_ERROR as error:
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
                except pika.compat.SOCKET_ERROR as error:
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

        except pika.compat.SOCKET_ERROR as error:
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
                    except pika.compat.SOCKET_ERROR as error:
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

        except pika.compat.SOCKET_ERROR as error:
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


class _TransportConnector(object):
    """Manages streaming transport setup

    """

    # TODO BaseConnection needs to implement
    # ioloop_interface.AbstractStreamProtocol

    def __init__(self, params, ioloop, stream_proto, on_done):
        """

        :param params:
        :param ioloop:
        :param stream_proto:
        :param on_done: on_done(None|Exception)`, will be invoked upon
            completion, where the arg value of None signals success, while an
            Exception-based object signals failure of the round after exhausting
            all remaining address records from DNS lookup.
        """
        self._params = params
        self._ioloop = ioloop
        self._stream_proto = stream_proto
        self._on_done = on_done
        self._sock = None
        self._addrinfo_iter = None

        # Initiate asynchronous getaddrinfo
        self._async_ref = self._ioloop.getaddrinfo(
            host=self._params.host,
            port=self._params.port,
            socktype=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
            on_done=self._on_getaddrinfo_done)

    def close(self):
        """Abruptly close this instance

        """
        if self._async_ref is not None:
            self._async_ref.cancel()

        if self._sock is not None:
            self._sock = None

    def start_next(self):
        """ Attempt to connect using the next address record, if any, from
        the previously-performed DNS lookup

        :raises StopIteration: if there aren't any more records to try

        """
        assert self._async_ref is None, \
            'start_next() called while async operation is in progress'

        self._attempt_connection(next(self._addrinfo_iter))

    def _on_getaddrinfo_done(self, addrinfos_or_exc):
        """Handles completion callback from asynchronous `getaddrinfo()`.

        :param sequence|Exception addrinfos_or_exc: address records returned by
            `getaddrinfo()` or an Exception from failure.
        """
        self._async_ref = None
        if isinstance(addrinfos_or_exc, Exception):
            exc = addrinfos_or_exc
            LOGGER.error('%r failed: %r', self._ioloop.getaddrinfo, exc)
            self._on_done(exc)
        else:
            addrinfos = addrinfos_or_exc
            LOGGER.debug('%r returned %s records', self._ioloop.getaddrinfo,
                         len(addrinfos_or_exc))
            if not addrinfos:
                LOGGER.debug('%r returned 0 records', self._ioloop.getaddrinfo)
                self._on_done(pika.exceptions.TransportError(
                    'getaddrinfo returned 0 records'))
            else:
                self._addrinfo_iter = iter(addrinfos)
                self.start_next()

    def _attempt_connection(self, addr_record):
        """Kick start a connection attempt using the given address record.

        :param sequence addr_record: a single `getaddrinfo()` address record

        """
        LOGGER.debug('Attempting to connect using address record %s',
                     addr_record)
        self._sock = socket.socket(addr_record[0],
                                   addr_record[1],
                                   addr_record[2])
        self._sock.setsockopt(pika.compat.SOL_TCP, socket.TCP_NODELAY, 1)
        # TODO Do we still need settimeout for non-blocking socket?
        self._sock.settimeout(self._params.socket_timeout)
        # TODO I would have expected sock to be the first arg below
        pika.tcp_socket_opts.set_sock_opts(self._params.tcp_options,
                                           self._sock)
        self._sock.setblocking(False)

        # Initiate asynchronous connection attempt
        addr = addr_record[4]
        LOGGER.error('Connected to server at %s', addr)
        self._async_ref = self._ioloop.connect_socket(
            self._sock,
            addr,
            on_done=self._on_socket_connection_done)

    def _on_socket_connection_done(self, exc):
        """Handle completion of asynchronous stream socket connection attempt.

        On failure, attempt to connect to the next address, if any, from DNS
        lookup.

        :param None|Exception exc: None on success; Exception-based object on
            failure

        """
        self._async_ref = None
        if exc is not None:
            LOGGER.error('%r connection attempt failed.', self._sock)
            self._sock = None
            try:
                # Try connecting with next address record
                self.start_next()
            except StopIteration:
                self._on_done(exc)
        else:
            # We succeeded in opening a TCP stream
            LOGGER.debug('%r connected.', self._sock)

            # Now attempt to set up a streaming transport
            ssl_context = self._get_ssl_context()
            server_hostname = None
            if ssl_context is not None:
                try:
                    server_hostname = self._params.ssl_options.server_hostname
                except AttributeError:
                    pass

            self._async_ref = self._ioloop.create_streaming_connection(
                protocol_factory=lambda: self._stream_proto,
                sock=self._sock,
                ssl_context=ssl_context,
                server_hostname=server_hostname,
                on_done=self._on_create_streaming_connection_done)

    def _on_create_streaming_connection_done(self, result):
        """Handle asynchronous completion of
        `AbstractAsyncServices.create_streaming_connection()`

        :param sequence|Exception result: On success, a two-tuple
            (transport, protocol); on failure, an Exception-based instance.

        """
        self._async_ref = None
        self._sock = None
        if isinstance(result, Exception):
            exc = result
            LOGGER.error('Attempt to create a streaming transport failed: %r',
                         exc)
            try:
                # Try connecting with next address record
                self.start_next()
            except StopIteration:
                self._on_done(exc)
        else:
            # We succeeded in setting up the streaming transport!
            # result is a two-tuple (transport, protocol)
            LOGGER.info('Transport connected: %r.', result)
            self._on_done(None)


    def _get_ssl_context(self):
        """Construct SSL Context, if needed, from connection parameters.

        :returns: None if SSL is not required; `ssl.SSLContext` instance
            otherwise.
        :rtype: ssl.SSLContext|None
        """
        # TODO All this will be unnecessary once pull request
        # https://github.com/pika/pika/pull/987 is accepted

        if not self._params.ssl:
            return None

        ssl_options = self._params.ssl_options or {}
        # our wrapped return sock

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
        else:
            # Use dummy wrap_socket call and extract ssl context
            ssl_sock = ssl.wrap_socket(socket.socket(), **ssl_options)
            context = ssl_sock.context
            ssl_sock.close()

        return context
