"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
import errno
import logging
import os
import socket

import pika.compat
import pika.exceptions
import pika.tcp_socket_opts

from pika import connection


LOGGER = logging.getLogger(__name__)


class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters"""

    def __init__(self,
                 parameters,
                 on_open_callback,
                 on_open_error_callback,
                 on_close_callback,
                 async_services):
        """Create a new instance of the Connection object.

        :param None|pika.connection.Parameters parameters: Connection parameters
        :param None|method on_open_callback: Method to call on connection open
        :param None|method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|BaseException)
        :param None|method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param async_interface.AbstractAsyncServices async_services:
            asynchronous services
        :raises: RuntimeError
        :raises: ValueError

        """
        if parameters and not isinstance(parameters, connection.Parameters):
            raise ValueError(
                'Expected instance of Parameters, not %r' % parameters)

        self._async = async_services
        self._transport = None  # type: async_interface.AbstractStreamTransport
        self._transport_mgr = None  # type: _TransportManager
        super(BaseConnection,
              self).__init__(parameters, on_open_callback,
                             on_open_error_callback, on_close_callback)

    def _init_connection_state(self):
        """Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped.

        """
        super(BaseConnection, self)._init_connection_state()

        self._transport_mgr = None
        self._transport = None

    def __repr__(self):

        # def get_socket_repr(sock):
        #     """Return socket info suitable for use in repr"""
        #     if sock is None:
        #         return None
        #
        #     sockname = None
        #     peername = None
        #     try:
        #         sockname = sock.getsockname()
        #     except pika.compat.SOCKET_ERROR:
        #         # closed?
        #         pass
        #     else:
        #         try:
        #             peername = sock.getpeername()
        #         except pika.compat.SOCKET_ERROR:
        #             # not connected?
        #             pass
        #
        #     return '%s->%s' % (sockname, peername)
        # TODO need helpful __repr__ in transports
        return ('<%s %s transport=%s params=%s>' %
                (self.__class__.__name__,
                 self._STATE_NAMES[self.connection_state],
                 self._transport, self.params))

    @property
    def ioloop(self):
        """
        :return: the native I/O loop instance underlying async services selected
            by user or the default selected by the specialized connection
            adapter (e.g., Twisted reactor, `asyncio.SelectorEventLoop`,
            `select_connection.IOLoop`, etc.)
        """
        return self._async.get_native_ioloop()

    def add_timeout(self, deadline, callback):
        """Add the callback to the IOLoop timer to fire after deadline
        seconds.

        :param float deadline: The number of seconds to wait to call callback
        :param method callback: The callback method
        :return: Handle that can be passed to `remove_timeout()` to cancel the
            callback.

        """
        return self._async.call_later(deadline, callback)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        """
        timeout_id.cancel()

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

        self._async.add_callback_threadsafe(callback)

    def _adapter_connect(self):
        """Perform one round of stream connection establishment (including SSL
        if requested by user) asynchronously. Upon completion of the round, they
        must invoke `Connection._on_stream_connection_done(None|BaseException)`,
        where the arg value of None signals success, while an exception instance
        (check for `BaseException`) signals failure of the round.

        """
        self._transport_mgr = _TransportManager(
            params=self.params,
            stream_proto=self,
            async_services=self._async,
            on_done=self._on_transport_manager_done)

    def _on_transport_manager_done(self, error):
        """`_TransportManager` completion callback

        :param None | BaseException error: None on success; exception on error.

        """
        self._transport_mgr.close()
        self._transport_mgr = None

        # Notify protocol
        self._on_stream_connection_done(error)

    def _adapter_disconnect(self):
        """Invoked if the connection is being told to disconnect

        """
        if self._transport_mgr is not None:
            self._transport_mgr.close()
            self._transport_mgr = None

        if self._transport is not None:
            self._transport.drop()
            self._transport = None

    def _adapter_emit_data(self, data):
        """Take ownership of data and send it to AMQP server as soon as
        possible.

        :param bytes data:

        """
        self._transport.write(data)

    def _adapter_get_write_buffer_size(self):
        """
        Subclasses must override this

        :return: Current size of output data buffered by the transport
        :rtype: int

        """
        return self._transport.get_write_buffer_size()

    def connection_made(self, transport):
        """Introduces transport to protocol after transport is connected.

        `async_interface.AbstractStreamProtocol` interface method.

        :param async_interface.AbstractStreamTransport transport:
        :raises Exception: Exception-based exception on error
        """
        self._transport = transport

    def connection_lost(self, error):
        """Called upon loss or closing of connection.

        `async_interface.AbstractStreamProtocol` interface method.

        NOTE: `connection_made()` and `connection_lost()` are each called just
        once and in that order. All other callbacks are called between them.

        :param BaseException | None error: An exception (check for
            `BaseException`) indicates connection failure. None indicates that
            connection was closed on this side, such as when it's aborted or
            when `AbstractStreamProtocol.eof_received()` returns a result that
            doesn't evaluate to True.
        :raises Exception: Exception-based exception on error
        """
        self._transport = None

        if error is None:
            # Most likely as result of `eof_received()`
            error = pika.compat.SOCKET_ERROR(errno.ECONNRESET,
                                             os.strerror(errno.ECONNRESET))

        try:
            error_code = error.errno
        except AttributeError:
            error_code = connection.InternalCloseReasons.SOCKET_ERROR

        self._on_terminate(error_code, repr(error))

    def eof_received(self):  # pylint: disable=R0201
        """Called after the remote peer shuts its write end of the connection.

        `async_interface.AbstractStreamProtocol` interface method.

        :return: A falsy value (including None) will cause the transport to
            close itself, resulting in an eventual `connection_lost()` call
            from the transport. If a truthy value is returned, it will be the
            protocol's responsibility to close/abort the transport.
        :rtype: falsy | truthy
        :raises Exception: Exception-based exception on error
        """
        # When we have nothing to send to the server over plaintext stream,
        # this is how a reset connection will typically present itself.
        #
        # Have transport tear down the connection and invoke our
        # `connection_lost` method
        return False

    def data_received(self, data):
        """Called to deliver incoming data from the server to the protocol.

        `async_interface.AbstractStreamProtocol` interface method.

        :param data: Non-empty data bytes.
        :raises Exception: Exception-based exception on error
        """
        self._on_data_available(data)


class _TransportManager(object):
    """Manages streaming transport setup

    """

    def __init__(self, params, stream_proto, async_services, on_done):
        """

        :param pika.connection.Params params:
        :param async_interface.AbstractStreamProtocol stream_proto:
        :param async_interface.AbstractAsyncServices async_services:
        :param callable on_done: on_done(None|BaseException)`, will be invoked
            upon completion, where the arg value of None signals success, while
            an exception object signals failure of the workflow after exhausting
            all remaining address records from DNS lookup.
        """
        self._params = params
        self._stream_proto = stream_proto
        self._async = async_services
        self._on_done = on_done

        self._sock = None  # type: socket.socket
        self._addrinfo_iter = None

        # Initiate asynchronous getaddrinfo
        self._async_ref = self._async.getaddrinfo(
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
            self._sock.close()
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

        :param sequence|BaseException addrinfos_or_exc: address records returned
            by `getaddrinfo()` or an exception object from failure.
        """
        self._async_ref = None
        if isinstance(addrinfos_or_exc, BaseException):
            exc = addrinfos_or_exc
            LOGGER.error('%r failed: %r', self._async.getaddrinfo, exc)
            self._on_done(exc)
        else:
            addrinfos = addrinfos_or_exc
            LOGGER.debug('%r returned %s records', self._async.getaddrinfo,
                         len(addrinfos_or_exc))
            if not addrinfos:
                LOGGER.debug('%r returned 0 records', self._async.getaddrinfo)
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
        # TODO I would have expected sock to be the first arg below
        pika.tcp_socket_opts.set_sock_opts(self._params.tcp_options,
                                           self._sock)
        self._sock.setblocking(False)

        # Initiate asynchronous connection attempt
        addr = addr_record[4]
        LOGGER.error('Connecting to server at %s', addr)
        self._async_ref = self._async.connect_socket(
            self._sock,
            addr,
            on_done=self._on_socket_connection_done)

    def _on_socket_connection_done(self, exc):
        """Handle completion of asynchronous stream socket connection attempt.

        On failure, attempt to connect to the next address, if any, from DNS
        lookup.

        :param None|BaseException exc: None on success; exception object on
            failure

        """
        self._async_ref = None
        if exc is not None:
            LOGGER.error('%r connection attempt failed.', self._sock)
            self._sock.close()
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
            ssl_context = server_hostname = None
            ssl_options = self._params.ssl_options
            if ssl_options is not None:
                ssl_context = ssl_options.context
                server_hostname = ssl_options.server_hostname
                if server_hostname is None:
                    server_hostname = self._params.host

            self._async_ref = self._async.create_streaming_connection(
                protocol_factory=lambda: self._stream_proto,
                sock=self._sock,
                ssl_context=ssl_context,
                server_hostname=server_hostname,
                on_done=self._on_create_streaming_connection_done)

            self._sock = None  # create_streaming_connection took ownership

    def _on_create_streaming_connection_done(self, result):
        """Handle asynchronous completion of
        `AbstractAsyncServices.create_streaming_connection()`

        :param sequence|BaseException result: On success, a two-tuple
            (transport, protocol); on failure, exception instance.

        """
        self._async_ref = None
        self._sock = None
        if isinstance(result, BaseException):
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
