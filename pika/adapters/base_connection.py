"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
from __future__ import annotations

import abc
import functools
import logging
from typing import Any, Callable, Optional, Sequence, Union

import pika.compat
import pika.exceptions
import pika.tcp_socket_opts

from pika.adapters.utils import connection_workflow, nbio_interface
from pika import connection

LOGGER = logging.getLogger(__name__)


class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters.

    This class abstracts I/O loop and transport services from pika core.

    """

    def __init__(
            self, 
            parameters: Optional[connection.Parameters], 
            on_open_callback: Optional[Callable[[connection.Connection], None]], 
            on_open_error_callback: Optional[Callable[[connection.Connection, BaseException], None]],
            on_close_callback: Optional[Callable[[connection.Connection, BaseException], None]], 
            nbio: nbio_interface.AbstractIOServices, 
            internal_connection_workflow: bool = True
    ) -> None:
        """Create a new instance of the Connection object.

        :param None|pika.connection.Parameters parameters: Connection parameters
        :param None|method on_open_callback: Method to call on connection open
        :param None | method on_open_error_callback: Called if the connection
            can't be established or connection establishment is interrupted by
            `Connection.close()`: on_open_error_callback(Connection, exception).
        :param None | method on_close_callback: Called when a previously fully
            open connection is closed:
            `on_close_callback(Connection, exception)`, where `exception` is
            either an instance of `exceptions.ConnectionClosed` if closed by
            user or broker or exception of another type that describes the cause
            of connection failure.
        :param pika.adapters.utils.nbio_interface.AbstractIOServices nbio:
            asynchronous services
        :param bool internal_connection_workflow: True for autonomous connection
            establishment which is default; False for externally-managed
            connection workflow via the `create_connection()` factory.
        :raises: RuntimeError
        :raises: ValueError

        """
        if parameters and not isinstance(parameters, connection.Parameters):
            raise ValueError(
                'Expected instance of Parameters, not {!r}'.format(parameters))

        self._nbio: nbio_interface.AbstractIOServices = nbio

        self._connection_workflow: Optional[connection_workflow.AbstractAMQPConnectionWorkflow] = None 
        self._transport: Optional[nbio_interface.AbstractStreamTransport] = None 

        self._got_eof: bool = False  # transport indicated EOF (connection reset)

        super().__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            internal_connection_workflow=internal_connection_workflow)

    def _init_connection_state(self) -> None:
        """Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped.

        """
        super()._init_connection_state()

        self._connection_workflow = None
        self._transport = None
        self._got_eof = False

    def __repr__(self) -> str:

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
        return ('<{} {} transport={} params={}>'.format(
            self.__class__.__name__, self._STATE_NAMES[self.connection_state],
            self._transport, self.params))

    @classmethod
    @abc.abstractmethod
    def create_connection(
        cls,
        connection_configs: Sequence[connection.Parameters],
        on_done: Callable[[Union[connection.Connection, connection_workflow.AMQPConnectorException]], None],
        custom_ioloop: Optional[Any] = None,
        workflow: Optional[connection_workflow.AbstractAMQPConnectionWorkflow] = None
    ) -> connection_workflow.AbstractAMQPConnectionWorkflow:
        """Asynchronously create a connection to an AMQP broker using the given
        configurations. Will attempt to connect using each config in the given
        order, including all compatible resolved IP addresses of the hostname
        supplied in each config, until one is established or all attempts fail.

        See also `_start_connection_workflow()`.

        :param sequence connection_configs: A sequence of one or more
            `pika.connection.Parameters`-based objects.
        :param callable on_done: as defined in
            `connection_workflow.AbstractAMQPConnectionWorkflow.start()`.
        :param object | None custom_ioloop: Provide a custom I/O loop that is
            native to the specific adapter implementation; if None, the adapter
            will use a default loop instance, which is typically a singleton.
        :param connection_workflow.AbstractAMQPConnectionWorkflow | None workflow:
            Pass an instance of an implementation of the
            `connection_workflow.AbstractAMQPConnectionWorkflow` interface;
            defaults to a `connection_workflow.AMQPConnectionWorkflow` instance
            with default values for optional args.
        :returns: Connection workflow instance in use. The user should limit
            their interaction with this object only to it's `abort()` method.
        :rtype: connection_workflow.AbstractAMQPConnectionWorkflow

        """
        raise NotImplementedError

    @classmethod
    def _start_connection_workflow(
        cls, 
        connection_configs: Sequence[connection.Parameters],
        connection_factory: Callable[[connection.Parameters], BaseConnection],
        nbio: nbio_interface.AbstractIOServices,
        workflow: Optional[connection_workflow.AbstractAMQPConnectionWorkflow],
        on_done: Callable[[Union[connection.Connection, connection_workflow.AMQPConnectorException]], None]
    ) -> connection_workflow.AbstractAMQPConnectionWorkflow:
        """Helper function for custom implementations of `create_connection()`.

        :param sequence connection_configs: A sequence of one or more
            `pika.connection.Parameters`-based objects.
        :param callable connection_factory: A function that takes
            `pika.connection.Parameters` as its only arg and returns a brand new
            `pika.connection.Connection`-based adapter instance each time it is
            called. The factory must instantiate the connection with
            `internal_connection_workflow=False`.
        :param pika.adapters.utils.nbio_interface.AbstractIOServices nbio:
        :param connection_workflow.AbstractAMQPConnectionWorkflow | None workflow:
            Pass an instance of an implementation of the
            `connection_workflow.AbstractAMQPConnectionWorkflow` interface;
            defaults to a `connection_workflow.AMQPConnectionWorkflow` instance
            with default values for optional args.
        :param callable on_done: as defined in
            :py:meth:`connection_workflow.AbstractAMQPConnectionWorkflow.start()`.
        :returns: Connection workflow instance in use. The user should limit
            their interaction with this object only to it's `abort()` method.
        :rtype: connection_workflow.AbstractAMQPConnectionWorkflow

        """
        if workflow is None:
            workflow = connection_workflow.AMQPConnectionWorkflow()
            LOGGER.debug('Created default connection workflow %r', workflow)

        if isinstance(workflow, connection_workflow.AMQPConnectionWorkflow):
            workflow.set_io_services(nbio)

        def create_connector() -> connection_workflow.AMQPConnector:
            """`AMQPConnector` factory."""
            return connection_workflow.AMQPConnector(
                lambda params: _StreamingProtocolShim(
                    connection_factory(params)),
                nbio)

        workflow.start(
            connection_configs=connection_configs,
            connector_factory=create_connector,
            native_loop=nbio.get_native_ioloop(),
            on_done=functools.partial(cls._unshim_connection_workflow_callback,
                                      on_done))  # type: ignore

        return workflow

    @property
    def ioloop(self) -> Any:
        """
        :returns: the native I/O loop instance underlying async services selected
            by user or the default selected by the specialized connection
            adapter (e.g., Twisted reactor, `asyncio.SelectorEventLoop`,
            `select_connection.IOLoop`, etc.)
        :rtype: object
        """
        return self._nbio.get_native_ioloop()

    def _adapter_call_later(self, delay: Union[int, float], callback: Callable[[], None]) -> object:
        """Implement
        :py:meth:`pika.connection.Connection._adapter_call_later()`.

        """
        return self._nbio.call_later(delay, callback)

    def _adapter_remove_timeout(self, timeout_id: object) -> None:
        """Implement
        :py:meth:`pika.connection.Connection._adapter_remove_timeout()`.

        """
        timeout_id.cancel()  # type: ignore

    def _adapter_add_callback_threadsafe(self, callback: Callable[..., Any]) -> None:
        """Implement
        :py:meth:`pika.connection.Connection._adapter_add_callback_threadsafe()`.

        """
        if not callable(callback):
            raise TypeError(
                'callback must be a callable, but got {!r}'.format(callback))

        self._nbio.add_callback_threadsafe(callback)

    def _adapter_connect_stream(self) -> None:
        """Initiate full-stack connection establishment asynchronously for
        internally-initiated connection bring-up.

        Upon failed completion, we will invoke
        `Connection._on_stream_terminated()`. NOTE: On success,
        the stack will be up already, so there is no corresponding callback.

        """
        self._connection_workflow = connection_workflow.AMQPConnectionWorkflow(
            _until_first_amqp_attempt=True)

        self._connection_workflow.set_io_services(self._nbio)

        def create_connector() -> connection_workflow.AMQPConnector:
            """`AMQPConnector` factory"""
            return connection_workflow.AMQPConnector(
                lambda _params: _StreamingProtocolShim(self), self._nbio)

        self._connection_workflow.start(
            [self.params],
            connector_factory=create_connector,
            native_loop=self._nbio.get_native_ioloop(),
            on_done=functools.partial(self._unshim_connection_workflow_callback,  # type: ignore
                                      self._on_connection_workflow_done))  # type: ignore

    @staticmethod
    def _unshim_connection_workflow_callback(
        user_on_done: Callable[[Union[BaseConnection, BaseException]], None], 
        shim_or_exc: Union[_StreamingProtocolShim, Exception]
    ) -> None:
        """

        :param callable user_on_done: user's `on_done` callback as defined in
            :py:meth:`connection_workflow.AbstractAMQPConnectionWorkflow.start()`.
        :param _StreamingProtocolShim | Exception shim_or_exc:
        """
        result = shim_or_exc
        if isinstance(result, _StreamingProtocolShim):
            result = result.conn

        user_on_done(result)

    def _abort_connection_workflow(self) -> None:
        """Asynchronously abort connection workflow. Upon
        completion, `Connection._on_stream_terminated()` will be called with None
        as the error argument.

        Assumption: may be called only while connection is opening.

        """
        assert not self._opened, (
            '_abort_connection_workflow() may be called only when '
            'connection is opening.')

        if self._transport is None:
            # NOTE: this is possible only when user calls Connection.close() to
            # interrupt internally-initiated connection establishment.
            # self._connection_workflow.abort() would not call
            # Connection.close() before pairing of connection with transport.
            assert self._internal_connection_workflow, (
                'Unexpected _abort_connection_workflow() call with '
                'no transport in external connection workflow mode.')

            # This will result in call to _on_connection_workflow_done() upon
            # completion
            self._connection_workflow.abort()  # type: ignore
        else:
            # NOTE: we can't use self._connection_workflow.abort() in this case,
            # because it would result in infinite recursion as we're called
            # from Connection.close() and _connection_workflow.abort() calls
            # Connection.close() to abort a connection that's already been
            # paired with a transport. During internally-initiated connection
            # establishment, AMQPConnectionWorkflow will discover that user
            # aborted the connection when it receives
            # pika.exceptions.ConnectionOpenAborted.

            # This completes asynchronously, culminating in call to our method
            # `connection_lost()`
            self._transport.abort()

    def _on_connection_workflow_done(self, conn_or_exc: Union[BaseConnection, Exception]) -> None:
        """`AMQPConnectionWorkflow` completion callback.

        :param BaseConnection | Exception conn_or_exc: Our own connection
            instance on success; exception on failure. See
            `AbstractAMQPConnectionWorkflow.start()` for details.

        """
        LOGGER.debug('Full-stack connection workflow completed: %r',
                     conn_or_exc)

        self._connection_workflow = None

        # Notify protocol of failure
        if isinstance(conn_or_exc, Exception):
            self._transport = None
            if isinstance(conn_or_exc,
                          connection_workflow.AMQPConnectionWorkflowAborted):
                LOGGER.info('Full-stack connection workflow aborted: %r',
                            conn_or_exc)
                # So that _handle_connection_workflow_failure() will know it's
                # not a failure
                conn_or_exc = None  # type: ignore
            else:
                LOGGER.error('Full-stack connection workflow failed: %r',
                             conn_or_exc)
                if (isinstance(conn_or_exc,
                               connection_workflow.AMQPConnectionWorkflowFailed)
                        and isinstance(
                            conn_or_exc.exceptions[-1], connection_workflow.
                            AMQPConnectorSocketConnectError)):
                    conn_or_exc = pika.exceptions.AMQPConnectionError(
                        conn_or_exc)

            self._handle_connection_workflow_failure(conn_or_exc)  # type: ignore
        else:
            # NOTE: On success, the stack will be up already, so there is no
            #       corresponding callback.
            assert conn_or_exc is self, \
                'Expected self conn={!r} from workflow, but got {!r}.'.format(
                    self, conn_or_exc)

    def _handle_connection_workflow_failure(self, error: Union[Exception, None]) -> None:
        """Handle failure of self-initiated stack bring-up and call
        `Connection._on_stream_terminated()` if connection is not in closed state
        yet. Called by adapter layer when the full-stack connection workflow
        fails.

        :param Exception | None error: exception instance describing the reason
            for failure or None if the connection workflow was aborted.
        """
        if error is None:
            LOGGER.info('Self-initiated stack bring-up aborted.')
        else:
            LOGGER.error('Self-initiated stack bring-up failed: %r', error)

        if not self.is_closed:
            self._on_stream_terminated(error)
        else:
            # This may happen when AMQP layer bring up was started but did not
            # complete
            LOGGER.debug('_handle_connection_workflow_failure(): '
                         'suppressing - connection already closed.')

    def _adapter_disconnect_stream(self) -> None:
        """Asynchronously bring down the streaming transport layer and invoke
        `Connection._on_stream_terminated()` asynchronously when complete.

        """
        if not self._opened:
            self._abort_connection_workflow()
        else:
            # This completes asynchronously, culminating in call to our method
            # `connection_lost()`
            self._transport.abort()  # type: ignore

    def _adapter_emit_data(self, data: bytes) -> None:
        """Take ownership of data and send it to AMQP server as soon as
        possible.

        :param bytes data:

        """
        self._transport.write(data)  # type: ignore

    def _proto_connection_made(self, transport: nbio_interface.AbstractStreamTransport) -> None:
        """Introduces transport to protocol after transport is connected.

        :py:class:`.utils.nbio_interface.AbstractStreamProtocol` implementation.

        :param nbio_interface.AbstractStreamTransport transport:
        :raises Exception: Exception-based exception on error

        """
        self._transport = transport

        # Let connection know that stream is available
        self._on_stream_connected()

    def _proto_connection_lost(self, error: Union[BaseException, None]) -> None:
        """Called upon loss or closing of TCP connection.

        :py:class:`.utils.nbio_interface.AbstractStreamProtocol` implementation.

        NOTE: `connection_made()` and `connection_lost()` are each called just
        once and in that order. All other callbacks are called between them.

        :param BaseException | None error: An exception (check for
            `BaseException`) indicates connection failure. None indicates that
            connection was closed on this side, such as when it's aborted or
            when `AbstractStreamProtocol.eof_received()` returns a falsy result.
        :raises Exception: Exception-based exception on error

        """
        self._transport = None

        if error is None:
            # Either result of `eof_received()` or abort
            if self._got_eof:
                error = pika.exceptions.StreamLostError(
                    'Transport indicated EOF')
        else:
            error = pika.exceptions.StreamLostError(
                f'Stream connection lost: {error!r}')

        LOGGER.log(logging.DEBUG if error is None else logging.ERROR,
                   'connection_lost: %r', error)

        self._on_stream_terminated(error)

    def _proto_eof_received(self) -> bool:  # pylint: disable=R0201
        """Called after the remote peer shuts its write end of the connection.
        :py:class:`.utils.nbio_interface.AbstractStreamProtocol` implementation.

        :returns: A falsy value (including None) will cause the transport to
            close itself, resulting in an eventual `connection_lost()` call
            from the transport. If a truthy value is returned, it will be the
            protocol's responsibility to close/abort the transport.
        :rtype: falsy|truthy
        :raises Exception: Exception-based exception on error

        """
        LOGGER.error('Transport indicated EOF.')

        self._got_eof = True

        # This is how a reset connection will typically present itself
        # when we have nothing to send to the server over plaintext stream.
        #
        # Have transport tear down the connection and invoke our
        # `connection_lost` method
        return False

    def _proto_data_received(self, data: bytes) -> None:
        """Called to deliver incoming data from the server to the protocol.

        :py:class:`.utils.nbio_interface.AbstractStreamProtocol` implementation.

        :param data: Non-empty data bytes.
        :raises Exception: Exception-based exception on error

        """
        self._on_data_available(data)


class _StreamingProtocolShim(nbio_interface.AbstractStreamProtocol):
    """Shim for callbacks from transport so that we BaseConnection can
    delegate to private methods, thus avoiding contamination of API with
    methods that look public, but aren't.

    """

    # Override AbstractStreamProtocol abstract methods to enable instantiation
    connection_made = None  # type: ignore
    connection_lost = None  # type: ignore
    eof_received = None  # type: ignore
    data_received = None  # type: ignore

    def __init__(self, conn: BaseConnection) -> None:
        """
        :param BaseConnection conn:
        """
        self.conn = conn
        # pylint: disable=W0212
        self.connection_made = conn._proto_connection_made  # type: ignore[has-type]
        self.connection_lost = conn._proto_connection_lost  # type: ignore[has-type]
        self.eof_received = conn._proto_eof_received  # type: ignore[has-type]
        self.data_received = conn._proto_data_received  # type: ignore[has-type]

    def __getattr__(self, attr: str) -> Any:
        """Proxy inexistent attribute requests to our connection instance
        so that AMQPConnectionWorkflow/AMQPConnector may treat the shim as an
        actual connection.

        """
        return getattr(self.conn, attr)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: {self.conn!r}'
