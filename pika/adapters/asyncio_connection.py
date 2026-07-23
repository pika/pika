"""Use pika with the Asyncio EventLoop."""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Sequence

from pika._utils import override
from pika.adapters import base_connection
from pika.adapters.utils import connection_workflow, io_services_utils, nbio_interface

if TYPE_CHECKING:
    from pika import connection

LOGGER = logging.getLogger(__name__)


class AsyncioConnection(base_connection.BaseConnection):
    """The AsyncioConnection runs on the Asyncio EventLoop."""

    def __init__(
            self,
            parameters: connection.Parameters | None = None,
            on_open_callback: Callable[[connection.Connection], None] |
        None = None,
            on_open_error_callback: Callable[
                [connection.Connection, BaseException], None] | None = None,
            on_close_callback: Callable[[connection.Connection, BaseException],
                                        None] | None = None,
            custom_ioloop: asyncio.AbstractEventLoop |
        nbio_interface.AbstractIOServices | None = None,
            internal_connection_workflow: bool = True) -> None:
        """
        Create a new instance of the AsyncioConnection class, connecting to RabbitMQ automatically.

        :param parameters: Connection parameters
        :param on_open_callback: The method to call when the connection
            is open
        :param on_open_error_callback: Callback (or None) with signature
            ``(Connection, BaseException) -> None``; called if the connection
            can't be established or connection establishment is interrupted by
            `Connection.close()`: on_open_error_callback(Connection, exception).
        :param on_close_callback: Callback (or None) with signature
            ``(Connection, BaseException) -> None``; called when a previously fully
            open connection is closed:
            `on_close_callback(Connection, exception)`, where `exception` is
            either an instance of `exceptions.ConnectionClosed` if closed by
            user or broker or exception of another type that describes the cause
            of connection failure.
        :param custom_ioloop: Optional custom loop object
            (``asyncio.AbstractEventLoop`` or
            ``nbio_interface.AbstractIOServices``). Defaults to the running event loop, or a new event
            loop when none is running.
        :param internal_connection_workflow: True for autonomous connection
            establishment which is default; False for externally-managed
            connection workflow via the `create_connection()` factory.
        """
        if isinstance(custom_ioloop, nbio_interface.AbstractIOServices):
            nbio = custom_ioloop
        else:
            nbio = _AsyncioIOServicesAdapter(custom_ioloop)

        super().__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            nbio,
            internal_connection_workflow=internal_connection_workflow)

    @classmethod
    @override
    def create_connection(
        cls,
        connection_configs: Sequence[connection.Parameters],
        on_done: Callable[[(connection.Connection |
                            connection_workflow.AMQPConnectorException)], None],
        custom_ioloop: asyncio.AbstractEventLoop | None = None,
        workflow: connection_workflow.AbstractAMQPConnectionWorkflow |
        None = None
    ) -> connection_workflow.AbstractAMQPConnectionWorkflow:
        """
        :param connection_configs: One or more connection parameter objects
        :param on_done: Callback to report when connection workflow is done
        :param custom_ioloop: Optional custom event loop to use for the connection workflow
        :param workflow: Optional connection workflow instance to use; if None,
            a default workflow will be created
        """
        nbio = _AsyncioIOServicesAdapter(custom_ioloop)

        def connection_factory(params) -> AsyncioConnection:
            """
            Connection factory.

            :param params: Connection parameters
            """
            if params is None:
                raise ValueError('Expected pika.connection.Parameters '
                                 'instance, but got None in params arg.')
            return cls(parameters=params,
                       custom_ioloop=nbio,
                       internal_connection_workflow=False)

        return cls._start_connection_workflow(
            connection_configs=connection_configs,
            connection_factory=connection_factory,
            nbio=nbio,
            workflow=workflow,
            on_done=on_done)


class _AsyncioIOServicesAdapter(io_services_utils.SocketConnectionMixin,
                                io_services_utils.StreamingConnectionMixin,
                                nbio_interface.AbstractFileDescriptorIOServices
                               ):
    """Implements
    :py:class:`.utils.nbio_interface.AbstractIOServices` interface
    on top of `asyncio`.

    NOTE:
    :py:class:`.utils.nbio_interface.AbstractFileDescriptorServices`
    interface is only required by the mixins.

    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """
        :param loop: If None, uses the
            running event loop via asyncio.get_running_loop(), or creates
            a new one via asyncio.new_event_loop() when no loop is running
            (e.g. when called from a non-async thread).

        """
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                if sys.platform == 'win32':
                    loop = asyncio.SelectorEventLoop()
                else:
                    loop = asyncio.new_event_loop()
        if sys.platform == 'win32' and not isinstance(
                loop, asyncio.SelectorEventLoop):
            raise TypeError(
                'On Windows, Pika requires an asyncio.SelectorEventLoop '
                'because it uses add_reader/add_writer. The running loop '
                f'is {type(loop).__name__}. Either pass a SelectorEventLoop '
                'via custom_ioloop, or run with '
                'asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop).')
        self._loop = loop

    @override
    def get_native_ioloop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @override
    def close(self) -> None:
        self._loop.close()

    @override
    def run(self) -> None:
        self._loop.run_forever()

    @override
    def stop(self) -> None:
        self._loop.stop()

    @override
    def add_callback_threadsafe(self, callback: Callable[[], None]) -> None:
        """
        :param callback: The callback to call from the thread
        """
        self._loop.call_soon_threadsafe(callback)

    @override
    def call_later(self, delay: float,
                   callback: Callable[[], None]) -> _TimerHandle:
        """
        :param delay: Delay in seconds
        :param callback: The callback to call after the delay
        """
        return _TimerHandle(self._loop.call_later(delay, callback))

    @override
    def getaddrinfo(self,
                    host: str,
                    port: int,
                    on_done: Callable[..., None],
                    family: int = 0,
                    socktype: int = 0,
                    proto: int = 0,
                    flags: int = 0) -> nbio_interface.AbstractIOReference:
        """
        :param host: Hostname or IP address
        :param port: TCP port number
        :param on_done: The callback to call with the result of getaddrinfo
        :param family: Socket address family (e.g. ``socket.AF_INET``)
        :param socktype: Socket type (e.g. ``socket.SOCK_STREAM``)
        :param proto: Protocol number (0 for default)
        :param flags: :func:`socket.getaddrinfo` flags
        """
        return self._schedule_and_wrap_in_io_ref(
            self._loop.getaddrinfo(host,
                                   port,
                                   family=family,
                                   type=socktype,
                                   proto=proto,
                                   flags=flags), on_done)

    @override
    def set_reader(self, fd: int, on_readable: Callable[[], None]) -> None:
        """
        :param fd: File descriptor
        :param on_readable: The callback to call when the file descriptor is readable
        """
        self._loop.add_reader(fd, on_readable)
        LOGGER.debug('set_reader(%s, _)', fd)

    @override
    def remove_reader(self, fd: int) -> bool:
        """
        :param fd: File descriptor
        """
        LOGGER.debug('remove_reader(%s)', fd)
        return self._loop.remove_reader(fd)

    @override
    def set_writer(self, fd: int, on_writable: Callable[[], None]) -> None:
        """
        :param fd: File descriptor
        :param on_writable: The callback to call when the file descriptor is writable
        """
        self._loop.add_writer(fd, on_writable)
        LOGGER.debug('set_writer(%s, _)', fd)

    @override
    def remove_writer(self, fd: int) -> bool:
        """
        :param fd: File descriptor
        """
        LOGGER.debug('remove_writer(%s)', fd)
        return self._loop.remove_writer(fd)

    def _schedule_and_wrap_in_io_ref(
        self, coro: Awaitable[Any],
        on_done: Callable[[base_connection.BaseConnection | BaseException],
                          None]
    ) -> _AsyncioIOReference:
        """
        Schedule the coroutine to run and return _AsyncioIOReference.

        :param coro: Coroutine to schedule.
        :param on_done: User callback that takes the completion result or exception as its only arg.
            It will not be called if the operation was cancelled.
        """
        if not callable(on_done):
            raise TypeError(
                f'on_done arg must be callable, but got {on_done!r}')

        return _AsyncioIOReference(asyncio.ensure_future(coro, loop=self._loop),
                                   on_done)


class _TimerHandle(nbio_interface.AbstractTimerReference):
    """This module's adaptation of `nbio_interface.AbstractTimerReference`."""

    def __init__(self, handle: asyncio.Handle) -> None:
        """

        :param handle:
        """
        self._handle: asyncio.Handle | None = handle

    @override
    def cancel(self) -> None:
        """Cancel the timer handle."""
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None


class _AsyncioIOReference(nbio_interface.AbstractIOReference):
    """This module's adaptation of `nbio_interface.AbstractIOReference`."""

    def __init__(
        self, future: asyncio.Future,
        on_done: Callable[[base_connection.BaseConnection | BaseException],
                          None]
    ) -> None:
        """
        :param future:
        :param on_done: user callback that takes the completion result
            or exception as its only arg. It will not be called if the operation
            was cancelled.

        """
        if not callable(on_done):
            raise TypeError(
                f'on_done arg must be callable, but got {on_done!r}')

        self._future = future

        def on_done_adapter(future: asyncio.Future) -> None:
            """Handle completion callback from the future instance
            :param future: The future that completed
            """
            # NOTE: Asyncio schedules callback for cancelled futures, but pika
            # doesn't want that
            if not future.cancelled():
                on_done(future.exception() or future.result())

        future.add_done_callback(on_done_adapter)

    @override
    def cancel(self) -> bool:
        """
        Cancel pending operation.

        :returns: False if was already done or cancelled; True otherwise
        """
        return self._future.cancel()
