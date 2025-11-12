"""Use pika with the Tornado IOLoop

"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Sequence, Union, TYPE_CHECKING

from tornado import ioloop

from pika.adapters.utils import nbio_interface, selector_ioloop_adapter
from pika.adapters import base_connection

if TYPE_CHECKING:
    from pika import connection
    from pika.adapters.utils import connection_workflow

LOGGER = logging.getLogger(__name__)


class TornadoConnection(base_connection.BaseConnection):
    """The TornadoConnection runs on the Tornado IOLoop.
    """

    def __init__(self,
                 parameters: Optional[connection.Parameters] = None,
                 on_open_callback: Optional[Callable[[connection.Connection],
                                                     None]] = None,
                 on_open_error_callback: Optional[Callable[
                     [connection.Connection, BaseException], None]] = None,
                 on_close_callback: Optional[Callable[
                     [connection.Connection, BaseException], None]] = None,
                 custom_ioloop: Optional[Union[
                     ioloop.IOLoop, nbio_interface.AbstractIOServices]] = None,
                 internal_connection_workflow: bool = True):
        """Create a new instance of the TornadoConnection class, connecting
        to RabbitMQ automatically.

        :param pika.connection.Parameters|None parameters: The connection
            parameters
        :param callable|None on_open_callback: The method to call when the
            connection is open
        :param callable|None on_open_error_callback: Called if the connection
            can't be established or connection establishment is interrupted by
            `Connection.close()`:
            on_open_error_callback(Connection, exception)
        :param callable|None on_close_callback: Called when a previously fully
            open connection is closed:
            `on_close_callback(Connection, exception)`, where `exception` is
            either an instance of `exceptions.ConnectionClosed` if closed by
            user or broker or exception of another type that describes the
            cause of connection failure
        :param ioloop.IOLoop|nbio_interface.AbstractIOServices|None custom_ioloop:
            Override using the global IOLoop in Tornado
        :param bool internal_connection_workflow: True for autonomous connection
            establishment which is default; False for externally-managed
            connection workflow via the `create_connection()` factory

        """
        if isinstance(custom_ioloop, nbio_interface.AbstractIOServices):
            nbio = custom_ioloop
        else:
            nbio = (selector_ioloop_adapter.SelectorIOServicesAdapter(
                custom_ioloop or ioloop.IOLoop.instance()))  # type: ignore
        super().__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            nbio,
            internal_connection_workflow=internal_connection_workflow)

    @classmethod
    def create_connection(
        cls,
        connection_configs: Sequence[connection.Parameters],
        on_done: Callable[[
            Union[connection.Connection,
                  connection_workflow.AMQPConnectorException]
        ], None],
        custom_ioloop: Optional[Any] = None,
        workflow: Optional[
            connection_workflow.AbstractAMQPConnectionWorkflow] = None
    ) -> connection_workflow.AbstractAMQPConnectionWorkflow:
        """Implement
        :py:classmethod::`pika.adapters.BaseConnection.create_connection()`.

        """
        nbio = selector_ioloop_adapter.SelectorIOServicesAdapter(
            custom_ioloop or ioloop.IOLoop.instance())  # type: ignore

        def connection_factory(
                params: Optional[connection.Parameters]) -> 'TornadoConnection':
            """Connection factory."""
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
