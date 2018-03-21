"""Use pika with the Tornado IOLoop

"""
import logging

from tornado import ioloop

from pika.adapters import base_connection, selector_ioloop_adapter


LOGGER = logging.getLogger(__name__)


class TornadoConnection(base_connection.BaseConnection):
    """The TornadoConnection runs on the Tornado IOLoop.
    """

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 custom_ioloop=None):
        """Create a new instance of the TornadoConnection class, connecting
        to RabbitMQ automatically

        :param pika.connection.Parameters parameters: Connection parameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param custom_ioloop: Override using the global IOLoop in Tornado

        """
        loop = custom_ioloop or ioloop.IOLoop.instance()
        super(TornadoConnection, self).__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            selector_ioloop_adapter.SelectorAsyncServicesAdapter(loop))
