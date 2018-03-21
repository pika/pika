"""Use pika with the Tornado IOLoop"""
import logging
import time

from tornado import ioloop

from pika.adapters import base_connection

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
        self.sleep_counter = 0
        self.ioloop = custom_ioloop or ioloop.IOLoop.instance()
        super(TornadoConnection, self).__init__(parameters,
                                                on_open_callback,
                                                on_open_error_callback,
                                                on_close_callback,
                                                self.ioloop)

    def _adapter_connect(self):
        """Connect to the remote socket, adding the socket to the IOLoop if
        connected.

        :rtype: bool

        """
        error = super(TornadoConnection, self)._adapter_connect()
        if not error:
            self.ioloop.add_handler(self.socket.fileno(),
                                    self._handle_connection_socket_events,
                                    self.event_state)
        return error

    def _adapter_disconnect(self):
        """Disconnect from the RabbitMQ broker"""
        if self.socket:
            self.ioloop.remove_handler(self.socket.fileno())
        super(TornadoConnection, self)._adapter_disconnect()

    def add_timeout(self, deadline, callback):
        """Add the callback to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param float deadline: The number of seconds to wait to call callback
        :param method callback: The callback method
        :rtype: str

        """
        return self.ioloop.add_timeout(time.time() + deadline, callback)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :rtype: str

        """
        return self.ioloop.remove_timeout(timeout_id)

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

        self.ioloop.add_callback(callback)
