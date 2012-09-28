# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
from tornado import ioloop
import time

from pika.adapters import base_connection


class TornadoConnection(base_connection.BaseConnection):

    def __init__(self, parameters=None,
                 on_open_callback=None,
                 reconnection_strategy=None,
                 io_loop=None,
                 stop_ioloop_on_close=False):
        super(TornadoConnection, self).__init__(parameters, on_open_callback,
                                                reconnection_strategy,
                                                stop_ioloop_on_close)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker"""
        super(TornadoConnection, self)._adapter_connect()
        self.ioloop = ioloop.IOLoop.instance()
        self.ioloop.add_handler(self.socket.fileno(),
                                self._handle_events,
                                self.event_state)
        self._on_connected()

    def _adapter_disconnect(self):
        """Disconnect from the RabbitMQ broker"""
        super(TornadoConnection, self)._adapter_disconnect()
        self.ioloop.remove_handler(self.socket.fileno())

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        return self.ioloop.add_timeout(time.time() + deadline, callback_method)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :rtype: str

        """
        return self.ioloop.remove_timeout(timeout_id)
