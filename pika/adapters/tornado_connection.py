# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
try:
    from tornado import ioloop
    IOLoop = ioloop.IOLoop
except ImportError:
    IOLoop = None

from warnings import warn

from pika.adapters.base_connection import BaseConnection
from pika.exceptions import AMQPConnectionError
from pika.reconnection_strategies import NullReconnectionStrategy

# Redefine our constants with Tornado's
if IOLoop:
    ERROR = ioloop.IOLoop.ERROR
    READ = ioloop.IOLoop.READ
    WRITE = ioloop.IOLoop.WRITE


class TornadoConnection(BaseConnection):

    def __init__(self, parameters=None,
                 on_open_callback=None,
                 reconnection_strategy=None,
                 callback_interval=250):

        # Validate we have Tornado installed
        if not IOLoop:
            raise ImportError("Tornado not installed")

        self.callback_interval = callback_interval
        self._pc = None

        BaseConnection.__init__(self, parameters, on_open_callback,
                                reconnection_strategy)

    def _adapter_connect(self):
        """
        Connect to the given host and port
        """
        # Setup our ioloop
        if self.ioloop is None:
            self.ioloop = IOLoop.instance()

        # Setup a periodic callbacks
        if self._pc is None:
            self._pc = ioloop.PeriodicCallback(self._manage_event_state,
                                               self.callback_interval,
                                               self.ioloop)

        try:
            # Connect to RabbitMQ and start polling
            BaseConnection._adapter_connect(self)
            self.start_poller()

            # Let everyone know we're connected
            self._on_connected()
        except AMQPConnectionError, e:
            # If we don't have RS just raise the exception
            if isinstance(self.reconnection, NullReconnectionStrategy):
                raise e
            # Trying to reconnect
            self.reconnection.on_connection_closed(self)

    def _handle_disconnect(self):
        """
        Called internally when we know our socket is disconnected already
        """
        self.stop_poller()

        BaseConnection._handle_disconnect(self)

    def _adapter_disconnect(self):
        """
        Disconnect from the RabbitMQ Broker
        """
        self.stop_poller()

        BaseConnection._adapter_disconnect(self)

    def start_poller(self):
        # Start periodic _manage_event_state
        self._pc.start()

        # Add the ioloop handler for the event state
        self.ioloop.add_handler(self.socket.fileno(),
                                self._handle_events,
                                self.event_state)

    def stop_poller(self):
        # Stop periodic _manage_event_state
        self._pc.stop()

        # Remove from the IOLoop
        self.ioloop.remove_handler(self.socket.fileno())