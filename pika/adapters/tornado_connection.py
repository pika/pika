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

# Redefine our constants with Tornado's
if IOLoop:
    ERROR = ioloop.IOLoop.ERROR
    READ = ioloop.IOLoop.READ
    WRITE = ioloop.IOLoop.WRITE


class TornadoConnection(BaseConnection):

    def __init__(self, parameters=None,
                 on_open_callback=None,
                 reconnection_strategy=None):

        # Validate we have Tornado installed
        if not IOLoop:
            raise ImportError("Tornado not installed")

        BaseConnection.__init__(self, parameters, on_open_callback,
                                reconnection_strategy)

    def _adapter_connect(self):
        """
        Connect to the given host and port
        """
        BaseConnection._adapter_connect(self)

        # Setup our ioloop
        self.ioloop = IOLoop.instance()

        # Setup a periodic callbacks
        _pc = ioloop.PeriodicCallback(self._manage_event_state,
                                      0.25,
                                      self.ioloop)
        _pc.start()

        # Add the ioloop handler for the event state
        self.ioloop.add_handler(self.socket.fileno(),
                                self._handle_events,
                                self.event_state)

        # Let everyone know we're connected
        self._on_connected()

    def _adapter_disconnect(self):
        """
        Disconnect from the RabbitMQ Broker
        """
        # Remove from the IOLoop
        self.ioloop.remove_handler(self.socket.fileno())

        # Close our socket since the Connection class told us to do so
        self.socket.close()

        # Let the developer know to look for this circumstance
        warn("Tornado IOLoop may be running but Pika has shutdown.")
