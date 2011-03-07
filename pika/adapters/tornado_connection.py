# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
from tornado.ioloop import IOLoop
from warnings import warn

from pika.adapters.base_connection import BaseConnection

# Redefine our constants with Tornado's
ERROR = tornado.ioloop.IOLoop.ERROR
READ = tornado.ioloop.IOLoop.READ
WRITE = tornado.ioloop.IOLoop.WRITE


class TornadoConnection(BaseConnection):

    def _adapter_connect(self, host, port):
        """
        Connect to the given host and port
        """
        BaseConnection._adapter_connect(self, host, port)

        # Setup our ioloop
        self.ioloop = IOLoop.instance()

        # Add the ioloop handler for the event state
        self.ioloop.add_handler(self.socket.fileno(),
                                self._handle_events,
                                self.event_state)

        # Let everyone know we're connected
        self._on_connected()

    def _adapter_disconnect(self):
        # Remove from the IOLoop
        self.ioloop.remove_handler(self.socket.fileno())

        # Close our socket since the Connection class told us to do so
        self.socket.close()

        # Let the developer know to look for this circumstance
        warn("Tornado IOLoop may be running but Pika has shutdown.")
