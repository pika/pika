# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import pika.log as log
import tornado.ioloop

from pika.adapters.base_connection import BaseConnection

# Redefine our constants with Tornado's
ERROR = tornado.ioloop.IOLoop.ERROR
READ = tornado.ioloop.IOLoop.READ
WRITE = tornado.ioloop.IOLoop.WRITE


class TornadoConnection(BaseConnection):

    @log.method_call
    def _adapter_connect(self, host, port):
        """
        Connect to the given host and port
        """
        BaseConnection._adapter_connect(self, host, port)

        # Setup our ioloop
        self.ioloop = tornado.ioloop.IOLoop.instance()

        # Add the ioloop handler for the event state
        self.ioloop.add_handler(self.socket.fileno(),
                                self._handle_events,
                                self.event_state)

        # Let everyone know we're connected
        self._on_connected()

    @log.method_call
    def _adapter_disconnect(self):
        # Remove from the IOLoop
        self.ioloop.remove_handler(self.socket.fileno())

        # Close our socket since the Connection class told us to do so
        self.socket.close()

        msg = "Tornado IOLoop may be running but Pika has shutdown."
        log.warning(msg)
