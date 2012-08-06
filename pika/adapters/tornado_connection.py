# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
try:
    from tornado import ioloop
    from tornado import stack_context
    IOLoop = ioloop.IOLoop
except ImportError:
    IOLoop = None

from pika.adapters.base_connection import BaseConnection
from pika.exceptions import AMQPConnectionError
from pika.reconnection_strategies import NullReconnectionStrategy

# Redefine our constants with Tornado's
if IOLoop:
    ERROR = ioloop.IOLoop.ERROR
    READ = ioloop.IOLoop.READ
    WRITE = ioloop.IOLoop.WRITE

import pika.log
import pika.callback
import time
import sys
import socket
import errno
import contextlib

class CallbackWrapper(object):
    __slots__ = ['handle', 'orig', 'one_shot', 'only']

    def __init__(self, handle, one_shot, only_caller=None):
        self.handle   = stack_context.wrap(handle)
        self.orig     = handle
        self.only     = only_caller
        self.one_shot = one_shot

    def __eq__(self, other):
        return self.orig == other.orig \
           and self.only == other.only \
           and self.one_shot == other.one_shot

    def __contains__(self, callback):
        return self.orig == callback

class CallbackManager(pika.callback.CallbackManager):
    CALLBACK_CLASS = CallbackWrapper

    def process(self, *args, **kwargs):
        with stack_context.NullContext():
            super(CallbackManager, self).process(*args, **kwargs)

class TornadoConnection(BaseConnection):

    def __init__(self, parameters=None,
                 on_open_callback=None,
                 reconnection_strategy=None,
                 callback_interval=250,
                 io_loop=None):

        # Validate we have Tornado installed
        if not IOLoop:
            raise ImportError("Tornado not installed")

        self.callback_interval = callback_interval
        self._pc = None
        self._default_ioloop = io_loop or IOLoop.instance()

        on_open_callback = stack_context.wrap(on_open_callback)
        
        with stack_context.StackContext(self._stack_context):
            BaseConnection.__init__(self, parameters, on_open_callback,
                                    reconnection_strategy, CallbackManager())

    @contextlib.contextmanager
    def _stack_context(self):
        try:
            yield
        except Exception:
            pika.log.warning("%s in Connection" % sys.exc_info()[0].__name__, exc_info = True)
            self.callbacks.process(0, '_on_connection_error', self, sys.exc_info())

    def add_on_error_callback(self, callback):
        """
        Add a callback notification when the connection raises an error.
        """
        self.callbacks.add(0, '_on_connection_error', callback, False)

    def _adapter_connect(self):
        """
        Connect to the given host and port
        """
        # Setup our ioloop
        if self.ioloop is None:
            self.ioloop = self._default_ioloop

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

        # Close up our Connection state
        self._on_connection_closed(None, True)

    def _adapter_disconnect(self):
        """
        Disconnect from the RabbitMQ Broker
        """
        self.stop_poller()

        # This is basically the same as BaseConnection._adapter_disconnect
        # Except this doesn't stop the ioloop

        # Close our socket
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except IOError, e:
            if e.errno != errno.ENOTCONN:
                raise

        self.socket.close()

        # Check our state on disconnect
        self._check_state_on_disconnect()
        self._on_connection_closed(None, True)

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
        try:
            self.ioloop.remove_handler(self.socket.fileno())
        except socket.error, e:
            pass
        except:
            pika.log.error("Error while removing handler for socket", exc_info = True)

    def add_timeout(self, deadline, callback):
        """
        Adds a timeout that will be called in `deadline` seconds.
        Not to be confused with tornado.ioloop.add_timeout()
        which accepts a timestamp or timedelta as the `deadline` argument.
        """
        # This override is required for the API
        # to be consistent with other Connection classes
        return self.ioloop.add_timeout(time.time() + deadline, callback)
