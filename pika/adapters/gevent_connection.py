"""Use pika with the gevent event loop"""

import socket
import logging

import gevent
import gevent.socket
import gevent.event
import gevent.queue

from pika.adapters import base_connection
from pika.adapters.utils import nbio_interface
from pika.adapters.utils import io_services_utils

LOGGER = logging.getLogger(__name__)


class GeventIOServices(io_services_utils.SocketConnectionMixin,
                       io_services_utils.StreamingConnectionMixin,
                       nbio_interface.AbstractIOServices,
                       nbio_interface.AbstractFileDescriptorServices):
    """GeventIOServices wraps gevent functionality in the
    nbio_interface.AbstractIOServices interface.
    """

    def __init__(self):
        """Create a new instance of GeventIOServices."""
        self._callbacks = gevent.queue.Queue()
        self._io_loop_running = False
        self._readers = {}
        self._writers = {}
        self._timers = {}
        self._timer_id = 0
        self._stop_before_start = False

    def get_native_ioloop(self):
        """Return the gevent event loop we're using.

        Note: We return self instead of the actual gevent hub because
        the base_connection.py code expects the return value to have
        certain methods that the Hub doesn't have.
        """
        return self

    def close(self):
        """Release IOLoop's resources."""
        self._io_loop_running = False
        self._callbacks = gevent.queue.Queue()
        self._readers = {}
        self._writers = {}
        self._timers = {}

    def run(self):
        """Run the I/O loop."""
        if self._io_loop_running:
            LOGGER.warning('GeventIOServices is already running')
            return

        # If stop was called before run, don't enter the loop
        if not self._io_loop_running and hasattr(self, '_stop_before_start') and self._stop_before_start:
            LOGGER.info("Not starting I/O loop because stop was called before start")
            self._stop_before_start = False
            return

        self._io_loop_running = True

        # Process callbacks until the IOLoop is stopped
        while self._io_loop_running:
            try:
                callback = self._callbacks.get(block=False)
                callback()
            except gevent.queue.Empty:
                # No callbacks, let gevent run other greenlets
                gevent.sleep(0)

    def stop(self):
        """Request exit from the ioloop."""
        LOGGER.debug("Stopping I/O loop")

        # If run hasn't been called yet, set flag to skip running the loop
        if not self._io_loop_running:
            LOGGER.debug("I/O loop stop requested before start")
            self._stop_before_start = True

        self._io_loop_running = False

    def add_callback_threadsafe(self, callback):
        """Add a callback to be run in the event loop thread."""
        self._callbacks.put(callback)

    def call_later(self, delay, callback):
        """Add the callback to the IOLoop timer."""
        self._timer_id += 1
        timer_id = self._timer_id

        greenlet = gevent.spawn_later(delay, self._timer_callback, timer_id, callback)
        self._timers[timer_id] = greenlet

        return _GeventTimerReference(timer_id, self)

    def _timer_callback(self, timer_id, callback):
        """Handle timer expiration."""
        if timer_id in self._timers:
            del self._timers[timer_id]
            callback()

    def getaddrinfo(self, host, port, on_done, family=0, socktype=0, proto=0, flags=0):
        """Perform the equivalent of socket.getaddrinfo() asynchronously."""
        def _getaddrinfo():
            try:
                result = socket.getaddrinfo(host, port, family, socktype, proto, flags)
                self.add_callback_threadsafe(lambda: on_done(result))
            except Exception as e:
                self.add_callback_threadsafe(lambda: on_done(e))

        greenlet = gevent.spawn(_getaddrinfo)
        return _GeventIOReference(greenlet)

    def set_reader(self, fd, on_readable):
        """Call the given callback when the file descriptor is readable."""
        if fd in self._readers:
            # Remove existing watcher if any
            self.remove_reader(fd)

        # Create watcher for read events
        watcher = gevent.get_hub().loop.io(fd, 1)  # 1 = READ
        watcher.start(lambda: self._handle_read(fd))
        self._readers[fd] = (watcher, on_readable)

    def remove_reader(self, fd):
        """Stop watching the given file descriptor for readability."""
        if fd in self._readers:
            watcher, _ = self._readers[fd]
            watcher.stop()
            del self._readers[fd]
            return True
        return False

    def set_writer(self, fd, on_writable):
        """Call the given callback when the file descriptor is writable."""
        if fd in self._writers:
            # Remove existing watcher if any
            self.remove_writer(fd)

        # Create watcher for write events
        watcher = gevent.get_hub().loop.io(fd, 2)  # 2 = WRITE
        watcher.start(lambda: self._handle_write(fd))
        self._writers[fd] = (watcher, on_writable)

    def remove_writer(self, fd):
        """Stop watching the given file descriptor for writability."""
        if fd in self._writers:
            watcher, _ = self._writers[fd]
            watcher.stop()
            del self._writers[fd]
            return True
        return False

    def _handle_read(self, fd):
        """Handle read event from gevent."""
        if fd in self._readers:
            _, callback = self._readers[fd]
            callback()

    def _handle_write(self, fd):
        """Handle write event from gevent."""
        if fd in self._writers:
            _, callback = self._writers[fd]
            callback()


class _GeventTimerReference(nbio_interface.AbstractTimerReference):
    """Timer reference implementation for GeventIOServices."""

    def __init__(self, timer_id, io_services):
        """Initialize the timer reference.

        :param int timer_id: The ID for the timer
        :param GeventIOServices io_services: The services implementation
        """
        self._timer_id = timer_id
        self._io_services = io_services

    def cancel(self):
        """Cancel the timer."""
        if self._timer_id in self._io_services._timers:
            greenlet = self._io_services._timers[self._timer_id]
            greenlet.kill()
            del self._io_services._timers[self._timer_id]


class _GeventIOReference(nbio_interface.AbstractIOReference):
    """IO reference implementation for GeventIOServices."""

    def __init__(self, greenlet):
        """Initialize the IO reference.

        :param gevent.Greenlet greenlet: The greenlet executing the IO operation
        """
        self._greenlet = greenlet

    def cancel(self):
        """Cancel the IO operation."""
        if self._greenlet and not self._greenlet.dead:
            self._greenlet.kill()
            return True
        return False


class GeventConnection(base_connection.BaseConnection):
    """The GeventConnection runs on the Gevent event loop."""

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 custom_ioloop=None,
                 internal_connection_workflow=True):
        """Create a new instance of the GeventConnection class, connecting
        to RabbitMQ automatically.

        :param pika.connection.Parameters parameters: Connection parameters
        :param callable on_open_callback: Called when the connection is open
        :param callable on_open_error_callback: Called if the connection can't
            be established or connection establishment is interrupted by
            `Connection.close()`
        :param callable on_close_callback: Called when the connection is closed
        :param GeventIOServices custom_ioloop: IOLoop instance to use instead of creating one
        :param bool internal_connection_workflow: Whether to use internal or
            external connection workflow
        """
        # Patch socket module before establishing connection
        self._patch_socket()

        # Create or use provided IO services
        self._io_services = custom_ioloop or GeventIOServices()

        super().__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            self._io_services,
            internal_connection_workflow)

    def _patch_socket(self):
        """Patch socket module for gevent compatibility if needed."""
        # Check if socket is already patched by gevent
        if not hasattr(socket, '_gevent_patch'):
            socket.socket = gevent.socket.socket
            socket.create_connection = gevent.socket.create_connection
            socket._gevent_patch = True

    def _adapter_call_later(self, delay, callback):
        """Implementation of
        :py:meth:`pika.connection.Connection._adapter_call_later`.

        """
        return self._nbio.call_later(delay, callback)

    def _adapter_remove_timeout(self, timeout_handle):
        """Implementation of
        :py:meth:`pika.connection.Connection._adapter_remove_timeout`.

        """
        timeout_handle.cancel()

    def _adapter_add_callback_threadsafe(self, callback):
        """Implementation of
        :py:meth:`pika.connection.Connection._adapter_add_callback_threadsafe`.

        """
        self._nbio.add_callback_threadsafe(callback)

    def _adapter_connect_stream(self):
        """Implementation of
        :py:meth:`pika.connection.Connection._adapter_connect_stream`.

        """
        # BaseConnection handles this via AMQPConnectionWorkflow
        super()._adapter_connect_stream()

    def _adapter_disconnect_stream(self):
        """Implementation of
        :py:meth:`pika.connection.Connection._adapter_disconnect_stream`.

        """
        # BaseConnection handles this
        super()._adapter_disconnect_stream()

    def _adapter_emit_data(self, data):
        """Implementation of
        :py:meth:`pika.connection.Connection._adapter_emit_data`.

        """
        self._transport.write(data)

    @classmethod
    def create_connection(cls,
                          connection_configs,
                          on_done,
                          custom_ioloop=None,
                          workflow=None):
        """Implement
        :py:classmethod:`pika.adapters.BaseConnection.create_connection()`.

        """
        if custom_ioloop is not None and not isinstance(
                custom_ioloop, GeventIOServices):
            raise ValueError(
                'GeventConnection: custom_ioloop must be a GeventIOServices '
                'instance or None, but got {!r}'.format(custom_ioloop))

        nbio = custom_ioloop or GeventIOServices()

        # Create a connection factory that doesn't pass custom_ioloop directly,
        # but rather passes our nbio through the required BaseConnection constructor arg
        def connection_factory(params):
            return cls(
                parameters=params,
                on_open_callback=None,
                on_open_error_callback=None,
                on_close_callback=None,
                internal_connection_workflow=False)

        return cls._start_connection_workflow(
            connection_configs=connection_configs,
            connection_factory=connection_factory,
            nbio=nbio,
            workflow=workflow,
            on_done=on_done)