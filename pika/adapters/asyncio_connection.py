"""Use pika with the Asyncio EventLoop"""
import asyncio
from functools import partial

from pika.adapters import base_connection


class IOLoopAdapter:
    def __init__(self, loop):
        """
        Basic adapter for asyncio event loop

        :type loop: asyncio.AbstractEventLoop
        :param loop: Asyncio Loop

        """
        self.loop = loop

        self.handlers = {}
        self.readers = set()
        self.writers = set()

    def close(self):
        """Release ioloop's resources.

        This method is intended to be called by the application or test code
        only after the ioloop's outermost `start()` call returns. After calling
        `close()`, no other interaction with the closed instance of ioloop
        should be performed.

        """
        self.loop.close()

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the EventLoop timer to fire after deadline
        seconds. Returns a Handle to the timeout.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: asyncio.Handle

        """
        return self.loop.call_later(deadline, callback_method)

    @staticmethod
    def remove_timeout(handle):
        """
        Cancel asyncio.Handle

        :type handle: asyncio.Handle
        :rtype: bool
        """
        return handle.cancel()

    def add_callback_threadsafe(self, callback):
        """Requests a call to the given function as soon as possible in the
        context of this IOLoop's thread.

        NOTE: This is the only thread-safe method offered by the IOLoop adapter.
         All other manipulations of the IOLoop adapter and its parent connection
         must be performed from the connection's thread.

        For example, a thread may request a call to the
        `channel.basic_ack` method of a connection that is running in a
        different thread via

        ```
        connection.add_callback_threadsafe(
            functools.partial(channel.basic_ack, delivery_tag=...))
        ```

        :param method callback: The callback method; must be callable.

        """
        self.loop.call_soon_threadsafe(callback)

    def add_handler(self, fd, cb, event_state):
        """ Registers the given handler to receive the given events for ``fd``.

        The ``fd`` argument is an integer file descriptor.

        The ``event_state`` argument is a bitwise or of the constants
        ``base_connection.BaseConnection.READ``, ``base_connection.BaseConnection.WRITE``,
        and ``base_connection.BaseConnection.ERROR``.

        """

        if fd in self.handlers:
            raise ValueError("fd {} added twice".format(fd))
        self.handlers[fd] = cb

        if event_state & base_connection.BaseConnection.READ:
            self.loop.add_reader(
                fd,
                partial(
                    cb,
                    fd=fd,
                    events=base_connection.BaseConnection.READ
                )
            )
            self.readers.add(fd)

        if event_state & base_connection.BaseConnection.WRITE:
            self.loop.add_writer(
                fd,
                partial(
                    cb,
                    fd=fd,
                    events=base_connection.BaseConnection.WRITE
                )
            )
            self.writers.add(fd)

    def remove_handler(self, fd):
        """ Stop listening for events on ``fd``. """

        if fd not in self.handlers:
            return

        if fd in self.readers:
            self.loop.remove_reader(fd)
            self.readers.remove(fd)

        if fd in self.writers:
            self.loop.remove_writer(fd)
            self.writers.remove(fd)

        del self.handlers[fd]

    def update_handler(self, fd, event_state):
        if event_state & base_connection.BaseConnection.READ:
            if fd not in self.readers:
                self.loop.add_reader(
                    fd,
                    partial(
                        self.handlers[fd],
                        fd=fd,
                        events=base_connection.BaseConnection.READ
                    )
                )
                self.readers.add(fd)
        else:
            if fd in self.readers:
                self.loop.remove_reader(fd)
                self.readers.remove(fd)

        if event_state & base_connection.BaseConnection.WRITE:
            if fd not in self.writers:
                self.loop.add_writer(
                    fd,
                    partial(
                        self.handlers[fd],
                        fd=fd,
                        events=base_connection.BaseConnection.WRITE
                    )
                )
                self.writers.add(fd)
        else:
            if fd in self.writers:
                self.loop.remove_writer(fd)
                self.writers.remove(fd)


    def start(self):
        """ Start Event Loop """
        if self.loop.is_running():
            return

        self.loop.run_forever()

    def stop(self):
        """ Stop Event Loop """
        if self.loop.is_closed():
            return

        self.loop.stop()


class AsyncioConnection(base_connection.BaseConnection):
    """ The AsyncioConnection runs on the Asyncio EventLoop.

    :param pika.connection.Parameters parameters: Connection parameters
    :param on_open_callback: The method to call when the connection is open
    :type on_open_callback: method
    :param on_open_error_callback: Method to call if the connection cant be opened
    :type on_open_error_callback: method
    :param asyncio.AbstractEventLoop loop: By default asyncio.get_event_loop()

    """
    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=False,
                 custom_ioloop=None):
        """ Create a new instance of the AsyncioConnection class, connecting
        to RabbitMQ automatically

        :param pika.connection.Parameters parameters: Connection parameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method
        :param on_open_error_callback: Method to call if the connection cant be opened
        :type on_open_error_callback: method
        :param asyncio.AbstractEventLoop loop: By default asyncio.get_event_loop()

        """
        self.sleep_counter = 0
        self.loop = custom_ioloop or asyncio.get_event_loop()
        self.ioloop = IOLoopAdapter(self.loop)

        super().__init__(
            parameters, on_open_callback,
            on_open_error_callback,
            on_close_callback, self.ioloop,
            stop_ioloop_on_close=stop_ioloop_on_close,
        )

    def _adapter_connect(self):
        """Connect to the remote socket, adding the socket to the EventLoop if
        connected.

        :rtype: bool

        """
        error = super()._adapter_connect()

        if not error:
            self.ioloop.add_handler(
                self.socket.fileno(),
                self._handle_events,
                self.event_state,
            )

        return error

    def _adapter_disconnect(self):
        """Disconnect from the RabbitMQ broker"""

        if self.socket:
            self.ioloop.remove_handler(
                self.socket.fileno()
            )

        super()._adapter_disconnect()

    def _handle_disconnect(self):
        # No other way to handle exceptions.ProbableAuthenticationError
        try:
            super()._handle_disconnect()
            super()._handle_write()
        except Exception as e:
            # FIXME: Pass None or other constant instead "-1"
            self._on_disconnect(-1, e)
