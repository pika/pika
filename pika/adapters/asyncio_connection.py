import asyncio
import logging
from functools import partial

from pika.adapters import base_connection


LOGGER = logging.getLogger(__name__)


class IOLoopAdapter:
    __slots__ = 'loop',

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def add_timeout(self, deadline, callback_method):
        return self.loop.call_later(deadline, callback_method)

    @staticmethod
    def remove_timeout(handle: asyncio.Handle):
        return handle.cancel()

    def add_handler(self, fd, cb, event_state):
        if event_state & base_connection.BaseConnection.READ:
            self.loop.add_reader(
                fd,
                partial(
                    cb,
                    fd=fd,
                    events=base_connection.BaseConnection.READ
                )
            )

        if event_state & base_connection.BaseConnection.WRITE:
            self.loop.add_writer(
                fd,
                partial(
                    cb,
                    fd=fd,
                    events=base_connection.BaseConnection.WRITE
                )
            )

    def remove_handler(self, fd):
        self.loop.remove_reader(fd)
        self.loop.remove_writer(fd)

    def update_handler(self, fd, event_state):
        raise NotImplementedError()

    def start(self):
        if loop.is_running():
            return

        self.loop.run_forever()

    def stop(self):
        if loop.is_closed():
            return

        self.loop.stop()


class AsyncioConnection(base_connection.BaseConnection):

    def __init__(self, parameters=None, on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None, loop=None):

        self.sleep_counter = 0
        self.loop = loop or asyncio.get_event_loop()
        self.ioloop = IOLoopAdapter(self.loop)

        super().__init__(parameters, on_open_callback,
                         on_open_error_callback,
                         on_close_callback, self.ioloop,
                         stop_ioloop_on_close=False)

    def _adapter_connect(self):
        error = super()._adapter_connect()

        if not error:
            self.ioloop.add_handler(
                self.socket.fileno(), self._handle_events, self.event_state
            )

        return error

    def _adapter_disconnect(self):
        if self.socket:
            self.ioloop.remove_handler(self.socket.fileno())

        super()._adapter_disconnect()

    def _handle_disconnect(self):
        try:
            super()._handle_disconnect()
            super()._handle_write()
        except Exception as e:
            self._on_disconnect(-1, e)
