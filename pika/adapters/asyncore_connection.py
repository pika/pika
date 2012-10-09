"""Use pika with the stdlib asyncore module"""
import asyncore
import logging
import time

from pika.adapters import base_connection

LOGGER = logging.getLogger(__name__)


class PikaDispatcher(asyncore.dispatcher):

    # Use epoll's constants to keep life easy
    READ = 0x0001
    WRITE = 0x0004
    ERROR = 0x0008

    def __init__(self, sock=None, map=None, event_callback=None):
        # Is an old style class...
        asyncore.dispatcher.__init__(self, sock, map)
        self._timeouts = dict()
        self._event_callback = event_callback
        self.events = self.READ | self.WRITE

    def add_timeout(self, deadline, handler):
        """Add a timeout with with given deadline, should return a timeout id.

        :param int deadline: The number of seconds to wait until calling handler
        :param method handler: The method to call at deadline
        :rtype: str

        """
        value = time.time() + deadline
        LOGGER.debug('Will call %r on or after %i', handler, value)
        timeout_id = '%.8f' % value
        self._timeouts[timeout_id] = {'timestamp': value, 'handler': handler}
        return timeout_id

    def readable(self):
        return bool(self.events & self.READ)

    def writable(self):
        return bool(self.events & self.WRITE)

    def handle_read(self):
        self._event_callback(self.socket, self.READ)

    def handle_write(self):
        self._event_callback(self.socket, self.WRITE, None, True)

    def process_timeouts(self):
        """Process the self._timeouts event stack"""
        start_time = time.time()
        for timeout_id in self._timeouts.keys():
            if self._timeouts[timeout_id]['timestamp'] <= start_time:
                handler = self._timeouts[timeout_id]['handler']
                del self._timeouts[timeout_id]
                handler()

    def remove_timeout(self, timeout_id):
        """Remove a timeout if it's still in the timeout stack

        :param str timeout_id: The timeout id to remove

        """
        if timeout_id in self._timeouts:
            del self._timeouts[timeout_id]

    def start(self):
        LOGGER.debug('Starting IOLoop')
        asyncore.loop()

    def stop(self):
        LOGGER.debug('Stopping IOLoop')
        self.close()

    def update_handler(self, fileno, events):
        """Set the events to the current events

        :param int fileno: The file descriptor
        :param int events: The event mask

        """
        self.events = events


class AsyncoreConnection(base_connection.BaseConnection):

    def _adapter_connect(self):
        """
        Connect to our RabbitMQ boker using AsyncoreDispatcher, then setting
        Pika's suggested buffer size for socket reading and writing. We pass
        the handle to self so that the AsyncoreDispatcher object can call back
        into our various state methods.
        """
        super(AsyncoreConnection, self)._adapter_connect()
        self.socket = PikaDispatcher(self.socket, None, self._handle_events)
        self.ioloop = self.socket
        self._on_connected()
