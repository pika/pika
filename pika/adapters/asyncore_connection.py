"""
Use Pika with the stdlib :py:mod:`asyncore` module.

"""
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


    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        value = {'deadline': time.time() + deadline,
                 'callback': callback_method}
        timeout_id = hash(frozenset(value.items()))
        self._timeouts[timeout_id] = value
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
            if self._timeouts[timeout_id]['deadline'] <= start_time:
                callback = self._timeouts[timeout_id]['callback']
                del self._timeouts[timeout_id]
                callback()

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

    def update_handler(self, fileno_unused, events):
        """Set the events to the current events

        :param int fileno_unused: The file descriptor
        :param int events: The event mask

        """
        self.events = events


class AsyncoreConnection(base_connection.BaseConnection):
    """The AsyncoreConnection adapter uses the stdlib asyncore module as an
    IOLoop for asyncronous client development.

    :param pika.connection.Parameters parameters: Connection parameters
    :param method on_open_callback: Method to call on connection open
    :param on_open_error_callback: Method to call if the connection cant
                                   be opened
    :type on_open_error_callback: method
    :param method on_close_callback: Method to call on connection close
    :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
    :raises: RuntimeError

    """
    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=True):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param on_open_error_callback: Method to call if the connection cant
                                       be opened
        :type on_open_error_callback: method
        :param method on_close_callback: Method to call on connection close
        :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
        :raises: RuntimeError

        """
        class ConnectingIOLoop(object):
            def add_timeout(self, duration, callback_method):
                time.sleep(duration)
                return callback_method()
        ioloop = ConnectingIOLoop()
        super(AsyncoreConnection, self).__init__(parameters, on_open_callback,
                                                 on_open_error_callback,
                                                 on_close_callback,
                                                 ioloop,
                                                 stop_ioloop_on_close)

    def _adapter_connect(self):
        """Connect to our RabbitMQ broker using AsyncoreDispatcher, then setting
        Pika's suggested buffer size for socket reading and writing. We pass
        the handle to self so that the AsyncoreDispatcher object can call back
        into our various state methods.

        """
        if super(AsyncoreConnection, self)._adapter_connect():
            self.socket = PikaDispatcher(self.socket, None, self._handle_events)
            self.ioloop = self.socket
            self._on_connected()
