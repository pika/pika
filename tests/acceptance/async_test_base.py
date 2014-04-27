import logging
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika

LOGGER = logging.getLogger(__name__)
PARAMETERS = pika.URLParameters('amqp://guest:guest@localhost:5672/%2f')
DEFAULT_TIMEOUT = 30


class AsyncTestCase(unittest.TestCase):

    ADAPTER = None
    TIMEOUT = DEFAULT_TIMEOUT

    def begin(self, channel):
        """Extend to start the actual tests on the channel"""
        raise AssertionError("AsyncTestCase.begin_test not extended")

    def start(self):
        self.connection = self.ADAPTER(PARAMETERS,
                                       self.on_open,
                                       self.on_open_error,
                                       self.on_closed)
        self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                   self.on_timeout)
        self.connection.ioloop.start()

    def stop(self):
        """close the connection and stop the ioloop"""
        LOGGER.info("Stopping test")
        self.connection.remove_timeout(self.timeout)
        self.timeout = None
        self.connection.close()

    def _stop(self):
        if hasattr(self, 'timeout') and self.timeout:
            self.connection.remove_timeout(self.timeout)
            self.timeout = None
        if hasattr(self, 'connection') and self.connection:
            self.connection.ioloop.stop()
            self.connection = None

    def tearDown(self):
        self._stop()

    def on_closed(self, connection, reply_code, reply_text):
        """called when the connection has finished closing"""
        LOGGER.debug("Connection Closed")
        self._stop()

    def on_open(self, connection):
        self.channel = connection.channel(self.begin)

    def on_open_error(self, connection):
        connection.ioloop.stop()
        raise AssertionError('Error connecting to RabbitMQ')

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        # force the ioloop to stop
        self.connection.ioloop.stop()
        raise AssertionError('Test timed out')


class BoundQueueTestCase(AsyncTestCase):

    def tearDown(self):
        """Cleanup auto-declared queue and exchange"""
        self._cconn = self.ADAPTER(PARAMETERS,
                                   self._on_cconn_open,
                                   self._on_cconn_error,
                                   self._on_cconn_closed)

    def start(self):
        self.exchange = 'e' + str(id(self))
        self.queue = 'q' + str(id(self))
        self.routing_key = self.__class__.__name__
        super(BoundQueueTestCase, self).start()

    def begin(self, channel):
        self.channel.exchange_declare(self.on_exchange_declared,
                                      self.exchange,
                                      exchange_type='direct',
                                      passive=False,
                                      durable=False,
                                      auto_delete=True)

    def on_exchange_declared(self, frame):
        self.channel.queue_declare(self.on_queue_declared,
                                   self.queue,
                                   passive=False,
                                   durable=False,
                                   exclusive=True,
                                   auto_delete=True,
                                   nowait=False,
                                   arguments={'x-expires': self.TIMEOUT})

    def on_queue_declared(self, frame):
        self.channel.queue_bind(self.on_ready,
                                self.queue,
                                self.exchange,
                                self.routing_key)

    def on_ready(self, frame):
        raise NotImplementedError

    def _on_cconn_closed(self, cconn):
        cconn.ioloop.stop()
        self._cconn = None

    def _on_cconn_error(self, connection):
        connection.ioloop.stop()
        raise AssertionError('Error connecting to RabbitMQ')

    def _on_cconn_open(self, connection):
        connection.channel(self._on_cconn_channel)

    def _on_cconn_channel(self, channel):
        channel.exchange_delete(None, self.exchange, nowait=True)
        channel.queue_delete(None, self.queue, nowait=True)
        self._cconn.close()
