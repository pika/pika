import select
import logging
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import platform
target = platform.python_implementation()

import pika
from pika import adapters
from pika.adapters import select_connection

LOGGER = logging.getLogger(__name__)
PARAMETERS = pika.URLParameters('amqp://guest:guest@localhost:5672/%2f')
DEFAULT_TIMEOUT = 15


class AsyncTestCase(unittest.TestCase):
    DESCRIPTION = ""
    ADAPTER = None
    TIMEOUT = DEFAULT_TIMEOUT


    def shortDescription(self):
        method_desc = super(AsyncTestCase, self).shortDescription()
        if self.DESCRIPTION:
            return "%s (%s)" % (self.DESCRIPTION, method_desc)
        else:
            return method_desc

    def begin(self, channel):
        """Extend to start the actual tests on the channel"""
        raise AssertionError("AsyncTestCase.begin_test not extended")

    def start(self, adapter=None):
        self.adapter = adapter or self.ADAPTER

        self.connection = self.adapter(PARAMETERS, self.on_open,
                                       self.on_open_error, self.on_closed)
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
        self._cconn = self.adapter(PARAMETERS, self._on_cconn_open,
                                   self._on_cconn_error, self._on_cconn_closed)

    def start(self, adapter=None):
        # PY3 compat encoding
        self.exchange = 'e' + str(id(self))
        self.queue = 'q' + str(id(self))
        self.routing_key = self.__class__.__name__
        super(BoundQueueTestCase, self).start(adapter)

    def begin(self, channel):
        self.channel.exchange_declare(self.on_exchange_declared, self.exchange,
                                      exchange_type='direct',
                                      passive=False,
                                      durable=False,
                                      auto_delete=True)

    def on_exchange_declared(self, frame):
        self.channel.queue_declare(self.on_queue_declared, self.queue,
                                   passive=False,
                                   durable=False,
                                   exclusive=True,
                                   auto_delete=True,
                                   nowait=False,
                                   arguments={'x-expires': self.TIMEOUT * 1000}
                                   )

    def on_queue_declared(self, frame):
        self.channel.queue_bind(self.on_ready, self.queue, self.exchange,
                                self.routing_key)

    def on_ready(self, frame):
        raise NotImplementedError

    def _on_cconn_closed(self, cconn, *args, **kwargs):
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

#
# In order to write test cases that will tested using all the Async Adapters
# write a class that inherits both from one of TestCase classes above and 
# from the AsyncAdapters class below. This allows you to avoid duplicating the
# test methods for each adapter in each test class.
#

class AsyncAdapters(object):

    def select_default_test(self):
        "SelectConnection:DefaultPoller"
        select_connection.POLLER_TYPE=None
        self.start(adapters.SelectConnection)

    def select_select_test(self):
        "SelectConnection:select"
        select_connection.POLLER_TYPE='select'
        self.start(adapters.SelectConnection)

    @unittest.skipIf(not hasattr(select, 'poll') 
        or not hasattr(select.poll(), 'modify'), "poll not supported")
    def select_poll_test(self):
        "SelectConnection:poll"
        select_connection.POLLER_TYPE='poll'
        self.start(adapters.SelectConnection)

    @unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
    def select_epoll_test(self):
        "SelectConnection:epoll"
        select_connection.POLLER_TYPE='epoll'
        self.start(adapters.SelectConnection)

    @unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
    def select_kqueue_test(self):
        "SelectConnection:kqueue"
        select_connection.POLLER_TYPE='kqueue'
        self.start(adapters.SelectConnection)

    def tornado_test(self):
        "TornadoConnection"
        self.start(adapters.TornadoConnection)

    @unittest.skipIf(target == 'PyPy', 'PyPy is not supported')
    @unittest.skipIf(adapters.LibevConnection is None, 'pyev is not installed')
    def libev_test(self):
        "LibevConnection"
        self.start(adapters.LibevConnection)


