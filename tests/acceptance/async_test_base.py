# Suppress pylint warnings concerning attribute defined outside __init__
# pylint: disable=W0201

# Suppress pylint messages concerning missing docstrings
# pylint: disable=C0111

from datetime import datetime
import select
import sys
import logging

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import platform
_TARGET = platform.python_implementation()

import uuid

try:
    from unittest import mock
except ImportError:
    import mock


import pika
from pika import adapters
from pika.adapters import select_connection


class AsyncTestCase(unittest.TestCase):
    DESCRIPTION = ""
    ADAPTER = None
    TIMEOUT = 15

    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parameters = pika.URLParameters(
            'amqp://guest:guest@localhost:5672/%2F')
        self._timed_out = False
        super(AsyncTestCase, self).setUp()

    def tearDown(self):
        self._stop()

    def shortDescription(self):
        method_desc = super(AsyncTestCase, self).shortDescription()
        if self.DESCRIPTION:
            return "%s (%s)" % (self.DESCRIPTION, method_desc)
        else:
            return method_desc

    def begin(self, channel):  # pylint: disable=R0201,W0613
        """Extend to start the actual tests on the channel"""
        self.fail("AsyncTestCase.begin_test not extended")

    def start(self, adapter=None):
        self.logger.info('start at %s', datetime.utcnow())
        self.adapter = adapter or self.ADAPTER

        self.connection = self.adapter(self.parameters, self.on_open,
                                       self.on_open_error, self.on_closed)
        self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                   self.on_timeout)
        self.connection.ioloop.start()
        self.assertFalse(self._timed_out)

    def stop(self):
        """close the connection and stop the ioloop"""
        self.logger.info("Stopping test")
        if self.timeout is not None:
            self.connection.remove_timeout(self.timeout)
            self.timeout = None
        self.connection.close()

    def _stop(self):
        if hasattr(self, 'timeout') and self.timeout is not None:
            self.logger.info("Removing timeout")
            self.connection.remove_timeout(self.timeout)
            self.timeout = None
        if hasattr(self, 'connection') and self.connection:
            self.logger.info("Stopping ioloop")
            self.connection.ioloop.stop()
            self.connection = None

    def on_closed(self, connection, reply_code, reply_text):
        """called when the connection has finished closing"""
        self.logger.info('on_closed: %r %r %r', connection,
                         reply_code, reply_text)
        self._stop()

    def on_open(self, connection):
        self.logger.debug('on_open: %r', connection)
        self.channel = connection.channel(self.begin)

    def on_open_error(self, connection, error):
        self.logger.error('on_open_error: %r %r', connection, error)
        connection.ioloop.stop()
        raise AssertionError('Error connecting to RabbitMQ')

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        self.logger.error('%s timed out; on_timeout called at %s',
            self, datetime.utcnow())
        self.timeout = None  # the dispatcher should have removed it
        self._timed_out = True
        # initiate cleanup
        self.stop()


class BoundQueueTestCase(AsyncTestCase):

    def start(self, adapter=None):
        # PY3 compat encoding
        self.exchange = 'e-' + self.__class__.__name__ + ':' + uuid.uuid1().hex
        self.queue = 'q-' + self.__class__.__name__ + ':' + uuid.uuid1().hex
        self.routing_key = self.__class__.__name__
        super(BoundQueueTestCase, self).start(adapter)

    def begin(self, channel):
        self.channel.exchange_declare(self.on_exchange_declared, self.exchange,
                                      exchange_type='direct',
                                      passive=False,
                                      durable=False,
                                      auto_delete=True)

    def on_exchange_declared(self, frame):  # pylint: disable=W0613
        self.channel.queue_declare(self.on_queue_declared, self.queue,
                                   passive=False,
                                   durable=False,
                                   exclusive=True,
                                   auto_delete=True,
                                   nowait=False,
                                   arguments={'x-expires': self.TIMEOUT * 1000})

    def on_queue_declared(self, frame):  # pylint: disable=W0613
        self.channel.queue_bind(self.on_ready, self.queue, self.exchange,
                                self.routing_key)

    def on_ready(self, frame):
        raise NotImplementedError


#
# In order to write test cases that will tested using all the Async Adapters
# write a class that inherits both from one of TestCase classes above and
# from the AsyncAdapters class below. This allows you to avoid duplicating the
# test methods for each adapter in each test class.
#

class AsyncAdapters(object):

    def start(self, adapter_class):
        raise NotImplementedError

    def select_default_test(self):
        """SelectConnection:DefaultPoller"""

        with mock.patch.multiple(select_connection, SELECT_TYPE=None):
            self.start(adapters.SelectConnection)

    def select_select_test(self):
        """SelectConnection:select"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='select'):
            self.start(adapters.SelectConnection)

    @unittest.skipIf(
        not hasattr(select, 'poll') or
        not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
    def select_poll_test(self):
        """SelectConnection:poll"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='poll'):
            self.start(adapters.SelectConnection)

    @unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
    def select_epoll_test(self):
        """SelectConnection:epoll"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='epoll'):
            self.start(adapters.SelectConnection)

    @unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
    def select_kqueue_test(self):
        """SelectConnection:kqueue"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='kqueue'):
            self.start(adapters.SelectConnection)

    def tornado_test(self):
        """TornadoConnection"""
        self.start(adapters.TornadoConnection)

    @unittest.skipIf(sys.version_info < (3, 4), "Asyncio available for Python 3.4+")
    def asyncio_test(self):
        """AsyncioConnection"""
        self.start(adapters.AsyncioConnection)

    @unittest.skipIf(_TARGET == 'PyPy', 'PyPy is not supported')
    @unittest.skipIf(adapters.LibevConnection is None, 'pyev is not installed')
    def libev_test(self):
        """LibevConnection"""
        self.start(adapters.LibevConnection)


