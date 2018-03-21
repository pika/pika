# Suppress pylint warnings concerning attribute defined outside __init__
# pylint: disable=W0201

# Suppress pylint messages concerning missing docstrings
# pylint: disable=C0111

import datetime
import os
import select
import ssl
import sys
import logging
import unittest
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
        self.parameters = pika.ConnectionParameters(
            host='localhost',
            port=5672)
        if self.should_use_tls():
            self.logger.info('testing using TLS/SSL connection to port 5671')
            self.parameters.port = 5671
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations('testdata/certs/ca_certificate.pem')
            context.load_cert_chain('testdata/certs/client_certificate.pem',
                                    'testdata/certs/client_key.pem')
            self.parameters.ssl_options = pika.SSLOptions(context)
        self._timed_out = False
        super(AsyncTestCase, self).setUp()

    @staticmethod
    def should_use_tls():
        if 'PIKA_TEST_TLS' in os.environ and \
                os.environ['PIKA_TEST_TLS'].lower() == 'true':
            return True
        return False

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

    def start(self, adapter, ioloop_factory):
        self.logger.info('start at %s', datetime.datetime.utcnow())
        self.adapter = adapter or self.ADAPTER

        self.connection = self.adapter(self.parameters,
                                       self.on_open,
                                       self.on_open_error,
                                       self.on_closed,
                                       custom_ioloop=ioloop_factory())
        try:
            self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                       self.on_timeout)
            self._run_ioloop()
            self.assertFalse(self._timed_out)
        finally:
            self.connection.ioloop.close()
            self.connection = None

    def stop_ioloop_only(self):
        """Request stopping of the connection's ioloop to end the test without
        closing the connection
        """
        self._safe_remove_test_timeout()
        self.connection.ioloop.stop()

    def stop(self):
        """close the connection and stop the ioloop"""
        self.logger.info("Stopping test")
        self._safe_remove_test_timeout()
        self.connection.close() # NOTE: on_closed() will stop the ioloop

    def _run_ioloop(self):
        """Some tests need to subclass this in order to bootstrap their test
        logic after we instantiate the connection and assign it to
        `self.connection`, but before we run the ioloop
        """
        self.connection.ioloop.start()

    def _safe_remove_test_timeout(self):
        if hasattr(self, 'timeout') and self.timeout is not None:
            self.logger.info("Removing timeout")
            self.connection.remove_timeout(self.timeout)
            self.timeout = None

    def _stop(self):
        if hasattr(self, 'connection') and self.connection is not None:
            self._safe_remove_test_timeout()
            self.logger.info("Stopping ioloop")
            self.connection.ioloop.stop()

    def on_closed(self, connection, reply_code, reply_text):
        """called when the connection has finished closing"""
        self.logger.info('on_closed: %r %r %r', connection,
                         reply_code, reply_text)
        self._stop()

    def on_open(self, connection):
        self.logger.debug('on_open: %r', connection)
        self.channel = connection.channel(on_open_callback=self.begin)

    def on_open_error(self, connection, error):
        self.logger.error('on_open_error: %r %r', connection, error)
        connection.ioloop.stop()
        raise AssertionError('Error connecting to RabbitMQ')

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        self.logger.error('%s timed out; on_timeout called at %s',
                          self, datetime.datetime.utcnow())
        self.timeout = None  # the dispatcher should have removed it
        self._timed_out = True
        # initiate cleanup
        self.stop()


class BoundQueueTestCase(AsyncTestCase):

    def start(self, adapter, ioloop_factory):
        # PY3 compat encoding
        self.exchange = 'e-' + self.__class__.__name__ + ':' + uuid.uuid1().hex
        self.queue = 'q-' + self.__class__.__name__ + ':' + uuid.uuid1().hex
        self.routing_key = self.__class__.__name__
        super(BoundQueueTestCase, self).start(adapter, ioloop_factory)

    def begin(self, channel):
        self.channel.exchange_declare(self.exchange,
                                      exchange_type='direct',
                                      passive=False,
                                      durable=False,
                                      auto_delete=True,
                                      callback=self.on_exchange_declared)

    def on_exchange_declared(self, frame):  # pylint: disable=W0613
        self.channel.queue_declare(self.queue,
                                   passive=False,
                                   durable=False,
                                   exclusive=True,
                                   auto_delete=True,
                                   arguments={'x-expires': self.TIMEOUT * 1000},
                                   callback=self.on_queue_declared)

    def on_queue_declared(self, frame):  # pylint: disable=W0613
        self.channel.queue_bind(self.queue, self.exchange,
                                self.routing_key,
                                callback=self.on_ready)

    def on_ready(self, frame):
        raise NotImplementedError


#
# In order to write test cases that will tested using all the Async Adapters
# write a class that inherits both from one of TestCase classes above and
# from the AsyncAdapters class below. This allows you to avoid duplicating the
# test methods for each adapter in each test class.
#

class AsyncAdapters(object):

    def start(self, adapter_class, ioloop_factory):
        """

        :param adapter_class: pika connection adapter class to test.
        :param ioloop_factory: to be called without args to instantiate a
           non-shared ioloop to be passed as the `custom_ioloop` arg to the
           `adapter_class` constructor. This is needed because some of the
           adapters default to using a singleton ioloop, which results in
           tests errors after prior tests close the ioloop to release resources,
           in order to eliminate ResourceWarning warnings concerning unclosed
           sockets from our adapters.
        :return:
        """
        raise NotImplementedError

    def select_default_test(self):
        """SelectConnection:DefaultPoller"""
        with mock.patch.multiple(select_connection, SELECT_TYPE=None):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    def select_select_test(self):
        """SelectConnection:select"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='select'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @unittest.skipIf(
        not hasattr(select, 'poll') or
        not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
    def select_poll_test(self):
        """SelectConnection:poll"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='poll'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
    def select_epoll_test(self):
        """SelectConnection:epoll"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='epoll'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
    def select_kqueue_test(self):
        """SelectConnection:kqueue"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='kqueue'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    def tornado_test(self):
        """TornadoConnection"""
        ioloop_factory = None
        if adapters.TornadoConnection is not None:
            import tornado.ioloop
            ioloop_factory = tornado.ioloop.IOLoop
        self.start(adapters.TornadoConnection, ioloop_factory)

    @unittest.skipIf(sys.version_info < (3, 4), "Asyncio available for Python 3.4+")
    def asyncio_test(self):
        """AsyncioConnection"""
        ioloop_factory = None
        if adapters.AsyncioConnection is not None:
            import asyncio
            ioloop_factory = asyncio.new_event_loop

        self.start(adapters.AsyncioConnection, ioloop_factory)
