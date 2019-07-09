"""Base test classes for async_adapter_tests.py

"""
import datetime
import functools
import os
import select
import sys
import logging
import unittest
import uuid

try:
    from unittest import mock  # pylint: disable=C0412
except ImportError:
    import mock

import pika
from pika import adapters
from pika.adapters import select_connection

from ..threaded_test_wrapper import create_run_in_thread_decorator

# invalid-name
# pylint: disable=C0103

# Suppress pylint warnings concerning attribute defined outside __init__
# pylint: disable=W0201

# Suppress pylint messages concerning missing docstrings
# pylint: disable=C0111

# protected-access
# pylint: disable=W0212


TEST_TIMEOUT = 15

# Decorator for running our tests in threads with timeout
# NOTE: we give it a little more time to give our I/O loop-based timeout logic
# sufficient time to mop up.
run_test_in_thread_with_timeout = create_run_in_thread_decorator(  # pylint: disable=C0103
    TEST_TIMEOUT * 1.1)


def make_stop_on_error_with_self(the_self=None):
    """Create a decorator that stops test if the decorated method exits
    with exception and causes the test to fail by re-raising that exception
    after ioloop exits.

    :param None | AsyncTestCase the_self: if None, will use the first arg of
        decorated method if it is an instance of AsyncTestCase, raising
        exception otherwise.

    """
    def stop_on_error_with_self_decorator(fun):

        @functools.wraps(fun)
        def stop_on_error_wrapper(*args, **kwargs):
            this = the_self
            if this is None and args and isinstance(args[0], AsyncTestCase):
                this = args[0]
            if not isinstance(this, AsyncTestCase):
                raise AssertionError('Decorated method is not an AsyncTestCase '
                                     'instance method: {!r}'.format(fun))
            try:
                return fun(*args, **kwargs)
            except Exception as error:  # pylint: disable=W0703
                this.logger.exception('Stopping test due to failure in %r: %r',
                                      fun, error)
                this.stop(error)

        return stop_on_error_wrapper

    return stop_on_error_with_self_decorator


# Decorator that stops test if AsyncTestCase-based method exits with
# exception and causes the test to fail by re-raising that exception after
# ioloop exits.
#
# NOTE: only use it to decorate instance methods where self arg is a
#    AsyncTestCase instance.
stop_on_error_in_async_test_case_method = make_stop_on_error_with_self()


def enable_tls():
    if 'PIKA_TEST_TLS' in os.environ and \
            os.environ['PIKA_TEST_TLS'].lower() == 'true':
        return True
    return False


class AsyncTestCase(unittest.TestCase):
    DESCRIPTION = ""
    ADAPTER = None
    TIMEOUT = TEST_TIMEOUT

    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parameters = self.new_connection_params()
        self._timed_out = False
        self._conn_open_error = None
        self._public_stop_requested = False
        self._conn_closed_reason = None
        self._public_stop_error_in = None  # exception passed to our stop()
        super(AsyncTestCase, self).setUp()

    def new_connection_params(self):
        """
        :rtype: pika.ConnectionParameters

        """
        if enable_tls():
            return self._new_tls_connection_params()
        else:
            return self._new_plaintext_connection_params()

    def _new_tls_connection_params(self):
        """
        :rtype: pika.ConnectionParameters

        """
        self.logger.info('testing using TLS/SSL connection to port 5671')
        url = 'amqps://localhost:5671/%2F?ssl_options=%7B%27ca_certs%27%3A%27testdata%2Fcerts%2Fca_certificate.pem%27%2C%27keyfile%27%3A%27testdata%2Fcerts%2Fclient_key.pem%27%2C%27certfile%27%3A%27testdata%2Fcerts%2Fclient_certificate.pem%27%7D'
        params = pika.URLParameters(url)
        return params

    @staticmethod
    def _new_plaintext_connection_params():
        """
        :rtype: pika.ConnectionParameters

        """
        return pika.ConnectionParameters(host='localhost', port=5672)

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
            self.timeout = self.connection._adapter_call_later(self.TIMEOUT,
                                                               self.on_timeout)
            self._run_ioloop()

            self.assertFalse(self._timed_out)
            self.assertIsNone(self._conn_open_error)
            # Catch unexpected loss of connection
            self.assertTrue(self._public_stop_requested,
                            'Unexpected end of test; connection close reason: '
                            '{!r}'.format(self._conn_closed_reason))
            if self._public_stop_error_in is not None:
                raise self._public_stop_error_in  # pylint: disable=E0702
        finally:
            self.connection._nbio.close()
            self.connection = None

    def stop_ioloop_only(self):
        """Request stopping of the connection's ioloop to end the test without
        closing the connection
        """
        self._safe_remove_test_timeout()
        self.connection._nbio.stop()

    def stop(self, error=None):
        """close the connection and stop the ioloop

        :param None | Exception error: if not None, will raise the given
            exception after ioloop exits.
        """
        if error is not None:
            if self._public_stop_error_in is None:
                self.logger.error('stop(): stopping with error=%r.', error)
            else:
                self.logger.error('stop(): replacing pending error=%r with %r',
                                  self._public_stop_error_in, error)
            self._public_stop_error_in = error

        self.logger.info('Stopping test')
        self._public_stop_requested = True
        if self.connection.is_open:
            self.connection.close() # NOTE: on_closed() will stop the ioloop
        elif self.connection.is_closed:
            self.logger.info(
                'Connection already closed, so just stopping ioloop')
            self._stop()

    def _run_ioloop(self):
        """Some tests need to subclass this in order to bootstrap their test
        logic after we instantiate the connection and assign it to
        `self.connection`, but before we run the ioloop
        """
        self.connection._nbio.run()

    def _safe_remove_test_timeout(self):
        if hasattr(self, 'timeout') and self.timeout is not None:
            self.logger.info("Removing timeout")
            self.connection._adapter_remove_timeout(self.timeout)
            self.timeout = None

    def _stop(self):
        if hasattr(self, 'connection') and self.connection is not None:
            self._safe_remove_test_timeout()
            self.logger.info("Stopping ioloop")
            self.connection._nbio.stop()

    def on_closed(self, connection, error):
        """called when the connection has finished closing"""
        self.logger.info('on_closed: %r %r', connection, error)
        self._conn_closed_reason = error
        self._stop()

    def on_open(self, connection):
        self.logger.debug('on_open: %r', connection)
        self.channel = connection.channel(
            on_open_callback=self.on_channel_opened)

    def on_open_error(self, connection, error):
        self._conn_open_error = error
        self.logger.error('on_open_error: %r %r', connection, error)
        self._stop()

    def on_channel_opened(self, channel):
        self.begin(channel)

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

    @run_test_in_thread_with_timeout
    def test_with_select_default(self):
        """SelectConnection:DefaultPoller"""
        with mock.patch.multiple(select_connection, SELECT_TYPE=None):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @run_test_in_thread_with_timeout
    def test_with_select_select(self):
        """SelectConnection:select"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='select'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @unittest.skipIf(
        not hasattr(select, 'poll') or
        not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
    @run_test_in_thread_with_timeout
    def test_with_select_poll(self):
        """SelectConnection:poll"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='poll'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
    @run_test_in_thread_with_timeout
    def test_with_select_epoll(self):
        """SelectConnection:epoll"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='epoll'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
    @run_test_in_thread_with_timeout
    def test_with_select_kqueue(self):
        """SelectConnection:kqueue"""

        with mock.patch.multiple(select_connection, SELECT_TYPE='kqueue'):
            self.start(adapters.SelectConnection, select_connection.IOLoop)

    @run_test_in_thread_with_timeout
    def test_with_tornado(self):
        """TornadoConnection"""
        ioloop_factory = None
        if adapters.tornado_connection.TornadoConnection is not None:
            import tornado.ioloop
            ioloop_factory = tornado.ioloop.IOLoop
        self.start(adapters.tornado_connection.TornadoConnection, ioloop_factory)

    @unittest.skipIf(sys.version_info < (3, 4),
                     'Asyncio is available only with Python 3.4+')
    @run_test_in_thread_with_timeout
    def test_with_asyncio(self):
        """AsyncioConnection"""
        ioloop_factory = None
        if adapters.asyncio_connection.AsyncioConnection is not None:
            import asyncio
            ioloop_factory = asyncio.new_event_loop

        self.start(adapters.asyncio_connection.AsyncioConnection, ioloop_factory)
