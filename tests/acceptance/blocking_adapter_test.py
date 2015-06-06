import logging
import socket
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import uuid

from forward_server import ForwardServer

import pika
import pika.exceptions
import pika.connection

LOGGER = logging.getLogger(__name__)
PARAMETERS_TEMPLATE = (
    'amqp://guest:guest@127.0.0.1:%(port)s/%%2f?socket_timeout=1')
DEFAULT_URL = PARAMETERS_TEMPLATE % {'port': 5672}
PARAMETERS = pika.URLParameters(DEFAULT_URL)
DEFAULT_TIMEOUT = 15



class BlockingTestCase(unittest.TestCase):

    def _connect(self,
                 url=DEFAULT_URL,
                 connection_class=pika.BlockingConnection,
                 impl_class=None):
        parameters = pika.URLParameters(url)
        connection = connection_class(parameters, _impl_class=impl_class)
        self.addCleanup(lambda: connection.close
                        if connection.is_open else None)

        connection._impl.add_timeout(
            self.TIMEOUT, # pylint: disable=E1101
            self._on_test_timeout)

        return connection

    def _on_test_timeout(self):
        """Called when test times out"""
        self.fail('Test timed out')


class TestSuddenBrokerDisconnectBeforeChannel(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """BlockingConnection resets properly on TCP/IP drop during channel()
        """
        with ForwardServer((PARAMETERS.host, PARAMETERS.port)) as fwd:
            self.connection = self._connect(
                PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            channel = self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection._impl.socket)


class TestNoAccessToFileDescriptorAfterConnectionClosed(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """BlockingConnection can't access file descriptor after
        ConnectionClosed
        """
        with ForwardServer((PARAMETERS.host, PARAMETERS.port)) as fwd:
            self.connection = self._connect(
                PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            channel = self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection._impl.socket)

        # Attempt to operate on the connection once again after ConnectionClosed
        self.assertIsNone(self.connection._impl.socket)
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            channel = self.connection.channel()


class TestConnectWithDownedBroker(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection to downed broker results in AMQPConnectionError

        """
        # Reserve a port for use in connect
        sock = socket.socket()
        self.addCleanup(sock.close)

        sock.bind(("127.0.0.1", 0))

        port = sock.getsockname()[1]

        sock.close()

        with self.assertRaises(pika.exceptions.AMQPConnectionError):
            self.connection = self._connect(
                PARAMETERS_TEMPLATE % {"port": port})


class TestDisconnectDuringConnectionStart(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_START
        """
        fwd = ForwardServer((PARAMETERS.host, PARAMETERS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connection_start')

            def _on_connection_start(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connection_start(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
            self._connect(
                PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]},
                impl_class=MySelectConnection)

            self.assertIsInstance(cm.exception,
                                  (pika.exceptions.ProbableAuthenticationError,
                                   pika.exceptions.ProbableAccessDeniedError))


class TestDisconnectDuringConnectionTune(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_TUNE
        """
        fwd = ForwardServer((PARAMETERS.host, PARAMETERS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connection_tune')

            def _on_connection_tune(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connection_tune(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.ProbableAccessDeniedError):
            self._connect(
                PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]},
                impl_class=MySelectConnection)


class TestDisconnectDuringConnectionProtocol(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_PROTOCOL
        """
        fwd = ForwardServer((PARAMETERS.host, PARAMETERS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connected')

            def _on_connected(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connected(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.IncompatibleProtocolError):
            self._connect(PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]},
                          impl_class=MySelectConnection)
