import logging
import socket
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import uuid

from forward_server import ForwardServer

import pika
import pika.exceptions

LOGGER = logging.getLogger(__name__)
PARAMETERS_TEMPLATE = 'amqp://guest:guest@localhost:%(port)s/%%2f'
PARAMETERS = pika.URLParameters(PARAMETERS_TEMPLATE % {'port': 5672})
DEFAULT_TIMEOUT = 15



class BlockingTestCase(unittest.TestCase):

    def _connect(self,
                 parameters=PARAMETERS,
                 connection_class=pika.BlockingConnection):
        connection = connection_class(parameters)
        self.addCleanup(lambda: self.connection.close
                        if self.connection.is_open else None)
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
                pika.URLParameters(
                    PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]}))

            self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                       self._on_test_timeout)


        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            channel = self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)

class TestNoAccessToFileDescriptorAfterConnectionClosed(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """BlockingConnection can't access file descriptor after \
        ConnectionClosed
        """
        with ForwardServer((PARAMETERS.host, PARAMETERS.port)) as fwd:
            self.connection = self._connect(
                pika.URLParameters(
                    PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]}))

            self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                       self._on_test_timeout)


        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            channel = self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)

        # Attempt to operate on the connection once again after ConnectionClosed
        self.assertIsNone(self.connection._read_poller)
        self.assertIsNone(self.connection.socket)
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

        sock.bind(("localhost", 0))


        with self.assertRaises(pika.exceptions.AMQPConnectionError):
            self.connection = self._connect(
                pika.URLParameters(
                    PARAMETERS_TEMPLATE % {"port": sock.getsockname()[1]})
            )

            # Should never get here
            self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                       self._on_test_timeout)


class TestReconnectWithDownedBroker(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection reconnect with downed broker
        """
        with ForwardServer((PARAMETERS.host, PARAMETERS.port)) as fwd:
            self.connection = self._connect(
                pika.URLParameters(
                    PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]}))

            self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                       self._on_test_timeout)

        # Once outside the context, the connection is broken

        # BlockingConnection should raise AMQPConnectionError
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            channel = self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)

        # Now attempt to reconnect
        with self.assertRaises(pika.exceptions.AMQPConnectionError):
            self.connection.connect()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)


class TestDisconnectDuringConnectionStart(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_START
        """
        fwd = ForwardServer((PARAMETERS.host, PARAMETERS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        # Establish and close connection
        self.connection = self._connect(
            pika.URLParameters(
                PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]}))

        self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                   self._on_test_timeout)

        self.connection.close()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)

        # Set up hook to disconnect on Connection.Start frame from broker
        self.connection.callbacks.add(
            0, pika.spec.Connection.Start,
            lambda *args, **kwards: fwd.stop())

        # Now, attempt to reconnect; depending on whether our
        # spec.Connection.Start callback gets called before or after the one
        # registered by the Connection class, we could be looking at either
        #     ProbableAuthenticationError in CONNECTION_START state or
        #     ProbableAccessDeniedError in CONNECTION_TUNE state.
        with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
            self.connection.connect()

        self.assertIsInstance(cm.exception,
                              (pika.exceptions.ProbableAuthenticationError,
                               pika.exceptions.ProbableAccessDeniedError))

        # Verify that connection state reflects a closed connection
        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)


class TestDisconnectDuringConnectionTune(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_TUNE
        """
        fwd = ForwardServer((PARAMETERS.host, PARAMETERS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        # Establish and close connection
        self.connection = self._connect(
            pika.URLParameters(
                PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]}))

        self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                   self._on_test_timeout)

        self.connection.close()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)

        # Set up hook to disconnect on Connection.Tune frame from broker
        self.connection.callbacks.add(
            0, pika.spec.Connection.Tune,
            lambda *args, **kwards: fwd.stop())

        # Now, attempt to reconnect
        with self.assertRaises(pika.exceptions.ProbableAccessDeniedError):
            self.connection.connect()

        # Verify that connection state reflects a closed connection
        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)


class TestDisconnectDuringConnectionProtocol(BlockingTestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_PROTOCOL
        """
        fwd = ForwardServer((PARAMETERS.host, PARAMETERS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MyConnection(pika.BlockingConnection):
            DROP_REQUESTED = False
            def _on_connected(self, *args, **kwargs):
                """Base method override for forcing TCP/IP connection loss"""
                if self.DROP_REQUESTED:
                    # Drop
                    fwd.stop()
                # Proceede to CONNECTION_PROTOCOL state
                return super(MyConnection, self)._on_connected(*args, **kwargs)

        # Establish and close connection
        self.connection = self._connect(
            pika.URLParameters(
                PARAMETERS_TEMPLATE % {"port": fwd.server_address[1]}),
            connection_class=MyConnection)

        self.timeout = self.connection.add_timeout(self.TIMEOUT,
                                                   self._on_test_timeout)

        self.connection.close()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)

        # Request TCP/IP connection loss during subsequent reconnect
        MyConnection.DROP_REQUESTED = True

        # Now, attempt to reconnect
        with self.assertRaises(pika.exceptions.IncompatibleProtocolError):
            self.connection.connect()

        # Verify that connection state reflects a closed connection
        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection.socket)
