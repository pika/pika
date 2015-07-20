# -*- coding: utf8 -*-
"""
Tests for connection parameters.

"""
import socket
try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika
from pika.adapters import base_connection
from pika.adapters import blocking_connection
from pika.adapters import select_connection
try:
    from pika.adapters import tornado_connection
except ImportError:
    tornado_connection = None
try:
    from pika.adapters import twisted_connection
except ImportError:
    twisted_connection = None
try:
    from pika.adapters import libev_connection
except ImportError:
    libev_connection = None

from pika import exceptions


def mock_timeout(*args, **kwargs):
    raise socket.timeout


class ConnectionTests(unittest.TestCase):

    def test_parameters(self):
        params = pika.ConnectionParameters(socket_timeout=0.5,
                                           retry_delay=0.1,
                                           connection_attempts=3)
        self.assertEqual(params.socket_timeout, 0.5)
        self.assertEqual(params.retry_delay, 0.1)
        self.assertEqual(params.connection_attempts, 3)

    @patch.object(socket.socket, 'settimeout')
    @patch.object(socket.socket, 'connect')
    def test_connection_timeout(self, connect, settimeout):
        connect.side_effect = mock_timeout
        with self.assertRaises(exceptions.AMQPConnectionError):
            params = pika.ConnectionParameters(socket_timeout=2.0)
            base_connection.BaseConnection(params)
        settimeout.assert_called_with(2.0)

    @patch.object(socket.socket, 'settimeout')
    @patch.object(socket.socket, 'connect')
    def test_blocking_connection_timeout(self, connect, settimeout):
        connect.side_effect = mock_timeout
        with self.assertRaises(exceptions.AMQPConnectionError):
            params = pika.ConnectionParameters(socket_timeout=2.0)
            blocking_connection.BlockingConnection(params)
        settimeout.assert_called_with(2.0)

    @patch.object(socket.socket, 'settimeout')
    @patch.object(socket.socket, 'connect')
    def test_select_connection_timeout(self, connect, settimeout):
        connect.side_effect = mock_timeout
        with self.assertRaises(exceptions.AMQPConnectionError):
            params = pika.ConnectionParameters(socket_timeout=2.0)
            select_connection.SelectConnection(params)
        settimeout.assert_called_with(2.0)

    @unittest.skipUnless(tornado_connection is not None,
                         'tornado is not installed')
    @patch.object(socket.socket, 'settimeout')
    @patch.object(socket.socket, 'connect')
    def test_tornado_connection_timeout(self, connect, settimeout):
        connect.side_effect = mock_timeout
        with self.assertRaises(exceptions.AMQPConnectionError):
            params = pika.ConnectionParameters(socket_timeout=2.0)
            tornado_connection.TornadoConnection(params)
        settimeout.assert_called_with(2.0)

    @unittest.skipUnless(twisted_connection is not None,
                         'twisted is not installed')
    @patch.object(socket.socket, 'settimeout')
    @patch.object(socket.socket, 'connect')
    def test_twisted_connection_timeout(self, connect, settimeout):
        connect.side_effect = mock_timeout
        with self.assertRaises(exceptions.AMQPConnectionError):
            params = pika.ConnectionParameters(socket_timeout=2.0)
            twisted_connection.TwistedConnection(params)
        settimeout.assert_called_with(2.0)

    @unittest.skipUnless(libev_connection is not None, 'pyev is not installed')
    @patch.object(socket.socket, 'settimeout')
    @patch.object(socket.socket, 'connect')
    def test_libev_connection_timeout(self, connect, settimeout):
        connect.side_effect = mock_timeout
        with self.assertRaises(exceptions.AMQPConnectionError):
            params = pika.ConnectionParameters(socket_timeout=2.0)
            libev_connection.LibevConnection(params)
        settimeout.assert_called_with(2.0)
