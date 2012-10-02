# -*- coding: utf8 -*-
"""
Tests for connection parameters.
"""
import socket
from mock import patch
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import ConnectionParameters, BaseConnection
from pika.exceptions import AMQPConnectionError


def mock_timeout(*args, **kwargs):
    raise socket.timeout


class ConnectionTests(unittest.TestCase):

    def test_parameters(self):
        params = ConnectionParameters(socket_timeout=0.5,
                                      retry_delay=0.1,
                                      connection_attempts=3,
                                      )
        self.assertEqual(params.socket_timeout, 0.5)
        self.assertEqual(params.retry_delay, 0.1)
        self.assertEqual(params.connection_attempts, 3)


    @patch.object(socket.socket, 'settimeout')
    @patch.object(socket.socket, 'connect')
    def test_connection_timeout(self, connect, settimeout):
        connect.side_effect = mock_timeout
        with self.assertRaises(AMQPConnectionError):
            BaseConnection(ConnectionParameters(socket_timeout=2.0))
        settimeout.assert_called_with(2.0)
