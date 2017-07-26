# -*- coding: utf8 -*-
"""
Tests for connection parameters.

"""

# Suppress "Access to a protected member" warnings
# pylint: disable=W0212

# Suppress "Missing docstring" messages
# pylint: disable=C0111


import errno
import socket
try:
    import mock
except ImportError:
    from unittest import mock

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


def mock_timeout(*_args, **_kwargs):
    raise socket.timeout(errno.ETIMEDOUT, 'timed out')


class ConnectionTests(unittest.TestCase):

    def test_parameters(self):
        params = pika.ConnectionParameters(socket_timeout=0.5,
                                           retry_delay=0.1,
                                           connection_attempts=3)
        self.assertEqual(params.socket_timeout, 0.5)
        self.assertEqual(params.retry_delay, 0.1)
        self.assertEqual(params.connection_attempts, 3)

    def test_base_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch('pika.adapters.base_connection.BaseConnection'
                            '.add_timeout',
                            return_value='timer'):
                with mock.patch('pika.adapters.base_connection.BaseConnection'
                                '._create_tcp_connection_socket',
                                return_value=mock.Mock(
                                    spec_set=socket.socket,
                                    connect=mock.Mock(
                                        side_effect=mock_timeout))) as create_sock_mock:
                    params = pika.ConnectionParameters(socket_timeout=2.0)
                    conn = base_connection.BaseConnection(params)
                    conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    def test_blocking_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            # NOTE BlockingConnection uses SelectConnection as its "impl"
            with mock.patch('pika.SelectConnection'
                            '._create_tcp_connection_socket',
                            return_value=mock.Mock(
                                spec_set=socket.socket,
                                connect=mock.Mock(
                                    side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                blocking_connection.BlockingConnection(params)
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    def test_select_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch('pika.SelectConnection'
                            '._create_tcp_connection_socket',
                            return_value=mock.Mock(
                                spec_set=socket.socket,
                                connect=mock.Mock(
                                    side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                conn = select_connection.SelectConnection(params)
                conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    @unittest.skipUnless(tornado_connection is not None,
                         'tornado is not installed')
    def test_tornado_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch('pika.TornadoConnection'
                            '._create_tcp_connection_socket',
                            return_value=mock.Mock(
                                spec_set=socket.socket,
                                connect=mock.Mock(
                                    side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                conn = tornado_connection.TornadoConnection(params)
                conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    @unittest.skipUnless(twisted_connection is not None,
                         'twisted is not installed')
    def test_twisted_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch('pika.TwistedConnection._create_tcp_connection_socket',
                            return_value=mock.Mock(
                                spec_set=socket.socket,
                                connect=mock.Mock(
                                    side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                conn = twisted_connection.TwistedConnection(params)
                conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    @unittest.skipUnless(libev_connection is not None, 'pyev is not installed')
    def test_libev_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch('pika.LibevConnection'
                            '._create_tcp_connection_socket',
                            return_value=mock.Mock(
                                spec_set=socket.socket,
                                connect=mock.Mock(
                                    side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                conn = libev_connection.LibevConnection(params)
                conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))
