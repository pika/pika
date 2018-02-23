# -*- coding: utf8 -*-
"""
Tests for connection parameters.

"""
import errno
import socket
import unittest

import mock

import pika
from pika import exceptions
try:
    from pika.adapters import asyncio_connection
except ImportError:
    asyncio_connection = None
from pika.adapters import base_connection
from pika.adapters import blocking_connection
from pika.adapters import select_connection
from pika.adapters import tornado_connection
from pika.adapters import twisted_connection


def mock_timeout(*_args, **_kwargs):
    raise socket.timeout(errno.ETIMEDOUT, 'timed out')


class ConnectionTests(unittest.TestCase):
    def test_parameters(self):
        params = pika.ConnectionParameters(
            socket_timeout=0.5, retry_delay=0.1, connection_attempts=3)
        self.assertEqual(params.socket_timeout, 0.5)
        self.assertEqual(params.retry_delay, 0.1)
        self.assertEqual(params.connection_attempts, 3)

    @unittest.skipIf(asyncio_connection is None, 'asyncio not available')
    def test_asyncio_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch(
                    'pika.adapters.asyncio_connection.AsyncioConnection'
                    '.add_timeout',
                    return_value='timer'):
                with mock.patch(
                        'pika.adapters.asyncio_connection.AsyncioConnection'
                        '._create_tcp_connection_socket',
                        return_value=mock.Mock(
                            spec_set=socket.socket,
                            connect=mock.Mock(side_effect=mock_timeout))
                ) as create_sock_mock:
                    params = pika.ConnectionParameters(socket_timeout=2.0)
                    ioloop = asyncio_connection.asyncio.new_event_loop()
                    self.addCleanup(ioloop.close)
                    conn = asyncio_connection.AsyncioConnection(
                        params,
                        custom_ioloop=ioloop)
                    conn._on_connect_timer()

        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    def test_base_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch(
                    'pika.adapters.base_connection.BaseConnection'
                    '.add_timeout',
                    return_value='timer'):
                with mock.patch(
                        'pika.adapters.base_connection.BaseConnection'
                        '._create_tcp_connection_socket',
                        return_value=mock.Mock(
                            spec_set=socket.socket,
                            connect=mock.Mock(side_effect=mock_timeout))
                ) as create_sock_mock:
                    params = pika.ConnectionParameters(socket_timeout=2.0)
                    conn = base_connection.BaseConnection(params)
                    conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    def test_blocking_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            # NOTE BlockingConnection uses SelectConnection as its "impl"
            with mock.patch(
                    'pika.SelectConnection'
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
            with mock.patch(
                    'pika.SelectConnection'
                    '._create_tcp_connection_socket',
                    return_value=mock.Mock(
                        spec_set=socket.socket,
                        connect=mock.Mock(
                            side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                conn = select_connection.SelectConnection(params)
                self.addCleanup(conn.ioloop.close)
                conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    def test_tornado_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch(
                    'pika.TornadoConnection'
                    '._create_tcp_connection_socket',
                    return_value=mock.Mock(
                        spec_set=socket.socket,
                        connect=mock.Mock(
                            side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                ioloop = tornado_connection.ioloop.IOLoop()
                self.addCleanup(ioloop.close)
                conn = tornado_connection.TornadoConnection(
                    params,
                    custom_ioloop=ioloop)
                conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))

    def test_twisted_connection_timeout(self):
        with self.assertRaises(exceptions.AMQPConnectionError) as err_ctx:
            with mock.patch(
                    'pika.TwistedConnection._create_tcp_connection_socket',
                    return_value=mock.Mock(
                        spec_set=socket.socket,
                        connect=mock.Mock(
                            side_effect=mock_timeout))) as create_sock_mock:
                params = pika.ConnectionParameters(socket_timeout=2.0)
                conn = twisted_connection.TwistedConnection(params)
                conn._on_connect_timer()
        create_sock_mock.return_value.settimeout.assert_called_with(2.0)
        self.assertIn('timeout', str(err_ctx.exception))
