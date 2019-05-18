"""
Tests for pika.base_connection.BaseConnection

"""

import socket
import unittest
import mock

import pika
import pika.tcp_socket_opts
from pika.adapters import base_connection


# pylint: disable=C0111,W0212,C0103

# If this is missing, set it manually. We need it to test tcp opt setting.
try:
    TCP_KEEPIDLE = socket.TCP_KEEPIDLE
except AttributeError:
    TCP_KEEPIDLE = 4


class ConstructibleBaseConnection(base_connection.BaseConnection):
    """Adds dummy overrides for `BaseConnection`'s abstract methods so
    that we can instantiate and test it.

    """
    @classmethod
    def create_connection(cls, *args, **kwargs):  # pylint: disable=W0221
        raise NotImplementedError


class BaseConnectionTests(unittest.TestCase):
    def setUp(self):
        with mock.patch.object(ConstructibleBaseConnection,
                               '_adapter_connect_stream'):
            self.connection = ConstructibleBaseConnection(
                None, None, None, None, None,
                internal_connection_workflow=True)
            self.connection._set_connection_state(
                ConstructibleBaseConnection.CONNECTION_OPEN)

    def test_repr(self):
        text = repr(self.connection)
        self.assertTrue(text.startswith('<ConstructibleBaseConnection'), text)

    def test_should_raise_value_exception_with_no_params_func_instead(self):
        self.assertRaises(ValueError, ConstructibleBaseConnection,
                          lambda: True, None, None, None, None,
                          internal_connection_workflow=True)

    def test_tcp_options_with_dict_tcp_options(self):

        tcp_options = dict(TCP_KEEPIDLE=60)
        params = pika.ConnectionParameters(tcp_options=tcp_options)
        self.assertEqual(params.tcp_options, tcp_options)

        with mock.patch.dict('pika.tcp_socket_opts._SUPPORTED_TCP_OPTIONS',
                             {'TCP_KEEPIDLE': TCP_KEEPIDLE}):
            sock_mock = mock.Mock()
            pika.tcp_socket_opts.set_sock_opts(params.tcp_options, sock_mock)

            expected = [
                mock.call.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE,
                                     1),
                mock.call.setsockopt(socket.SOL_TCP, TCP_KEEPIDLE, 60)
            ]
            self.assertEqual(sock_mock.method_calls, expected)

    def test_tcp_options_with_invalid_tcp_options(self):

        tcp_options = dict(TCP_EVIL_OPTION=1234)
        params = pika.ConnectionParameters(tcp_options=tcp_options)
        self.assertEqual(params.tcp_options, tcp_options)

        sock_mock = mock.Mock()
        pika.tcp_socket_opts.set_sock_opts(params.tcp_options, sock_mock)

        keepalive_call = mock.call.setsockopt(socket.SOL_SOCKET,
                                              socket.SO_KEEPALIVE, 1)
        self.assertNotIn(keepalive_call, sock_mock.method_calls)

    def test_tcp_options_with_none_tcp_options(self):

        params = pika.ConnectionParameters(tcp_options=None)
        self.assertIsNone(params.tcp_options)

        sock_mock = mock.Mock()
        pika.tcp_socket_opts.set_sock_opts(params.tcp_options, sock_mock)

        keepalive_call = mock.call.setsockopt(socket.SOL_SOCKET,
                                              socket.SO_KEEPALIVE, 1)
        self.assertNotIn(keepalive_call, sock_mock.method_calls)
