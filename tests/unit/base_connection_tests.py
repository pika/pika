"""
Tests for pika.base_connection.BaseConnection

"""
try:
    import mock
except:
    from unittest import mock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika
from pika.adapters import base_connection


class BaseConnectionTests(unittest.TestCase):

    def setUp(self):
        with mock.patch('pika.connection.Connection.connect'):
            self.connection = base_connection.BaseConnection()
            self.connection._set_connection_state(
                base_connection.BaseConnection.CONNECTION_OPEN)

    def test_repr(self):
        text = repr(self.connection)
        self.assertTrue(text.startswith('<BaseConnection'), text)

    def test_should_raise_value_exception_with_no_params_func_instead(self):

        def foo():
            return True

        self.assertRaises(ValueError, base_connection.BaseConnection, foo)

    def test_ssl_wrap_socket_with_none_ssl_options(self):

        params = pika.ConnectionParameters(ssl_options=None)
        self.assertIsNone(params.ssl_options)

        with mock.patch('pika.connection.Connection.connect'):
            conn = base_connection.BaseConnection(parameters=params)

            with mock.patch('pika.adapters.base_connection'
                            '.ssl.wrap_socket') as wrap_socket_mock:
                sock_mock = mock.Mock()
                conn._wrap_socket(sock_mock)

                wrap_socket_mock.assert_called_once_with(
                    sock_mock,
                    do_handshake_on_connect=conn.DO_HANDSHAKE)

    def test_ssl_wrap_socket_with_dict_ssl_options(self):

        ssl_options = dict(ssl='options', handshake=False)
        params = pika.ConnectionParameters(ssl_options=ssl_options)
        self.assertEqual(params.ssl_options, ssl_options)

        with mock.patch('pika.connection.Connection.connect'):
            conn = base_connection.BaseConnection(parameters=params)

            with mock.patch('pika.adapters.base_connection'
                            '.ssl.wrap_socket') as wrap_socket_mock:
                sock_mock = mock.Mock()
                conn._wrap_socket(sock_mock)

                wrap_socket_mock.assert_called_once_with(
                    sock_mock,
                    do_handshake_on_connect=conn.DO_HANDSHAKE,
                    ssl='options',
                    handshake=False)
