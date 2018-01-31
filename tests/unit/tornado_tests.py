"""
Tests for pika.adapters.tornado_connection

"""
import unittest

import mock

from pika.adapters import tornado_connection


class TornadoConnectionTests(unittest.TestCase):
    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_tornado_connection_call_parent(self, mock_init):
        obj = tornado_connection.TornadoConnection()
        mock_init.assert_called_once_with(
            None, None, None, None,
            tornado_connection.ioloop.IOLoop.instance(), False)
