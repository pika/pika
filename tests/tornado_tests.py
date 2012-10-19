"""
Tests for pika.adapters.tornado_connection

"""
from tornado import ioloop
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika.adapters import tornado_connection


class TornadoConnectionTests(unittest.TestCase):

    def test_tornado_connection_ioloop_override(self):
        ioloop_value = mock.Mock()
        obj = tornado_connection.TornadoConnection(custom_ioloop=ioloop_value)
        self.assertEqual(obj.ioloop, ioloop_value)

    def test_tornado_connection_ioloop_singleton(self):
        ioloop_instance = ioloop.IOLoop.instance()
        obj = tornado_connection.TornadoConnection()
        self.assertEqual(obj.ioloop, ioloop_instance)

    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_tornado_connection_call_parent(self, mock_init):
        obj = tornado_connection.TornadoConnection()
        mock_init.called_once_with(None, None, False)

    @mock.patch('pika.adapters.base_connection.BaseConnection._adapter_disconnect')
    def test_tornado_adapter_disconnect_call_parent(self, adapter_disconnect):
        obj = tornado_connection.TornadoConnection()
        obj._adapter_disconnect()
        adapter_disconnect.assert_called_once_with()

    @mock.patch('time.time')
    def test_add_timeout(self, time):
        time.return_value = 20
        ioloop_value = mock.Mock()
        callback = mock.Mock()
        obj = tornado_connection.TornadoConnection(custom_ioloop=ioloop_value)
        obj.add_timeout(10, callback)
        ioloop_value.add_timeout.called_once_with(30, callback)

    def test_remove_timeout(self):
        ioloop_value = mock.Mock()
        callback = mock.Mock()
        obj = tornado_connection.TornadoConnection(custom_ioloop=ioloop_value)
        timeout_id = obj.add_timeout(10, callback)
        obj.remove_timeout(timeout_id)
        ioloop_value.remove_timeout.called_once_with(timeout_id)
