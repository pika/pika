"""
Tests for pika.adapters.tornado_connection

"""
import unittest

import mock

from pika.adapters import tornado_connection, selector_ioloop_adapter


class TornadoConnectionTests(unittest.TestCase):
    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_tornado_connection_call_parent(self, mock_init):
        _SelectorAsyncServicesAdapter = (
            selector_ioloop_adapter.SelectorAsyncServicesAdapter)
        bucket = []

        def construct_async_services_adapter(ioloop):
            adapter = _SelectorAsyncServicesAdapter(ioloop)
            bucket.append(adapter)
            return adapter

        with mock.patch(
                'pika.adapters.selector_ioloop_adapter.SelectorAsyncServicesAdapter',
                side_effect=construct_async_services_adapter):
            obj = tornado_connection.TornadoConnection()
        mock_init.assert_called_once_with(
            None, None, None, None,
            bucket[0])

        self.assertIs(bucket[0].get_native_ioloop(),
                      tornado_connection.ioloop.IOLoop.instance())
