"""
Tests for pika.adapters.tornado_connection

"""
import unittest

import mock

from pika.adapters import tornado_connection
from pika.adapters.utils import selector_ioloop_adapter


# missing-docstring
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103


class TornadoConnectionTests(unittest.TestCase):
    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_tornado_connection_call_parent(self, mock_init):
        _SelectorIOServicesAdapter = (
            selector_ioloop_adapter.SelectorIOServicesAdapter)
        bucket = []

        def construct_io_services_adapter(ioloop):
            adapter = _SelectorIOServicesAdapter(ioloop)
            bucket.append(adapter)
            return adapter

        with mock.patch('pika.adapters.utils.selector_ioloop_adapter.SelectorIOServicesAdapter',
                        side_effect=construct_io_services_adapter):
            tornado_connection.TornadoConnection()
        mock_init.assert_called_once_with(
            None, None, None, None,
            bucket[0],
            internal_connection_workflow=True)

        self.assertIs(bucket[0].get_native_ioloop(),
                      tornado_connection.ioloop.IOLoop.instance())
