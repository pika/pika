"""
Tests for pika.adapters.tornado_connection

"""
import platform

try:
    from tornado import ioloop
except ImportError:
    ioloop = None

try:
    import mock
except ImportError:
    from unittest import mock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

try:
    from pika.adapters import tornado_connection
except ImportError:
    tornado_connection = None

from pika import __version__
from pika import connection


class TornadoConnectionTests(unittest.TestCase):

    @unittest.skipIf(ioloop is None, 'requires Tornado')
    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_tornado_connection_call_parent(self, mock_init):
        obj = tornado_connection.TornadoConnection()
        mock_init.called_once_with(None, None, False)

    def test_client_properties_default(self):
        expectation = {
            'product': connection.PRODUCT,
            'platform': 'Python %s' % platform.python_version(),
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See http://pika.rtfd.org',
            'version': __version__
        }
        with mock.patch('pika.connection.Connection.connect'):
            conn = tornado_connection.TornadoConnection()
            self.assertDictEqual(conn._client_properties, expectation)

    def test_client_properties_override(self):
        expectation = {
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            }
        }
        override = {'product': 'My Product',
                    'platform': 'Your platform',
                    'version': '0.1',
                    'information': 'this is my app'}
        expectation.update(override)
        with mock.patch('pika.connection.Connection.connect'):
            conn = tornado_connection.TornadoConnection(
                    client_properties=override)
            self.assertDictEqual(conn._client_properties, expectation)
