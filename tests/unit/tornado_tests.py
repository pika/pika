"""
Tests for pika.adapters.tornado_connection

"""
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


class TornadoConnectionTests(unittest.TestCase):

    @unittest.skipIf(ioloop is None, 'requires Tornado')
    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_tornado_connection_call_parent(self, mock_init):
        obj = tornado_connection.TornadoConnection()
        mock_init.called_once_with(None, None, False)
