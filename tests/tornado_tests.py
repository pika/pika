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

    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_tornado_connection_call_parent(self, mock_init):
        obj = tornado_connection.TornadoConnection()
        mock_init.called_once_with(None, None, False)
