"""
Tests for pika.adapters.twisted_connection
"""
try:
    from twisted.internet import reactor as ioloop
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
    from pika.adapters import twisted_connection
except ImportError:
    twisted_connection = None


class TwistedProtocolConnectionTests(unittest.TestCase):

    @unittest.skipIf(ioloop is None, 'requires Twisted')
    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_twisted_protocol_connection_call_parent(self, mock_init):
        obj = twisted_connection.TwistedProtocolConnection(None, on_close_callback=self._on_close)
        mock_init.called_once_with(None, on_close_callback=self._on_close)

    @staticmethod
    def _on_close(connection, reply_code, reply_text):
        pass

