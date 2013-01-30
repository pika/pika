"""
Tests for pika.connection.Connection

"""
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import connection
from pika import channel


class ConnectionTests(unittest.TestCase):

    @mock.patch('pika.connection.Connection._adapter_connect')
    def setUp(self, connect):
        self.connection = connection.Connection()
        self.channel = mock.Mock(spec=channel.Channel)
        self.channel.is_open = True
        self.connection._channels[1] = self.channel
        self.connection._set_connection_state(connection.Connection.CONNECTION_OPEN)

    def tearDown(self):
        del self.connection
        del self.channel

    @mock.patch('pika.connection.Connection._send_connection_close')
    def test_close_closes_open_channels(self, send_connection_close):
        self.connection.close()
        self.channel.close.assert_called_once_with(200, 'Normal shutdown')

    @mock.patch('pika.connection.Connection._send_connection_close')
    def test_close_sent(self, send_connection_close):
        self.connection.close()
        send_connection_close.assert_called_once_with(200, 'Normal shutdown')
