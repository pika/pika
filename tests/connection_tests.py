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

    @mock.patch('pika.connection.Connection.connect')
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

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_close_ready_open_channels(self, on_close_ready):
        """if open channels _on_close_ready shouldn't be called"""
        self.connection.close()
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_close_ready_no_open_channels(self, on_close_ready):
        self.connection._channels = dict()
        self.connection.close()
        self.assertTrue(on_close_ready.called,
                        '_on_close_ready should have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_closeok_no_open_channels(self, on_close_ready):
        """Should call _on_close_ready if connection is closing and there are
        no open channels

        """
        self.connection._channels = dict()
        self.connection.close()
        self.assertTrue(on_close_ready.called,
                        '_on_close_ready should been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_closeok_open_channels(self, on_close_ready):
        """if connection is closing but channels remain open do not call
        _on_close_ready

        """
        self.connection.close()
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_closeok_non_closing_state(self, on_close_ready):
        """if connection isn't closing _on_close_ready should not be called"""
        self.connection._on_channel_closeok(mock.Mock())
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    def test_on_disconnect(self):
        """if connection isn't closing _on_close_ready should not be called"""
        self.connection._on_disconnect(0, 'Undefined')
        self.assertTrue(self.channel._on_close.called,
                        'channel._on_close should have been called')
        method_frame = self.channel._on_close.call_args[0][0]
        self.assertEqual(method_frame.method.reply_code, 0)
        self.assertEqual(method_frame.method.reply_text, 'Undefined')
