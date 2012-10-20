"""
Tests for pika.channel.ContentFrameDispatcher

"""
import collections
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import channel
from pika import connection
from pika import exceptions
from pika import frame
from pika import spec


class ChannelTest(unittest.TestCase):

    @mock.patch('pika.connection.Connection')
    def setUp(self, connection):
        self.connection = connection
        self._on_open_callback = mock.Mock()
        self.obj = channel.Channel(self.connection, 1,
                                   self._on_open_callback)

    def tearDown(self):
        del self.connection
        del self._on_open_callback
        del self.obj

    def test_init_invalid_channel_number(self):
        self.assertRaises(exceptions.InvalidChannelNumber,
                          channel.Channel,
                          'Foo', self.connection)

    def test_init_channel_number(self):
        self.assertEqual(self.obj.channel_number, 1)

    def test_init_callbacks(self):
        self.assertEqual(self.obj.callbacks, self.connection.callbacks)

    def test_init_connection(self):
        self.assertEqual(self.obj.connection, self.connection)

    def test_init_frame_dispatcher(self):
        self.assertIsInstance(self.obj.frame_dispatcher,
                              channel.ContentFrameDispatcher)

    def test_init_blocked(self):
        self.assertIsInstance(self.obj._blocked, collections.deque)

    def test_init_blocking(self):
        self.assertEqual(self.obj._blocking, None)

    def test_init_flow(self):
        self.assertEqual(self.obj._flow, None)

    def test_init_has_on_flow_callback(self):
        self.assertEqual(self.obj._has_on_flow_callback, False)

    def test_init_on_open_callback(self):
        self.assertEqual(self.obj._on_open_callback, self._on_open_callback)

    def test_init_state(self):
        self.assertEqual(self.obj._state, channel.Channel.CLOSED)

    def test_init_cancelled(self):
        self.assertEqual(self.obj._cancelled, list())

    def test_init_consumers(self):
        self.assertEqual(self.obj._consumers, dict())

    def test_init_pending(self):
        self.assertEqual(self.obj._pending, dict())

    def test_init_on_get_ok_callback(self):
        self.assertEqual(self.obj._on_get_ok_callback, None)

    def test_init_on_flow_ok_callback(self):
        self.assertEqual(self.obj._on_flow_ok_callback, None)

    def test_init_reply_code(self):
        self.assertEqual(self.obj._reply_code, None)

    def test_init_reply_text(self):
        self.assertEqual(self.obj._reply_text, None)

    def test_add_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_callback(mock_callback, [spec.Basic.Qos])
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Basic.Qos,
                                                              mock_callback,
                                                              True)

    def test_add_callback_multiple_replies(self):
        mock_callback = mock.Mock()
        self.obj.add_callback(mock_callback, [spec.Basic.Qos, spec.Basic.QosOk])
        calls = [mock.call(self.obj.channel_number, spec.Basic.Qos,
                           mock_callback, True),
                 mock.call(self.obj.channel_number, spec.Basic.QosOk,
                           mock_callback, True)]
        self.connection.callbacks.add.assert_has_calls(calls)

    def test_add_on_basic_cancel_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_basic_cancel_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Basic.Cancel,
                                                              mock_callback,
                                                              False)

    def test_add_on_close_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_close_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Channel.Close,
                                                              mock_callback,
                                                              False)

    def test_add_on_flow_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_flow_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Channel.Flow,
                                                              mock_callback,
                                                              False)

    def test_add_on_return_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_return_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              '_on_basic_return',
                                                              mock_callback,
                                                              False)

    def test_basic_ack_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.basic_ack)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_ack_channel_calls_rpc(self, rpc):
        self.obj._set_state(self.obj.OPEN)
        self.obj.basic_ack(1, False)
        rpc.assert_called_once_with(spec.Basic.Ack(1, False))
