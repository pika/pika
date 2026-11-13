"""Channel number reuse during close notifications."""

import unittest
from unittest import mock

from pika import exceptions, frame, spec
from tests.unit.connection_tests import ConstructibleConnection


class ChannelReopenTests(unittest.TestCase):

    def setUp(self):
        self.connection = ConstructibleConnection(
            internal_connection_workflow=False)
        self.connection._set_connection_state(self.connection.CONNECTION_OPEN)
        self.connection._opened = True
        self.connection._adapter_emit_data = mock.Mock()
        self.channel = self.connection.channel(5)
        self.receive(spec.Channel.OpenOk())

    def receive(self, method):
        self.connection._process_frame(frame.Method(5, method))

    def reopen(self, channel, reason):
        self.assertIs(channel, self.channel)
        self.assertTrue(channel.is_closed)
        self.replacement = self.connection.channel(5, self.on_open)

    def on_open(self, channel):
        self.assertIs(channel, self.replacement)
        channel.basic_qos(callback=self.qos_done)

    def verify_replacement(self):
        self.assertIs(self.connection._channels[5], self.replacement)
        self.assertTrue(self.replacement.is_opening)
        self.qos_done = mock.Mock()
        self.receive(spec.Channel.OpenOk())
        self.assertTrue(self.replacement.is_open)
        self.receive(spec.Basic.QosOk())
        self.qos_done.assert_called_once()

    def test_client_close_callback_reopens_number_and_preserves_later_callbacks(
            self):
        later = mock.Mock()
        self.channel.add_on_close_callback(self.reopen)
        self.channel.add_on_close_callback(later)
        self.channel.close()
        self.receive(spec.Channel.CloseOk())

        later.assert_called_once_with(self.channel,
                                      self.channel._closing_reason)
        self.verify_replacement()

    def test_broker_close_callback_reopens_number(self):
        self.channel.add_on_close_callback(self.reopen)
        self.receive(spec.Channel.Close(404, 'missing queue'))
        self.assertIsInstance(self.channel._closing_reason,
                              exceptions.ChannelClosedByBroker)
        self.verify_replacement()

    def test_old_channel_callback_changes_do_not_affect_replacement(self):
        self.channel.add_on_close_callback(self.reopen)
        self.channel.close()
        self.receive(spec.Channel.CloseOk())
        callback = mock.Mock()
        self.replacement.add_callback(callback, [spec.Basic.QosOk])
        self.assertFalse(self.channel.remove_callback(callback))
        stale = mock.Mock()
        self.channel.add_callback(stale, [spec.Basic.QosOk])
        self.channel._cleanup()

        self.verify_replacement()
        callback.assert_called_once()
        stale.assert_not_called()

    def test_callback_exception_after_reopen_preserves_replacement(self):
        error = RuntimeError('close observer failed')

        def reopen_and_raise(channel, reason):
            self.reopen(channel, reason)
            raise error

        self.channel.add_on_close_callback(reopen_and_raise)
        self.channel.close()
        with self.assertRaises(RuntimeError) as raised:
            self.receive(spec.Channel.CloseOk())
        self.assertIs(raised.exception, error)
        self.verify_replacement()

    def test_connection_close_after_reopen_waits_for_replacement(self):
        self.connection._on_close_ready = mock.Mock()
        self.channel.add_on_close_callback(self.reopen)
        self.channel.add_on_close_callback(lambda *_: self.connection.close())
        self.channel.close()
        self.receive(spec.Channel.CloseOk())

        self.assertIs(self.connection._channels[5], self.replacement)
        self.assertTrue(self.replacement.is_closing)
        self.connection._on_close_ready.assert_not_called()
        self.receive(spec.Channel.OpenOk())
        self.receive(spec.Channel.CloseOk())
        self.assertEqual(self.connection._channels, {})
        self.connection._on_close_ready.assert_called_once()

    def test_reopen_when_connection_is_closing_is_rejected_before_cleanup(self):
        self.connection._on_close_ready = mock.Mock()

        def on_closed(channel, reason):
            with self.assertRaises(exceptions.ConnectionWrongStateError):
                self.connection.channel(5)
            self.assertIs(self.connection._channels[5], channel)
            self.connection._on_close_ready.assert_not_called()

        self.channel.add_on_close_callback(on_closed)
        self.connection.close()
        self.receive(spec.Channel.CloseOk())
        self.assertEqual(self.connection._channels, {})
        self.connection._on_close_ready.assert_called_once()

    def test_simultaneous_close_keeps_number_until_closeok(self):
        self.channel.add_on_close_callback(self.reopen)
        self.channel.close()
        self.receive(spec.Channel.Close(404, 'missing queue'))
        with self.assertRaisesRegex(ValueError, 'already in use'):
            self.connection.channel(5)
        self.receive(spec.Channel.CloseOk())
        self.verify_replacement()

    def test_broker_close_while_client_close_is_blocked_allows_reopen(self):
        self.channel.add_on_close_callback(self.reopen)
        self.channel.queue_declare('missing',
                                   passive=True,
                                   callback=lambda _: None)
        self.channel.close()
        self.assertTrue(self.channel._blocked)
        self.receive(spec.Channel.Close(404, 'missing queue'))
        self.verify_replacement()

    def test_second_close_observer_cannot_replace_opening_channel(self):

        def reopen_again(channel, reason):
            with self.assertRaisesRegex(ValueError, 'already in use'):
                self.connection.channel(5)

        self.channel.add_on_close_callback(self.reopen)
        self.channel.add_on_close_callback(reopen_again)
        self.channel.close()
        self.receive(spec.Channel.CloseOk())
        self.verify_replacement()

    def test_cleanup_error_preserves_exception_and_releases_old_state(self):
        error = RuntimeError('cleanup failed')
        self.channel._add_on_cleanup_callback(mock.Mock(side_effect=error))
        self.channel._consumers['consumer'] = mock.Mock()
        self.channel._set_cookie(object())
        self.channel.add_on_close_callback(self.reopen)
        with self.assertRaises(RuntimeError) as raised:
            self.receive(spec.Channel.Close(404, 'missing queue'))

        self.assertIs(raised.exception, error)
        self.assertEqual(self.connection._channels, {})
        self.assertEqual(self.connection.callbacks.keys(5), [])
        self.assertEqual(self.channel.consumer_tags, [])
        self.assertIsNone(self.channel._get_cookie())

    def test_transport_loss_rejects_reopen_and_clears_channel_state(self):
        error = exceptions.StreamLostError('connection lost')

        def on_closed(channel, reason):
            self.assertIs(reason, error)
            self.assertTrue(self.connection.is_closed)
            with self.assertRaises(exceptions.ConnectionWrongStateError):
                self.connection.channel(5)

        self.channel.add_on_close_callback(on_closed)
        self.connection._on_stream_terminated(error)
        self.assertTrue(self.channel.is_closed)
        self.assertEqual(self.connection._channels, {})
        self.assertEqual(self.connection.callbacks.keys(5), [])
