"""Tests for pika.adapters.blocking_connection.BlockingChannel."""

import unittest
from collections import deque
from unittest import mock

from pika import channel, exceptions, frame, spec
from pika.adapters import blocking_connection

BLOCKING_CHANNEL = 'pika.adapters.blocking_connection.BlockingChannel'
BLOCKING_CONNECTION = 'pika.adapters.blocking_connection.BlockingConnection'


class ChannelTemplate(channel.Channel):
    channel_number = 1


class BlockingChannelTests(unittest.TestCase):

    @mock.patch(BLOCKING_CONNECTION)
    def _create_connection(self, connection=None):
        return connection

    def setUp(self):
        self.connection = self._create_connection()
        self.channel_impl_mock = mock.Mock(spec=ChannelTemplate,
                                           is_closing=False,
                                           is_closed=False,
                                           is_open=True)
        self.obj = blocking_connection.BlockingChannel(self.channel_impl_mock,
                                                       self.connection)

    def tearDown(self):
        del self.connection
        del self.obj

    def test_init_initial_value_confirmation(self):
        self.assertFalse(self.obj._delivery_confirmation)

    def test_init_initial_value_pending_events(self):
        self.assertEqual(self.obj._pending_events, deque())

    def test_init_initial_value_buback_return(self):
        self.assertIsNone(self.obj._puback_return)

    def test_basic_consume_legacy_parameter_queue(self):
        # This is for the unlikely scenario where only
        # the first parameter is updated
        with self.assertRaises(TypeError):
            self.obj.basic_consume('queue', 'whoops this should be a callback')

    def test_basic_consume_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.basic_consume(mock.Mock(), 'queue')

    def test_queue_declare_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.queue_declare(mock.Mock(), 'queue')

    def test_exchange_declare_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.exchange_declare(mock.Mock(), 'exchange')

    def test_queue_bind_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.queue_bind(mock.Mock(), 'queue', 'exchange')

    def _broker_cancelled_consumer(self, auto_ack):
        info = blocking_connection._ConsumerInfo(
            'ctag', auto_ack=auto_ack, on_message_callback=mock.Mock())
        info.state = blocking_connection._ConsumerInfo.CANCELLED_BY_BROKER
        self.obj._consumer_infos['ctag'] = info
        self.channel_impl_mock._consumers = {}
        self.channel_impl_mock.channel_number = 1
        self.obj._pending_events.extend([
            blocking_connection._ConsumerDeliveryEvt(
                spec.Basic.Deliver(consumer_tag='ctag', delivery_tag=7),
                spec.BasicProperties(), b''),
            blocking_connection._ConsumerCancellationEvt(
                frame.Method(1, spec.Basic.Cancel(consumer_tag='ctag'))),
        ])

    def test_basic_cancel_does_not_cancel_a_broker_cancelled_consumer_again(
            self):
        self._broker_cancelled_consumer(auto_ack=False)
        events = mock.Mock()
        events.attach_mock(self.channel_impl_mock.basic_reject, 'reject')
        events.attach_mock(self.connection._flush_output, 'flush')

        self.assertEqual(self.obj.basic_cancel('ctag'), [])

        self.channel_impl_mock.basic_cancel.assert_not_called()
        self.assertEqual([c[0] for c in events.mock_calls], ['reject', 'flush'])
        # Waiting on anything but "always ready" would never end: no reply is coming.
        self.assertEqual(self.connection._flush_output.call_args[0][1:],
                         self.obj._ALWAYS_READY_WAITERS)
        self.assertEqual(len(self.obj._pending_events), 0)

    def test_basic_cancel_returns_a_broker_cancelled_auto_ack_consumers_messages(
            self):
        self._broker_cancelled_consumer(auto_ack=True)

        messages = self.obj.basic_cancel('ctag')

        self.channel_impl_mock.basic_cancel.assert_not_called()
        self.assertEqual([m[0].delivery_tag for m in messages], [7])
        self.assertEqual(len(self.obj._pending_events), 0)

    def test_basic_cancel_of_a_broker_cancelled_consumer_on_a_closing_channel_raises(
            self):
        self._broker_cancelled_consumer(auto_ack=True)
        self.channel_impl_mock._raise_if_not_open.side_effect = (
            exceptions.ChannelWrongStateError('Channel is closing.'))

        with self.assertRaises(exceptions.ChannelWrongStateError):
            self.obj.basic_cancel('ctag')

    def test_basic_cancel_legacy_parameter(self):
        with self.assertRaises(TypeError):
            self.obj.basic_cancel(mock.Mock(), 'tag')

    def test_basic_get_legacy_parameter(self):
        with self.assertRaises(TypeError):
            self.obj.basic_get(mock.Mock())

    def test_basic_consume(self):
        with mock.patch.object(self.obj._impl, '_generate_consumer_tag'):
            self.channel_impl_mock._generate_consumer_tag.return_value = 'ctag0'
            self.channel_impl_mock.basic_consume.return_value = 'ctag0'

            self.obj.basic_consume('queue', mock.Mock())

            self.assertEqual(self.obj._consumer_infos['ctag0'].state,
                             blocking_connection._ConsumerInfo.ACTIVE)

    def test_context_manager(self):
        with self.obj:
            self.assertFalse(self.channel_impl_mock.close.called)
        self.channel_impl_mock.close.assert_called_once_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_context_manager_does_not_suppress_exception(self):

        class TestException(Exception):
            pass

        with self.assertRaises(TestException), self.obj:
            self.assertFalse(self.channel_impl_mock.close.called)
            raise TestException()
        self.channel_impl_mock.close.assert_called_once_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_context_manager_exit_with_closed_channel(self):
        with self.obj as chan:
            self.assertFalse(self.channel_impl_mock.close.called)
            chan.close()
        self.channel_impl_mock.close.assert_called_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_consumer_tags_property(self):
        with mock.patch.object(self.obj._impl, '_generate_consumer_tag'):
            self.assertEqual(0, len(self.obj.consumer_tags))
            self.channel_impl_mock._generate_consumer_tag.return_value = 'ctag0'
            self.channel_impl_mock.basic_consume.return_value = 'ctag0'
            self.obj.basic_consume('queue', mock.Mock())
            self.assertEqual(1, len(self.obj.consumer_tags))
            self.assertIn('ctag0', self.obj.consumer_tags)
