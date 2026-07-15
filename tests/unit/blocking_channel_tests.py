"""Tests for pika.adapters.blocking_connection.BlockingChannel."""

import unittest
from collections import deque
from unittest import mock

from pika import channel
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

        with self.assertRaises(TestException):
            with self.obj:
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
