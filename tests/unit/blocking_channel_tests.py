# -*- coding: utf8 -*-
"""
Tests for pika.adapters.blocking_connection.BlockingChannel

"""
from collections import deque
import unittest

import mock

from pika.adapters import blocking_connection
from pika import channel

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
        channel_impl_mock = mock.Mock(
            spec=ChannelTemplate,
            is_closing=False,
            is_closed=False,
            is_open=True)
        self.obj = blocking_connection.BlockingChannel(channel_impl_mock,
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

    def test_basic_consume(self):
        with mock.patch.object(self.obj._impl, '_generate_consumer_tag'):
            self.obj._impl._generate_consumer_tag.return_value = 'ctag0'
            self.obj._impl.basic_consume.return_value = 'ctag0'

            self.obj.basic_consume(mock.Mock(), "queue")

            self.assertEqual(self.obj._consumer_infos['ctag0'].state,
                             blocking_connection._ConsumerInfo.ACTIVE)

    def test_context_manager(self):
        with self.obj as chan:
            self.assertFalse(chan._impl.close.called)
        chan._impl.close.assert_called_once_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_context_manager_does_not_suppress_exception(self):
        class TestException(Exception):
            pass

        with self.assertRaises(TestException):
            with self.obj as chan:
                self.assertFalse(chan._impl.close.called)
                raise TestException()
        chan._impl.close.assert_called_once_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_context_manager_exit_with_closed_channel(self):
        with self.obj as chan:
            self.assertFalse(chan._impl.close.called)
            chan.close()
        chan._impl.close.assert_called_with(
            reply_code=0, reply_text='Normal shutdown')
