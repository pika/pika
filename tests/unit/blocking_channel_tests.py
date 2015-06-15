# -*- coding: utf8 -*-
"""
Tests for pika.adapters.blocking_connection.BlockingChannel

"""
from collections import deque
import logging

try:
    import mock
except ImportError:
    from unittest import mock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika.adapters import blocking_connection
from pika import callback
from pika import channel
from pika import frame
from pika import spec

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
        channelImplMock = mock.Mock(spec=ChannelTemplate,
                                    is_closing=False,
                                    is_closed=False,
                                    is_open=True)
        self.obj = blocking_connection.BlockingChannel(channelImplMock,
                                                       self.connection)
    def tearDown(self):
        del self.connection
        del self.obj

    def test_init_initial_value_confirmation(self):
        self.assertFalse(self.obj._delivery_confirmation)

    def test_init_initial_value_pending_events(self):
        self.assertEqual(self.obj._pending_events, deque())

    def test_init_initial_value_returned_messages(self):
        self.assertListEqual(self.obj._returned_messages, list())

    def test_has_event_and_get_event(self):
        self.obj.create_consumer("queue")
        self.assertFalse(self.obj.has_event())

        self.obj._pending_events.append("test1")
        self.assertTrue(self.obj.has_event())

        self.obj._pending_events.append("test2")
        self.assertTrue(self.obj.has_event())

        self.assertEqual(self.obj.get_event(), "test1")
        self.assertTrue(self.obj.has_event())

        self.assertEqual(self.obj.get_event(), "test2")
        self.assertFalse(self.obj.has_event())
