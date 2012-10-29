# -*- coding: utf8 -*-
"""
Tests for pika.adapters.blocking_connection.BlockingChannel

"""
import logging
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika.adapters import blocking_connection
from pika import callback
from pika import frame
from pika import spec

BLOCKING_CHANNEL = 'pika.adapters.blocking_connection.BlockingChannel'
BLOCKING_CONNECTION = 'pika.adapters.blocking_connection.BlockingConnection'


class BlockingChannelTests(unittest.TestCase):

    @mock.patch(BLOCKING_CONNECTION)
    def _create_connection(self, connection=None):
        return connection

    def setUp(self):
        self.connection = self._create_connection()
        with mock.patch(BLOCKING_CHANNEL + '.open') as _open:
            self.obj = blocking_connection.BlockingChannel(self.connection, 1)
            self._open = _open

    def tearDown(self):
        del self.connection
        del self.obj

    def test_init_initial_value_confirmation(self):
        self.assertFalse(self.obj._confirmation)

    def test_init_initial_value_force_data_events_override(self):
        self.assertFalse(self.obj._force_data_events_override)

    def test_init_initial_value_frames(self):
        self.assertDictEqual(self.obj._frames, dict())

    def test_init_initial_value_replies(self):
        self.assertListEqual(self.obj._replies, list())

    def test_init_initial_value_wait(self):
        self.assertFalse(self.obj._wait)

    def test_init_open_called(self):
        self._open.assert_called_once_with()

