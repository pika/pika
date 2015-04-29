# -*- coding: utf8 -*-
"""
Tests for pika.adapters.blocking_connection.BlockingConnection

"""
import socket

import mock
from mock import patch
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika
from pika.adapters import blocking_connection


class BlockingConnectionMockTemplate(blocking_connection.BlockingConnection):
    _socket_timeouts = None
    connection_state = None


class BlockingConnectionTests(unittest.TestCase):
    @patch.object(blocking_connection.BlockingConnection, 'connect',
                  spec_set=blocking_connection.BlockingConnection.connect)
    def test_handle_timeout_when_closing_exceeds_threshold(self, connect_mock):
        connection = BlockingConnectionMockTemplate()

        connection_patch = patch.multiple(
            connection,
            _on_connection_closed=mock.DEFAULT,
            _socket_timeouts=connection.SOCKET_TIMEOUT_CLOSE_THRESHOLD,
            connection_state=connection.CONNECTION_CLOSING)

        with connection_patch as mocks:
            connection._handle_timeout()

        self.assertEqual(mocks["_on_connection_closed"].call_count, 1)

    @patch.object(blocking_connection.BlockingConnection, 'connect',
                  spec_set=blocking_connection.BlockingConnection.connect)
    def test_handle_timeout_when_closing_below_threshold(self, connect_mock):
        connection = BlockingConnectionMockTemplate()

        connection_patch = patch.multiple(
            connection,
            _on_connection_closed=mock.DEFAULT,
            _socket_timeouts=connection.SOCKET_TIMEOUT_CLOSE_THRESHOLD / 2,
            connection_state=connection.CONNECTION_CLOSING)

        with connection_patch as mocks:
            connection._handle_timeout()

        self.assertEqual(mocks["_on_connection_closed"].call_count, 0)

    @patch.object(blocking_connection.BlockingConnection, 'connect',
                  spec_set=blocking_connection.BlockingConnection.connect)
    def test_handle_timeout_when_not_closing_exceeds_threshold(self,
                                                               connect_mock):
        connection = BlockingConnectionMockTemplate()

        connection_patch = patch.multiple(
            connection,
            _on_connection_closed=mock.DEFAULT,
            _socket_timeouts=connection.SOCKET_TIMEOUT_THRESHOLD,
            connection_state=connection.CONNECTION_OPEN)

        with connection_patch as mocks:
            connection._handle_timeout()

        self.assertEqual(mocks["_on_connection_closed"].call_count, 1)

    @patch.object(blocking_connection.BlockingConnection, 'connect',
                  spec_set=blocking_connection.BlockingConnection.connect)
    def test_handle_timeout_when_not_closing_below_threshold(self,
                                                             connect_mock):
        connection = BlockingConnectionMockTemplate()

        connection_patch = patch.multiple(
            connection,
            _on_connection_closed=mock.DEFAULT,
            _socket_timeouts=connection.SOCKET_TIMEOUT_THRESHOLD / 2,
            connection_state=connection.CONNECTION_OPEN)

        with connection_patch as mocks:
            connection._handle_timeout()

        self.assertEqual(mocks["_on_connection_closed"].call_count, 0)
