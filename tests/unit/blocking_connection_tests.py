# -*- coding: utf8 -*-
"""
Tests for pika.adapters.blocking_connection.BlockingConnection

"""
import socket

from pika.exceptions import AMQPConnectionError

try:
    from unittest import mock
    patch = mock.patch
except ImportError:
    import mock
    from mock import patch
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika
from pika.adapters import blocking_connection


class BlockingConnectionMockTemplate(blocking_connection.BlockingConnection):
    pass

class SelectConnectionTemplate(blocking_connection.SelectConnection):
    is_closed = False
    is_closing = False
    is_open = True
    outbound_buffer = []
    _channels = dict()


class BlockingConnectionTests(unittest.TestCase):
    """TODO: test properties"""

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_constructor(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        select_connection_class_mock.assert_called_once_with(
            parameters='params',
            on_open_callback=mock.ANY,
            on_open_error_callback=mock.ANY,
            on_close_callback=mock.ANY,
            stop_ioloop_on_close=mock.ANY)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._opened_result.set_value_once(
            select_connection_class_mock.return_value)

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection._process_io_for_connection_setup()

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup_fails_with_open_error(
            self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        exc_value = pika.exceptions.AMQPConnectionError('failed')
        connection._open_error_result.set_value_once(
            select_connection_class_mock.return_value, exc_value)

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
                connection._process_io_for_connection_setup()

            self.assertEqual(cm.exception, exc_value)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._opened_result.set_value_once(
            select_connection_class_mock.return_value)

        connection._flush_output(lambda: False, lambda: True)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output_user_initiated_close(self,
                                               select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = True
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value,
            200, 'success')

        connection._flush_output(lambda: False, lambda: True)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output_server_initiated_error_close(
            self,
            select_connection_class_mock):

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = False
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value, 404, 'not found')

        with self.assertRaises(pika.exceptions.ConnectionClosed) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (404, 'not found'))

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output_server_initiated_no_error_close(self,
                                                       select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = False
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value,
            200, 'ok')

        with self.assertRaises(pika.exceptions.ConnectionClosed) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(
            cm.exception.args,
            ())

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_close(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._impl._channels = {1: mock.Mock()}

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

        select_connection_class_mock.return_value.close.assert_called_once_with(
            200, 'text')

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    @patch.object(blocking_connection, 'BlockingChannel',
                  spec_set=blocking_connection.BlockingChannel)
    def test_channel(self, blocking_channel_class_mock,
                     select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            channel = connection.channel()

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_sleep(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection.sleep(0.00001)

    def test_connection_attempts_with_timeout(self):
        # for whatever conn_attempt we try:
        for conn_attempt in (1, 2, 5):
            # retry_delay of 0 to not wait uselessly during the retry process.
            params = pika.ConnectionParameters(connection_attempts=conn_attempt, retry_delay=0)
            with self.assertRaises(AMQPConnectionError) as ctx:
                with mock.patch('socket.socket.connect',
                                side_effect=socket.timeout) as connect_mock:
                    pika.BlockingConnection(parameters=params)
            # as any attempt will timeout (directly),
            # at the end there must be exactly that count of socket.connect() method calls:
            self.assertEqual(conn_attempt, connect_mock.call_count)
            # and each must be with the following arguments (always the same):
            connect_mock.assert_has_calls(conn_attempt * [mock.call(('127.0.0.1', 5672))])
            # and the raised error must then looks like:
            self.assertEqual('Connection to 127.0.0.1:5672 failed: timeout', str(ctx.exception))
