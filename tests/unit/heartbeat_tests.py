"""
Tests for pika.heartbeat

"""
import unittest

import mock

from pika import connection, frame, heartbeat
import pika.exceptions


# protected-access
# pylint: disable=W0212

# missing-docstring
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103


class ConstructableConnection(connection.Connection):
    """Adds dummy overrides for `Connection`'s abstract methods so
    that we can instantiate and test it.

    """
    def _adapter_connect_stream(self):
        pass

    def _adapter_disconnect_stream(self):
        raise NotImplementedError

    def call_later(self, delay, callback):
        raise NotImplementedError

    def remove_timeout(self, timeout_id):
        raise NotImplementedError

    def _adapter_emit_data(self, data):
        raise NotImplementedError

    def _adapter_add_callback_threadsafe(self, callback):
        raise NotImplementedError

    def _adapter_call_later(self, deadline, callback):
        raise NotImplementedError

    def _adapter_remove_timeout(self, timeout_id):
        raise NotImplementedError

class HeartbeatTests(unittest.TestCase):

    INTERVAL = 60
    SEND_INTERVAL = float(INTERVAL) / 2
    CHECK_INTERVAL = INTERVAL + 5

    def setUp(self):
        self.mock_conn = mock.Mock(spec_set=ConstructableConnection())
        self.mock_conn.bytes_received = 100
        self.mock_conn.bytes_sent = 100
        self.mock_conn._heartbeat_checker = mock.Mock(spec=heartbeat.HeartbeatChecker)
        self.obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)

    def tearDown(self):
        del self.obj
        del self.mock_conn

    def test_constructor_assignment_connection(self):
        self.assertIs(self.obj._connection, self.mock_conn)

    def test_constructor_assignment_intervals(self):
        self.assertEqual(self.obj._send_interval, self.SEND_INTERVAL)
        self.assertEqual(self.obj._check_interval, self.CHECK_INTERVAL)

    def test_constructor_initial_bytes_received(self):
        # Note: _bytes_received is initialized by calls
        # to _start_check_timer which calls _update_counters
        # which reads the initial values from the connection
        self.assertEqual(self.obj._bytes_received,
                         self.mock_conn.bytes_received)

    def test_constructor_initial_bytes_sent(self):
        # Note: _bytes_received is initialized by calls
        # to _start_check_timer which calls _update_counters
        # which reads the initial values from the connection
        self.assertEqual(self.obj._bytes_sent,
                         self.mock_conn.bytes_sent)

    def test_constructor_initial_heartbeat_frames_received(self):
        self.assertEqual(self.obj._heartbeat_frames_received, 0)

    def test_constructor_initial_heartbeat_frames_sent(self):
        self.assertEqual(self.obj._heartbeat_frames_sent, 0)

    def test_constructor_initial_idle_byte_intervals(self):
        self.assertEqual(self.obj._idle_byte_intervals, 0)

    @mock.patch('pika.heartbeat.HeartbeatChecker._start_send_timer')
    def test_constructor_called_start_send_timer(self, timer):
        heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        timer.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._start_check_timer')
    def test_constructor_called_start_check_timer(self, timer):
        heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        timer.assert_called_once_with()

    def test_bytes_received_on_connection(self):
        self.mock_conn.bytes_received = 128
        self.assertEqual(self.obj.bytes_received_on_connection, 128)

    def test_connection_is_idle_false(self):
        self.assertFalse(self.obj.connection_is_idle)

    def test_connection_is_idle_true(self):
        self.obj._idle_byte_intervals = self.INTERVAL
        self.assertTrue(self.obj.connection_is_idle)

    def test_received(self):
        self.obj.received()
        self.assertTrue(self.obj._heartbeat_frames_received, 1)

    @mock.patch('pika.heartbeat.HeartbeatChecker._close_connection')
    def test_send_heartbeat_not_closed(self, close_connection):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj._send_heartbeat()
        close_connection.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._close_connection')
    def test_check_heartbeat_not_closed(self, close_connection):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        self.mock_conn.bytes_received = 128
        obj._check_heartbeat()
        close_connection.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._close_connection')
    def test_check_heartbeat_missed_bytes(self, close_connection):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj._idle_byte_intervals = self.INTERVAL
        obj._check_heartbeat()
        close_connection.assert_called_once_with()

    def test_check_heartbeat_increment_no_bytes(self):
        self.mock_conn.bytes_received = 100
        self.obj._bytes_received = 100
        self.obj._check_heartbeat()
        self.assertEqual(self.obj._idle_byte_intervals, 1)

    def test_check_heartbeat_increment_bytes(self):
        self.mock_conn.bytes_received = 100
        self.obj._bytes_received = 128
        self.obj._check_heartbeat()
        self.assertEqual(self.obj._idle_byte_intervals, 0)

    @mock.patch('pika.heartbeat.HeartbeatChecker._update_counters')
    def test_check_heartbeat_update_counters(self, update_counters):
        heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        update_counters.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._send_heartbeat_frame')
    def test_send_heartbeat_sends_heartbeat_frame(self, send_heartbeat_frame):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj._send_heartbeat()
        send_heartbeat_frame.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._start_send_timer')
    def test_send_heartbeat_start_timer(self, start_send_timer):
        heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        start_send_timer.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._start_check_timer')
    def test_check_heartbeat_start_timer(self, start_check_timer):
        heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        start_check_timer.assert_called_once_with()

    def test_connection_close(self):
        self.obj._idle_byte_intervals = 3
        self.obj._idle_heartbeat_intervals = 4
        self.obj._close_connection()
        reason = self.obj._STALE_CONNECTION % self.obj._timeout
        self.mock_conn._terminate_stream.assert_called_once_with(mock.ANY)

        self.assertIsInstance(self.mock_conn._terminate_stream.call_args[0][0],
                              pika.exceptions.AMQPHeartbeatTimeout)
        self.assertEqual(
            self.mock_conn._terminate_stream.call_args[0][0].args[0],
            reason)

    def test_has_received_data_false(self):
        self.obj._bytes_received = 100
        self.assertFalse(self.obj._has_received_data)

    def test_has_received_data_true(self):
        self.mock_conn.bytes_received = 128
        self.obj._bytes_received = 100
        self.assertTrue(self.obj._has_received_data)

    def test_new_heartbeat_frame(self):
        self.assertIsInstance(self.obj._new_heartbeat_frame(), frame.Heartbeat)

    def test_send_heartbeat_send_frame_called(self):
        frame_value = self.obj._new_heartbeat_frame()
        with mock.patch.object(self.obj, '_new_heartbeat_frame') as new_frame:
            new_frame.return_value = frame_value
            self.obj._send_heartbeat_frame()
            self.mock_conn._send_frame.assert_called_once_with(frame_value)

    def test_send_heartbeat_counter_incremented(self):
        self.obj._send_heartbeat_frame()
        self.assertEqual(self.obj._heartbeat_frames_sent, 1)

    def test_start_send_timer_called(self):
        want = [mock.call(self.SEND_INTERVAL, self.obj._send_heartbeat),
                mock.call(self.CHECK_INTERVAL, self.obj._check_heartbeat)]
        got = self.mock_conn._adapter_call_later.call_args_list
        self.assertEqual(got, want)

    def test_update_counters_bytes_received(self):
        self.mock_conn.bytes_received = 256
        self.obj._update_counters()
        self.assertEqual(self.obj._bytes_received, 256)

    def test_update_counters_bytes_sent(self):
        self.mock_conn.bytes_sent = 256
        self.obj._update_counters()
        self.assertEqual(self.obj._bytes_sent, 256)
