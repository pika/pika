"""
Tests for pika.heartbeat

"""
import unittest

import mock

from pika import connection, frame, heartbeat


class HeartbeatTests(unittest.TestCase):

    INTERVAL = 60
    SEND_INTERVAL = float(INTERVAL) / 2
    CHECK_INTERVAL = (float(INTERVAL) * 3) / 2

    def setUp(self):
        self.mock_conn = mock.Mock(spec=connection.Connection)
        self.mock_conn.bytes_received = 100
        self.mock_conn.bytes_sent = 100
        self.mock_conn.heartbeat = mock.Mock(spec=heartbeat.HeartbeatChecker)
        self.obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)

    def tearDown(self):
        del self.obj
        del self.mock_conn

    def test_constructor_assignment_connection(self):
        self.assertIs(self.obj._connection, self.mock_conn)

    def test_constructor_assignment_intervals(self):
        self.assertEqual(self.obj._timeout, self.INTERVAL)
        self.assertEqual(self.obj._send_interval, self.SEND_INTERVAL)
        self.assertEqual(self.obj._check_interval, self.CHECK_INTERVAL)

    def test_constructor_initial_bytes_received(self):
        self.assertEqual(self.obj._bytes_received, 0)

    def test_constructor_initial_bytes_sent(self):
        self.assertEqual(self.obj._bytes_received, 0)

    def test_constructor_initial_heartbeat_frames_received(self):
        self.assertEqual(self.obj._heartbeat_frames_received, 0)

    def test_constructor_initial_heartbeat_frames_sent(self):
        self.assertEqual(self.obj._heartbeat_frames_sent, 0)

    def test_constructor_initial_idle_byte_intervals(self):
        self.assertEqual(self.obj._idle_byte_intervals, 0)

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_send_timer')
    def test_constructor_called_setup_send_timer(self, timer):
        heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        timer.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_check_timer')
    def test_constructor_called_setup_check_timer(self, timer):
        heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        timer.assert_called_once_with()

    def test_active_true(self):
        self.mock_conn.heartbeat = self.obj
        self.assertTrue(self.obj.active)

    def test_active_false(self):
        self.mock_conn.heartbeat = mock.Mock()
        self.assertFalse(self.obj.active)

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
        obj.send_heartbeat()
        close_connection.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._close_connection')
    def test_check_heartbeat_not_closed(self, close_connection):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.check_heartbeat()
        close_connection.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._close_connection')
    def test_check_heartbeat_missed_bytes(self, close_connection):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj._idle_byte_intervals = self.INTERVAL
        obj.check_heartbeat()
        close_connection.assert_called_once_with()

    def test_check_heartbeat_increment_no_bytes(self):
        self.mock_conn.bytes_received = 100
        self.obj._bytes_received = 100
        self.obj.check_heartbeat()
        self.assertEqual(self.obj._idle_byte_intervals, 1)

    def test_check_heartbeat_increment_bytes(self):
        self.mock_conn.bytes_received = 100
        self.obj._bytes_received = 128
        self.obj.check_heartbeat()
        self.assertEqual(self.obj._idle_byte_intervals, 0)

    @mock.patch('pika.heartbeat.HeartbeatChecker._update_counters')
    def test_check_heartbeat_update_counters(self, update_counters):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.check_heartbeat()
        update_counters.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._send_heartbeat_frame')
    def test_send_heartbeat_sends_heartbeat_frame(self, send_heartbeat_frame):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.send_heartbeat()
        send_heartbeat_frame.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._start_send_timer')
    def test_send_heartbeat_start_timer(self, start_send_timer):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.send_heartbeat()
        start_send_timer.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._start_check_timer')
    def test_check_heartbeat_start_timer(self, start_check_timer):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.check_heartbeat()
        start_check_timer.assert_called_once_with()

    def test_connection_close(self):
        self.obj._idle_byte_intervals = 3
        self.obj._idle_heartbeat_intervals = 4
        self.obj._close_connection()
        reason = self.obj._STALE_CONNECTION % self.obj._timeout
        self.mock_conn.close.assert_called_once_with(
            self.obj._CONNECTION_FORCED, reason)
        self.mock_conn._on_terminate.assert_called_once_with(
            self.obj._CONNECTION_FORCED, reason)

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

    def test_setup_timer_called(self):
        self.mock_conn.add_timeout.assert_called_once_with(
            self.SEND_INTERVAL, self.obj.send_and_check)

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_send_timer')
    def test_start_send_timer_not_active(self, setup_send_timer):
        self.obj._start_send_timer()
        setup_send_timer.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_check_timer')
    def test_start_check_timer_not_active(self, setup_check_timer):
        self.obj._start_check_timer()
        setup_check_timer.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_send_timer')
    def test_start_timer_active(self, setup_send_timer):
        self.mock_conn.heartbeat = self.obj
        self.obj._start_send_timer()
        self.assertTrue(setup_send_timer.called)

    def test_update_counters_bytes_received(self):
        self.mock_conn.bytes_received = 256
        self.obj._update_counters()
        self.assertEqual(self.obj._bytes_received, 256)

    def test_update_counters_bytes_sent(self):
        self.mock_conn.bytes_sent = 256
        self.obj._update_counters()
        self.assertEqual(self.obj._bytes_sent, 256)
