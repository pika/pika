"""
Tests for pika.heartbeat

"""
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import connection
from pika import frame
from pika import heartbeat

class HeartbeatTests(unittest.TestCase):

    INTERVAL = 5

    def setUp(self):
        self.mock_conn = mock.Mock(spec=connection.Connection)
        self.mock_conn.bytes_received = 100
        self.mock_conn.bytes_sent = 100
        self.mock_conn.heartbeat = mock.Mock(spec=heartbeat.HeartbeatChecker)
        self.obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)

    def tearDown(self):
        del self.obj
        del self.mock_conn

    def test_default_initialization_max_idle_count(self):
        self.assertEqual(self.obj._max_idle_count,
                         self.obj.MAX_IDLE_COUNT)

    def test_constructor_assignment_connection(self):
        self.assertEqual(self.obj._connection, self.mock_conn)

    def test_constructor_assignment_heartbeat_interval(self):
        self.assertEqual(self.obj._interval, self.INTERVAL)

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

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_timer')
    def test_constructor_called_setup_timer(self, timer):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
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
    def test_send_and_check_not_closed(self, close_connection):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.send_and_check()
        close_connection.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._close_connection')
    def test_send_and_check_missed_bytes(self, close_connection):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj._idle_byte_intervals = self.INTERVAL
        obj.send_and_check()
        close_connection.assert_called_once_with()

    def test_send_and_check_increment_no_bytes(self):
        self.mock_conn.bytes_received = 100
        self.obj._bytes_received = 100
        self.obj.send_and_check()
        self.assertEqual(self.obj._idle_byte_intervals, 1)

    def test_send_and_check_increment_bytes(self):
        self.mock_conn.bytes_received = 100
        self.obj._bytes_received = 128
        self.obj.send_and_check()
        self.assertEqual(self.obj._idle_byte_intervals, 0)

    @mock.patch('pika.heartbeat.HeartbeatChecker._update_counters')
    def test_send_and_check_update_counters(self, update_counters):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.send_and_check()
        update_counters.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._send_heartbeat_frame')
    def test_send_and_check_send_heartbeat_frame(self, send_heartbeat_frame):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.send_and_check()
        send_heartbeat_frame.assert_called_once_with()

    @mock.patch('pika.heartbeat.HeartbeatChecker._start_timer')
    def test_send_and_check_start_timer(self, start_timer):
        obj = heartbeat.HeartbeatChecker(self.mock_conn, self.INTERVAL)
        obj.send_and_check()
        start_timer.assert_called_once_with()

    def test_connection_close(self):
        self.obj._idle_byte_intervals = 3
        self.obj._idle_heartbeat_intervals = 4
        self.obj._close_connection()
        self.mock_conn.close.assert_called_once_with(self.obj._CONNECTION_FORCED,
                                                     self.obj._STALE_CONNECTION %
                                                     (self.obj._max_idle_count *
                                                      self.obj._interval))

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
        self.obj._send_heartbeat_frame()
        self.mock_conn._send_frame.assert_called_once()

    def test_send_heartbeat_counter_incremented(self):
        self.obj._send_heartbeat_frame()
        self.assertEqual(self.obj._heartbeat_frames_sent, 1)

    def test_setup_timer_called(self):
        self.obj._setup_timer()
        self.mock_conn.add_timeout.called_once_with(self.INTERVAL,
                                                    self.obj.send_and_check)

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_timer')
    def test_start_timer_not_active(self, setup_timer):
        self.obj._start_timer()
        setup_timer.assert_not_called()

    @mock.patch('pika.heartbeat.HeartbeatChecker._setup_timer')
    def test_start_timer_active(self, setup_timer):
        self.mock_conn.heartbeat = self.obj
        self.obj._start_timer()
        self.assertTrue(setup_timer.called)

    def test_update_counters_bytes_received(self):
        self.mock_conn.bytes_received = 256
        self.obj._update_counters()
        self.assertEqual(self.obj._bytes_received, 256)

    def test_update_counters_bytes_sent(self):
        self.mock_conn.bytes_sent = 256
        self.obj._update_counters()
        self.assertEqual(self.obj._bytes_sent, 256)
