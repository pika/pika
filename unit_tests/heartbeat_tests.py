__author__ = 'gmr'

import sys
sys.path.insert(0, '..')

import mock
import unittest

from pika import frame
from pika import heartbeat

_BYTES_RECEIVED = 1024
_BYTES_SENT = 2048
_HEARTBEAT = frame.Heartbeat()
_INTERVAL = 60


def _connection_close(code, message):
    pass


class HeartbeatCheckerTest(unittest.TestCase):


    def setUp(self):

        self._connection = mock.Mock()
        self._connection.add_timeout = mock.Mock()
        self._connection.bytes_received = _BYTES_RECEIVED
        self._connection.bytes_sent = _BYTES_SENT
        self._connection.close = mock.mocksignature(_connection_close)
        self._connection.force_reconnect = mock.Mock()
        self._connection.send_frame = mock.Mock()

        self._heartbeat = \
                heartbeat.HeartbeatChecker(self._connection, _INTERVAL)
        self._heartbeat._new_heartbeat_frame = \
                mock.Mock(return_value=_HEARTBEAT)

    def tearDown(self):
        del self._connection
        del self._heartbeat

    def test_connection_bytes_received(self):
        self.assertEqual(_BYTES_RECEIVED,
                         self._heartbeat._connection_bytes_received())

    def test_check_missed_heartbeat_responses(self):
        self._heartbeat._received = _BYTES_RECEIVED
        self.assertEqual(1, self._heartbeat._missed_heartbeat_responses())

    def test_check_missed_heartbeat_responses_reset(self):
        self._heartbeat._received = _BYTES_RECEIVED
        self._heartbeat._missed_heartbeat_responses()
        self._connection.bytes_received += _BYTES_RECEIVED
        self.assertEqual(0, self._heartbeat._missed_heartbeat_responses())

    def test_connection_close(self):
        code_expectation = self._heartbeat._CONNECTION_FORCED
        expectation = self._heartbeat._STALE_CONNECTION % _INTERVAL
        self._heartbeat._missed = 1
        self._heartbeat._close_connection()
        self._connection.close.mock.assert_called_with(code_expectation,
                                                       expectation)

    def test_connection_reconnect(self):
        self._heartbeat._close_connection()
        self._connection.force_reconnect.mock.assert_called()

    def test_sent_and_check_too_many_missed(self):
        code_expectation = self._heartbeat._CONNECTION_FORCED
        expectation = self._heartbeat._STALE_CONNECTION % (_INTERVAL * 4)
        self._heartbeat._received = _BYTES_RECEIVED
        self._heartbeat._missed = 3
        self._heartbeat.send_and_check()
        self._connection.close.mock.assert_called_with(code_expectation,
                                                       expectation)
        self.assertTrue(self._connection.force_reconnect.called)

    def test_heartbeat_in_use(self):
        self._connection.heartbeat = self._heartbeat
        self.assertTrue(self._heartbeat._in_use())

    def test_heartbeat_not_in_use(self):
        self.assertFalse(self._heartbeat._in_use())

    def test_should_send_heartbeat(self):
        self._heartbeat._sent = _BYTES_SENT
        self.assertTrue(self._heartbeat._should_send_heartbeat_frame())

    def test_send_heartbeat_frame(self):
        self._heartbeat._send_heartbeat_frame()
        self._connection.send_frame.assert_called_with(_HEARTBEAT)

    def test_start_timer(self):
        self._connection.heartbeat = self._heartbeat
        self._heartbeat._setup_timer = mock.Mock()
        self._heartbeat._start_timer()
        self.assertTrue(self._heartbeat._setup_timer.called)

    def test_start_timer_no_setup(self):
        self._heartbeat._setup_timer = mock.Mock()
        self._heartbeat._start_timer()
        self.assertFalse(self._heartbeat._setup_timer.called)

    def test_update_byte_counts(self):
        self._heartbeat._update_byte_counts()
        self.assertEqual(_BYTES_RECEIVED, self._heartbeat._received)
        self.assertEqual(_BYTES_SENT, self._heartbeat._sent)

    def test_send_and_check_send_heartbeat(self):
        self._heartbeat._sent = _BYTES_SENT
        self._heartbeat.send_and_check()
        self._connection.send_frame.assert_called_with(_HEARTBEAT)
