"""Tests for pika.adapters.thread_safe_connection"""
import threading
import unittest
from unittest.mock import MagicMock, patch

from pika.adapters.thread_safe_connection import ThreadSafeChannel, ThreadSafeConnection

# pylint: disable=W0212,C0111,C0103


class ThreadSafeChannelTests(unittest.TestCase):

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        conn = MagicMock()
        return ThreadSafeChannel(raw_ch, conn), raw_ch, conn

    def test_basic_publish_routes_through_add_callback_threadsafe(self):
        ch, raw_ch, conn = self._make_channel()
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'hello')
        conn.add_callback_threadsafe.assert_called_once()

    def test_basic_publish_does_not_call_raw_channel_directly(self):
        ch, raw_ch, conn = self._make_channel()
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'hello')
        raw_ch.basic_publish.assert_not_called()

    def test_basic_publish_callback_calls_raw_channel(self):
        ch, raw_ch, conn = self._make_channel()
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'msg',
                         properties='props',
                         mandatory=True)
        # Extract and invoke the scheduled callback manually
        scheduled_cb = conn.add_callback_threadsafe.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_publish.assert_called_once_with(
            exchange='ex',
            routing_key='rk',
            body=b'msg',
            properties='props',
            mandatory=True,
        )

    def test_properties_delegate_to_raw_channel(self):
        ch, raw_ch, conn = self._make_channel()
        self.assertEqual(ch.channel_number, raw_ch.channel_number)
        self.assertEqual(ch.is_open, raw_ch.is_open)
        self.assertEqual(ch.is_closed, raw_ch.is_closed)

    def test_concurrent_publishes_all_scheduled(self):
        """All publishes from N threads must each schedule exactly one callback."""
        ch, raw_ch, conn = self._make_channel()
        n = 20
        barrier = threading.Barrier(n)

        def publish():
            barrier.wait()
            ch.basic_publish(exchange='', routing_key='q', body=b'x')

        threads = [threading.Thread(target=publish) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(conn.add_callback_threadsafe.call_count, n)


class ThreadSafeConnectionTests(unittest.TestCase):

    def _make_connection(self,
                         on_open_error_callback=None,
                         on_close_callback=None):
        """
        Construct a ThreadSafeConnection with a mocked SelectConnection.

        The mock immediately fires on_open_callback so __init__ unblocks.
        """
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False

            # Capture the on_open_callback passed to SelectConnection
            # and call it immediately so _connected_event gets set.
            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            conn = ThreadSafeConnection(
                parameters='params',
                on_open_error_callback=on_open_error_callback,
                on_close_callback=on_close_callback,
            )
            # Stop the background thread immediately since ioloop.start is mocked
            conn._ioloop_thread.join(timeout=1)

        return conn, mock_conn, mock_ioloop

    def test_ioloop_thread_is_started(self):
        conn, _, _ = self._make_connection()
        # Thread may have already exited (mocked ioloop.start returns instantly)
        self.assertIsNotNone(conn._ioloop_thread)

    def test_is_open_delegates_to_inner_connection(self):
        conn, mock_conn, _ = self._make_connection()
        mock_conn.is_open = True
        self.assertTrue(conn.is_open)

    def test_is_closed_delegates_to_inner_connection(self):
        conn, mock_conn, _ = self._make_connection()
        mock_conn.is_closed = True
        self.assertTrue(conn.is_closed)

    def test_add_callback_threadsafe_delegates(self):
        conn, mock_conn, _ = self._make_connection()
        cb = MagicMock()
        conn.add_callback_threadsafe(cb)
        mock_conn.add_callback_threadsafe.assert_called_once_with(cb)

    def test_channel_schedules_open_via_add_callback_threadsafe(self):
        conn, mock_conn, _ = self._make_connection()

        # Simulate the IOLoop executing the scheduled callback synchronously
        def execute_scheduled(cb):
            # The callback calls connection.channel(on_open_callback=...)
            # We intercept that and immediately fire on_open_callback.
            mock_raw_ch = MagicMock()

            def fake_channel(on_open_callback):
                on_open_callback(mock_raw_ch)

            mock_conn.channel.side_effect = fake_channel
            cb()

        mock_conn.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()
        self.assertIsInstance(ch, ThreadSafeChannel)

    def test_channel_raises_when_connection_already_closed(self):
        conn, mock_conn, _ = self._make_connection()
        reason = Exception('already closed')
        conn._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            conn.channel()

        self.assertIs(ctx.exception, reason)

    def test_channel_raises_when_connection_closes_while_waiting(self):
        conn, mock_conn, _ = self._make_connection()
        reason = Exception('connection lost')

        def close_while_waiting(cb):
            # Simulate the connection closing instead of the channel opening
            conn._on_connection_closed(mock_conn, reason)

        mock_conn.add_callback_threadsafe.side_effect = close_while_waiting

        with self.assertRaises(Exception) as ctx:
            conn.channel()

        self.assertIs(ctx.exception, reason)

    def test_on_connection_open_error_sets_event_and_stops_ioloop(self):
        error = Exception('refused')
        captured = {}

        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                captured['on_open_error_callback'] = on_open_error_callback
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            # Fire the error callback from within ioloop.start() so that
            # self._connection is already assigned when the callback runs.
            def ioloop_start():
                captured['on_open_error_callback'](mock_conn, error)

            mock_ioloop.start.side_effect = ioloop_start

            with self.assertRaises(Exception) as ctx:
                ThreadSafeConnection(parameters='params')

        self.assertIs(ctx.exception, error)
        mock_ioloop.stop.assert_called_once()

    def test_on_connection_closed_stops_ioloop(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._on_connection_closed(mock_conn, 'reason')
        mock_ioloop.stop.assert_called_once()

    def test_on_connection_closed_calls_user_callback(self):
        user_cb = MagicMock()
        conn, mock_conn, _ = self._make_connection(on_close_callback=user_cb)
        conn._on_connection_closed(mock_conn, 'reason')
        user_cb.assert_called_once_with(mock_conn, 'reason')

    def test_close_schedules_close_and_joins_thread(self):
        conn, mock_conn, _ = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn.close()
        mock_conn.add_callback_threadsafe.assert_called_with(mock_conn.close)
        conn._ioloop_thread.join.assert_called_once()


if __name__ == '__main__':
    unittest.main()
