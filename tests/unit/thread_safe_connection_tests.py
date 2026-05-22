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
        wrapper = MagicMock()
        wrapper._closed_reason = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        return ThreadSafeChannel(raw_ch, wrapper), raw_ch, wrapper

    def test_basic_publish_routes_through_add_callback_threadsafe(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'hello')
        wrapper.add_callback_threadsafe.assert_called_once()

    def test_basic_publish_does_not_call_raw_channel_directly(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'hello')
        raw_ch.basic_publish.assert_not_called()

    def test_basic_publish_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'msg',
                         properties='props',
                         mandatory=True)
        # Extract and invoke the scheduled callback manually
        scheduled_cb = wrapper.add_callback_threadsafe.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_publish.assert_called_once_with(
            exchange='ex',
            routing_key='rk',
            body=b'msg',
            properties='props',
            mandatory=True,
        )

    def test_basic_publish_callback_swallows_channel_wrong_state_error(self):
        """ChannelWrongStateError inside the scheduled callback must not
        propagate to the IOLoop — that would crash the IOLoop thread."""
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_publish.side_effect = ChannelWrongStateError(
            'channel closed')
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')
        scheduled_cb = wrapper.add_callback_threadsafe.call_args[0][0]
        scheduled_cb()  # must not raise
        raw_ch.basic_publish.assert_called_once()

    def test_basic_ack_callback_swallows_channel_wrong_state_error(self):
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_ack.side_effect = ChannelWrongStateError('channel closed')
        ch.basic_ack(delivery_tag=1)
        wrapper.add_callback_threadsafe.call_args[0][0]()  # must not raise
        raw_ch.basic_ack.assert_called_once()

    def test_basic_nack_callback_swallows_channel_wrong_state_error(self):
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_nack.side_effect = ChannelWrongStateError('channel closed')
        ch.basic_nack(delivery_tag=1)
        wrapper.add_callback_threadsafe.call_args[0][0]()  # must not raise
        raw_ch.basic_nack.assert_called_once()

    def test_basic_reject_callback_swallows_channel_wrong_state_error(self):
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_reject.side_effect = ChannelWrongStateError(
            'channel closed')
        ch.basic_reject(delivery_tag=1)
        wrapper.add_callback_threadsafe.call_args[0][0]()  # must not raise
        raw_ch.basic_reject.assert_called_once()

    def test_basic_publish_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_basic_ack_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_ack(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_basic_nack_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_nack(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_basic_reject_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_reject(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_basic_ack_routes_through_add_callback_threadsafe(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_ack(delivery_tag=42, multiple=True)
        wrapper.add_callback_threadsafe.assert_called_once()
        raw_ch.basic_ack.assert_not_called()

    def test_basic_ack_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_ack(delivery_tag=42, multiple=True)
        scheduled_cb = wrapper.add_callback_threadsafe.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_ack.assert_called_once_with(delivery_tag=42, multiple=True)

    def test_basic_nack_routes_through_add_callback_threadsafe(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_nack(delivery_tag=7, multiple=False, requeue=False)
        wrapper.add_callback_threadsafe.assert_called_once()
        raw_ch.basic_nack.assert_not_called()

    def test_basic_nack_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_nack(delivery_tag=7, multiple=False, requeue=False)
        scheduled_cb = wrapper.add_callback_threadsafe.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_nack.assert_called_once_with(delivery_tag=7,
                                                  multiple=False,
                                                  requeue=False)

    def test_basic_reject_routes_through_add_callback_threadsafe(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_reject(delivery_tag=3, requeue=False)
        wrapper.add_callback_threadsafe.assert_called_once()
        raw_ch.basic_reject.assert_not_called()

    def test_basic_reject_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_reject(delivery_tag=3, requeue=False)
        scheduled_cb = wrapper.add_callback_threadsafe.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_reject.assert_called_once_with(delivery_tag=3,
                                                    requeue=False)

    def test_queue_declare_blocks_and_returns_result(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_queue_declare(queue, passive, durable, exclusive,
                                   auto_delete, arguments, callback):
                callback(mock_frame)

            raw_ch.queue_declare.side_effect = fake_queue_declare
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        result = ch.queue_declare(queue='my_queue', durable=True)

        self.assertIs(result, mock_frame)
        raw_ch.queue_declare.assert_called_once_with(
            queue='my_queue',
            passive=False,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments=None,
            callback=unittest.mock.ANY,
        )

    def test_queue_declare_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('connection closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.queue_declare(queue='my_queue')

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_queue_declare_raises_when_connection_closes_while_waiting(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('connection lost')

        def close_while_waiting(cb):
            # Simulate the connection closing instead of Queue.DeclareOk arriving
            with wrapper._channel_waiters_lock:
                wrapper._closed_reason = reason
                for evt, err in wrapper._blocking_waiters:
                    err[0] = reason
                    evt.set()
                wrapper._blocking_waiters.clear()

        wrapper.add_callback_threadsafe.side_effect = close_while_waiting

        with self.assertRaises(Exception) as ctx:
            ch.queue_declare(queue='my_queue')

        self.assertIs(ctx.exception, reason)

    def test_basic_qos_blocks_and_returns_result(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_qos(prefetch_size, prefetch_count, global_qos, callback):
                callback(mock_frame)

            raw_ch.basic_qos.side_effect = fake_qos
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        result = ch.basic_qos(prefetch_count=10)

        self.assertIs(result, mock_frame)
        raw_ch.basic_qos.assert_called_once_with(
            prefetch_size=0,
            prefetch_count=10,
            global_qos=False,
            callback=unittest.mock.ANY,
        )

    def test_basic_qos_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_qos()

        self.assertIs(ctx.exception, reason)

    def test_basic_consume_blocks_and_returns_consumer_tag(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        tag = ch.basic_consume(queue='q', on_message_callback=MagicMock())

        self.assertEqual(tag, 'ctag1')

    def test_basic_consume_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_consume(queue='q', on_message_callback=MagicMock())

        self.assertIs(ctx.exception, reason)

    def test_basic_cancel_blocks_and_returns_result(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_cancel(consumer_tag, callback):
                callback(mock_frame)

            raw_ch.basic_cancel.side_effect = fake_cancel
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        result = ch.basic_cancel('ctag1')

        self.assertIs(result, mock_frame)
        raw_ch.basic_cancel.assert_called_once_with(
            consumer_tag='ctag1',
            callback=unittest.mock.ANY,
        )

    def test_basic_cancel_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_cancel('ctag1')

        self.assertIs(ctx.exception, reason)

    def test_channel_close_blocks_until_closed(self):
        from pika.exceptions import ChannelClosedByClient
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False

        def execute_and_fire(cb):
            close_callbacks = []
            raw_ch.add_on_close_callback.side_effect = close_callbacks.append
            raw_ch.close.side_effect = lambda reply_code, reply_text: \
                close_callbacks[0](raw_ch, ChannelClosedByClient(reply_code, reply_text))
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.close()  # must not raise

        raw_ch.close.assert_called_once_with(reply_code=0,
                                             reply_text='Normal shutdown')

    def test_channel_close_raises_when_connection_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('connection closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.close()

        self.assertIs(ctx.exception, reason)

    def test_channel_close_is_noop_when_channel_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = True

        def execute_immediately(cb):
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_immediately

        ch.close()  # must not raise; channel is already closed

        raw_ch.close.assert_not_called()

    def test_properties_delegate_to_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        self.assertEqual(ch.channel_number, raw_ch.channel_number)
        self.assertEqual(ch.is_open, raw_ch.is_open)
        self.assertEqual(ch.is_closed, raw_ch.is_closed)

    def test_concurrent_publishes_all_scheduled(self):
        """All publishes from N threads must each schedule exactly one callback."""
        ch, raw_ch, wrapper = self._make_channel()
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

        self.assertEqual(wrapper.add_callback_threadsafe.call_count, n)


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
            mock_conn.is_closing = False

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
        conn, _, mock_ioloop = self._make_connection()
        mock_ioloop.start.assert_called_once()

    def test_is_open_delegates_to_inner_connection(self):
        conn, mock_conn, _ = self._make_connection()
        mock_conn.is_open = True
        self.assertTrue(conn.is_open)

    def test_is_closed_delegates_to_inner_connection(self):
        conn, mock_conn, _ = self._make_connection()
        mock_conn.is_closed = True
        self.assertTrue(conn.is_closed)

    def test_add_callback_threadsafe_delegates(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        cb = MagicMock()
        conn.add_callback_threadsafe(cb)
        mock_ioloop.add_callback_threadsafe.assert_called_once_with(cb)

    def test_channel_schedules_open_via_add_callback_threadsafe(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        # Simulate the IOLoop executing the scheduled callback synchronously
        def execute_scheduled(cb):
            # The callback calls connection.channel(on_open_callback=...)
            # We intercept that and immediately fire on_open_callback.
            mock_raw_ch = MagicMock()

            def fake_channel(on_open_callback):
                on_open_callback(mock_raw_ch)

            mock_conn.channel.side_effect = fake_channel
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()
        self.assertIsInstance(ch, ThreadSafeChannel)

    def test_channel_wraps_self_not_inner_connection(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_scheduled(cb):
            mock_raw_ch = MagicMock()

            def fake_channel(on_open_callback):
                on_open_callback(mock_raw_ch)

            mock_conn.channel.side_effect = fake_channel
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()
        self.assertIs(ch._wrapper, conn)

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

        mock_conn.ioloop.add_callback_threadsafe.side_effect = close_while_waiting

        with self.assertRaises(Exception) as ctx:
            conn.channel()

        self.assertIs(ctx.exception, reason)

    def test_ioloop_crash_wakes_blocked_channel_waiter(self):
        """A thread blocked in channel() must be unblocked if the IOLoop crashes."""
        crash = RuntimeError('simulated IOLoop crash')
        gate = threading.Event()

        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False
            mock_conn.is_closing = False

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            # ioloop.start() blocks until gate is set, then crashes.
            def ioloop_start():
                gate.wait(timeout=5)
                raise crash

            mock_ioloop.start.side_effect = ioloop_start

            conn = ThreadSafeConnection(parameters='params')

        exc_holder = [None]

        def try_channel():
            try:
                conn.channel()
            except Exception as exc:
                exc_holder[0] = exc

        t = threading.Thread(target=try_channel)
        t.start()
        # Give the thread time to enter channel() and register its waiter.
        import time
        time.sleep(0.05)
        gate.set()
        t.join(timeout=2)

        self.assertFalse(t.is_alive(),
                         'channel() thread must not hang after IOLoop crash')
        self.assertIs(exc_holder[0], crash)

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
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = False
        conn.close()
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        # The scheduled callback must delegate to the inner connection's close.
        mock_ioloop.add_callback_threadsafe.call_args[0][0]()
        mock_conn.close.assert_called_once()
        conn._ioloop_thread.join.assert_called_once_with(timeout=10)

    def test_close_force_stops_ioloop_after_timeout(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = True
        conn.close(timeout=0.1)
        self.assertEqual(mock_ioloop.add_callback_threadsafe.call_count, 2)
        # First call: graceful close wrapper
        mock_ioloop.add_callback_threadsafe.call_args_list[0][0][0]()
        mock_conn.close.assert_called_once()
        # Second call: force-stop the IOLoop
        self.assertIs(
            mock_ioloop.add_callback_threadsafe.call_args_list[1][0][0],
            mock_ioloop.stop)
        self.assertEqual(conn._ioloop_thread.join.call_count, 2)
        # Force-stop must set _closed_reason so subsequent callers do not hang.
        self.assertIsNotNone(conn._closed_reason)

    def test_close_force_stop_wakes_blocked_waiters(self):
        """Threads blocked when the IOLoop is force-stopped must be woken."""
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = True

        waiter_event = threading.Event()
        waiter_error: list[BaseException | None] = [None]
        conn._blocking_waiters.append((waiter_event, waiter_error))

        conn.close(timeout=0.1)

        self.assertTrue(waiter_event.is_set())
        self.assertIsNotNone(waiter_error[0])
        self.assertIsNotNone(conn._closed_reason)
        self.assertEqual(conn._blocking_waiters, [])

    def test_close_safe_close_suppresses_connection_wrong_state_error(self):
        """_safe_close must swallow ConnectionWrongStateError so it does not
        propagate through the IOLoop and crash the IOLoop thread."""
        from pika.exceptions import ConnectionWrongStateError
        conn, mock_conn, _ = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = False
        mock_conn.close.side_effect = ConnectionWrongStateError(
            'already closing')

        def run_callback_inline(cb):
            cb()  # execute synchronously — must not raise

        mock_conn.ioloop.add_callback_threadsafe.side_effect = run_callback_inline
        conn.close()  # must not raise
        mock_conn.close.assert_called_once()

    def test_close_is_noop_when_already_closed(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._closed_reason = Exception('already closed')
        conn._ioloop_thread = MagicMock()
        conn.close()
        mock_ioloop.add_callback_threadsafe.assert_not_called()
        conn._ioloop_thread.join.assert_not_called()

    def test_close_from_ioloop_thread_calls_connection_close_directly(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        # Simulate close() being called from within the IOLoop thread itself
        conn._ioloop_thread = threading.current_thread()
        conn.close()
        mock_conn.close.assert_called_once()
        mock_ioloop.add_callback_threadsafe.assert_not_called()

    def test_context_manager(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = False
        with conn as c:
            self.assertIs(c, conn)
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        mock_ioloop.add_callback_threadsafe.call_args[0][0]()
        mock_conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
