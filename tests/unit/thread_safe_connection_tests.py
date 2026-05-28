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
        ch, _raw_ch, wrapper = self._make_channel()
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'hello')
        wrapper.add_callback_threadsafe.assert_called_once()

    def test_basic_publish_does_not_call_raw_channel_directly(self):
        ch, raw_ch, _wrapper = self._make_channel()
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
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_basic_ack_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_ack(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_basic_nack_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_nack(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_basic_reject_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
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
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('connection closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.queue_declare(queue='my_queue')

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_queue_declare_raises_when_connection_closes_while_waiting(self):
        ch, _raw_ch, wrapper = self._make_channel()
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
        ch, _raw_ch, wrapper = self._make_channel()
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

    def test_basic_consume_wraps_callback_with_thread_safe_channel(self):
        """on_message_callback must receive the ThreadSafeChannel, not the raw channel."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        received = []

        def my_callback(channel, method, properties, body):
            received.append(channel)

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', b'body')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)

        # Wait for the worker pool to drain so the assertion is deterministic.
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(len(received), 1)
        self.assertIsInstance(received[0], ThreadSafeChannel)
        self.assertIs(received[0], ch)

    def test_basic_consume_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
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
        ch, _raw_ch, wrapper = self._make_channel()
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
        ch, _raw_ch, wrapper = self._make_channel()
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

    def test_channel_abort_swallows_close_errors(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('connection already closed')
        wrapper._closed_reason = reason

        # close() would raise; abort() must swallow.
        ch.abort()

    def test_channel_abort_calls_close(self):
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = True  # make close() a noop

        def execute_immediately(cb):
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_immediately

        ch.abort(reply_code=320, reply_text='shutting down', timeout=5)
        # Verify the close path ran (work pool was shut down via close())
        self.assertTrue(ch._pool_shutdown)

    def test_properties_delegate_to_raw_channel(self):
        ch, raw_ch, _wrapper = self._make_channel()
        self.assertEqual(ch.channel_number, raw_ch.channel_number)
        self.assertEqual(ch.is_open, raw_ch.is_open)
        self.assertEqual(ch.is_closed, raw_ch.is_closed)

    def test_concurrent_publishes_all_scheduled(self):
        """All publishes from N threads must each schedule exactly one callback."""
        ch, _raw_ch, wrapper = self._make_channel()
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


class ConsumerWorkPoolTests(unittest.TestCase):
    """Tests for the per-channel consumer dispatch pool."""

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

    def test_callback_runs_on_worker_thread_not_ioloop(self):
        """on_message_callback must execute on the pool thread, not the
        thread that invoked the wrapped callback (which is the IOLoop thread)."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        callback_thread = []

        def my_callback(channel, method, properties, body):
            callback_thread.append(threading.current_thread())

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', b'body')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(len(callback_thread), 1)
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-consumer', callback_thread[0].name)

    def test_callbacks_execute_in_order(self):
        """Multiple deliveries must be dispatched in FIFO order."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        order = []

        def my_callback(channel, method, properties, body):
            order.append(body)

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                for i in range(10):
                    on_message_callback(raw_ch, 'method', 'props', i)
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(order, list(range(10)))

    def test_callback_exception_does_not_kill_pool(self):
        """An exception in a delivery callback must not prevent subsequent
        deliveries from being processed."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        delivered = []

        def my_callback(channel, method, properties, body):
            delivered.append(body)
            if body == 'explode':
                raise RuntimeError('boom')

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', 'explode')
                on_message_callback(raw_ch, 'method', 'props', 'after')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(delivered, ['explode', 'after'])

    def test_channel_close_waits_for_pending_callbacks(self):
        """close() must wait for in-flight callbacks to complete before returning."""
        import time

        from pika.exceptions import ChannelClosedByClient

        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        completed = threading.Event()

        def slow_callback(channel, method, properties, body):
            time.sleep(0.05)
            completed.set()

        call_count = [0]

        def execute_and_fire(cb):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: basic_consume setup
                def fake_consume(queue, on_message_callback, auto_ack,
                                 exclusive, consumer_tag, arguments, callback):
                    on_message_callback(raw_ch, 'method', 'props', b'body')
                    callback(mock_frame)

                raw_ch.basic_consume.side_effect = fake_consume
                cb()
            else:
                # Second call: channel close
                close_callbacks = []
                raw_ch.add_on_close_callback.side_effect = close_callbacks.append
                raw_ch.close.side_effect = lambda reply_code, reply_text: \
                    close_callbacks[-1](raw_ch, ChannelClosedByClient(reply_code, reply_text))
                cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=slow_callback)
        ch.close()

        self.assertTrue(completed.is_set())

    def test_pool_uses_single_worker(self):
        """The pool must use exactly one worker thread (for ordering)."""
        ch, _, _ = self._make_channel()
        self.assertEqual(ch._consumer_work_pool._max_workers, 1)

    def test_callback_exception_is_logged(self):
        """Exceptions in delivery callbacks must be logged, not swallowed silently."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'

        def exploding_callback(channel, method, properties, body):
            raise ValueError('test error')

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', b'body')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        with patch(
                'pika.adapters.thread_safe_connection.LOGGER') as mock_logger:
            ch.basic_consume(queue='q', on_message_callback=exploding_callback)
            ch._consumer_work_pool.shutdown(wait=True)
            mock_logger.exception.assert_called_once()
            self.assertIn('Unhandled exception',
                          mock_logger.exception.call_args[0][0])

    def test_submit_after_pool_shutdown_does_not_raise(self):
        """Delivery arriving after pool shutdown must not crash the IOLoop."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        wrapped_cb_holder = []

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                wrapped_cb_holder.append(on_message_callback)
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=MagicMock())

        # Shut down the pool, then simulate a late delivery
        ch._shutdown_pool()
        # Must not raise RuntimeError
        wrapped_cb_holder[0](raw_ch, 'method', 'props', b'late')

    def test_shutdown_pool_is_idempotent(self):
        """Calling _shutdown_pool() multiple times must not raise."""
        ch, _, _ = self._make_channel()
        ch._shutdown_pool()
        ch._shutdown_pool()  # second call must be a no-op

    def test_add_on_return_callback_routes_through_add_callback_threadsafe(
            self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.add_on_return_callback(MagicMock())
        wrapper.add_callback_threadsafe.assert_called_once()
        raw_ch.add_on_return_callback.assert_not_called()

    def test_add_on_return_callback_registers_on_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()

        def execute_immediately(cb):
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_immediately

        ch.add_on_return_callback(MagicMock())
        raw_ch.add_on_return_callback.assert_called_once()

    def test_add_on_return_callback_dispatches_on_worker_thread(self):
        """Returned messages must run on the per-channel worker, not IOLoop."""
        ch, raw_ch, wrapper = self._make_channel()
        callback_thread = []
        received = []

        def user_cb(channel, method, properties, body):
            callback_thread.append(threading.current_thread())
            received.append((channel, method, properties, body))

        def execute_and_fire(cb):
            cb()
            # Capture the wrapped callback the wrapper handed to raw_ch
            wrapped = raw_ch.add_on_return_callback.call_args[0][0]
            # Simulate the broker returning a message
            wrapped(raw_ch, 'method', 'props', b'returned-body')

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.add_on_return_callback(user_cb)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(len(received), 1)
        # First arg must be the ThreadSafeChannel, not the raw channel
        self.assertIs(received[0][0], ch)
        self.assertEqual(received[0][1:], ('method', 'props', b'returned-body'))
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-consumer', callback_thread[0].name)

    def test_add_on_return_callback_after_pool_shutdown_logs_and_drops(self):
        """A returned message arriving after pool shutdown must not raise."""
        ch, raw_ch, wrapper = self._make_channel()
        wrapped_holder = []

        def execute_and_fire(cb):
            cb()
            wrapped_holder.append(raw_ch.add_on_return_callback.call_args[0][0])

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.add_on_return_callback(MagicMock())
        ch._shutdown_pool()
        # Late return must be silently dropped
        wrapped_holder[0](raw_ch, 'method', 'props', b'late')

    def test_add_on_return_callback_raises_when_connection_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.add_on_return_callback(MagicMock())

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_add_on_cancel_callback_routes_through_add_callback_threadsafe(
            self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.add_on_cancel_callback(MagicMock())
        wrapper.add_callback_threadsafe.assert_called_once()
        raw_ch.add_on_cancel_callback.assert_not_called()

    def test_add_on_cancel_callback_registers_on_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()

        def execute_immediately(cb):
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_immediately

        ch.add_on_cancel_callback(MagicMock())
        raw_ch.add_on_cancel_callback.assert_called_once()

    def test_add_on_cancel_callback_dispatches_on_worker_thread(self):
        """Server-initiated cancel must run on the per-channel worker."""
        ch, raw_ch, wrapper = self._make_channel()
        callback_thread = []
        received = []

        def user_cb(method_frame):
            callback_thread.append(threading.current_thread())
            received.append(method_frame)

        def execute_and_fire(cb):
            cb()
            wrapped = raw_ch.add_on_cancel_callback.call_args[0][0]
            wrapped('cancel-frame')

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.add_on_cancel_callback(user_cb)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(received, ['cancel-frame'])
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-consumer', callback_thread[0].name)

    def test_add_on_cancel_callback_after_pool_shutdown_logs_and_drops(self):
        ch, raw_ch, wrapper = self._make_channel()
        wrapped_holder = []

        def execute_and_fire(cb):
            cb()
            wrapped_holder.append(raw_ch.add_on_cancel_callback.call_args[0][0])

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        ch.add_on_cancel_callback(MagicMock())
        ch._shutdown_pool()
        wrapped_holder[0]('late-cancel')  # must not raise

    def test_add_on_cancel_callback_raises_when_connection_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.add_on_cancel_callback(MagicMock())

        self.assertIs(ctx.exception, reason)
        wrapper.add_callback_threadsafe.assert_not_called()

    def test_return_listener_exception_is_logged_not_lost(self):
        """An exception raised inside a return listener must be logged
        (not silently dropped on an unobserved Future)."""
        ch, raw_ch, wrapper = self._make_channel()

        def boom(channel, method, properties, body):
            raise RuntimeError('return-listener boom')

        def execute_and_fire(cb):
            cb()
            wrapped = raw_ch.add_on_return_callback.call_args[0][0]
            wrapped(raw_ch, 'method', 'props', b'body')

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            ch.add_on_return_callback(boom)
            ch._consumer_work_pool.shutdown(wait=True)

        self.assertTrue(
            any('return listener' in msg for msg in cm.output),
            msg=f'Expected "return listener" in logs, got: {cm.output}')

    def test_cancel_listener_exception_is_logged_not_lost(self):
        ch, raw_ch, wrapper = self._make_channel()

        def boom(method_frame):
            raise RuntimeError('cancel-listener boom')

        def execute_and_fire(cb):
            cb()
            wrapped = raw_ch.add_on_cancel_callback.call_args[0][0]
            wrapped('cancel-frame')

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            ch.add_on_cancel_callback(boom)
            ch._consumer_work_pool.shutdown(wait=True)

        self.assertTrue(
            any('cancel listener' in msg for msg in cm.output),
            msg=f'Expected "cancel listener" in logs, got: {cm.output}')

    def test_publisher_confirm_callback_exception_is_logged_not_lost(self):
        ch, raw_ch, wrapper = self._make_channel()
        ok_frame = MagicMock()

        def boom(method_frame):
            raise RuntimeError('confirm boom')

        def execute_and_fire(cb):

            def fake_confirm(ack_nack_callback, callback):
                ack_nack_callback('ack-frame')  # fire user cb via wrapper
                callback(ok_frame)

            raw_ch.confirm_delivery.side_effect = fake_confirm
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            ch.confirm_delivery(boom)
            ch._consumer_work_pool.shutdown(wait=True)

        self.assertTrue(
            any('publisher confirm callback' in msg for msg in cm.output),
            msg=
            f'Expected "publisher confirm callback" in logs, got: {cm.output}')


class BlockingMethodTimeoutTests(unittest.TestCase):
    """Tests for timeout behavior on blocking methods."""

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

    def test_queue_declare_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            # Schedule the callback but the mock never fires _on_declare
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.queue_declare = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.queue_declare(queue='q', timeout=0.05)

        self.assertIn('queue_declare', str(ctx.exception))

    def test_basic_qos_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.basic_qos = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.basic_qos(prefetch_count=1, timeout=0.05)

        self.assertIn('basic_qos', str(ctx.exception))

    def test_basic_consume_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.basic_consume = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.basic_consume(queue='q',
                             on_message_callback=MagicMock(),
                             timeout=0.05)

        self.assertIn('basic_consume', str(ctx.exception))

    def test_basic_cancel_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.basic_cancel = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.basic_cancel('ctag1', timeout=0.05)

        self.assertIn('basic_cancel', str(ctx.exception))

    def test_channel_close_does_not_raise_on_timeout(self):
        """close() should log and continue, not raise TimeoutError."""
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.close = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        # Must not raise - close() treats timeout as "channel is dead"
        ch.close(timeout=0.05)

    def test_default_timeout_succeeds_when_response_is_immediate(self):
        """RPC methods succeed normally when the response arrives before timeout."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_qos(prefetch_size, prefetch_count, global_qos, callback):
                callback(mock_frame)

            raw_ch.basic_qos.side_effect = fake_qos
            cb()

        wrapper.add_callback_threadsafe.side_effect = execute_and_fire

        result = ch.basic_qos(prefetch_count=1)
        self.assertIs(result, mock_frame)

    def test_timeout_unregisters_waiter(self):
        """A timed-out RPC must not leave a stale entry in _blocking_waiters."""
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.queue_declare = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError):
            ch.queue_declare(queue='q', timeout=0.05)

        self.assertEqual(wrapper._blocking_waiters, [])

    def test_basic_get_timeout_unregisters_waiter(self):
        """basic_get timeout must not leak a waiter."""
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.add_callback = MagicMock()
            raw_ch.basic_get = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError):
            ch.basic_get(queue='q', timeout=0.05)

        self.assertEqual(wrapper._blocking_waiters, [])

    def test_close_timeout_unregisters_waiter(self):
        """close() timeout must not leak a waiter."""
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.close = MagicMock()
            cb()

        wrapper.add_callback_threadsafe.side_effect = never_respond

        ch.close(timeout=0.05)
        self.assertEqual(wrapper._blocking_waiters, [])


class ConsumerPoolConnectionIntegrationTests(unittest.TestCase):
    """Tests for connection-level consumer pool lifecycle management."""

    def _make_connection(self):
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

            conn = ThreadSafeConnection(parameters='params')
            conn._ioloop_thread.join(timeout=1)

        return conn, mock_conn, mock_ioloop

    def test_connection_tracks_channels(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_scheduled(cb):
            mock_raw_ch = MagicMock()
            mock_conn.channel.side_effect = lambda on_open_callback: \
                on_open_callback(mock_raw_ch)
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()
        self.assertIn(ch, conn._channels)

    def test_connection_close_shuts_down_channel_pools(self):
        """Pool shutdown happens after ioloop.start() returns, not inside
        _on_connection_closed (which runs on the IOLoop thread)."""
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_scheduled(cb):
            mock_raw_ch = MagicMock()
            mock_conn.channel.side_effect = lambda on_open_callback: \
                on_open_callback(mock_raw_ch)
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()

        # _on_connection_closed wakes waiters but does NOT shut down pools
        conn._on_connection_closed(mock_conn, Exception('gone'))
        self.assertFalse(ch._pool_shutdown)

        # Pool shutdown happens when _run_ioloop calls it after ioloop.start()
        # returns. Simulate that by calling it directly.
        conn._shutdown_all_consumer_pools()
        self.assertTrue(ch._pool_shutdown)

    def test_ioloop_crash_shuts_down_channel_pools(self):
        """If the IOLoop crashes, all channel pools must be shut down."""
        crash = RuntimeError('IOLoop crash')
        gate = threading.Event()

        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            def ioloop_start():
                gate.wait(timeout=5)
                raise crash

            mock_ioloop.start.side_effect = ioloop_start
            conn = ThreadSafeConnection(parameters='params')

        # Manually add a channel to the tracking list
        ch = ThreadSafeChannel(MagicMock(), conn)
        with conn._channel_waiters_lock:
            conn._channels.append(ch)

        # Trigger the IOLoop crash
        gate.set()
        conn._ioloop_thread.join(timeout=2)

        self.assertTrue(ch._pool_shutdown)

    def test_force_stop_shuts_down_channel_pools(self):
        """Force-stop path in close() must shut down channel pools."""
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = True

        ch = ThreadSafeChannel(MagicMock(), conn)
        with conn._channel_waiters_lock:
            conn._channels.append(ch)

        conn.close(timeout=0.1)

        self.assertTrue(ch._pool_shutdown)

    def test_init_raises_timeout_error(self):
        """Constructor must raise TimeoutError if connection is not established
        within the specified timeout."""
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                # Never call on_open_callback - simulates stalled handshake
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn
            mock_ioloop.start = MagicMock()  # IOLoop does nothing

            with self.assertRaises(TimeoutError) as ctx:
                ThreadSafeConnection(parameters='params', timeout=0.05)

            self.assertIn('timed out', str(ctx.exception))
            mock_ioloop.add_callback_threadsafe.assert_called()

    def test_channel_raises_timeout_error(self):
        """channel() must raise TimeoutError if Channel.OpenOk never arrives."""
        conn, mock_conn, mock_ioloop = self._make_connection()

        def never_respond(cb):
            # Schedule the callback but Channel.OpenOk never fires
            mock_conn.channel = MagicMock()
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            conn.channel(timeout=0.05)

        self.assertIn('channel open', str(ctx.exception))

    def test_channel_timeout_unregisters_waiter(self):
        """channel() timeout must not leak a waiter."""
        conn, mock_conn, mock_ioloop = self._make_connection()

        def never_respond(cb):
            mock_conn.channel = MagicMock()
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError):
            conn.channel(timeout=0.05)

        self.assertEqual(conn._blocking_waiters, [])


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
        _conn, _, mock_ioloop = self._make_connection()
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
        conn, _mock_conn, mock_ioloop = self._make_connection()
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
        conn, _mock_conn, _ = self._make_connection()
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
        conn, _mock_conn, _mock_ioloop = self._make_connection()
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
        conn, _mock_conn, mock_ioloop = self._make_connection()
        conn._closed_reason = Exception('already closed')
        conn._ioloop_thread = MagicMock()
        conn.close()
        mock_ioloop.add_callback_threadsafe.assert_not_called()
        conn._ioloop_thread.join.assert_not_called()

    def test_close_from_ioloop_thread_calls_connection_close_directly(self):
        conn, mock_conn, _mock_ioloop = self._make_connection()
        # Simulate close() being called from within the IOLoop thread itself
        conn._ioloop_thread = threading.current_thread()
        conn.close()
        mock_conn.close.assert_called_once()

    def test_abort_swallows_close_errors(self):
        conn, _mock_conn, _ = self._make_connection()
        # Force conn.close() to raise
        with patch.object(conn, 'close', side_effect=Exception('boom')):
            conn.abort()  # must not raise

    def test_abort_delegates_to_close(self):
        conn, _mock_conn, _ = self._make_connection()
        with patch.object(conn, 'close') as mock_close:
            conn.abort(timeout=5)
            mock_close.assert_called_once_with(timeout=5)

    def test_blocked_callback_routes_through_add_callback_threadsafe(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        # Wipe call count from setup
        mock_ioloop.add_callback_threadsafe.reset_mock()
        conn.add_on_connection_blocked_callback(MagicMock())
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        mock_conn.add_on_connection_blocked_callback.assert_not_called()

    def test_blocked_callback_registers_on_raw_connection(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_immediately(cb):
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_immediately
        conn.add_on_connection_blocked_callback(MagicMock())
        mock_conn.add_on_connection_blocked_callback.assert_called_once()

    def test_blocked_callback_dispatches_on_worker_thread(self):
        import concurrent.futures
        conn, mock_conn, mock_ioloop = self._make_connection()
        # Recreate the work pool because _make_connection lets _run_ioloop
        # exit, which shuts the pool down.
        conn._connection_work_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='pika-conn-test')
        conn._connection_pool_shutdown = False
        callback_thread = []
        received = []

        def user_cb(connection, method_frame):
            callback_thread.append(threading.current_thread())
            received.append((connection, method_frame))

        def execute_and_fire(cb):
            cb()
            wrapped = mock_conn.add_on_connection_blocked_callback.call_args[0][
                0]
            wrapped(mock_conn, 'blocked-frame')

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        conn.add_on_connection_blocked_callback(user_cb)
        conn._shutdown_connection_pool()

        self.assertEqual(len(received), 1)
        self.assertIs(received[0][0], conn)
        self.assertEqual(received[0][1], 'blocked-frame')
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-conn', callback_thread[0].name)

    def test_blocked_callback_after_pool_shutdown_logs_and_drops(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        wrapped_holder = []

        def execute_and_fire(cb):
            cb()
            wrapped_holder.append(
                mock_conn.add_on_connection_blocked_callback.call_args[0][0])

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        conn.add_on_connection_blocked_callback(MagicMock())
        conn._shutdown_connection_pool()
        wrapped_holder[0](mock_conn, 'late-blocked')  # must not raise

    def test_blocked_callback_raises_when_connection_closed(self):
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        reason = Exception('closed')
        conn._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            conn.add_on_connection_blocked_callback(MagicMock())

        self.assertIs(ctx.exception, reason)

    def test_unblocked_callback_routes_through_add_callback_threadsafe(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        mock_ioloop.add_callback_threadsafe.reset_mock()
        conn.add_on_connection_unblocked_callback(MagicMock())
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        mock_conn.add_on_connection_unblocked_callback.assert_not_called()

    def test_unblocked_callback_dispatches_on_worker_thread(self):
        import concurrent.futures
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._connection_work_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='pika-conn-test')
        conn._connection_pool_shutdown = False
        received = []

        def user_cb(connection, method_frame):
            received.append((connection, method_frame))

        def execute_and_fire(cb):
            cb()
            wrapped = mock_conn.add_on_connection_unblocked_callback.call_args[
                0][0]
            wrapped(mock_conn, 'unblocked-frame')

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        conn.add_on_connection_unblocked_callback(user_cb)
        conn._shutdown_connection_pool()

        self.assertEqual(len(received), 1)
        self.assertIs(received[0][0], conn)
        self.assertEqual(received[0][1], 'unblocked-frame')

    def test_unblocked_callback_raises_when_connection_closed(self):
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        reason = Exception('closed')
        conn._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            conn.add_on_connection_unblocked_callback(MagicMock())

        self.assertIs(ctx.exception, reason)

    def test_connection_pool_shutdown_is_idempotent(self):
        conn, _, _ = self._make_connection()
        conn._shutdown_connection_pool()
        conn._shutdown_connection_pool()  # second call must be a no-op

    def test_close_force_stop_shuts_down_connection_pool(self):
        """When close() force-stops the IOLoop, the connection pool must be
        shut down so no callback worker thread is left dangling."""
        conn, _mock_conn, _ = self._make_connection()
        conn._ioloop_thread = MagicMock()
        # First join() returns with thread still alive, second returns dead
        conn._ioloop_thread.is_alive.return_value = True
        conn.close(timeout=0.01)
        self.assertTrue(conn._connection_pool_shutdown)

    def test_connection_event_listener_exception_is_logged_not_lost(self):
        """A blocked-listener exception must be logged, not silently lost
        on an unobserved Future."""
        import concurrent.futures
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._connection_work_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='pika-conn-test')
        conn._connection_pool_shutdown = False

        def boom(connection, method_frame):
            raise RuntimeError('blocked-listener boom')

        def execute_and_fire(cb):
            cb()
            wrapped = mock_conn.add_on_connection_blocked_callback.call_args[0][
                0]
            wrapped(mock_conn, 'blocked-frame')

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            conn.add_on_connection_blocked_callback(boom)
            conn._shutdown_connection_pool()

        self.assertTrue(
            any('add_on_connection_blocked_callback' in msg
                for msg in cm.output),
            msg=f'Expected listener label in logs, got: {cm.output}')

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
