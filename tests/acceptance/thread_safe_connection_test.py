"""
Integration tests for Connection and Channel.

These tests require a RabbitMQ broker listening on 127.0.0.1:5672 with the
default guest/guest credentials.  They will fail, not skip, if the broker is
unreachable - consistent with the rest of the acceptance suite.

Each test covers a scenario that unit tests with mocks cannot: real socket I/O,
real AMQP frame exchange, and real concurrent threads.
"""

import contextlib
import threading
import time
import unittest
import uuid

import pika
from pika.adapters.thread_safe_connection import Channel, Connection
from pika.exceptions import AMQPConnectionError
from tests.misc.forward_server import ForwardServer
from tests.misc.test_utils import retry_assertion, safe_close

DEFAULT_PARAMS = pika.ConnectionParameters(
    host='127.0.0.1',
    port=5672,
    credentials=pika.PlainCredentials('guest', 'guest'),
)

# Timeout used for join() calls that assert a thread does not hang.
BLOCKING_CALL_TIMEOUT = 10


class ThreadSafeTestCaseBase(unittest.TestCase):

    def _connect(self, parameters=None):
        conn = Connection(parameters or DEFAULT_PARAMS)
        self.addCleanup(safe_close, conn)
        return conn

    @staticmethod
    def _unique_queue():
        return f'tsc-test-{uuid.uuid4().hex}'


class TestBasicLifecycle(ThreadSafeTestCaseBase):
    """Happy path: connect, channel, declare, publish, consume, close."""

    def test(self):
        conn = self._connect()
        self.assertTrue(conn.is_open)

        ch = conn.channel()
        self.assertIsInstance(ch, Channel)
        self.assertTrue(ch.is_open)

        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)

        ch.basic_publish(exchange='', routing_key=queue, body=b'hello')

        # Consume the message synchronously using basic_get via
        # add_callback_threadsafe so we can stay on the IOLoop thread.
        received = []
        done = threading.Event()

        def _get():

            def _on_get(ch_, method, props, body):
                received.append(body)
                done.set()

            conn.add_callback_threadsafe(lambda: conn._connection.channel(
                on_open_callback=lambda raw_ch: raw_ch.basic_get(
                    queue=queue, callback=_on_get, auto_ack=True)))

        # Give basic_publish time to deliver, then poll via passive declare.
        @retry_assertion(timeout_sec=5)
        def assert_message_arrived():
            frame = ch.queue_declare(queue=queue, passive=True)
            assert frame is not None
            self.assertGreaterEqual(frame.method.message_count, 1)

        assert_message_arrived()

        conn.close()
        self.assertTrue(conn.is_closed)


class TestConfirmCallbackValidation(ThreadSafeTestCaseBase):

    def test(self):
        conn = self._connect()
        ch = conn.channel()
        for callback in (None, 0, 'callback'):
            with self.subTest(callback=callback), self.assertRaises(TypeError):
                ch.confirm_delivery(callback)
        self.assertIsNone(ch.next_publish_seq_no)

        confirmations = []
        confirmed = threading.Event()

        def on_confirm(method_frame):
            confirmations.append(method_frame.method)
            confirmed.set()

        ch.confirm_delivery(on_confirm)
        with self.assertRaises(TypeError):
            ch.confirm_delivery(None)
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, exclusive=True)
        ch.basic_publish(exchange='', routing_key=queue, body=b'confirmed')
        self.assertTrue(confirmed.wait(BLOCKING_CALL_TIMEOUT))
        self.assertIsInstance(confirmations[0], pika.spec.Basic.Ack)
        self.assertEqual(confirmations[0].delivery_tag, 1)


class TestConcurrentPublishing(ThreadSafeTestCaseBase):
    """
    N threads publish simultaneously; all messages must reach the broker.

    This is the core regression test for the original _tx_buffers race (IndexError: pop from empty
    deque, issues #1144 and #511).
    """

    def test(self):
        n = 10
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)

        barrier = threading.Barrier(n)
        errors = []

        def publish(i):
            try:
                barrier.wait()
                ch.basic_publish(
                    exchange='',
                    routing_key=queue,
                    body=f'msg-{i}'.encode(),
                )
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=publish, args=(i,)) for i in range(n)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=BLOCKING_CALL_TIMEOUT)
            self.assertFalse(t.is_alive(), 'publish thread did not finish')

        self.assertEqual([], errors,
                         f'exceptions from publish threads: {errors}')

        @retry_assertion(timeout_sec=10)
        def assert_all_arrived():
            frame = ch.queue_declare(queue=queue, passive=True)
            assert frame is not None
            self.assertEqual(frame.method.message_count, n)

        assert_all_arrived()


class TestConcurrentPublishAndConsume(ThreadSafeTestCaseBase):
    """
    Producer threads and a consumer coexist; every message is acked.

    Exercises basic_qos, basic_consume, and basic_ack (from the IOLoop thread inside the delivery
    callback).
    """

    def test(self):
        n = 20
        n_publishers = 4
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)
        ch.basic_qos(prefetch_count=5)

        received = []
        lock = threading.Lock()

        def on_message(channel, method, properties, body):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            with lock:
                received.append(body)

        consumer_tag = ch.basic_consume(queue=queue,
                                        on_message_callback=on_message)
        self.assertIsInstance(consumer_tag, str)
        self.assertTrue(len(consumer_tag) > 0)

        msgs_per_thread = n // n_publishers
        barrier = threading.Barrier(n_publishers)
        errors = []

        def publish_batch(thread_id):
            try:
                barrier.wait()
                for i in range(msgs_per_thread):
                    ch.basic_publish(
                        exchange='',
                        routing_key=queue,
                        body=f'thread-{thread_id}-msg-{i}'.encode(),
                    )
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=publish_batch, args=(i,))
            for i in range(n_publishers)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=BLOCKING_CALL_TIMEOUT)
            self.assertFalse(t.is_alive(), 'publisher thread did not finish')

        self.assertEqual([], errors)

        @retry_assertion(timeout_sec=10)
        def assert_all_consumed():
            with lock:
                self.assertEqual(len(received), n)

        assert_all_consumed()

        ch.basic_cancel(consumer_tag)


class TestBrokerDropBlockedInChannel(ThreadSafeTestCaseBase):
    """
    A thread blocked in channel() must unblock when the broker drops.

    Before the _blocking_waiters escape hatch, this would hang forever.
    """

    def test(self):
        fwd = ForwardServer(
            remote_addr=(DEFAULT_PARAMS.host, DEFAULT_PARAMS.port),
            local_linger_args=(1, 0),
        )
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        params = pika.ConnectionParameters(
            host='127.0.0.1',
            port=fwd.server_address[1],
            credentials=pika.PlainCredentials('guest', 'guest'),
        )
        conn = Connection(params)
        self.addCleanup(safe_close, conn)

        # Intercept add_callback_threadsafe so channel() registers its waiter
        # but the channel never actually opens before we drop the connection.
        real_act = conn._connection.ioloop.add_callback_threadsafe
        scheduled = threading.Event()

        def intercepted(cb):
            scheduled.set()  # signal that channel() has registered its waiter
            # Do NOT forward - the channel open never happens.

        conn._connection.ioloop.add_callback_threadsafe = intercepted

        exc_holder = [None]

        def try_channel():
            try:
                conn.channel()
            except Exception as exc:
                exc_holder[0] = exc

        t = threading.Thread(target=try_channel)
        t.start()

        # Wait for channel() to register its waiter, then drop the connection.
        scheduled.wait(timeout=5)
        fwd.stop()
        # Restore so _on_connection_closed can schedule ioloop.stop.
        conn._connection.ioloop.add_callback_threadsafe = real_act

        t.join(timeout=BLOCKING_CALL_TIMEOUT)
        self.assertFalse(t.is_alive(),
                         'channel() blocked forever after broker disconnect')
        self.assertIsNotNone(exc_holder[0],
                             'channel() should have raised after disconnect')


class TestBrokerDropBlockedInQueueDeclare(ThreadSafeTestCaseBase):
    """
    A thread blocked in queue_declare() must unblock when the broker drops.

    Exercises the _blocking_waiters escape hatch added to queue_declare() in the re-review
    architectural fix.
    """

    def test(self):
        fwd = ForwardServer(
            remote_addr=(DEFAULT_PARAMS.host, DEFAULT_PARAMS.port),
            local_linger_args=(1, 0),
        )
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        params = pika.ConnectionParameters(
            host='127.0.0.1',
            port=fwd.server_address[1],
            credentials=pika.PlainCredentials('guest', 'guest'),
        )
        conn = Connection(params)
        self.addCleanup(safe_close, conn)

        # Open the channel before intercepting so we get a real channel object.
        ch = conn.channel()

        # Same intercept trick: let queue_declare register its waiter, then
        # drop the connection before the broker responds.
        real_act = conn._connection.ioloop.add_callback_threadsafe
        scheduled = threading.Event()

        def intercepted(cb):
            scheduled.set()

        conn._connection.ioloop.add_callback_threadsafe = intercepted

        exc_holder = [None]

        def try_declare():
            try:
                ch.queue_declare(queue=self._unique_queue())
            except Exception as exc:
                exc_holder[0] = exc

        t = threading.Thread(target=try_declare)
        t.start()

        scheduled.wait(timeout=5)
        fwd.stop()
        conn._connection.ioloop.add_callback_threadsafe = real_act

        t.join(timeout=BLOCKING_CALL_TIMEOUT)
        self.assertFalse(
            t.is_alive(),
            'queue_declare() blocked forever after broker disconnect')
        self.assertIsNotNone(
            exc_holder[0],
            'queue_declare() should have raised after disconnect')


class TestConcurrentClose(ThreadSafeTestCaseBase):
    """
    Close() called from multiple threads simultaneously must not crash or hang.

    Before _safe_close and the _closed_reason guard, one of the racing close() calls could schedule
    connection.close() when the connection was already closing, raising ConnectionWrongStateError
    inside the IOLoop and killing the IOLoop thread.
    """

    def test(self):
        conn = self._connect()
        ch = conn.channel()
        ch.queue_declare(queue=self._unique_queue(), exclusive=True)

        n = 5
        barrier = threading.Barrier(n)
        errors = []

        def close():
            try:
                barrier.wait()
                conn.close()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=close) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=BLOCKING_CALL_TIMEOUT)
            self.assertFalse(t.is_alive(), 'close() thread did not finish')

        self.assertEqual([], errors)
        self.assertTrue(conn.is_closed)


class TestCloseFromConsumerCallbackOnBrokerClosedChannel(
        ThreadSafeTestCaseBase):
    """
    A broker-closed channel must not stop close() from returning promptly.

    Acking an unknown delivery tag makes the broker answer 406 and close the
    channel, which runs `_on_broker_close` and drops it from `_channels` while
    this callback is still on the worker.  Distinct from
    `TestCloseFromConsumerCallback`, whose channel stays open and tracked.

    This passes under the pre-#1761 `_channels` search as well, because the
    prune that misclassified the worker also dropped its pool from the
    shutdown sweep that would have joined it back, so neither predicate hangs
    here.  The predicate itself is pinned by
    `ConnectionTests.test_is_pool_worker_thread_survives_its_channel_being_dropped`;
    this is an end-to-end no-hang guard over the same path.
    """

    def test_returns_promptly(self):
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)
        ch.basic_publish(exchange='', routing_key=queue, body=b'stop')

        done = threading.Event()
        elapsed = []
        tracked = []

        def on_message(channel, method, properties, body):
            # 406 PRECONDITION_FAILED closes the channel broker-side.
            with contextlib.suppress(Exception):
                channel.basic_ack(delivery_tag=method.delivery_tag + 10000)
            # Record rather than assert: an exception here is swallowed by
            # _safe_dispatch, which would fail this as a missing close().
            deadline = time.monotonic() + BLOCKING_CALL_TIMEOUT
            while ch in conn._channels and time.monotonic() < deadline:
                time.sleep(0.05)
            tracked.append(ch in conn._channels)
            start = time.monotonic()
            conn.close(timeout=BLOCKING_CALL_TIMEOUT)
            elapsed.append(time.monotonic() - start)
            done.set()

        ch.basic_consume(queue=queue,
                         on_message_callback=on_message,
                         auto_ack=True)

        self.assertTrue(
            done.wait(timeout=BLOCKING_CALL_TIMEOUT * 3),
            'close() from a consumer callback on a broker-closed channel did '
            'not return')
        self.assertEqual(
            tracked, [False], 'the broker never closed the channel, so the '
            'pruned-channel path was not exercised')
        self.assertLess(
            elapsed[0], 2,
            f'close() blocked for {elapsed[0]:.2f}s instead of returning '
            f'immediately from a pool worker')


class TestCloseFromConsumerCallback(ThreadSafeTestCaseBase):
    """
    Connection.close() called from a consumer's on_message_callback must return promptly.

    Regression test for issue #1686. close() used to join the IOLoop thread unconditionally, but the
    IOLoop thread's own post-close cleanup tail (_shutdown_all_consumer_pools) joins every channel's
    consumer worker - including the very worker running this callback - before it can exit. Joining
    from inside the callback therefore self-deadlocked: this thread waiting on the IOLoop thread,
    which was waiting on this thread to return from the callback that called close(). With the fix,
    close() detects it is running on a pool worker thread and returns immediately after scheduling the
    close, letting the worker (and then the IOLoop thread) exit normally once the callback returns.
    """

    def test_bounded_timeout_returns_promptly(self):
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)
        ch.basic_publish(exchange='', routing_key=queue, body=b'stop')

        done = threading.Event()
        elapsed = [None]

        def on_message(channel, method, _properties, _body):
            channel.basic_ack(method.delivery_tag)
            started = time.monotonic()
            conn.close(timeout=BLOCKING_CALL_TIMEOUT)
            elapsed[0] = time.monotonic() - started
            done.set()

        ch.basic_consume(queue=queue, on_message_callback=on_message)

        self.assertTrue(
            done.wait(timeout=BLOCKING_CALL_TIMEOUT),
            'conn.close() called from the consumer callback did not return '
            f'within {BLOCKING_CALL_TIMEOUT}s')
        self.assertLess(
            elapsed[0], 2,
            'conn.close() blocked instead of returning immediately when '
            'called from the consumer callback')

        retry_assertion(BLOCKING_CALL_TIMEOUT)(
            lambda: self.assertTrue(conn.is_closed))()

    def test_unbounded_timeout_does_not_deadlock(self):
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)
        ch.basic_publish(exchange='', routing_key=queue, body=b'stop')

        done = threading.Event()

        def on_message(channel, method, _properties, _body):
            channel.basic_ack(method.delivery_tag)
            conn.close(timeout=None)
            done.set()

        ch.basic_consume(queue=queue, on_message_callback=on_message)

        self.assertTrue(
            done.wait(timeout=BLOCKING_CALL_TIMEOUT),
            'conn.close(timeout=None) called from the consumer callback '
            'deadlocked')

        retry_assertion(BLOCKING_CALL_TIMEOUT)(
            lambda: self.assertTrue(conn.is_closed))()


class TestAddCallbackThreadsafeAfterClose(ThreadSafeTestCaseBase):
    """
    add_callback_threadsafe() after a real close() raises and runs nothing.

    close() returns only after the IOLoop thread has exited, so a callback accepted afterwards could
    never run.  The caller gets the recorded close reason (an :class:`AMQPConnectionError`), the
    same exception every other method on the connection raises once closed, rather than silent
    acceptance.
    """

    def test(self):
        conn = self._connect()
        conn.close()
        self.assertTrue(conn.is_closed)

        called = threading.Event()

        with self.assertRaises(AMQPConnectionError):
            conn.add_callback_threadsafe(called.set)

        self.assertFalse(called.is_set())


class TestContextManager(ThreadSafeTestCaseBase):
    """The context manager must close the connection on exit."""

    def test(self):
        params = pika.ConnectionParameters(
            host='127.0.0.1',
            port=5672,
            credentials=pika.PlainCredentials('guest', 'guest'),
        )
        with Connection(params) as conn:
            self.assertTrue(conn.is_open)
            ch = conn.channel()
            queue = self._unique_queue()
            ch.queue_declare(queue=queue, exclusive=True)
            ch.basic_publish(exchange='', routing_key=queue, body=b'ctx')

        self.assertTrue(conn.is_closed)


class TestPerRPCCallbacksDoNotAccumulate(ThreadSafeTestCaseBase):
    """
    A long-lived channel must not grow a callback per RPC it serves.

    ``add_on_close_callback`` registers with ``one_shot=False`` and every RPC passes a distinct
    closure, so anything left behind is held until the channel closes and is rescanned by every
    later registration.  Unit tests assert the removal calls; this asserts the observable
    consequence against a real broker.
    """

    @staticmethod
    def _callback_count(ch, key):
        raw = ch._channel
        # CallbackManager normalizes the prefix to a string.
        stack = raw.callbacks._stack.get(str(raw.channel_number), {})
        return len(stack.get(key, []))

    def test(self):
        n = 20
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, exclusive=True)

        close_baseline = self._callback_count(ch, '_on_channel_close')
        empty_baseline = self._callback_count(ch, 'Basic.GetEmpty')
        for _ in range(50):
            ch.queue_declare(queue=queue, exclusive=True)

        # At most one cleanup may still be in flight on the IOLoop thread.
        retry_assertion(BLOCKING_CALL_TIMEOUT)(lambda: self.assertLessEqual(
            self._callback_count(ch, '_on_channel_close'), close_baseline + 1))(
            )

        # The Basic.GetEmpty one-shot self-consumes only when it fires, so a
        # get that returns a message leaves its own behind for
        # _release_close_callback to drop.  Publish one message per get and
        # wait for all to arrive so every get returns a message and exercises
        # that removal.
        for i in range(n):
            ch.basic_publish(exchange='',
                             routing_key=queue,
                             body=f'msg-{i}'.encode())

        @retry_assertion(timeout_sec=BLOCKING_CALL_TIMEOUT)
        def assert_all_arrived():
            frame = ch.queue_declare(queue=queue, passive=True)
            assert frame is not None
            self.assertEqual(frame.method.message_count, n)

        assert_all_arrived()

        for _ in range(n):
            method, _properties, body = ch.basic_get(queue=queue, auto_ack=True)
            self.assertIsNotNone(method, 'expected a message, got an empty get')
            self.assertIsNotNone(body)

        # The GetEmpty one-shots from message-returning gets must be dropped,
        # and the close callbacks each get registered must not accumulate.
        retry_assertion(BLOCKING_CALL_TIMEOUT)(lambda: self.assertLessEqual(
            self._callback_count(ch, 'Basic.GetEmpty'), empty_baseline + 1))()
        retry_assertion(BLOCKING_CALL_TIMEOUT)(lambda: self.assertLessEqual(
            self._callback_count(ch, '_on_channel_close'), close_baseline + 1))(
            )


if __name__ == '__main__':
    unittest.main()


class TestPublishDuringConfirmSelectRoundTrip(ThreadSafeTestCaseBase):
    """
    A publish issued while Confirm.SelectOk is in flight must carry the broker's tag.

    The broker numbers from the first publish it processes after Confirm.Select, so a publish made
    during the round trip is tag 1.  Arming the wrapper's counter on the calling thread once
    confirm_delivery returned skipped it, leaving every later tag one behind the broker's (issue
    #1683).
    """

    def test(self):
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)

        acked = []
        reported = []
        select_sent = threading.Event()

        raw_confirm_delivery = ch._channel.confirm_delivery

        def _hooked(*args, **kwargs):
            result = raw_confirm_delivery(*args, **kwargs)
            # Confirm.Select is written and Confirm.SelectOk is still in
            # flight.  The hook only makes the timing deterministic; any thread
            # publishing while another is inside confirm_delivery lands in the
            # same window.
            select_sent.set()
            return result

        ch._channel.confirm_delivery = _hooked

        def _publish(body):
            ch.basic_publish(
                exchange='',
                routing_key=queue,
                body=body,
                on_publish=lambda tag, b=body: reported.append(
                    (b.decode(), tag)),
            )

        def _on_confirm(method_frame):
            acked.append(method_frame.method.delivery_tag)

        enabler = threading.Thread(target=ch.confirm_delivery,
                                   args=(_on_confirm,))
        enabler.start()
        try:
            self.assertTrue(select_sent.wait(BLOCKING_CALL_TIMEOUT),
                            'Confirm.Select was never written')
            _publish(b'A')
        finally:
            enabler.join(timeout=BLOCKING_CALL_TIMEOUT)
        self.assertFalse(enabler.is_alive(), 'confirm_delivery did not return')

        _publish(b'B')
        _publish(b'C')

        # The broker may ack with multiple=True, so assert how far the
        # confirmations reached rather than counting individual tags.
        @retry_assertion(timeout_sec=BLOCKING_CALL_TIMEOUT)
        def assert_all_confirmed():
            assert acked, 'no publisher confirm arrived'
            self.assertEqual(max(acked), 3)

        assert_all_confirmed()

        self.assertEqual(reported, [('A', 1), ('B', 2), ('C', 3)])
        self.assertEqual(ch.next_publish_seq_no, 4)


class TestConfirmDeliveryRetryAgainstRealChannel(ThreadSafeTestCaseBase):
    """
    A retried confirm_delivery must not raise or reset the counter on a real broker.

    A confirm_delivery that timed out may still have reached the broker, so the retry sends a second
    Confirm.Select to an already-confirming channel.  The wrapper's own retry unit test uses a mock
    raw channel, so this exercises the real pika Channel accepting that second Confirm.Select and
    the counter surviving it (issue #1683).
    """

    def test(self):
        conn = self._connect()
        ch = conn.channel()
        queue = self._unique_queue()
        ch.queue_declare(queue=queue, durable=False, exclusive=True)

        acked = []

        def _on_confirm(method_frame):
            acked.append(method_frame.method.delivery_tag)

        ch.confirm_delivery(_on_confirm)

        reported = []

        def _publish(body):
            ch.basic_publish(
                exchange='',
                routing_key=queue,
                body=body,
                on_publish=lambda tag, b=body: reported.append(
                    (b.decode(), tag)),
            )

        _publish(b'A')

        # Simulate the state left by a confirm_delivery that timed out after
        # the broker already enabled confirms: the wrapper never cached the
        # Confirm.SelectOk, so the retry is let through the guard.
        ch._confirm_select_ok = None

        # confirm_delivery blocks on the round trip, so by the time it returns
        # the IOLoop has already run publish A, which was scheduled ahead of
        # it; the counter is therefore a deterministic 1 (property 2).  The real
        # channel must accept this second Confirm.Select without raising, and
        # the retry must not reset the counter the broker never reset.
        ch.confirm_delivery(_on_confirm)
        self.assertEqual(ch.next_publish_seq_no, 2)

        # If the retry had reset the counter, B would be tag 1; tag 2 proves it
        # was preserved across the second Confirm.Select.
        _publish(b'B')

        @retry_assertion(timeout_sec=BLOCKING_CALL_TIMEOUT)
        def assert_all_confirmed():
            assert acked, 'no publisher confirm arrived'
            self.assertEqual(max(acked), 2)

        assert_all_confirmed()

        self.assertEqual(reported, [('A', 1), ('B', 2)])
        self.assertEqual(ch.next_publish_seq_no, 3)


class TestCloseRunsQueuedCallbacksWhileConnectionWorks(ThreadSafeTestCaseBase):
    """
    A close must not leave the broker holding work the callbacks already did.

    #1763: the pools used to drain after the connection was torn down, so the acknowledgements those
    callbacks issued never reached the broker and it redelivered work that had already been
    performed. The broker's own count is the assertion because it is the only place the outcome is
    visible: on this path `basic_ack` raises inside the callback, and on the channel-close path it
    fails as a log line while the caller sees success.
    """

    N = 30

    def test_every_ack_lands(self):
        queue = self._unique_queue()
        setup = self._connect()
        sch = setup.channel()
        sch.queue_declare(queue=queue, durable=True)
        self.addCleanup(self._delete_queue, queue)
        for _ in range(self.N):
            sch.basic_publish(exchange='', routing_key=queue, body=b'x')
        setup.close(timeout=BLOCKING_CALL_TIMEOUT)

        conn = self._connect()
        ch = conn.channel()
        lock = threading.Lock()
        ran = []

        def consumer(channel, method, properties, body):
            # Slow enough that a backlog builds up behind the first few.
            time.sleep(0.05)
            with lock:
                ran.append(method.delivery_tag)
            channel.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_consume(queue=queue,
                         on_message_callback=consumer,
                         auto_ack=False)
        # Close once the consumer is under way but well short of the end, so a
        # backlog is queued rather than drained. Waiting on observed progress
        # rather than a fixed sleep keeps this independent of machine speed.
        deadline = time.monotonic() + BLOCKING_CALL_TIMEOUT
        while time.monotonic() < deadline:
            with lock:
                if len(ran) >= 3:
                    break
            time.sleep(0.02)
        queued = ch._consumer_work_pool._queue.qsize()
        conn.close(timeout=BLOCKING_CALL_TIMEOUT)

        self.assertGreater(
            queued, 0, 'nothing was queued when close() was called, so this '
            'test did not exercise the drain')
        with lock:
            delivered = len(ran)
        remaining = self._ready_count(queue)
        self.assertEqual(
            remaining, 0,
            f'the broker still holds {remaining} of {self.N} messages after '
            f'{delivered} callbacks ran, so their acknowledgements '
            f'never reached it and that work will be redelivered')

    def _ready_count(self, queue):
        conn = self._connect()
        ch = conn.channel()
        frame = ch.queue_declare(queue=queue, durable=True, passive=True)
        return frame.method.message_count

    def _delete_queue(self, queue):
        conn = Connection(DEFAULT_PARAMS)
        try:
            conn.channel().queue_delete(queue=queue)
        finally:
            safe_close(conn)


class TestCloseUnderBackPressureLosesNothing(ThreadSafeTestCaseBase):
    """
    A close must cancel the consumer even while the IOLoop is applying back-pressure.

    With the work queue full the IOLoop blocks in `submit`, so anything the close needs it to do is
    answered slowly. A bound that fires there abandons the cancellation, the broker keeps
    dispatching into a pool that is about to shut down, and those deliveries are dropped at submit.
    `auto_ack` makes that unrecoverable, since the broker keeps no copy.

    This asserts the end-to-end outcome for a backlog that is still arriving; it is kept cheap, so
    it does not reach the regime where a seconds-long cap would fire. The bounds themselves are
    pinned by `ChannelCloseErrorPathTests`, and the cost of getting them wrong is recorded on
    `docs/internal/thread-safe-connection.md`.
    """

    N = 400
    MAXSIZE = 10

    def test_nothing_is_lost(self):
        queue = self._unique_queue()
        setup = self._connect()
        sch = setup.channel()
        sch.queue_declare(queue=queue, durable=True)
        self.addCleanup(self._delete_queue, queue)
        for _ in range(self.N):
            sch.basic_publish(exchange='', routing_key=queue, body=b'x' * 8192)
        setup.close(timeout=BLOCKING_CALL_TIMEOUT)

        conn = Connection(DEFAULT_PARAMS, work_queue_maxsize=self.MAXSIZE)
        self.addCleanup(safe_close, conn)
        ch = conn.channel()
        lock = threading.Lock()
        ran = []

        def consumer(channel, method, properties, body):
            time.sleep(0.005)
            with lock:
                ran.append(1)

        ch.basic_consume(queue=queue,
                         on_message_callback=consumer,
                         auto_ack=True)
        # Close while the broker is still dispatching and the queue is full.
        deadline = time.monotonic() + BLOCKING_CALL_TIMEOUT
        while time.monotonic() < deadline:
            with lock:
                if len(ran) >= 5:
                    break
            time.sleep(0.01)
        queued = ch._consumer_work_pool._queue.qsize()
        conn.close(timeout=BLOCKING_CALL_TIMEOUT * 3)
        time.sleep(1.0)
        with lock:
            delivered = len(ran)

        self.assertEqual(
            queued, self.MAXSIZE,
            f'the work queue held {queued} of {self.MAXSIZE} at close, so the '
            f'back-pressure this test needs was not present')
        remaining = self._ready_count(queue)
        self.assertEqual(
            delivered + remaining, self.N,
            f'{self.N - delivered - remaining} of {self.N} auto_ack messages '
            f'reached neither a callback nor the queue, so they are lost')

    def _ready_count(self, queue):
        conn = self._connect()
        ch = conn.channel()
        return ch.queue_declare(queue=queue, durable=True,
                                passive=True).method.message_count

    def _delete_queue(self, queue):
        conn = Connection(DEFAULT_PARAMS)
        try:
            conn.channel().queue_delete(queue=queue)
        finally:
            safe_close(conn)


class TestCloseCompletesTheHandshakeAfterALongDrain(ThreadSafeTestCaseBase):
    """
    A close whose callbacks use the whole budget must still close the connection.

    No phase reserves a share of the budget, because capping a phase below the broker's pace costs
    messages (#1763). The handshake is the exception: it is one round trip whose duration does not
    depend on the backlog, so it gets a floor. Without it the join is handed zero, `ioloop.stop`
    runs ahead of the reply, and the socket is abandoned open with the broker still holding the
    connection.
    """

    N = 400

    def test_connection_is_really_closed(self):
        queue = self._unique_queue()
        setup = self._connect()
        sch = setup.channel()
        sch.queue_declare(queue=queue, durable=True)
        self.addCleanup(self._delete_queue, queue)
        for _ in range(self.N):
            sch.basic_publish(exchange='', routing_key=queue, body=b'x' * 100)
        setup.close(timeout=BLOCKING_CALL_TIMEOUT)

        conn = Connection(DEFAULT_PARAMS)
        self.addCleanup(safe_close, conn)
        ch = conn.channel()
        ran = []

        def consumer(channel, method, properties, body):
            time.sleep(0.02)
            ran.append(1)
            channel.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_consume(queue=queue,
                         on_message_callback=consumer,
                         auto_ack=False)
        # A budget far smaller than the backlog needs, so the drain spends all
        # of it and the handshake has to come out of the floor.
        deadline = time.monotonic() + BLOCKING_CALL_TIMEOUT
        while time.monotonic() < deadline and len(ran) < 3:
            time.sleep(0.01)
        conn.close(timeout=1)

        impl = conn._connection
        self.assertTrue(
            impl.is_closed,
            f'the connection is in state {impl.connection_state} with its '
            f'transport still attached, so the close was abandoned half done')
        self.assertIsNone(impl._transport)

    def _delete_queue(self, queue):
        conn = Connection(DEFAULT_PARAMS)
        try:
            conn.channel().queue_delete(queue=queue)
        finally:
            safe_close(conn)
