"""
Integration tests for Connection and Channel.

These tests require a RabbitMQ broker listening on 127.0.0.1:5672 with the
default guest/guest credentials.  They will fail, not skip, if the broker is
unreachable - consistent with the rest of the acceptance suite.

Each test covers a scenario that unit tests with mocks cannot: real socket I/O,
real AMQP frame exchange, and real concurrent threads.
"""

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
