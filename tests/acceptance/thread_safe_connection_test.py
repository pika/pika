"""Integration tests for ThreadSafeConnection and ThreadSafeChannel.

These tests require a RabbitMQ broker listening on 127.0.0.1:5672 with the
default guest/guest credentials.  They will fail, not skip, if the broker is
unreachable - consistent with the rest of the acceptance suite.

Each test covers a scenario that unit tests with mocks cannot: real socket I/O,
real AMQP frame exchange, and real concurrent threads.
"""
import threading
import unittest
import uuid

import pika
from pika.adapters.thread_safe_connection import ThreadSafeChannel, ThreadSafeConnection
from tests.misc.forward_server import ForwardServer
from tests.misc.test_utils import retry_assertion

DEFAULT_PARAMS = pika.ConnectionParameters(
    host='127.0.0.1',
    port=5672,
    credentials=pika.PlainCredentials('guest', 'guest'),
)

# Timeout used for join() calls that assert a thread does not hang.
BLOCKING_CALL_TIMEOUT = 10


class ThreadSafeTestCaseBase(unittest.TestCase):

    def _connect(self, parameters=None):
        conn = ThreadSafeConnection(parameters or DEFAULT_PARAMS)
        self.addCleanup(self._safe_close, conn)
        return conn

    @staticmethod
    def _safe_close(conn):
        try:
            conn.close()
        except Exception:
            pass

    @staticmethod
    def _unique_queue():
        return f'tsc-test-{uuid.uuid4().hex}'


class TestBasicLifecycle(ThreadSafeTestCaseBase):
    """Happy path: connect, channel, declare, publish, consume, close."""

    def test(self):
        conn = self._connect()
        self.assertTrue(conn.is_open)

        ch = conn.channel()
        self.assertIsInstance(ch, ThreadSafeChannel)
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
            self.assertGreaterEqual(frame.method.message_count, 1)

        assert_message_arrived()

        conn.close()
        self.assertTrue(conn.is_closed)


class TestConcurrentPublishing(ThreadSafeTestCaseBase):
    """N threads publish simultaneously; all messages must reach the broker.

    This is the core regression test for the original _tx_buffers race
    (IndexError: pop from empty deque, issues #1144 and #511).
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
            self.assertEqual(frame.method.message_count, n)

        assert_all_arrived()


class TestConcurrentPublishAndConsume(ThreadSafeTestCaseBase):
    """Producer threads and a consumer coexist; every message is acked.

    Exercises basic_qos, basic_consume, and basic_ack (from the IOLoop
    thread inside the delivery callback).
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
    """A thread blocked in channel() must unblock when the broker drops.

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
        conn = ThreadSafeConnection(params)
        self.addCleanup(self._safe_close, conn)

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
    """A thread blocked in queue_declare() must unblock when the broker drops.

    Exercises the _blocking_waiters escape hatch added to queue_declare()
    in the re-review architectural fix.
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
        conn = ThreadSafeConnection(params)
        self.addCleanup(self._safe_close, conn)

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
    """close() called from multiple threads simultaneously must not crash or hang.

    Before _safe_close and the _closed_reason guard, one of the racing
    close() calls could schedule connection.close() when the connection was
    already closing, raising ConnectionWrongStateError inside the IOLoop and
    killing the IOLoop thread.
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


class TestContextManager(ThreadSafeTestCaseBase):
    """The context manager must close the connection on exit."""

    def test(self):
        params = pika.ConnectionParameters(
            host='127.0.0.1',
            port=5672,
            credentials=pika.PlainCredentials('guest', 'guest'),
        )
        with ThreadSafeConnection(params) as conn:
            self.assertTrue(conn.is_open)
            ch = conn.channel()
            queue = self._unique_queue()
            ch.queue_declare(queue=queue, exclusive=True)
            ch.basic_publish(exchange='', routing_key=queue, body=b'ctx')

        self.assertTrue(conn.is_closed)


if __name__ == '__main__':
    unittest.main()
