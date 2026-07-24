"""
Integration tests for the ThreadSafeConnection bounded work queue.

These tests require a RabbitMQ broker listening on 127.0.0.1:5672 with the
default guest/guest credentials.  They will fail, not skip, if the broker is
unreachable - consistent with the rest of the acceptance suite.

They exercise the bounded-queue paths added in #1651 against a real broker,
which unit tests with mocks cannot: a slow consumer overflowing the queue tears
the connection down with a WorkQueueFullError (Finding 1), close() honours its
timeout even with a wedged consumer (Finding 2), and a merely-slow consumer that
keeps pace does NOT trip overflow (the back-pressure negative control).
"""

import threading
import time
import unittest
import uuid

import pika
from pika.adapters.thread_safe_connection import ThreadSafeConnection
from pika.exceptions import WorkQueueFullError
from tests.misc.test_utils import retry_assertion

DEFAULT_PARAMS = pika.ConnectionParameters(
    host='127.0.0.1',
    port=5672,
    credentials=pika.PlainCredentials('guest', 'guest'),
)

# Timeout used for join() calls that assert a thread does not hang.
BLOCKING_CALL_TIMEOUT = 10


class ThreadSafeBoundedQueueTestCaseBase(unittest.TestCase):

    def _connect(self, **kwargs):
        conn = ThreadSafeConnection(DEFAULT_PARAMS, **kwargs)
        self.addCleanup(self._safe_close, conn)
        return conn

    @staticmethod
    def _safe_close(conn):
        try:
            conn.close(timeout=BLOCKING_CALL_TIMEOUT)
        except Exception:
            pass

    @staticmethod
    def _unique_queue():
        return f'tsbq-test-{uuid.uuid4().hex}'

    def _declare_and_fill(self, ch, queue, count):
        """Declare *queue* and publish *count* small messages into it."""
        ch.queue_declare(queue=queue, durable=False, exclusive=True)
        for i in range(count):
            ch.basic_publish(exchange='',
                             routing_key=queue,
                             body=f'msg-{i}'.encode())


class TestSlowConsumerOverflowClosesConnection(
        ThreadSafeBoundedQueueTestCaseBase):
    """
    A consumer that wedges lets the bounded queue overflow and tear the connection down with a
    WorkQueueFullError the application can catch by type.

    This is the Finding 1 regression test: before the bounded queue the IOLoop
    grew an unbounded backlog; before the exception-visibility fix (6a5f4a73) the
    overflow surfaced as an opaque StreamLostError instead of WorkQueueFullError.
    """

    def test(self):
        close_reason = []
        closed = threading.Event()

        def on_close(_conn, reason):
            close_reason.append(reason)
            closed.set()

        # maxsize=2 with a 1 s put timeout so overflow surfaces quickly once the
        # single worker wedges on its first delivery.
        conn = self._connect(on_close_callback=on_close,
                             work_queue_maxsize=2,
                             work_queue_put_timeout=1.0)
        ch = conn.channel()
        queue = self._unique_queue()
        self._declare_and_fill(ch, queue, count=50)

        release = threading.Event()
        self.addCleanup(release.set)

        def wedged_consumer(channel, method, properties, body):
            # Block the single worker so the queue backs up and overflows.
            release.wait(BLOCKING_CALL_TIMEOUT * 3)

        ch.basic_consume(queue=queue,
                         on_message_callback=wedged_consumer,
                         auto_ack=True)

        self.assertTrue(closed.wait(BLOCKING_CALL_TIMEOUT),
                        'connection did not close on queue overflow')
        self.assertIsInstance(
            close_reason[0], WorkQueueFullError,
            f'expected WorkQueueFullError, got {close_reason[0]!r}')
        self.assertTrue(conn.is_closed)


class TestCloseHonorsTimeoutWithWedgedConsumer(
        ThreadSafeBoundedQueueTestCaseBase):
    """
    Close() must return within its timeout even when a consumer callback is wedged, rather than
    hanging on the stuck worker.

    This is the Finding 2 regression test: before the bounded-join fix the IOLoop
    thread's pool-drain tail waited out the wedged worker forever, so close()
    never returned.
    """

    def test(self):
        conn = self._connect(work_queue_maxsize=2, work_queue_put_timeout=1.0)
        ch = conn.channel()
        queue = self._unique_queue()
        self._declare_and_fill(ch, queue, count=10)

        release = threading.Event()
        self.addCleanup(release.set)
        wedged = threading.Event()

        def wedged_consumer(channel, method, properties, body):
            wedged.set()
            release.wait(BLOCKING_CALL_TIMEOUT * 6)

        ch.basic_consume(queue=queue,
                         on_message_callback=wedged_consumer,
                         auto_ack=True)

        # Wait until the worker is actually wedged inside the callback.
        self.assertTrue(wedged.wait(BLOCKING_CALL_TIMEOUT),
                        'consumer never started')

        # close() must return promptly despite the wedged worker.  Give it a
        # short timeout and assert it returns within a generous multiple of that
        # (the force path may wait up to ~2x timeout) - well under the 6x
        # BLOCKING_CALL_TIMEOUT the callback would otherwise block for.
        close_timeout = 1.0
        done = threading.Event()

        def do_close():
            conn.close(timeout=close_timeout)
            done.set()

        closer = threading.Thread(target=do_close)
        closer.start()
        self.assertTrue(
            done.wait(BLOCKING_CALL_TIMEOUT),
            'close() did not honour its timeout with a wedged consumer')
        closer.join(timeout=BLOCKING_CALL_TIMEOUT)
        self.assertFalse(closer.is_alive())


class TestSlowConsumerKeepingPaceDoesNotOverflow(
        ThreadSafeBoundedQueueTestCaseBase):
    """
    A consumer that is slow but keeps pace must NOT trip overflow: the connection stays open and
    every message is consumed.

    This is the back-pressure negative control - it proves the bounded queue
    applies back-pressure to merely-slow consumers rather than tearing the
    connection down, so overflow is reserved for genuinely stuck callbacks.
    """

    def test(self):
        n = 20
        close_reason = []

        def on_close(_conn, reason):
            close_reason.append(reason)

        # A small queue plus a per-message delay guarantees the queue is under
        # sustained pressure, but each delay (0.05 s) stays well under the 5 s
        # put timeout so no put ever times out.
        conn = self._connect(on_close_callback=on_close,
                             work_queue_maxsize=2,
                             work_queue_put_timeout=5.0)
        ch = conn.channel()
        queue = self._unique_queue()
        self._declare_and_fill(ch, queue, count=n)

        received = []
        lock = threading.Lock()

        def slow_consumer(channel, method, properties, body):
            time.sleep(0.05)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            with lock:
                received.append(body)

        ch.basic_consume(queue=queue,
                         on_message_callback=slow_consumer,
                         auto_ack=False)

        @retry_assertion(timeout_sec=BLOCKING_CALL_TIMEOUT * 2)
        def assert_all_consumed():
            with lock:
                self.assertEqual(len(received), n)

        assert_all_consumed()

        self.assertEqual([], close_reason,
                         f'connection closed unexpectedly: {close_reason}')
        self.assertTrue(conn.is_open)


if __name__ == '__main__':
    unittest.main()
