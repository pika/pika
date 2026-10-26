"""
Acceptance tests for `max_inbound_message_body_size`.

These need a RabbitMQ broker listening on 127.0.0.1:5672 with the default guest/guest credentials. A
publisher on a connection with the default limit sends a body just above a consumer's small limit,
so the broker delivers it and the consumer's connection has to refuse it.
"""

import threading
import unittest
import uuid
import warnings

import pika
from pika import exceptions
from pika.adapters.thread_safe_connection import Connection
from tests.misc.test_utils import safe_close

DEFAULT_PARAMS = pika.ConnectionParameters(
    host='127.0.0.1',
    port=5672,
    credentials=pika.PlainCredentials('guest', 'guest'),
)

LIMIT = 1024
WAIT = 10


def _params(limit):
    return pika.ConnectionParameters(
        host=DEFAULT_PARAMS.host,
        port=DEFAULT_PARAMS.port,
        credentials=DEFAULT_PARAMS.credentials,
        max_inbound_message_body_size=limit,
    )


class InboundSizeLimitTestBase(unittest.TestCase):

    def setUp(self):
        self.queue = f'inbound-size-limit-{uuid.uuid4().hex}'
        self.publisher = Connection(DEFAULT_PARAMS)
        self.addCleanup(safe_close, self.publisher)
        ch = self.publisher.channel()
        ch.queue_declare(queue=self.queue, durable=True)
        self.addCleanup(self._delete_queue)

    def _delete_queue(self):
        conn = Connection(DEFAULT_PARAMS)
        try:
            conn.channel().queue_delete(queue=self.queue)
        finally:
            safe_close(conn)

    def _publish(self, size):
        ch = self.publisher.channel()
        ch.basic_publish(exchange='', routing_key=self.queue, body=b'x' * size)
        ch.close()


class TestThreadSafeConnection(InboundSizeLimitTestBase):

    def _consume(self, size):
        closed = threading.Event()
        reasons = []
        received = []

        def on_close(_conn, reason):
            reasons.append(reason)
            closed.set()

        consumer = Connection(_params(LIMIT), on_close_callback=on_close)
        self.addCleanup(safe_close, consumer)
        ch = consumer.channel()
        got = threading.Event()

        def on_message(_ch, _method, _props, body):
            received.append(body)
            got.set()

        ch.basic_consume(queue=self.queue,
                         on_message_callback=on_message,
                         auto_ack=True)
        self._publish(size)
        return consumer, closed, reasons, got, received

    def test_a_body_above_the_limit_closes_the_connection(self):
        consumer, closed, reasons, _got, received = self._consume(LIMIT + 1)

        self.assertTrue(closed.wait(WAIT), 'the consumer was not closed')
        self.assertEqual(1, len(reasons))
        self.assertIsInstance(reasons[0], exceptions.MessageBodyTooLargeError)
        self.assertEqual((LIMIT + 1, LIMIT), reasons[0].args)
        self.assertEqual([], received)
        self.assertTrue(consumer.is_closed)

    def test_a_body_at_the_limit_is_delivered(self):
        consumer, closed, _reasons, got, received = self._consume(LIMIT)

        self.assertTrue(got.wait(WAIT), 'the message was not delivered')
        self.assertEqual([b'x' * LIMIT], received)
        self.assertFalse(closed.is_set())
        self.assertTrue(consumer.is_open)


class TestBlockingConnection(InboundSizeLimitTestBase):

    def test_a_body_above_the_limit_raises_from_the_next_call(self):
        self._publish(LIMIT + 1)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            consumer = pika.BlockingConnection(_params(LIMIT))
        self.addCleanup(safe_close, consumer)
        ch = consumer.channel()

        with self.assertRaises(exceptions.MessageBodyTooLargeError) as raised:
            ch.basic_get(queue=self.queue, auto_ack=False)
        self.assertEqual((LIMIT + 1, LIMIT), raised.exception.args)


if __name__ == '__main__':
    unittest.main()
