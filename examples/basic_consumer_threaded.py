"""Example: consuming with ThreadSafeConnection.

ThreadSafeChannel dispatches each delivery on a dedicated per-channel worker
thread, not the IOLoop thread.  Long-running work in the callback no longer
stalls heartbeats: the IOLoop keeps running on its own thread while the
worker is busy.  Deliveries on a single channel are processed serially on
that worker thread, so prefetch controls how many unacked messages the
broker will hand out, not the worker thread count.

Calls to ThreadSafeChannel methods (basic_ack, basic_publish, etc.) are
safe from any thread, including the worker thread that delivers the
message - so there is no need to bounce the ack through
add_callback_threadsafe as the BlockingConnection-based example required.

Pairs with examples/basic_publisher_threaded.py.  Run this consumer first.
"""
import logging
import random
import threading
import time

import pika
from pika.adapters.thread_safe_connection import ThreadSafeConnection
from pika.exceptions import ConnectionClosed
from pika.exchange_type import ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

EXCHANGE_NAME = 'test_exchange'
QUEUE_NAME = 'standard'
ROUTING_KEY = 'standard_key'


def on_message(ch, method_frame, _header_frame, body):
    thread_id = threading.get_ident()
    LOGGER.info('Worker thread %s received delivery_tag=%s body=%r', thread_id,
                method_frame.delivery_tag, body)
    # Simulate slow work; heartbeats keep flowing on the IOLoop thread,
    # so the broker does not close the connection.
    time.sleep(random.randint(1, 10))

    try:
        ch.basic_ack(delivery_tag=method_frame.delivery_tag)
        LOGGER.info('Acked delivery_tag=%s', method_frame.delivery_tag)
    except ConnectionClosed:
        # Connection closed (likely Ctrl-C) while this worker was sleeping.
        # The broker will redeliver this un-acked message to the next
        # consumer, so dropping it here is safe.
        LOGGER.info('Skipping ack for delivery_tag=%s: connection closed',
                    method_frame.delivery_tag)


def main():
    # Heartbeat is short to demonstrate that it survives long-running
    # work on the per-channel worker thread.
    parameters = pika.ConnectionParameters(
        credentials=pika.PlainCredentials('guest', 'guest'),
        heartbeat=5,
    )

    LOGGER.info('Connecting ...')
    conn = ThreadSafeConnection(parameters)
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE_NAME,
                        exchange_type=ExchangeType.direct,
                        durable=True)
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.queue_bind(queue=QUEUE_NAME,
                  exchange=EXCHANGE_NAME,
                  routing_key=ROUTING_KEY)

    # prefetch=1 keeps the in-flight count small while we demonstrate
    # serial processing on the worker thread.  Increase it to let the
    # broker deliver multiple messages ahead of the worker; the worker
    # still processes them one at a time.
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    LOGGER.info('Consuming from %s.  Ctrl-C to stop.', QUEUE_NAME)
    try:
        # The IOLoop thread is already running in the background.
        # Block here until the user interrupts.
        threading.Event().wait()
    except KeyboardInterrupt:
        LOGGER.info('Interrupted, closing connection')
        # close() blocks until the IOLoop thread has shut down the
        # per-channel worker.  An in-flight callback that calls
        # basic_ack after this point will see ConnectionClosedByClient
        # and is expected to handle it (see on_message above).
        conn.close()


if __name__ == '__main__':
    main()
