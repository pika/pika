"""Example: publishing from multiple threads using ThreadSafeConnection.

ThreadSafeConnection runs SelectConnection's IOLoop in a dedicated background
thread.  Every call to channel.basic_publish() is routed through
add_callback_threadsafe so that _tx_buffers is only ever touched from the
IOLoop thread, eliminating the IndexError race seen in issues #1144 and #511.

Pairs with examples/basic_consumer_threaded.py.  Run the consumer first.
"""
import logging
import threading

import pika
from pika.adapters.thread_safe_connection import ThreadSafeConnection
from pika.exchange_type import ExchangeType

LOG_FORMAT = ("%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
              "-35s %(lineno) -5d: %(message)s")
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

EXCHANGE_NAME = 'test_exchange'
QUEUE_NAME = 'standard'
ROUTING_KEY = 'standard_key'


def main():
    parameters = pika.ConnectionParameters(
        credentials=pika.PlainCredentials('guest', 'guest'))

    LOGGER.info('Connecting ...')
    conn = ThreadSafeConnection(parameters)

    # channel() blocks until the channel is open.
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE_NAME,
                        exchange_type=ExchangeType.direct,
                        durable=True)
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.queue_bind(queue=QUEUE_NAME,
                  exchange=EXCHANGE_NAME,
                  routing_key=ROUTING_KEY)
    LOGGER.info('Topology declared: %s -> %s -> %s', EXCHANGE_NAME, ROUTING_KEY,
                QUEUE_NAME)

    # Publish from 5 threads simultaneously - safe with ThreadSafeConnection.
    n_threads = 5
    barrier = threading.Barrier(n_threads)

    def publish(i):
        barrier.wait()  # all threads start at the same time
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=f'message {i}'.encode(),
            properties=pika.BasicProperties(
                content_type='text/plain',
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        LOGGER.info('Thread %d scheduled publish', i)

    threads = [
        threading.Thread(target=publish, args=(i,))
        for i in range(1, n_threads + 1)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    LOGGER.info('All publishes scheduled, closing connection')
    conn.close()
    LOGGER.info('Done')


if __name__ == '__main__':
    main()
