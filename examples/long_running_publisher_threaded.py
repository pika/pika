"""
Example: a long-running publisher using ThreadSafeConnection.

This is the ThreadSafeConnection counterpart to
examples/long_running_publisher.py.

The BlockingConnection version has to run its own background thread calling
process_data_events() so that heartbeats keep flowing while the application
is idle between publishes, and it must bounce every publish onto that thread
via add_callback_threadsafe.  That is the classic "heartbeat vs. work"
tension of a single-threaded design: nothing happens on the connection
unless the application keeps handing control back to pika.

ThreadSafeConnection removes the tension entirely.  Its SelectConnection
IOLoop runs on its own dedicated background thread, so heartbeats are sent on
time no matter how long the main thread sleeps between publishes.  The main
thread just calls basic_publish whenever it has something to send - the call
is fire-and-forget and safe from any thread.
"""

import logging
import time

import pika
from pika.adapters.thread_safe_connection import ThreadSafeConnection

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

QUEUE_NAME = 'downstream_queue'


def main():
    # A short heartbeat shows that the IOLoop thread keeps the connection
    # alive even while the main thread spends most of its time sleeping -
    # far longer than the heartbeat interval.
    parameters = pika.ConnectionParameters(
        credentials=pika.PlainCredentials('guest', 'guest'),
        heartbeat=5,
    )

    LOGGER.info('Connecting ...')
    conn = ThreadSafeConnection(parameters)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)

    LOGGER.info('Publishing once every 10s.  Ctrl-C to stop.')
    try:
        i = 0
        while True:
            i += 1
            message = f'Message {i}'
            LOGGER.info('Publishing: %r', message)
            # Fire-and-forget; no need to hand control back to pika to keep
            # the connection alive - the IOLoop thread does that for us.
            ch.basic_publish(exchange='',
                             routing_key=QUEUE_NAME,
                             body=message.encode())
            # The main thread can sleep (or do slow CPU-bound work) for far
            # longer than the heartbeat interval without dropping the
            # connection.
            time.sleep(10)
    except KeyboardInterrupt:
        LOGGER.info('Interrupted, closing connection')
    finally:
        conn.close()
        LOGGER.info('Done')


if __name__ == '__main__':
    main()
