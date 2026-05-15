"""Example: publishing from multiple threads using ThreadSafeConnection.

ThreadSafeConnection runs SelectConnection's IOLoop in a dedicated background
thread.  Every call to channel.basic_publish() is routed through
add_callback_threadsafe so that _tx_buffers is only ever touched from the
IOLoop thread, eliminating the IndexError race seen in issues #1144 and #511.
"""
import logging
import pprint
import queue
import threading

import pika
from pika.adapters.thread_safe_connection import ThreadSafeConnection

LOG_FORMAT = ("%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
              "-35s %(lineno) -5d: %(message)s")
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

QUEUE_NAME = 'my_queue'


def main():
    parameters = pika.ConnectionParameters(
        credentials=pika.PlainCredentials('guest', 'guest'))

    LOGGER.info('Connecting ...')
    conn = ThreadSafeConnection(parameters)

    # Open a channel and declare the queue from the calling thread.
    # channel() blocks until the channel is open.
    ch = conn.channel()

    # queue_declare must run in the IOLoop thread; use add_callback_threadsafe
    # and a Queue to get the result back.
    result_q = queue.Queue(1)

    def _declare():

        def _on_declare(method_frame):
            result_q.put(method_frame.method)

        ch._channel.queue_declare(
            queue=QUEUE_NAME,
            durable=True,
            callback=_on_declare,
        )

    conn.add_callback_threadsafe(_declare)
    declare_result = result_q.get()
    LOGGER.info('Queue declared: %s', pprint.pformat(declare_result))

    # Publish from 5 threads simultaneously — safe with ThreadSafeConnection.
    n_threads = 5
    barrier = threading.Barrier(n_threads)

    def publish(i):
        barrier.wait()  # all threads start at the same time
        ch.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
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

    LOGGER.info('All publishes scheduled — closing connection')
    conn.close()
    LOGGER.info('Done')


if __name__ == '__main__':
    main()
