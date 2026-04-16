# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import threading
import pika
import queue
import pprint

LOG_FORMAT = ("%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
              "-35s %(lineno) -5d: %(message)s")
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)


class ExampleThreadSafePublisher(object):
    """This is an example of a publisher that uses
    a dedicated connection thread and the
    `add_callback_threadsafe` method to safely publish
    from multiple threads, without requiring a separate
    pika connection per thread.

    """

    QUEUE_NAME = "my_queue"
    ROUTING_KEY = "my_queue"

    def __init__(self, connection_parameters: pika.ConnectionParameters):
        self._connection_parameters = connection_parameters
        self._connected_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread = None
        self._connection = None
        self._channel = None

    def __enter__(self):
        LOGGER.info("Creating new thread for rabbitmq connection")
        self._thread = threading.Thread(target=self._rabbitmq_thread)
        self._thread.start()
        self._connected_event.wait()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        LOGGER.info("Stopping connection thread")
        self._stop_event.set()
        self._thread.join()
        LOGGER.info("Connection thread stopped")

    def _rabbitmq_thread(self):
        self._connection = pika.BlockingConnection(self._connection_parameters)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.QUEUE_NAME, durable=True)
        self._connected_event.set()
        while not self._stop_event.is_set():
            self._connection.process_data_events(time_limit=1)
        self._channel.close()
        self._connection.close()

    def publish(self, msg: str):
        """Thread safe publish method. Blocks until the message
        is actually published on the connection thread.

        NOTE: blocking can be removed if confirmation is not needed,
        making the method a fire-and-forget.
        """
        rq = queue.Queue(1)
        cb = functools.partial(self._publish, rq, msg)
        self._connection.add_callback_threadsafe(cb)
        rq.get()

    def _publish(self, rq: queue.Queue, msg: str):
        p = pika.BasicProperties(content_type="text/plain",
                                 delivery_mode=pika.DeliveryMode.Persistent)
        self._channel.basic_publish(
            exchange="",
            routing_key=self.ROUTING_KEY,
            body=msg,
            properties=p,
            mandatory=True,
        )
        rq.put(True)

    def queue_declare(self, queue_name: str, passive: bool):
        """Thread safe queue declare. Blocks the calling thread
        until the result is returned from the connection thread.
        """
        rq = queue.Queue(1)
        cb = functools.partial(self._queue_declare,
                               rq,
                               queue_name=queue_name,
                               passive=passive)
        self._connection.add_callback_threadsafe(cb)
        return rq.get()

    def _queue_declare(self,
                       rq: queue.Queue,
                       queue_name: str,
                       passive: bool = False):
        result = self._channel.queue_declare(
            queue=queue_name,
            passive=passive,
            durable=True,
            auto_delete=False,
            exclusive=False,
        )
        rv = (
            result.method.queue,
            result.method.consumer_count,
            result.method.message_count,
        )
        rq.put(rv)


if __name__ == "__main__":
    credentials = pika.PlainCredentials("guest", "guest")
    connection_parameters = pika.ConnectionParameters(credentials=credentials)

    with ExampleThreadSafePublisher(connection_parameters) as publisher:
        threads: list[threading.Thread] = []

        # Publish from 5 threads to demonstrate thread safety
        for i in range(1, 6):
            thread = threading.Thread(target=publisher.publish,
                                      args=(f"message {i}",))
            threads.append(thread)
            thread.start()

        LOGGER.info("Waiting for publish threads to finish")
        for thread in threads:
            thread.join()

        # Checks that all 5 messages were published (message_count)
        rv = publisher.queue_declare(queue_name=publisher.QUEUE_NAME,
                                     passive=True)
        LOGGER.info("Result: %r", pprint.pformat(rv))

    LOGGER.info("Exiting")
