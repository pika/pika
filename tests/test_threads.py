import unittest
import pika

import thread

class TestThreads(unittest.TestCase):
    def test_threads(self):
        def fun():
            conn_a = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
            ch_a = conn_a.channel()
            ch_a.queue_declare(queue="test", durable=True, \
                                exclusive=False, auto_delete=False)
            ch_a.basic_publish(exchange='', routing_key="test", body="Hello World!")
            ch_a.basic_get(queue="test", no_ack=True)
            conn_a.close()

        for i in range(3): # works with the value of 1.
            thread.start_new_thread(fun, ())


if __name__ == '__main__':
    import run
    run.run_unittests(globals())
