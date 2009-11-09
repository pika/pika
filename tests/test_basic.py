import unittest
import pika


class TestConcurrency(unittest.TestCase):
    def test_publish_after_consume(self):
        conn_a = pika.AsyncoreConnection(pika.ConnectionParameters('127.0.0.1'))
        ch_a = conn_a.channel()
        ch_a.queue_declare(queue="test", durable=True, \
                            exclusive=False, auto_delete=False)
        
        def handle_delivery(_channel, method, header, body):
            ch_a.basic_ack(delivery_tag = method.delivery_tag)
            conn_a.close()

        tag = ch_a.basic_consume(handle_delivery, queue='test')

        ch_a.basic_publish(exchange='', routing_key="test", body="Hello World!",)
        pika.asyncore_loop()

if __name__ == '__main__':
    import run
    run.run_unittests(globals())
