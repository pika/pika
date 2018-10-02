import pika
from threading import Thread

HOST = 'localhost'
USER = 'guest'
PASSWORD = 'guest'
VHOST = '/'
MSG = "Test message for RabbitMQ"
QUEUE = 'accessed_queue'


class RabbitMQ():

    def __init__(self,
                 exchange='messages',
                 host='localhost',
                 user='guest',
                 password='guest',
                 virtual_host='/'):
        self.exchange = exchange
        self.virtual_host = virtual_host
        self.credentials = pika.PlainCredentials(user, password)
        self.parameters = pika.ConnectionParameters(
            host=host, virtual_host=virtual_host, credentials=self.credentials)
        self.rmq_connect()

    def close(self):
        self.connection.close()

    def rmq_connect(self):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='direct',
            durable=True,
            auto_delete=False)
        self._queues_declare()

    def _queues_declare(self):
        self.channel.queue_declare(QUEUE, durable=True, auto_delete=False)
        self.channel.queue_bind(
            exchange=self.exchange, queue=QUEUE, routing_key=QUEUE)

    def send_msg(self, msg, queue):
        properties = pika.BasicProperties(
            delivery_mode=2, user_id="guest", app_id="validate-user")
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=queue,
            body=msg,
            properties=properties)


def producer():
    rmq_connection = RabbitMQ(
        host=HOST, user=USER, password=PASSWORD, virtual_host=VHOST)
    rmq_connection.send_msg(MSG, QUEUE)


if __name__ == '__main__':
    Thread(target=producer).start()