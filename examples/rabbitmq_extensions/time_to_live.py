import pika


HOST = 'localhost'
USER = 'guest'
PASSWORD = 'guest'
VHOST = '/'
TTL = 100000  # milliseconds
MSG = "Test message for RabbitMQ"
QUEUE = 'ttl_queue'


class RabbitMQ():
    def __init__(self, exchange="messages", host='localhost',
                 user='guest', password='guest', virtual_host="/", queue=""):
        self.exchange = exchange
        self.virtual_host = virtual_host
        self.queue = queue
        self.credentials = pika.PlainCredentials(
            user, password)
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
        self.channel.queue_declare(
            queue=self.queue, durable=True, auto_delete=False)
        self.channel.queue_bind(exchange=self.exchange,
                                queue=self.queue, routing_key=self.queue)

    def send_msg(self, msg, ttl=0):
        properties = pika.BasicProperties(delivery_mode=2, expiration=str(ttl))
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.queue,
            body=msg,
            properties=properties)


if __name__ == '__main__':
    rmq_connection = RabbitMQ(
        host=HOST,
        user=USER,
        password=PASSWORD,
        virtual_host=VHOST,
        queue=QUEUE)
    rmq_connection.send_msg(msg=MSG, ttl=TTL)
