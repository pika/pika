import pika

msg = "TestTTL"

HOST = 'localhost'
USER = 'guest'
PASSWORD = 'guest'
VHOST = '/'
TTL = 10000  # milliseconds
DEAD_LETTER_QUEUE = 'dead_letter'
TTL_QUEUE = 'dl_ttl_queue'


class RabbitMQ():
    def __init__(
            self,
            exchange='messages',
            host='localhost',
            user='guest',
            password='guest',
            virtual_host='/'):
        self.exchange = exchange
        self.virtual_host = virtual_host
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
        self._queues_declare()

    def _queues_declare(self):
        self.channel.queue_declare(
            DEAD_LETTER_QUEUE, durable=True, auto_delete=False)
        self.channel.queue_declare(
            queue=TTL_QUEUE, durable=True, auto_delete=False, arguments={
                'x-dead-letter-exchange': 'messages',
                'x-dead-letter-routing-key': DEAD_LETTER_QUEUE, })
        for queue in [TTL_QUEUE, DEAD_LETTER_QUEUE]:
            self.channel.queue_bind(exchange=self.exchange,
                                    queue=queue, routing_key=queue)

    def send_msg(self, msg, queue, ttl=0):
        properties = pika.BasicProperties(delivery_mode=2, expiration=str(ttl))
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=queue,
            body=msg,
            properties=properties)


if __name__ == '__main__':
    rmq_connection = RabbitMQ(
        host=HOST, user=USER, password=PASSWORD, virtual_host=VHOST)
    rmq_connection.send_msg(msg, TTL_QUEUE, ttl=TTL)
