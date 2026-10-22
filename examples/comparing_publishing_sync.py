import pika
from pika.exchange_type import ExchangeType

parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')

connection = pika.BlockingConnection(parameters)

channel = connection.channel()

# Declare the exchange we publish to. Without this the broker answers the
# publish with a 404 and closes the channel, which a publish without delivery
# confirmations never waits around to notice.
channel.exchange_declare(exchange='test_exchange',
                         exchange_type=ExchangeType.direct,
                         durable=True)

channel.basic_publish(
    'test_exchange', 'test_routing_key', b'message body value',
    pika.BasicProperties(content_type='text/plain',
                         delivery_mode=pika.DeliveryMode.Transient))

connection.close()
