import pika

parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')

connection = pika.BlockingConnection(parameters)

channel = connection.channel()

channel.basic_publish(
    'test_exchange', 'test_routing_key', 'message body value',
    pika.BasicProperties(content_type='text/plain',
                         delivery_mode=pika.DeliveryMode.Transient))

connection.close()
