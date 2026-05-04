import pika

# Open a connection to RabbitMQ on localhost using all default parameters
connection = pika.BlockingConnection()

# Open the channel
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False)

# Turn on delivery confirmations
channel.confirm_delivery()

# Send a message
try:
    channel.basic_publish(exchange='test',
                          routing_key='test',
                          body='Hello World!',
                          properties=pika.BasicProperties(content_type='text/plain',
                                                          delivery_mode=pika.DeliveryMode.Transient))
    print('Message publish was confirmed')
except pika.exceptions.UnroutableError:
    print('Message could not be confirmed')
