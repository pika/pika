import pika

connection = pika.BlockingConnection()
channel = connection.channel()

# Declare the queue we are about to read from, so the example is self-contained.
# `durable=True` is required by RabbitMQ 4.3; see examples/README.md.
channel.queue_declare(queue='test', durable=True)

# Drain anything a previous run left, so both branches below are reachable.
while channel.basic_get('test', auto_ack=True)[0] is not None:
    pass

# Put a single message there to get.
channel.basic_publish(exchange='', routing_key='test', body=b'Hello World!')


def get_one():
    """Get a message if there is one, which is the thing `basic_get` has to
    teach: it answers either a delivery or nothing at all."""
    method_frame, header_frame, body = channel.basic_get('test')
    if method_frame:
        print(method_frame, header_frame, body)
        channel.basic_ack(method_frame.delivery_tag)
    else:
        print('No message returned')


# The first call finds the message, the second finds the queue empty.
get_one()
get_one()
