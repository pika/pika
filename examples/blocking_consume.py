import pika


def on_message(channel, method_frame, header_frame, body):
    print(method_frame.delivery_tag)
    print(body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


connection = pika.BlockingConnection()
channel = connection.channel()

# Declare the queue we are about to consume from, so the example is
# self-contained. `durable=True` is required by RabbitMQ 4.3; see
# examples/README.md.
channel.queue_declare(queue='test', durable=True)

# Publish one message so there is something for `on_message` to receive.
channel.basic_publish(exchange='', routing_key='test', body=b'Hello World!')

channel.basic_consume('test', on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
