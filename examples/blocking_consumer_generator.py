import pika

connection = pika.BlockingConnection()
channel = connection.channel()

# Declare the queue we are about to consume from, so the example is
# self-contained. `durable=True` is required by RabbitMQ 4.3; see
# examples/README.md.
channel.queue_declare(queue='test', durable=True)

# Publish the ten messages the loop below expects, so it terminates.
for number in range(10):
    channel.basic_publish(exchange='',
                          routing_key='test',
                          body=f'Hello World {number}!'.encode())

# Get ten messages and break out
for method_frame, properties, body in channel.consume('test'):

    # Display the message parts
    print(method_frame)
    print(properties)
    print(body)

    # Acknowledge the message
    channel.basic_ack(method_frame.delivery_tag)

    # Escape out of the loop after 10 messages
    if method_frame.delivery_tag == 10:
        break

# Cancel the consumer and return any pending messages
requeued_messages = channel.cancel()
print(f'Requeued {requeued_messages} messages')

# Close the channel and the connection
channel.close()
connection.close()
