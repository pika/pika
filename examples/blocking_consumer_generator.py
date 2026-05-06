import pika

connection = pika.BlockingConnection()
channel = connection.channel()

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
print('Requeued %i messages' % requeued_messages)

# Close the channel and the connection
channel.close()
connection.close()
