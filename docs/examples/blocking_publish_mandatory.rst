Ensuring message delivery with the mandatory flag
=================================================

The following example demonstrates how to check if a message is delivered by setting the mandatory flag and handling exceptions when using the BlockingConnection::

    import pika
    import pika.exceptions

    # Open a connection to RabbitMQ on localhost using all default parameters
    connection = pika.BlockingConnection()

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False)

    # Enabled delivery confirmations. This is REQUIRED.
    channel.confirm_delivery()

    # Send a message
    try:
        channel.basic_publish(exchange='test',
                              routing_key='test',
                              body='Hello World!',
                              properties=pika.BasicProperties(content_type='text/plain',
                                                              delivery_mode=1),
                              mandatory=True)
        print('Message was published')
    except pika.exceptions.UnroutableError:
        print('Message was returned')
