Ensuring message delivery with the mandatory flag
=================================================

The following example demonstrates how to check if a message is delivered by setting the mandatory flag and checking the return result when using the BlockingConnection::

    import pika

    # Open a connection to RabbitMQ on localhost using all default parameters
    connection = pika.BlockingConnection()

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False)

    # Send a message
    if channel.basic_publish(exchange='test',
                             routing_key='test',
                             body='Hello World!',
                             properties=pika.BasicProperties(content_type='text/plain',
                                                             delivery_mode=1),
                             mandatory=True):
        print 'Message was published'
    else:
        print 'Message was returned'
