Using Delivery Confirmations with the BlockingConnection
========================================================

The following code demonstrates how to turn on delivery confirmations with the BlockingConnection and how to check for confirmation from RabbitMQ::

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
    if channel.basic_publish(exchange='test',
                             routing_key='test',
                             body='Hello World!',
                             properties=pika.BasicProperties(content_type='text/plain',
                                                             delivery_mode=1)):
        print 'Message publish was confirmed'
    else:
        print 'Message could not be confirmed'
