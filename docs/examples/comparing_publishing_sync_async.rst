Comparing Message Publishing with BlockingConnection and SelectConnection
=========================================================================

For those doing simple, non-asynchronous programing, :py:meth:`pika.adapters.blocking_connection.BlockingConnection` proves to be the easiest way to get up and running with Pika to publish messages.

In the following example, a connection is made to RabbitMQ listening to port *5672* on *localhost* using the username *guest* and password *guest* and virtual host */*. Once connected, a channel is opened and a message is published to the *test_exchange* exchange using the *test_routing_key* routing key. The BasicProperties value passed in sets the message to delivery mode *1* (non-persisted) with a content-type of *text/plain*. Once the message is published, the connection is closed::

  import pika

  parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')

  connection = pika.BlockingConnection(parameters)

  channel = connection.channel()

  channel.basic_publish('test_exchange',
                        'test_routing_key',
                        'message body value',
                        pika.BasicProperties(content_type='text/plain',
                                             delivery_mode=1))

  connection.close()


In contrast, using :py:meth:`pika.adapters.select_connection.SelectConnection` and the other asynchronous adapters is more complicated and less pythonic, but when used with other asynchronous services can have tremendous performance improvements. In the following code example, all of the same parameters and values are used as were used in the previous example::

    import pika

    # Step #3
    def on_open(connection):

        connection.channel(on_channel_open)

    # Step #4
    def on_channel_open(channel):

        channel.basic_publish('test_exchange',
                                'test_routing_key',
                                'message body value',
                                pika.BasicProperties(content_type='text/plain',
                                                     delivery_mode=1))

        connection.close()

    # Step #1: Connect to RabbitMQ
    parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')

    connection = pika.SelectConnection(parameters=parameters,
                                       on_open_callback=on_open)

    try:

        # Step #2 - Block on the IOLoop
        connection.ioloop.start()

    # Catch a Keyboard Interrupt to make sure that the connection is closed cleanly
    except KeyboardInterrupt:

        # Gracefully close the connection
        connection.close()

        # Start the IOLoop again so Pika can communicate, it will stop on its own when the connection is closed
        connection.ioloop.start()

