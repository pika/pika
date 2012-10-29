Connecting to RabbitMQ with Callback-Passing Style
==================================================

When you connect to RabbitMQ with an asynchronous adapter, you are writing event
oriented code. The connection adapter will block on the IOLoop that is watching
to see when pika should read data from and write data to RabbitMQ. Because you're
now blocking on the IOLoop, you will receive callback notifications when specific
events happen.

Example Code
------------
In the example, there are three steps that take place:

1. Setup the connection to RabbitMQ
2. Start the IOLoop
3. Once connected, the on_open method will be called by Pika with a handle to
   the connection. In this method, a new channel will be opened on the connection.
4. Once the channel is opened, you can do your other actions, whether they be
   publishing messages, consuming messages or other RabbitMQ related activities.::

    import pika

    # Step #3
    def on_open(connection):
        connection.channel(on_channel_open)

    # Step #4
    def on_channel_open(channel):
        channel.basic_publish('exchange_name',
                              'routing_key',
                              'Test Message',
                              pika.BasicProperties(content_type='text/plain',
                                                   type='example'))

    # Step #1: Connect to RabbitMQ
    connection = pika.SelectConnection(on_open_callback=on_open)

    try:
        # Step #2 - Block on the IOLoop
        connection.ioloop.start()

    # Catch a Keyboard Interrupt to make sure that the connection is closed cleanly
    except KeyboardInterrupt:

        # Gracefully close the connection
        connection.close()

        # Start the IOLoop again so Pika can communicate, it will stop on its own when the connection is closed
        connection.ioloop.start()
