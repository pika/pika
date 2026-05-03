Using the Blocking Connection to consume messages from RabbitMQ
===============================================================

.. _example_blocking_basic_consume:

The :py:meth:`BlockingChannel.basic_consume <pika.adapters.blocking_connection.BlockingChannel.basic_consume>`  method assign a callback method to be called every time that RabbitMQ delivers messages to your consuming application.

When pika calls your method, it will pass in the channel, a :py:class:`pika.spec.Basic.Deliver` object with the delivery tag, the redelivered flag, the routing key that was used to put the message in the queue, and the exchange the message was published to. The third argument will be a :py:class:`pika.spec.BasicProperties` object and the last will be the message body.

Example of consuming messages and acknowledging them::

    import pika


    def on_message(channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.basic_consume('test', on_message)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()

Example of using more connection parameters. This example will connect using TLS, but not verify hostname and not a client provided certificate. It will set the heartbeat to 150 seconds and use a maximum of 10 channels. It will use a specific vhost, username and password. It will set the client provided connection_name for easy identification::

    import pika, os, ssl

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode=False
    
    parameters = pika.ConnectionParameters(
        host="serverhostname.com",
        port=5671,
        heartbeat=150,
        ssl_options=pika.SSLOptions(context),
        virtual_host="rwgvqgbl",
        channel_max=10,
        credentials=pika.PlainCredentials('rwgvqgbl', 'password'),
        client_properties={'connection_name':'Pika connection with TLS, channel_max'})

    def on_message(channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)
        print()
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='test')
    channel.basic_consume('test', on_message)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()
