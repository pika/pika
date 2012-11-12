Using the Blocking Connection to consume messages from RabbitMQ
===============================================================

.. _example_blocking_basic_get:

The :py:meth:`BlockingChannel.basic_consume` method assign a callback method to be called every time that RabbitMQ delivers messages to your consuming application.

If the server return a message, the first item in the tuple will be a :py:meth:`pika.spec.Basic.Deliver` object with the current message count, the redelivered flag, the routing key that was used to put the message in the queue, and the exchange the message was published to. The second item will be a :py:meth:`pika.spec.Basic.Properties` object and the third will be the message body.

Example of consuming messages and acknowledging them::

        import pika


        def on_message(channel, method_frame, header_frame, body):
            print method_frame.delivery_tag
            print body
            print
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)


        connection = pika.BlockingConnection()
        channel = connection.channel()
        channel.basic_consume(on_message, 'test')
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        connection.close()
