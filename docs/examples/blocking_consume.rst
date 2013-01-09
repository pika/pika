Using the Blocking Connection to consume messages from RabbitMQ
===============================================================

.. _example_blocking_basic_consume:

The :py:meth:`BlockingChannel.basic_consume <pika.adapters.blocking_connection.BlockingChannel.basic_consume>`  method assign a callback method to be called every time that RabbitMQ delivers messages to your consuming application.

When pika calls your method, it will pass in the channel, a :py:class:`pika.spec.Basic.Deliver` object with the delivery tag, the redelivered flag, the routing key that was used to put the message in the queue, and the exchange the message was published to. The third argument will be a :py:class:`pika.spec.BasicProperties` object and the last will be the message body.

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
