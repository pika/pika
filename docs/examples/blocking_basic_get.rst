Using the Blocking Connection to get a message from RabbitMQ
============================================================

.. _example_blocking_basic_get:

The :py:meth:`BlockingChannel.basic_get <pika.adapters.blocking_connection.BlockingChannel.basic_get>`  method will return a tuple with the members.

If the server returns a message, the first item in the tuple will be a :py:class:`pika.spec.Basic.GetOk` object with the current message count, the redelivered flag, the routing key that was used to put the message in the queue, and the exchange the message was published to. The second item will be a :py:class:`pika.spec.BasicProperties` object and the third will be the message body.

If the server did not return a message a tuple of None, None, None will be returned.

Example of getting a message and acknowledging it::

        import pika

        connection = pika.BlockingConnection()
        channel = connection.channel()
        method_frame, header_frame, body = channel.basic_get('test')
        if method_frame:
            print method_frame, header_frame, body
            channel.basic_ack(method_frame.delivery_tag)
        else:
            print 'No message returned'
