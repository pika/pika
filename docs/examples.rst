Examples
========

The following basic examples are available in the `examples directory on github <https://github.com/tonyg/pika/tree/master/examples>`_:

Asynchronous Examples
---------------------

demo_get
^^^^^^^^
This example demonstrates how to use :py:meth:`channel.Channel.basic_get` and acknowledge the message using :py:meth`channel.Channel.basic_ack`::

    import logging
    import pika
    import sys

    from pika.adapters import SelectConnection

    logging.basicConfig(level=logging.INFO)

    connection = None
    channel = None


    def on_connected(connection):
        global channel
        logging.info("demo_get: Connected to RabbitMQ")
        connection.channel(on_channel_open)


    def on_channel_open(channel_):
        global channel
        logging.info("demo_get: Received our Channel")
        channel = channel_
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False,
                              callback=on_queue_declared)


    def on_queue_declared(frame):
        logging.info("demo_get: Queue Declared")
        connection.add_timeout(1, basic_get)


    def basic_get():
        logging.info("Invoking Basic.Get")
        channel.basic_get(callback=handle_delivery, queue="test")
        connection.add_timeout(1, basic_get)


    def handle_delivery(channel, method, header, body):
        logging.info("demo_get.handle_delivery")
        logging.info("method=%r" % method)
        logging.info("header=%r" % header)
        logging.info("  body=%r" % body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    if __name__ == '__main__':
        host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
        parameters = pika.ConnectionParameters(host)
        connection = SelectConnection(parameters, on_connected)
        try:
            connection.ioloop.start()
        except KeyboardInterrupt:
            connection.close()
            connection.ioloop.start()

demo_receive
^^^^^^^^^^^^
This example shows how to use :py:meth:`channel.Channel.basic_consume` to receive messages from RabbitMQ and then acknowledge them using :py:meth`channel.Channel.basic_ack`::

    import logging
    import sys
    import pika

    from pika.adapters import SelectConnection

    logging.basicConfig(level=logging.INFO)

    connection = None
    channel = None


    def on_connected(connection):
        global channel
        logging.info("demo_receive: Connected to RabbitMQ")
        connection.channel(on_channel_open)


    def on_channel_open(channel_):
        global channel
        channel = channel_
        logging.info("demo_receive: Received our Channel")
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False,
                              callback=on_queue_declared)


    def on_queue_declared(frame):
        logging.info("demo_receive: Queue Declared")
        channel.basic_consume(handle_delivery, queue='test')


    def handle_delivery(channel, method, header, body):
        logging.info("demo_receive.handle_delivery")
        logging.info("  method: %r" % method)
        logging.info("  header: %r" % header)
        logging.info("    body: %r" % body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    if __name__ == '__main__':
        host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
        parameters = pika.ConnectionParameters(host)
        connection = SelectConnection(parameters, on_connected)
        try:
            connection.ioloop.start()
        except KeyboardInterrupt:
            connection.close()
            connection.ioloop.start()

demo_send
^^^^^^^^^
This example shows how to use :py:meth:`channel.Channel.basic_deliver` to send a message to RabbitMQ::

    import logging
    import sys
    import pika
    import time

    from pika.adapters import SelectConnection

    logging.basicConfig(level=logging.INFO)

    connection = None
    channel = None


    def on_connected(connection):
        logging.info("demo_send: Connected to RabbitMQ")
        connection.channel(on_channel_open)


    def on_channel_open(channel_):
        global channel
        channel = channel_
        logging.info("demo_send: Received our Channel")
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False,
                              callback=on_queue_declared)


    def on_queue_declared(frame):
        logging.info("demo_send: Queue Declared")
        for x in xrange(0, 10):
            message = "Hello World #%i: %.8f" % (x, time.time())
            logging.info("Sending: %s" % message)
            channel.basic_publish(exchange='',
                                  routing_key="test",
                                  body=message,
                                  properties=pika.BasicProperties(
                                  content_type="text/plain",
                                  delivery_mode=2))  # persist

        # Close our connection
        connection.close()

    if __name__ == '__main__':
        host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
        parameters = pika.ConnectionParameters(host)
        connection = SelectConnection(parameters, on_connected)
        try:
            connection.ioloop.start()
        except KeyboardInterrupt:
            connection.close()
            connection.ioloop.start()

Blocking Examples
-----------------

demo_get
^^^^^^^^
This example demonstrates how to use :py:meth:`channel.Channel.basic_get` and acknowledge the message using :py:meth`channel.Channel.basic_ack` while using the :ref:`adapters_blocking_connection_BlockingConnection`::

    import logging
    import sys
    import pika
    import time

    from pika.adapters import BlockingConnection

    logging.basicConfig(level=logging.INFO)

    if __name__ == '__main__':
        # Connect to RabbitMQ
        host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
        parameters = pika.ConnectionParameters(host)
        connection = BlockingConnection(parameters)

        # Open the channel
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False)

        # Initialize our timers and loop until external influence stops us
        while connection.is_open:

            # Call basic get which returns the 3 frame types
            method_frame, header_frame, body = channel.basic_get(queue="test")

            # It can be empty if the queue is empty so don't do anything
            if method_frame.NAME == 'Basic.GetEmpty':
                logging.info("Empty Basic.Get Response (Basic.GetEmpty)")

            # We have data
            else:
                logging.info('Method: %s' % method_frame)
                logging.info('Header: %s' % header_frame)
                logging.info('  Body: %s' % body)

                # Acknowledge the receipt of the data
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            # No need to pound rabbit, sleep for a while. If you want messages as
            # fast as you can get them, use Basic.Consume
            time.sleep(1)

demo_receive
^^^^^^^^^^^^
This example shows how to use :py:meth:`channel.Channel.basic_consume` to receive messages from RabbitMQ and then acknowledge them using :py:meth`channel.Channel.basic_ack` while using the :ref:`adapters_blocking_connection_BlockingConnection`::

    import logging
    import pika
    import sys

    from pika.adapters import BlockingConnection

    logging.basicConfig(level=logging.INFO)


    def handle_delivery(channel, method, header, body):
        # Receive the data in 3 frames from RabbitMQ
        logging.info("demo_receive.handle_delivery")
        logging.info("  method: %r" % method)
        logging.info("  header: %r" % header)
        logging.info("    body: %r" % body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    if __name__ == '__main__':
        # Connect to RabbitMQ
        host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
        parameters = pika.ConnectionParameters(host)
        connection = BlockingConnection(parameters)

        # Open the channel
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False)

        # We're stuck looping here since this is a blocking adapter
        channel.basic_consume(handle_delivery, queue='test')

demo_send
^^^^^^^^^
This example shows how to use :py:meth:`channel.Channel.basic_deliver` to send a message to RabbitMQ while using the :ref:`adapters_blocking_connection_BlockingConnection`::

    import logging
    import pika
    import sys
    import time

    from pika.adapters import BlockingConnection

    logging.basicConfig(level=logging.INFO)


    if __name__ == '__main__':
        # Connect to RabbitMQ
        host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
        parameters = pika.ConnectionParameters(host)
        connection = BlockingConnection(parameters)

        # Open the channel
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False)

        # Initialize our timers and loop until external influence stops us
        count = 0
        start_time = time.time()
        while True:
            # Construct a message and send it
            message = "BlockingConnection.channel.basic_publish #%i" % count
            channel.basic_publish(exchange='',
                                  routing_key="test",
                                  body=message,
                                  properties=pika.BasicProperties(
                                  content_type="text/plain",
                                  delivery_mode=1))  # persistent
            count += 1
            if count % 1000 == 0:
                duration = time.time() - start_time
                logging.info("%i Messages Sent: %.8f per second" % (count,
                                                                    count / \
                                                                    duration))

For other examples including the use of Tornado, see the `examples directory on github <https://github.com/tonyg/pika/tree/master/examples>`_.
