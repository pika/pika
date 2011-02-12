Examples
========

The following basic examples are available in the `examples directory on github <https://github.com/tonyg/pika/tree/master/examples>`_. For additional examples including the use of Tornado, see the `examples directory on github <https://github.com/tonyg/pika/tree/master/examples>`_.

Asynchronous Examples
---------------------

demo_get
^^^^^^^^
This example demonstrates how to use :py:meth:`Channel.basic_get() <channel.Channel.basic_get>` and acknowledge the message using :py:meth:`Channel.basic_ack() <channel.Channel.basic_ack>`::

    import sys
    import pika
    import time

    pika.log.setup(color=True)

    connection = None
    channel = None

    # Import all adapters for easier experimentation
    from pika.adapters import *


    def on_connected(connection):
        pika.log.info("demo_send: Connected to RabbitMQ")
        connection.channel(on_channel_open)


    def on_channel_open(channel_):
        global channel
        channel = channel_
        pika.log.info("demo_send: Received our Channel")
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False,
                              callback=on_queue_declared)


    def on_queue_declared(frame):
        pika.log.info("demo_send: Queue Declared")
        for x in xrange(0, 10):
            message = "Hello World #%i: %.8f" % (x, time.time())
            pika.log.info("Sending: %s" % message)
            channel.basic_publish(exchange='',
                                  routing_key="test",
                                  body=message,
                                  properties=pika.BasicProperties(
                                  content_type="text/plain",
                                  delivery_mode=1))

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

demo_receive
^^^^^^^^^^^^
This example shows how to use :py:meth:`Channel.basic_consume() <channel.Channel.basic_consume>` to receive messages from RabbitMQ and then acknowledge them using :py:meth:`Channel.basic_ack() <channel.Channel.basic_ack>`::

    import sys
    import pika

    # Import all adapters for easier experimentation
    from pika.adapters import *

    pika.log.setup(pika.log.DEBUG, color=True)

    connection = None
    channel = None


    def on_connected(connection):
        global channel
        pika.log.info("demo_receive: Connected to RabbitMQ")
        connection.channel(on_channel_open)


    def on_channel_open(channel_):
        global channel
        channel = channel_
        pika.log.info("demo_receive: Received our Channel")
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False,
                              callback=on_queue_declared)


    def on_queue_declared(frame):
        pika.log.info("demo_receive: Queue Declared")
        channel.basic_consume(handle_delivery, queue='test')


    def handle_delivery(channel, method_frame, header_frame, body):
        pika.log.info("Basic.Deliver %s delivery-tag %i: %s",
                      header_frame.content_type,
                      method_frame.delivery_tag,
                      body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

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
This example shows how to use :py:meth:`Channel.basic_publish() <channel.Channel.basic_publish>` to send a message to RabbitMQ::

    import sys
    import pika
    import time

    pika.log.setup(color=True)

    connection = None
    channel = None

    # Import all adapters for easier experimentation
    from pika.adapters import *


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
                                      delivery_mode=1))

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
This example demonstrates how to use :py:meth:`BlockingChannel.basic_get() <blocking_connection.BlockingChannel.basic_get>` and acknowledge the message using :py:meth:`BlockingChannel.basic_ack() <blocking_connection.BlockingChannel.basic_ack>` while using the :ref:`adapters_blocking_connection_BlockingConnection`::

    import sys
    import pika
    import time

    from pika.adapters import BlockingConnection

    pika.log.setup(color=True)

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
                pika.log.info("Empty Basic.Get Response (Basic.GetEmpty)")

            # We have data
            else:
                pika.log.info("Basic.GetOk %s delivery-tag %i: %s",
                              header_frame.content_type,
                              method_frame.delivery_tag,
                              body)

                # Acknowledge the receipt of the data
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            # No need to pound rabbit, sleep for a while. If you want messages as
            # fast as you can get them, use Basic.Consume
            time.sleep(1)

demo_receive
^^^^^^^^^^^^
This example shows how to use :py:meth:`BlockingChannel.basic_consume() <blocking_connection.BlockingChannel.basic_consume>` to receive messages from RabbitMQ and then acknowledge them using :py:meth:`BlockingChannel.basic_ack() <blocking_connection.BlockingChannel.basic_ack>` while using the :ref:`adapters_blocking_connection_BlockingConnection`::

    import pika
    import sys

    from pika.adapters import BlockingConnection

    pika.log.setup(color=True)


    def handle_delivery(channel, method_frame, header_frame, body):
        # Receive the data in 3 frames from RabbitMQ
        pika.log.info("Basic.Deliver %s delivery-tag %i: %s",
                      header_frame.content_type,
                      method_frame.delivery_tag,
                      body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

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
This example shows how to use :py:meth:`BlockingChannel.basic_publish() <blocking_connection.BlockingChannel.basic_publish>` to send a message to RabbitMQ while using the :ref:`adapters_blocking_connection_BlockingConnection`::

    import pika
    import sys
    import time

    from pika.adapters import BlockingConnection

    pika.log.setup(color=True)

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
                                  delivery_mode=1))
            count += 1
            if count % 1000 == 0:
                duration = time.time() - start_time
                pika.log.info("%i Messages Sent: %.8f per second",
                              count, count / duration)

