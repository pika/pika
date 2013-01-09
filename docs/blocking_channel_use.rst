Using the BlockingChannel
=========================
Channels are the main communication mechanism in pika for interacting with RabbitMQ. Pika automatically chooses the appropriate channel object for you to use when you create one with a connection adapter.

Example of creating a BlockingChannel::

    import pika

    # Create our connection object
    connection = pika.BlockingConnection()

    # The returned object will be a blocking channel
    channel = connection.channel()

The BlockingChannel implements blocking semantics for most things that one would use callback-passing-style for with the :py:class:`Channel <pika.channel.Channel>` class. In addition, the BlockingChannel class implements a :doc:`generator that allows you to consume messages <examples/blocking_consumer_generator>` without using callbacks.

BlockingChannel
---------------
.. automodule:: pika.adapters.blocking_connection
.. autoclass:: pika.adapters.blocking_connection.BlockingChannel
  :members:
  :inherited-members:
