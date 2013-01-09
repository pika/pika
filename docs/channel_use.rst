Using the Channel Object
========================
Channels are the main communication mechanism in pika for interacting with RabbitMQ. Pika automatically chooses the appropriate channel object for you to use when you create one with a connection adapter:

Example of creating a Channel:

    import pika

    # Create our connection object
    connection = pika.SelectConnection(on_channel_open)

    def on_open(connection):

        # The returned object will be a blocking channel
        channel = connection.channel(on_channel_open)

    def on_channel_open(channel):

        # It is also passed into the event that is fired when the channel is opened
        print channel

When using the :py:class:`BlockingConnection <pika.adapters.blocking_connection.BlockingConnection>` adapter, the channel object returned is a :py:class:`BlockingChannel <pika.adapters.blocking_connection.BlockingChannel>`.

Channel
-------
.. automodule:: pika.channel
.. autoclass:: pika.channel.Channel
  :members:
  :inherited-members:

