Using the Channel Object
========================
Channels are the main communication mechanism in pika for interacting with RabbitMQ. The Channel class is the core class used in all adapters but is overriden in The blocking_connection adapter to add blocking behaviors where appropriate.

Channel
-------
.. automodule:: pika.channel
.. autoclass:: pika.channel.Channel
  :members:
  :inherited-members:

BlockingChannel
----------------------
.. automodule:: pika.adapters.blocking_connection
.. autoclass:: pika.adapters.blocking_connection.BlockingChannel
  :members:
  :inherited-members:
