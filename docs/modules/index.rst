Core Class and Module Documentation
===================================
For the end user, Pika is organized into a small set of objects for all communication with RabbitMQ.

- A :doc:`connection adapter <adapters/index>` is used to connect to RabbitMQ and manages the connection.
- :doc:`Connection parameters <parameters>` are used to instruct the :class:`~pika.connection.Connection` object how to connect to RabbitMQ.
- :doc:`credentials` are used to encapsulate all authentication information for the :class:`~pika.connection.ConnectionParameters` class.
- A :class:`~pika.channel.Channel` object is used to communicate with RabbitMQ via the AMQP RPC methods.
- :doc:`exceptions` are raised at various points when using Pika when something goes wrong.

.. toctree::
   :hidden:
   :maxdepth: 1

   adapters/index
   channel
   connection
   credentials
   exceptions
   parameters
   spec
