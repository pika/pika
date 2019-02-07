Asyncio Consumer
================

The following example implements a consumer using the :class:`Asyncio adapter
<pika.adapters.asyncio_connection.AsyncioConnection>` for the `Asyncio library
<https://docs.python.org/3/library/asyncio.html>`_ that will respond to RPC
commands sent from RabbitMQ. For example, it will reconnect if RabbitMQ closes
the connection and will shutdown if RabbitMQ cancels the consumer or closes the
channel. While it may look intimidating, each method is very short and
represents a individual actions that a consumer can do.

`Asyncio Consumer Example <https://github.com/pika/pika/blob/master/examples/asyncio_consumer_example.py>`_
