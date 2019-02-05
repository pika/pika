Asynchronous publisher example
==============================

The following example implements a publisher that will respond to RPC commands
sent from RabbitMQ and uses delivery confirmations. It will reconnect if
RabbitMQ closes the connection and will shutdown if RabbitMQ closes the
channel. While it may look intimidating, each method is very short and
represents a individual actions that a publisher can do.

`Asynchronous Publisher Example <https://github.com/pika/pika/blob/master/examples/asynchronous_publisher_example.py>`_
