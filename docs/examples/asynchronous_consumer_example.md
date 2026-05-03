Asynchronous consumer example
=============================

The following example implements a consumer that will respond to RPC commands
sent from RabbitMQ. For example, it will reconnect if RabbitMQ closes the
connection and will shutdown if RabbitMQ cancels the consumer or closes the
channel. While it may look intimidating, each method is very short and
represents a individual actions that a consumer can do.

`Asynchronous Consumer Example <https://github.com/pika/pika/blob/master/examples/asynchronous_consumer_example.py>`_
