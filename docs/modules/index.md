# Core Class and Module Documentation
For the end user, Pika is organized into a small set of objects for all communication with RabbitMQ.

- A [connection adapter](adapters/index.md) is used to connect to RabbitMQ and manages the connection.
- [Connection parameters](parameters.md) are used to instruct the `Connection` object how to connect to RabbitMQ.
- [credentials](credentials.md) are used to encapsulate all authentication information for the `ConnectionParameters` class.
- A `Channel` object is used to communicate with RabbitMQ via the AMQP RPC methods.
- [exceptions](exceptions.md) are raised at various points when using Pika when something goes wrong.
