# Tornado Connection Adapter

::: pika.adapters.tornado_connection

Be sure to check out the [asynchronous examples](../../examples.md) including the Tornado specific [consumer](../../examples/tornado_consumer.md) example.

## Interacting with Pika from another thread
`pika.adapters.tornado_connection.TornadoConnection`'s I/O loop provides
`add_callback()` to allow interacting with Pika from another thread.

## Class Reference

::: pika.adapters.tornado_connection.TornadoConnection
    options:
      members: true
      inherited_members: true
