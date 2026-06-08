# Tornado Connection Adapter

> [!WARNING]
> `TornadoConnection` is deprecated and will be removed in Pika 2.0. Use
> [`ThreadSafeConnection`](thread_safe.md) instead, which runs its own IOLoop
> on a background thread and works with any framework, including Tornado.

::: pika.adapters.tornado_connection

Be sure to check out the [asynchronous examples](../../examples.md) including the Tornado specific [consumer](../../examples/tornado_consumer.md) example.

## Interacting with Pika from another thread
`pika.adapters.tornado_connection.TornadoConnection`'s I/O loop provides `add_callback()` to allow interacting with Pika from another thread.

## Class Reference

::: pika.adapters.tornado_connection.TornadoConnection
    options:
      members: true
      inherited_members: true
