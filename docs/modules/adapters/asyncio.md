# Asyncio Connection Adapter

::: pika.adapters.asyncio_connection

Be sure to check out the [asynchronous examples](../../examples.md) including the asyncio specific [consumer](../../examples/asyncio_consumer.md) example.

## Interacting with Pika from another thread
`pika.adapters.asyncio_connection.AsyncioConnection`'s I/O loop exposes `call_soon_threadsafe()` to allow interacting with Pika from another thread.

## Class Reference

::: pika.adapters.asyncio_connection.AsyncioConnection
    options:
      members: true
      inherited_members: true
