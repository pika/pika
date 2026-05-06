# Blocking Connection Adapter

::: pika.adapters.blocking_connection

Be sure to check out examples in [examples](../../examples.md).

## Interacting with Pika from another thread

`pika.BlockingConnection` abstracts its I/O loop from the application and thus exposes `pika.BlockingConnection.add_callback_threadsafe()`  to allow interacting with Pika from another thread.

## Class Reference

::: pika.adapters.blocking_connection.BlockingConnection
    options:
      members: true
      inherited_members: true

::: pika.adapters.blocking_connection.BlockingChannel
    options:
      members: true
      inherited_members: true
