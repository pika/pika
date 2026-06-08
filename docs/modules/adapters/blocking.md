# Blocking Connection Adapter

> [!WARNING]
> `BlockingConnection` is deprecated and will be removed in Pika 2.0. Because
> the calling thread *is* the IOLoop, any blocking user code stalls heartbeats
> unless `process_data_events()` is called periodically, and the adapter is not
> thread-safe. Use [`ThreadSafeConnection`](thread_safe.md) instead, which runs
> its own IOLoop on a background thread and provides a thread-safe blocking API.

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
