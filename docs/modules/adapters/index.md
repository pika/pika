# Connection Adapters
Pika uses connection adapters to provide a flexible method for adapting pika's core communication to different IOLoop implementations. In addition to asynchronous adapters, there is the `BlockingConnection` adapter that provides a more idiomatic procedural approach to using Pika.

## Adapters

- [ThreadSafeConnection](thread_safe.md)
- [BlockingConnection](blocking.md) *(deprecated, removed in Pika 2.0)*
- [SelectConnection](select.md)
- [AsyncioConnection](asyncio.md)
- [TornadoConnection](tornado.md) *(deprecated, removed in Pika 2.0)*
- [TwistedProtocolConnection](twisted.md) *(deprecated, removed in Pika 2.0)*
- [GeventConnection](gevent.md) *(deprecated, removed in Pika 2.0)*

> [!WARNING]
> `BlockingConnection`, `TornadoConnection`, `TwistedProtocolConnection`, and
> `GeventConnection` are deprecated and will be removed in Pika 2.0. The async
> adapters exist to integrate Pika into a framework's event loop — a constraint
> that no longer applies — and `BlockingConnection` runs the IOLoop on the
> calling thread, which makes heartbeats fragile and the adapter non-thread-safe.
> [`ThreadSafeConnection`](thread_safe.md) runs its own IOLoop on a background
> thread and works with any framework (asyncio, Tornado, Twisted, Gevent,
> Django, Flask, or plain synchronous code) without adapter-specific
> integration.

## Threading

### Using ThreadSafeConnection (recommended)

[ThreadSafeConnection](thread_safe.md) is the simplest way to use Pika from multiple threads. It runs the IOLoop in a background thread and provides a blocking, thread-safe API. Consumer callbacks run on a dedicated worker thread, so slow processing never stalls heartbeats, and you can call `basic_ack`, `basic_publish`, and other channel methods directly from the callback without any special coordination:

```python
from pika.adapters.thread_safe_connection import ThreadSafeConnection

conn = ThreadSafeConnection(pika.ConnectionParameters('localhost'))
ch = conn.channel()

def on_message(channel, method, properties, body):
    # This runs on a worker thread - safe to do blocking work here
    process(body)
    channel.basic_ack(method.delivery_tag)  # safe, no add_callback_threadsafe needed

ch.basic_consume('work', on_message)
```

See the [Thread-Safe Consumer](../../examples/threaded_consumer.md) and [Thread-Safe Publisher](../../examples/threaded_publisher.md) examples.

### Using add_callback_threadsafe (manual approach)

For other adapters (`BlockingConnection`, `SelectConnection`, `AsyncioConnection`, etc.), each connection instance is confined to a single thread. The single-threaded usage constraint may result in a dropped AMQP/stream connection due to AMQP heartbeat timeout in consumers that take a long time to process an incoming message. A common solution is to delegate processing of the incoming messages to another thread, while the connection adapter's thread continues to service its I/O loop's message pump, permitting AMQP heartbeats and other I/O to be serviced in a timely fashion.

Messages processed in another thread may not be acknowledged directly from that thread, since all accesses to the connection adapter instance must be from a single thread, which is the thread running the adapter's I/O loop. This is accomplished by requesting a callback to be executed in the adapter's I/O loop thread. For example, the callback function's implementation might look like this:

```python
def ack_message(channel, delivery_tag):
    """Note that `channel` must be the same Pika channel instance via which
    the message being acknowledged was retrieved (AMQP protocol constraint).
    """
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't acknowledge this message;
        # log and/or do something that makes sense for your app in this case.
        pass

```
The code running in the other thread may request the `ack_message()` function to be executed in the connection adapter's I/O loop thread using an adapter-specific mechanism:

- `pika.BlockingConnection` abstracts its I/O loop from the application and
  thus exposes `pika.BlockingConnection.add_callback_threadsafe()`. Refer to
  this method's docstring for additional information. For example:

```python
connection.add_callback_threadsafe(functools.partial(ack_message, channel, delivery_tag))

```
Please see the documentation of other adapters for their specific methods.

This threadsafe callback request mechanism may also be used to delegate publishing of messages, etc., from a background thread to the connection adapter's thread.

## Connection recovery

Pika does not provide automatic connection recovery - applications handle reconnection themselves.

Different connection adapters take different approaches to connection recovery.

For `pika.BlockingConnection` adapter exception handling can be used to check for connection errors. Here is a very basic example:

```python
import pika

while True:
    try:
        connection = pika.BlockingConnection()
        channel = connection.channel()
        channel.basic_consume('test', on_message_callback)
        channel.start_consuming()
    # Don't recover if connection was closed by broker
    except pika.exceptions.ConnectionClosedByBroker:
        break
    # Don't recover on channel errors
    except pika.exceptions.AMQPChannelError:
        break
    # Recover on all other connection errors
    except pika.exceptions.AMQPConnectionError:
        continue

```
This example can be found in `examples/consume_recover.py`.

Generic operation retry libraries such as [retry](https://github.com/invl/retry) can be used. Decorators make it possible to configure some additional recovery behaviours, like delays between retries and limiting the number of retries:

```python
from retry import retry


@retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
def consume():
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.basic_consume('test', on_message_callback)

    try:
        channel.start_consuming()
    # Don't recover connections closed by server
    except pika.exceptions.ConnectionClosedByBroker:
        pass


consume()

```
This example can be found in `examples/consume_recover_retry.py`.

For asynchronous adapters, use `on_close_callback` to react to connection failure events. This callback can be used to clean up and recover the connection.

An example of recovery using `on_close_callback` can be found in `examples/asynchronous_consumer_example.py`.
