# Thread-Safe Connection Adapter

`ThreadSafeConnection` wraps `SelectConnection` and runs its IOLoop in a dedicated background thread. All channel operations are routed through `add_callback_threadsafe` so that socket I/O stays confined to a single thread. External threads can call any `ThreadSafeChannel` method concurrently without coordination.

This eliminates the need to manually schedule callbacks via `add_callback_threadsafe` when publishing or acknowledging from multiple threads.

## When to use ThreadSafeConnection

Use `ThreadSafeConnection` when your application needs to publish or consume messages from multiple threads. It provides a blocking, synchronous API similar to `BlockingConnection` but with full thread safety:

- Multiple threads can call `basic_publish` simultaneously
- Consumer callbacks run on a dedicated worker thread, not the IOLoop thread, so slow processing does not stall heartbeats
- Channel methods (`basic_ack`, `queue_declare`, etc.) are safe to call from within consumer callbacks

## Basic usage

```python
import threading
import pika
from pika.adapters.thread_safe_connection import ThreadSafeConnection

conn = ThreadSafeConnection(pika.ConnectionParameters('localhost'))
ch = conn.channel()

ch.queue_declare('work')

# Publish from multiple threads simultaneously
for i in range(5):
    threading.Thread(
        target=ch.basic_publish,
        kwargs=dict(exchange='', routing_key='work', body=f'msg-{i}'.encode()),
    ).start()

conn.close()
```

## Consumer threading model

The `on_message_callback` registered with `basic_consume` is dispatched on a per-channel worker thread. This means:

- Blocking operations (database writes, HTTP calls, even `queue_declare` or `basic_qos`) are safe inside delivery callbacks
- The IOLoop thread is never starved by slow consumer processing, so heartbeats are always sent on time
- Messages are delivered to the callback in order (single worker thread per channel)
- All `ThreadSafeChannel` methods (`basic_ack`, `basic_nack`, `basic_reject`, `basic_publish`) are safe to call from within the callback

## Work queue bounds and back-pressure

Each worker thread consumes from a bounded queue holding at most `work_queue_maxsize` pending dispatches (default 1000, matching the RabbitMQ Java client). When a slow callback lets the queue fill up, further enqueues block the IOLoop thread, which stops reading from the socket and applies back-pressure to the broker through TCP flow control instead of buffering events in memory without limit.

Heartbeats are not sent while the IOLoop is blocked, so the broker's heartbeat timeout bounds how long a wedged consumer can stall the connection before the broker closes it.

Both knobs are constructor parameters on `ThreadSafeConnection`:

```python
conn = ThreadSafeConnection(
    pika.ConnectionParameters('localhost'),
    work_queue_maxsize=1000,     # 0 means unbounded
    work_queue_put_timeout=30.0, # seconds; must be positive, default 30 s
)
```

`work_queue_put_timeout` defaults to 30 seconds and must be a positive number; `None` (infinite) is rejected with `ValueError` because it lets a full queue stall or deadlock the IOLoop thread. Set an override comfortably below the negotiated heartbeat-death window (twice the heartbeat interval) so a stall surfaces as `WorkQueueFullError` rather than an opaque broker-side close. When the queue stays full for that long, `pika.exceptions.WorkQueueFullError` is raised on the IOLoop thread, tearing the connection down rather than silently dropping the event (which would lose an auto-acked delivery or force redelivery of a manually-acked one).

## Comparison with other adapters

| | BlockingConnection | SelectConnection + add_callback_threadsafe | ThreadSafeConnection |
|---|---|---|---|
| Thread safety | Not thread-safe | Manual callback scheduling required | Fully thread-safe |
| Consumer threading | Same thread as IOLoop | Same thread as IOLoop | Dedicated worker thread |
| Heartbeat risk | Slow consumers can stall heartbeats | Slow consumers can stall heartbeats | IOLoop runs independently |
| API style | Synchronous, blocking | Asynchronous, callback-based | Synchronous, blocking |

## Class Reference

::: pika.adapters.thread_safe_connection.ThreadSafeConnection
    options:
      members: true

::: pika.adapters.thread_safe_connection.ThreadSafeChannel
    options:
      members: true
