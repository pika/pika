# Thread-Safe Connection Adapter

`Connection` wraps `SelectConnection` and runs its IOLoop in a dedicated background thread. All channel operations are routed through `add_callback_threadsafe` so that socket I/O stays confined to a single thread. External threads can call any `Channel` method concurrently without coordination.

This eliminates the need to manually schedule callbacks via `add_callback_threadsafe` when publishing or acknowledging from multiple threads.

## When to use Connection

Use `Connection` when your application needs to publish or consume messages from multiple threads. It provides a blocking, synchronous API similar to `BlockingConnection` but with full thread safety:

- Multiple threads can call `basic_publish` simultaneously
- Consumer callbacks run on a dedicated worker thread, not the IOLoop thread, so slow processing does not stall heartbeats
- Channel methods (`basic_ack`, `queue_declare`, etc.) are safe to call from within consumer callbacks

## Basic usage

```python
import threading
import pika

conn = pika.Connection(pika.ConnectionParameters('localhost'))
ch = conn.channel()

# RabbitMQ 4.3 and later refuse a queue that is neither durable nor exclusive.
ch.queue_declare('work', durable=True)

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
- Slow consumer processing does not starve the IOLoop thread, so heartbeats keep flowing while a callback works. Filling the work queue is the one case that blocks the IOLoop; see below
- Messages are delivered to the callback in order (single worker thread per channel)
- Acknowledging and publishing (`basic_ack`, `basic_nack`, `basic_reject`, `basic_publish`) are safe to call from within the callback
- `close()` is the one method that is not: see [What a close does to queued callbacks](#what-a-close-does-to-queued-callbacks)

## What a close does to queued callbacks

`Connection.close` and `Channel.close` cancel the channel's consumers, wait for the broker to confirm each cancellation, and then run the callbacks already queued before closing anything. A delivery that has reached the client is therefore passed to your callback, and the acknowledgement that callback issues still reaches the broker. Note that closing a channel or connection cancels its consumers, which is visible to the broker and to anything watching consumer counts.

All of it is bounded by the `timeout` you pass to `close`, shared across the cancellations, the queued callbacks and the close itself, plus a one-second floor on the close handshake. A `Connection.close` whose broker never answers can take up to twice the timeout plus that second, because force-stopping the IOLoop is given a second budget.

If the budget runs out before the backlog is done, the close proceeds and a warning names the pool, and messages are lost for an `auto_ack` consumer. The callbacks already queued still run on a daemon thread, but they can no longer acknowledge anything, and deliveries the broker sends after the pool is shut down are discarded without ever reaching a callback. How many depends on `work_queue_maxsize` and on how fast your callback is. Anything unacknowledged is redelivered; with `auto_ack=True` the broker has already discarded its copy, so those are gone. **Pass `timeout=None` if losing none of them matters more than bounding the close**, which is the only setting that guarantees it.

Two cases skip this and behave as they did before 1.5.0, running queued callbacks after the teardown instead: `close()` called from the connection's own IOLoop thread, and `close()` called from inside a delivery or event callback, where waiting for the queue would mean waiting for the calling callback to return. Neither can be fixed on the close path, since the guarantee needs a queue to drain and the caller is sitting in it, so a callback that wants to shut the connection down should set a flag the thread that owns the connection watches and close from there. #1770 covers whether pika should refuse the call instead.

## Work queue bounds and back-pressure

Each worker thread consumes from a bounded queue holding at most `work_queue_maxsize` pending dispatches (default 1000, matching the RabbitMQ Java client). When a slow callback lets the queue fill up, further enqueues block the IOLoop thread, which stops reading from the socket and applies back-pressure to the broker through TCP flow control instead of buffering events in memory without limit.

Heartbeats are not sent while the IOLoop is blocked, so the broker's heartbeat timeout bounds how long a wedged consumer can stall the connection before the broker closes it.

Both knobs are constructor parameters on `Connection`:

```python
conn = pika.Connection(
    pika.ConnectionParameters('localhost'),
    work_queue_maxsize=1000,     # 0 means unbounded
    work_queue_put_timeout=30.0, # seconds; must be positive, default 30 s
)
```

`work_queue_put_timeout` defaults to 30 seconds and must be a positive number; `None` (infinite) is rejected with `ValueError` because it lets a full queue stall or deadlock the IOLoop thread. Set an override comfortably below the negotiated heartbeat-death window (twice the heartbeat interval) so a stall surfaces as `WorkQueueFullError` rather than an opaque broker-side close. When the queue stays full for that long, `pika.exceptions.WorkQueueFullError` is raised on the IOLoop thread, tearing the connection down rather than silently dropping the event (which would lose an auto-acked delivery or force redelivery of a manually-acked one).

## Comparison with other adapters

| | BlockingConnection | SelectConnection + add_callback_threadsafe | Connection |
|---|---|---|---|
| Thread safety | Not thread-safe | Manual callback scheduling required | Fully thread-safe |
| Consumer threading | Same thread as IOLoop | Same thread as IOLoop | Dedicated worker thread |
| Heartbeat risk | Slow consumers can stall heartbeats | Slow consumers can stall heartbeats | IOLoop runs independently |
| API style | Synchronous, blocking | Asynchronous, callback-based | Synchronous, blocking |

## Class Reference

::: pika.adapters.thread_safe_connection.Connection
    options:
      members: true

::: pika.adapters.thread_safe_connection.Channel
    options:
      members: true

## Defaults

::: pika.adapters.thread_safe_connection
    options:
      members:
        - DEFAULT_RPC_TIMEOUT
        - DEFAULT_WORK_QUEUE_MAXSIZE
        - DEFAULT_WORK_QUEUE_PUT_TIMEOUT
      show_if_no_docstring: true
      show_root_heading: false
