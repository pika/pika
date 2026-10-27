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

## What happens when your callback raises

An exception out of a callback is logged at `ERROR` with its traceback, and the next message is delivered. Every exception is treated the same way, including `SystemExit` and `KeyboardInterrupt`: pika contains it so that one failing dispatch cannot cost the channel its worker thread and strand the messages queued behind it.

Nothing else happens, and that is worth knowing before you rely on it. The channel stays open, `is_open` keeps reporting `True`, no close callback fires, and the exception does not reach your code by any route other than the log. **So a consumer whose callback fails on every message keeps consuming**, and with `auto_ack=True` the broker has already dropped its copy of each one, so the rest of the backlog is acknowledged and discarded while the log fills up. With `auto_ack=False` nothing is lost, but nothing comes back quickly either: an unacknowledged message is redelivered only once the channel or connection closes, or once your callback nacks or rejects it, so until then it is held and invisible to other consumers. If you have set a prefetch count, a callback that fails on every message also stops receiving new ones once that many are unacknowledged. Measured with `prefetch_count=3` and 20 messages queued: the callback saw 3, the other 17 stayed ready in the queue, and the 3 returned to it when the connection closed.

Handle the exception inside your callback, then, because that is the only place that can decide what a failure means. Two things not to expect of the alternatives, both measured:

- **`basic_cancel` from inside the callback does not stop the messages already in flight.** It returns promptly and tells the broker to send no more, but every delivery already dispatched or queued still reaches your callback afterwards, which is the same window a cancel always has. Measured with 20 queued messages: the cancel returned in 0.00 s and all 20 were still delivered.
- **Under `auto_ack=True` nothing your callback does can save a message.** The broker drops its copy when it dispatches, so a delivery is already gone by the time your code sees it, whether your callback returns early, raises, or cancels. If losing messages to a failing callback is unacceptable, consume with `auto_ack=False`, with the redelivery timing above in mind.

A flag your callback checks on entry is enough to stop *processing* the rest of a backlog, and a flag the thread that owns the connection watches is how to shut down from a callback, since `close()` from inside one is the call that does not work, for the reason given in [What a close does to queued callbacks](#what-a-close-does-to-queued-callbacks). #1754 covers giving the application a way to observe a raising callback rather than having to catch everything itself.

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
    shutdown_drain_timeout=30.0, # seconds; 0 abandons queued work, default 30 s
)
```

`work_queue_put_timeout` defaults to 30 seconds and must be a positive number; `None` (infinite) is rejected with `ValueError` because it lets a full queue stall or deadlock the IOLoop thread. Set an override comfortably below the negotiated heartbeat-death window (twice the heartbeat interval) so a stall surfaces as `WorkQueueFullError` rather than an opaque broker-side close. When the queue stays full for that long, `pika.exceptions.WorkQueueFullError` is raised on the IOLoop thread, tearing the connection down rather than silently dropping the event (which would lose an auto-acked delivery or force redelivery of a manually-acked one).

`shutdown_drain_timeout` bounds a different wait: once the IOLoop thread's loop has exited, it runs the work already queued for each pool before the thread ends. Without a bound a wedged callback holds that thread forever, which no caller asked for. It defaults to 30 seconds and must not be negative; `0` is allowed and abandons the queued work immediately, which is coherent for a caller who wants the process gone, and `None` is rejected. A worker still running when the budget expires is left to finish on its daemon thread, with a warning. It compounds with `work_queue_put_timeout`: a wedged consumer can hold the IOLoop in `submit` for one budget and here for another.

## Comparison with other adapters

|  | BlockingConnection | SelectConnection + add_callback_threadsafe | Connection |
| -- | -- | -- | -- |
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
