# The thread-safe connection

The long-form record for `pika/adapters/thread_safe_connection.py`, which provides `pika.Connection` and its `Channel`. The published docs describe the contract; this is the behaviour behind it and the measurements that pinned it down.

## How it works

`Connection` wraps `SelectConnection` and runs its IOLoop on a dedicated background thread. Every channel operation from an external thread is routed through `add_callback_threadsafe`, so `_tx_buffers` has a single owner and socket I/O stays on one thread. `Channel` exposes a blocking API over that, built on `_blocking_rpc`, which schedules the call on the IOLoop thread and waits for the broker's response.

Deliveries and publisher confirms are dispatched on a per-channel worker thread from `_BoundedWorkPool` rather than on the IOLoop thread. One worker per channel means deliveries on a channel are processed serially, so prefetch controls how many messages are outstanding rather than how many threads run. See the first invariant below for which callbacks go where.

## Invariants

**Which thread a user callback runs on depends on the callback.** Delivery, publisher-confirm, return and cancel callbacks go to the channel's worker pool. `Connection.Blocked` and `Unblocked` go to a separate connection-level pool. Four run on the IOLoop thread: `basic_publish`'s `on_publish`, `on_close_callback` and `on_open_error_callback`, and anything passed to `add_callback_threadsafe`.

**Only the pool-dispatched callbacks and `on_publish` are exception-safe.** `_safe_dispatch` wraps those, logging a failure instead of letting it reach the IOLoop. The other three IOLoop-thread paths are unwrapped, so an exception there exits the IOLoop thread. `_run_ioloop` catches it, records `_closed_reason` and wakes every blocked caller, but `is_open` still reports True because nothing closed the underlying connection: #1743. `_safe_dispatch` also catches `Exception` rather than `BaseException`, and `_BoundedWorkPool._run_worker` has no guard at all, so a `BaseException` from a pool callback exits that worker without the logging the `Exception` path gives.

**No worker thread is free of back-pressure.** Every pool is a `_BoundedWorkPool`, the per-channel ones and the connection-level one for `Connection.Blocked` and `Unblocked` alike, each constructed with the same `work_queue_maxsize` and `work_queue_put_timeout`. A pool's queue holds `work_queue_maxsize` pending dispatches, 1000 by default. When a slow callback fills it, the IOLoop thread blocks while enqueuing the next item, sends no heartbeats for the duration, and raises `WorkQueueFullError` if the queue stays full for `work_queue_put_timeout` seconds. So "a worker thread means slow callbacks cannot stall heartbeats" is true only while the callback keeps pace. Published prose that states it absolutely is wrong.

**Confirm listeners share that single worker and queue with deliveries.** A confirm handler doing synchronous network I/O per ack is therefore subject to the same stall.

## Why the confirm listener runs on the worker thread

The RabbitMQ Java and .NET clients run their confirm listener inline on the I/O thread. pika's wrapper diverges so that a slow listener does not hold up heartbeats, accepting the back-pressure described above as the cost. The same reasoning puts delivery callbacks there.

Idempotency also diverges from nothing: a second `confirm_delivery` on the same channel returns the cached `Confirm.SelectOk` without sending a frame or resetting the delivery-tag counter, which matches Java and .NET.

## Measurements

Every figure below names its configuration, because none of them reproduces without one. `utils/measure_close_loss.py` reproduces the close figures against a live broker, taking the message count, body size, work-queue size, callback cost, timeout and acknowledgement mode as arguments. It is not wired into CI: a run takes as long as the callbacks it times, and it reports counts rather than a verdict. It discriminates between versions, which is how to tell it is measuring the close and not the harness: at 400 messages of 8 KiB with `--timeout none` it reports 339 of 400 lost on 1.4.x behaviour and none once the close drains first.

**An ack does not mean the message reached a queue.** An unroutable publish sent with `mandatory=True` is answered with Basic.Return and then Basic.Ack. In the broker, `rabbit_channel.erl` runs both for one publish: `process_routing_mandatory(true, [], ...)` sends the return, and `process_routing_confirm(MsgSeqNo, [], ...)` calls `record_confirms` for the same sequence number. Measured against 4.3.4, a mandatory publish to an exchange with no matching binding produced `[('RETURN', 'NO_ROUTE'), ('ACK', 1)]` in that order. Code that treats Basic.Ack as proof of routing therefore has to register `add_on_return_callback` as well.

**`next_publish_seq_no` cannot be read after a publish to learn that publish's tag.** `basic_publish` ends by scheduling the work on the IOLoop thread; the counter advances inside that scheduled function. Measured against 4.3.4 with a single publisher thread, three back-to-back publishes read the counter immediately after each call and got the same value all three times, while `on_publish` reported tags 1, 2 and 3 and the counter settled at 4 a moment later. How many reads are stale varies by run, which is what a race looks like; separate runs gave all three stale and the last two stale. `on_publish` is handed the tag the broker will confirm and is the supported way to learn it. The property's own docstring warns only about concurrent publishers, which understates the hazard.

**RabbitMQ 4.3 and later refuse a queue that is neither durable nor exclusive,** answering 541 and closing the connection rather than the channel. pika still defaults `durable=False`, so every example here passes `durable=True`. `examples/README.md` carries the per-version measurements.

## Known gaps

- `confirm_delivery` accepts a non-callable, because the wrapper hands its own closure to the core channel and the `callable()` guard in `pika/channel.py` never sees the caller's object. Confirms are still enabled and the counter still arms; it is the caller's listener that never runs, failing with a logged `TypeError` once per confirmation: #1742.
- An exception from `add_callback_threadsafe`, `on_close_callback` or `on_open_error_callback` exits the IOLoop thread while `is_open` keeps reporting True: #1743.
- Whether the confirm callback should be required at all, and how the API compares with the Java, Go and .NET clients, is under review for 2.0: #1740.
