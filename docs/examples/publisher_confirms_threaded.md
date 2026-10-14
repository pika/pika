# Thread-Safe Publisher Confirms

This example demonstrates publisher confirms with `Connection`. It is the asynchronous counterpart to [Delivery Confirmations](blocking_delivery_confirmations.md), which uses `BlockingConnection`.

Where the blocking version calls `confirm_delivery()` with no arguments and catches `UnroutableError` synchronously, `Channel.confirm_delivery(ack_nack_callback)` registers a callback that the broker's `Basic.Ack` / `Basic.Nack` frames are delivered to. That callback runs on the channel's worker thread rather than the IOLoop thread, so the confirm handler may safely call any `Channel` method and heartbeats keep flowing while it works. It shares the single worker thread and bounded queue with delivery callbacks, so a handler that cannot keep up with the confirm rate eventually blocks the IOLoop; see [work queue bounds](../modules/adapters/thread_safe.md#work-queue-bounds-and-back-pressure).

The callback is required, and takes a single Basic.Ack or Basic.Nack frame. There is no separate nack callback, so `isinstance` is how the two are told apart; `delivery_tag` says which publish is confirmed and `multiple` says whether every earlier tag is confirmed with it.

```python
from pika import spec

def on_confirm(method_frame):
    tag = method_frame.method.delivery_tag
    multiple = method_frame.method.multiple
    if isinstance(method_frame.method, spec.Basic.Ack):
        ...   # `tag`, and every earlier tag if `multiple`
    else:
        ...   # Basic.Nack: the broker rejected it

ch.confirm_delivery(on_confirm)
```

Learn the tag to match against from `basic_publish`'s `on_publish` callback, which is handed the tag the broker will confirm. Do not read `next_publish_seq_no` after a publish to infer it, because the counter advances later on the IOLoop thread.

An ack is not proof that the message reached a queue: an unroutable publish sent with `mandatory=True` is answered with Basic.Return *and then* Basic.Ack. Register `add_on_return_callback` as well when routing matters, as [Mandatory publishing](blocking_publish_mandatory.md) does for the blocking adapter.

Both hazards are measured, with the broker behaviour behind them, in `docs/internal/thread-safe-connection.md`.

`examples/publisher_confirms_threaded.py`:

```python
--8<-- "examples/publisher_confirms_threaded.py"
```
