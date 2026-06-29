# Thread-Safe Publisher Confirms

This example demonstrates publisher confirms with `ThreadSafeConnection`. It is the asynchronous counterpart to [Delivery Confirmations](blocking_delivery_confirmations.md), which uses `BlockingConnection`.

Where the blocking version calls `confirm_delivery()` with no arguments and catches `UnroutableError` synchronously, `ThreadSafeChannel.confirm_delivery(ack_nack_callback)` registers a callback that the broker's `Basic.Ack` / `Basic.Nack` frames are delivered to. That callback runs on the channel's worker thread — not the IOLoop thread — so a slow confirm handler never stalls heartbeats, and it may safely call any `ThreadSafeChannel` method.

Each publish is tagged with a monotonically increasing delivery tag, exposed through the `on_publish` callback and the `next_publish_seq_no` property.

`examples/publisher_confirms_threaded.py`:

```python
--8<-- "examples/publisher_confirms_threaded.py"
```
