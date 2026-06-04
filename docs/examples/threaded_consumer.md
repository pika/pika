# Thread-Safe Consumer

This example demonstrates consuming messages with `ThreadSafeConnection`. Consumer callbacks run on a dedicated worker thread, so blocking operations inside the callback do not stall heartbeats.

`ThreadSafeChannel` methods (`basic_ack`, `basic_publish`, `queue_declare`, etc.) are safe to call directly from within the callback without using `add_callback_threadsafe`.

Pairs with [Thread-Safe Publisher](threaded_publisher.md). Run this consumer first.

`examples/basic_consumer_threaded.py`:

```python
--8<-- "examples/basic_consumer_threaded.py"
```
