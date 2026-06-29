# Thread-Safe Long-Running Publisher

This example demonstrates a long-running publisher with `ThreadSafeConnection`. It is the counterpart to the `BlockingConnection`-based `examples/long_running_publisher.py`.

The blocking version has to run its own background thread calling `process_data_events()` so heartbeats keep flowing while the application is idle, and it bounces every publish onto that thread with `add_callback_threadsafe` — the classic "heartbeat vs. work" tension of a single-threaded design.

`ThreadSafeConnection` removes that tension: its IOLoop runs on a dedicated background thread, so heartbeats are sent on time no matter how long the main thread sleeps between publishes. The main thread simply calls `basic_publish` whenever it has something to send.

`examples/long_running_publisher_threaded.py`:

```python
--8<-- "examples/long_running_publisher_threaded.py"
```
