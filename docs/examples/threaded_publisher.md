# Thread-Safe Publisher

This example demonstrates publishing from multiple threads using `ThreadSafeConnection`. Every call to `basic_publish` is routed through the IOLoop thread internally, so there is no need for external synchronization.

Pairs with [Thread-Safe Consumer](threaded_consumer.md). Run the consumer first.

`examples/basic_publisher_threaded.py`:

```python
--8<-- "examples/basic_publisher_threaded.py"
```
