## Frequently Asked Questions

### Is Pika thread safe?

Pika provides [`ThreadSafeConnection`](modules/adapters/thread_safe.md) for multi-threaded applications. It wraps `SelectConnection` with a dedicated IOLoop thread and exposes a blocking API that is safe to call from any number of threads simultaneously. Consumer callbacks run on a per-channel worker thread, so blocking work inside a callback does not stall heartbeats or require manual callback scheduling.

```python
from pika.adapters.thread_safe_connection import ThreadSafeConnection

conn = ThreadSafeConnection(pika.ConnectionParameters('localhost'))
ch = conn.channel()
# safe to call from any thread
ch.basic_publish(exchange='', routing_key='q', body=b'hello')
```

The other connection adapters (`BlockingConnection`, `SelectConnection`, `AsyncioConnection`, etc.) are **not** thread-safe. Each connection instance is confined to the thread that created it. The only safe cross-thread operation on these adapters is calling `add_callback_threadsafe` to schedule a callback in the connection's IOLoop thread. See [connection adapters](modules/adapters/index.md) for details.

### How do I report a bug with Pika?

The [main Pika repository](https://github.com/pika/pika) is hosted on [GitHub](https://github.com), and we use the issue tracker at [github.com/pika/pika/issues](https://github.com/pika/pika/issues).

### Is there a mailing list for Pika?

Yes. Pika's mailing list is available on [Google Groups](https://groups.google.com/g/pika-python), and the email address is `pika-python@googlegroups.com`.

Traditionally, questions about Pika have also been asked on the [RabbitMQ mailing list](https://groups.google.com/g/rabbitmq-users).

### How can I contribute to Pika?

You can [fork the project on GitHub](https://help.github.com/en/articles/fork-a-repo/) and open
[pull requests](https://help.github.com/en/articles/about-pull-requests/)
when you believe you have something solid to add to the main repository.
