## Frequently Asked Questions

### Is Pika thread safe?

Pika provides [`Connection`](modules/adapters/thread_safe.md) for multi-threaded applications. It wraps `SelectConnection` with a dedicated IOLoop thread and exposes a blocking API that is safe to call from any number of threads simultaneously. Consumer callbacks run on a per-channel worker thread, so blocking work inside a callback does not stall heartbeats or require manual callback scheduling.

```python
from pika.adapters.thread_safe_connection import Connection

conn = Connection(pika.ConnectionParameters('localhost'))
ch = conn.channel()
# safe to call from any thread
ch.basic_publish(exchange='', routing_key='q', body=b'hello')
```

The other connection adapters (`BlockingConnection`, `SelectConnection`, `AsyncioConnection`, etc.) are **not** thread-safe. Each connection instance is confined to the thread that created it. The only safe cross-thread operation on these adapters is calling `add_callback_threadsafe` to schedule a callback in the connection's IOLoop thread. See [connection adapters](modules/adapters/index.md) for details.

### Why does mypy report new errors on my consumer callback after upgrading?

Pika ships a `py.typed` marker, so a type checker reads pika's own annotations. `pika.spec` is generated code, and it was previously excluded from pika's `mypy` configuration, which left every field of a decoded frame typed as `Any`. `Any` silences all checking, so code like this passed:

```python
def on_message(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
```

`pika.spec` is now annotated and checked, so the same line reports:

```
error: Argument "delivery_tag" to "basic_ack" of "BlockingChannel"
has incompatible type "int | None"; expected "int"
```

The errors are accurate rather than newly introduced behavior. Two properties of the generated code cause them:

- **Fields are optional.** Every generated constructor defaults its arguments to `None`, so `delivery_tag` is `int | None`. A frame decoded off the wire always carries the field, but the type cannot express that.
- **String fields may be bytes.** AMQP `shortstr` is a length-prefixed byte string with no declared encoding. Pika decodes it as UTF-8 and falls back to the raw `bytes` when that fails, so `routing_key`, `exchange`, and `consumer_tag` are `str | bytes | None`.

An `assert` narrows the optional away, and `isinstance` narrows the `bytes`:

```python
def on_message(ch: BlockingChannel, method: Basic.Deliver,
               properties: BasicProperties, body: bytes) -> None:
    assert method.delivery_tag is not None
    ch.basic_ack(delivery_tag=method.delivery_tag)

    routing_key = method.routing_key
    assert isinstance(routing_key, str)
    ch.basic_publish(exchange='', routing_key=routing_key, body=body)
```

Narrowing only the optional is not always enough for a string field, since `str | bytes` remains. Whether that matters depends on the operation: `routing_key.upper()` checks either way, because `bytes` also has `upper()`, while passing the value back into `basic_publish`, using it as a `dict[str, ...]` key, or concatenating it with a `str` all require the `isinstance` narrowing above.

Use `typing.cast` instead of `assert` where the check should carry no runtime cost. Note that `assert` statements are stripped under `python -O`.

`pyright` reported most of these already, so its users see little change.

### How do I report a bug with Pika?

The [main Pika repository](https://github.com/pika/pika) is hosted on [GitHub](https://github.com), and we use the issue tracker at [github.com/pika/pika/issues](https://github.com/pika/pika/issues).

### Is there a mailing list for Pika?

Yes. Pika's mailing list is available on [Google Groups](https://groups.google.com/g/pika-python), and the email address is `pika-python@googlegroups.com`.

Traditionally, questions about Pika have also been asked on the [RabbitMQ mailing list](https://groups.google.com/g/rabbitmq-users).

### How can I contribute to Pika?

You can [fork the project on GitHub](https://help.github.com/en/articles/fork-a-repo/) and open
[pull requests](https://help.github.com/en/articles/about-pull-requests/)
when you believe you have something solid to add to the main repository.
