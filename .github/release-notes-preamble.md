[GitHub Milestone](https://github.com/pika/pika/milestone/24?closed=1)

## Upgrading to 1.5.0

Five changes are worth reading before you upgrade. Two can turn a passing build red, and two change what a working application observes when it cancels a consumer or closes a channel.

### `pika.Connection` is new, and is the connection to use

1.5.0 introduces `pika.Connection` and its `Channel`, a connection that runs its own IOLoop on a background thread and offers a thread-safe blocking API that does not stall heartbeats while your code processes a message. There is nothing to migrate *from*: these classes are new in this release, and `pika.Connection` is the documented import.

`from pika.adapters.thread_safe_connection import Connection` also works, though the module moves in 2.0 (see #1681). `Channel` is not exported from `pika`, because channels come from `Connection.channel()`.

If you are starting new code, use `pika.Connection`. If you have existing code on one of the adapters below, it keeps working in 1.5.0 and the deprecation notice tells you where it is going.

### The other adapters now emit a `DeprecationWarning`, which may fail your tests

`BlockingConnection`, `TornadoConnection`, `TwistedProtocolConnection` and `GeventConnection` each warn from their constructor that they will be removed in Pika 2.0. They all still work.

The upgrade risk is that **a `DeprecationWarning` raised from a constructor is a hard error for anyone running with warnings-as-errors**, which is a common test configuration. It fires at construction, before any I/O, so it does not depend on reaching a broker:

```
E       DeprecationWarning: BlockingConnection is deprecated and will be removed in Pika 2.0. ...
pika/adapters/blocking_connection.py:367: DeprecationWarning
```

The same happens under `python -W error` and `warnings.simplefilter('error')`. `BlockingConnection` is pika's most widely used entry point, so this is both likely and likely to be misread as pika being broken rather than as an intentional notice. Outside a test runner it is usually invisible, because Python's default filters hide `DeprecationWarning` unless it is triggered from `__main__`; pytest is what turns it back on.

Either filter it or migrate. The warning uses `stacklevel=2`, so it is reported against the line that constructs the connection rather than against pika, and a filter scoped to the pika module does not match. Match on the message instead:

```toml
# pyproject.toml, for pytest
filterwarnings = [
    "ignore:.*will be removed in Pika 2\\.0:DeprecationWarning",
]
```

```bash
python -W 'ignore:BlockingConnection is deprecated' your_app.py
```

or migrate to `Connection`, which runs its own IOLoop on a background thread and provides a thread-safe blocking API that does not stall heartbeats on slow message processing.

### Cancelling a consumer no longer rejects the messages still arriving for it

A `Basic.Cancel` does not stop the broker immediately: the specification allows it to keep dispatching until it sends `Basic.CancelOk`, and pika used to reject every delivery that arrived in that window, or discard it outright for an `auto_ack` consumer. Discarding loses the message, because an `auto_ack` delivery has no copy left at the broker, and rejecting is a decision pika was making on your behalf that no other client makes. Measured on RabbitMQ 4.3.6, closing a channel with an `auto_ack` consumer part way through 400 messages, `work_queue_maxsize=50`, a 50 ms callback and `close(timeout=10)`: 339 of 400 lost before this change and 181 after at 8 KiB bodies, 202 and 180 at 64 bytes. What remains after the change is lost because that timeout is smaller than 400 callbacks at 50 ms need, not because of anything the close does wrong: `close(timeout=None)` in the same configuration loses none. Quote the whole configuration with any figure from this area, because the message count, body size, queue size, callback cost and timeout all move it.

Those deliveries are now passed to your callback instead, which is what the Java client does, as long as the channel is still open.

One thing follows for existing code: a consumer callback can now run for a message that arrives after you called `basic_cancel`, so a callback that assumed cancellation was immediate may see further deliveries. `BlockingChannel.basic_cancel` still rejects them for an `auto_ack=False` consumer, so they return to the queue at once as before, and its return value is unchanged; the channel no longer does that rejecting, the wrapper does. The generator-style `BlockingChannel.cancel` discards them instead, which #1767 covers.

### `channel.close()` on an asynchronous adapter still discards those deliveries

A channel that is closing discards them, because a callback could not acknowledge one anyway and the attempt would abort the connection; the Java client discards them at the same point. `Channel.close` cancels each consumer without waiting for the broker to confirm, so this is what a bare `channel.close()` does on `SelectConnection`, `AsyncioConnection`, `TornadoConnection` and `GeventConnection`. A discarded delivery is redelivered to the next consumer with `auto_ack=False` and lost with `auto_ack=True`. Pika now logs a warning naming the count as the channel closes.

`pika.Connection` and `BlockingConnection` are unaffected and keep every delivery across a close, since both cancel their consumers and wait for the broker to confirm before closing anything. To get the same guarantee from an asynchronous adapter, cancel each consumer with a callback and close the channel from the last `Basic.CancelOk`; the [channel documentation](https://pika.github.io/pika/latest/modules/channel/#closing-a-channel-with-active-consumers) has the pattern. Pika 2.0 removes these adapters, which leaves `Connection` and its `Channel` as the only close path and so retires the gap rather than fixing it.

### `pika.spec` is now annotated, which may fail your type checker

Pika ships a `py.typed` marker, so a type checker reads pika's own annotations. `pika.spec` is generated code and was previously excluded from pika's `mypy` configuration, which left every field of a decoded frame typed as `Any`. `Any` silences all checking, so a downstream `mypy` run that was clean can go red on upgrade without any change on your end. The most common line in every pika tutorial is affected:

```
error: Argument "delivery_tag" to "basic_ack" of "BlockingChannel" has incompatible type "int | None"; expected "int"  [arg-type]
error: Argument "routing_key" to "basic_publish" of "BlockingChannel" has incompatible type "str | bytes | None"; expected "str"  [arg-type]
```

Two properties of the generated code account for it. Every generated constructor defaults its arguments to `None`, so fields are optional even though a frame off the wire always carries them. And AMQP `shortstr` has no declared encoding, so pika falls back to raw `bytes` on invalid UTF-8, making `routing_key`, `exchange` and `consumer_tag` all `str | bytes | None`.

The errors are accurate and the annotations are an improvement, so nothing here should be reverted. The FAQ covers the narrowing idioms: https://pika.github.io/pika/latest/faq/

`pyright` users see little change, since it reported most of these already.

The same notes are in the changelog, which also carries the full version history: https://pika.github.io/pika/latest/changelog/
