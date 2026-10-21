# Channel

## Closing a channel with active consumers

A `Basic.Cancel` does not stop the broker immediately. The specification allows it to keep dispatching for a cancelled consumer until it sends `Basic.CancelOk`, so there is a window in which further deliveries arrive for a consumer you have already cancelled. Pika passes those deliveries to the consumer callback, which is what the RabbitMQ Java client does.

!!! warning

    `Channel.close()` cancels each consumer without waiting for that confirmation, so any delivery the broker had already dispatched is discarded rather than passed to the callback. A callback could not acknowledge one anyway: the channel is closing and refuses the frame. A discarded delivery is redelivered to the next consumer when `auto_ack=False`, and **lost when `auto_ack=True`**, because an `auto_ack` delivery has no copy left at the broker. Pika logs a warning naming the count as the channel closes.

    This applies to `channel.close()` on `SelectConnection`, `AsyncioConnection`, `TornadoConnection` and `GeventConnection`. [`pika.Connection`](adapters/thread_safe.md) and [`BlockingConnection`](adapters/blocking.md) are unaffected, because both cancel their consumers and wait for the broker to confirm before closing anything.

To close a channel on one of the asynchronous adapters without losing those deliveries, cancel each consumer with a callback and close from the last `Basic.CancelOk`:

```python
def stop_consuming(channel, consumer_tags):
    """Close `channel` once the broker has confirmed every cancellation."""
    pending = set(consumer_tags)

    def on_cancelok(method_frame):
        pending.discard(method_frame.method.consumer_tag)
        if not pending:
            channel.close()

    for consumer_tag in consumer_tags:
        channel.basic_cancel(consumer_tag, callback=on_cancelok)
```

Each delivery that arrives before its `Basic.CancelOk` reaches the consumer callback, so a callback written on the assumption that cancelling is immediate may still run, and may still acknowledge.

Pika 2.0 removes the asynchronous adapters, leaving [`Connection`](adapters/thread_safe.md) and its `Channel` as the only close path, so the gap is retired rather than fixed.

::: pika.channel

## Channel

::: pika.channel.Channel
    options:
      members: true
      inherited_members: true
      members_order: source
