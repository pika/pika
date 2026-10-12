# Using the BlockingChannel.consume generator to consume messages

<a id="example_blocking_consumer_generator"></a>

The `BlockingChannel.consume` method is a generator that will return a tuple of method, properties and body.

When you escape out of the loop, be sure to call `BlockingChannel.cancel` to requeue any messages the generator is still holding.

!!! note
    `cancel` returns 0 and nothing else. The count has been vestigial since 0.10.0: only messages consumed with `auto_ack=False` can be requeued, and any arriving after the `Basic.Cancel` are rejected by the implementation channel, so the number is not knowable. Do not read the return value as a requeue count.

Example of consuming messages and acknowledging them:

```python
--8<-- "examples/blocking_consumer_generator.py"
```
The example publishes the ten messages it then consumes, so it terminates on its own. Its output on stdout looks like this, abbreviated to the first and last message. The consumer tag is a fresh UUID each run, and `BlockingConnection` also writes a deprecation warning to stderr:

```text
$ python examples/blocking_consumer_generator.py
<Basic.Deliver(['consumer_tag=ctag1.5530b02c97654528aacbde82ba5cc271', 'delivery_tag=1', 'exchange=', 'redelivered=False', 'routing_key=test'])>
<BasicProperties>
b'Hello World 0!'
...
<Basic.Deliver(['consumer_tag=ctag1.5530b02c97654528aacbde82ba5cc271', 'delivery_tag=10', 'exchange=', 'redelivered=False', 'routing_key=test'])>
<BasicProperties>
b'Hello World 9!'
Requeued 0 messages
```

If the queue already held messages, they are delivered first and take the low delivery tags, so the example's own ten arrive after them and the loop breaks before reaching all of them. Delivery tags are per channel and always start at 1.
