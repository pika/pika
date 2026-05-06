# Using the BlockingChannel.consume generator to consume messages

<a id="example_blocking_basic_get"></a>

The `BlockingChannel.consume` method is a generator that will return a tuple of method, properties and body.

When you escape out of the loop, be sure to call consumer.cancel() to return any unprocessed messages.

Example of consuming messages and acknowledging them:

```python
--8<-- "examples/blocking_consumer_generator.py"
```
If you have pending messages in the test queue, your output should look something like:

```text
    (pika)gmr-0x02:pika gmr$ python blocking_nack.py
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=1', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=2', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=3', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=4', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=5', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=6', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=7', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=8', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=9', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    <Basic.Deliver(['consumer_tag=ctag1.0', 'redelivered=True', 'routing_key=test', 'delivery_tag=10', 'exchange=test'])>
    <BasicProperties(['delivery_mode=1', 'content_type=text/plain'])>
    Hello World!
    Requeued 1894 messages
```
