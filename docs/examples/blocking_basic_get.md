# Using the Blocking Connection to get a message from RabbitMQ

<a id="example_blocking_basic_get"></a>

The `BlockingChannel.basic_get`  method will return a tuple with the members.

If the server returns a message, the first item in the tuple will be a `pika.spec.Basic.GetOk` object with the current message count, the redelivered flag, the routing key that was used to put the message in the queue, and the exchange the message was published to. The second item will be a `BasicProperties` object and the third will be the message body.

If the server did not return a message a tuple of None, None, None will be returned.

Example of getting a message and acknowledging it:

```python
--8<-- "examples/blocking_basic_get.py"
```
