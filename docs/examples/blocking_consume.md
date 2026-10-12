# Using the Blocking Connection to consume messages from RabbitMQ

<a id="example_blocking_basic_consume"></a>

The `BlockingChannel.basic_consume`  method assign a callback method to be called every time that RabbitMQ delivers messages to your consuming application.

When pika calls your method, it will pass in the channel, a `pika.spec.Basic.Deliver` object with the delivery tag, the redelivered flag, the routing key that was used to put the message in the queue, and the exchange the message was published to. The third argument will be a `pika.spec.BasicProperties` object and the last will be the message body.

Example of consuming messages and acknowledging them:

```python
--8<-- "examples/blocking_consume.py"
```
Example of using more connection parameters. This example connects over TLS, verifying the broker against a CA certificate and presenting a client certificate of its own. It names its virtual host, username and password explicitly rather than relying on the defaults, sets the heartbeat to 150 seconds, caps the connection at 10 channels, and sets the client provided connection_name for easy identification:

```python
--8<-- "examples/blocking_consume_tls.py"
```
