# Using the Blocking Connection to consume messages from RabbitMQ

<a id="example_blocking_basic_consume"></a>

The `BlockingChannel.basic_consume`  method assign a callback method to be called every time that RabbitMQ delivers messages to your consuming application.

When pika calls your method, it will pass in the channel, a `pika.spec.Basic.Deliver` object with the delivery tag, the redelivered flag, the routing key that was used to put the message in the queue, and the exchange the message was published to. The third argument will be a `pika.spec.BasicProperties` object and the last will be the message body.

Example of consuming messages and acknowledging them:

```python
--8<-- "examples/blocking_consume.py"
```
Example of using more connection parameters, and of a common TLS problem with a hosted broker: the certificate does not name the address you dial, so hostname verification fails on a certificate that is otherwise perfectly valid. The example dials 127.0.0.1, which the test certificate does not name, and passes `server_hostname` so verification checks the name the certificate does carry rather than switching verification off. It also names its virtual host, username and password explicitly rather than relying on the defaults, sets the heartbeat to 150 seconds, caps the connection at 10 channels, and sets the client provided connection_name for easy identification:

```python
--8<-- "examples/blocking_consume_tls.py"
```
