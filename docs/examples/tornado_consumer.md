# Tornado consumer example

The following example implements a consumer using the `Tornado adapter` for the [Tornado framework](https://www.tornadoweb.org/) that will respond to RPC commands sent from RabbitMQ. For example, it will reconnect if RabbitMQ closes the connection and will shutdown if RabbitMQ cancels the consumer or closes the channel. While it may look intimidating, each method is very short and represents an individual action a consumer can take.

`examples/tornado_consumer.py`:

```python
--8<-- "examples/tornado_consumer.py"
```
