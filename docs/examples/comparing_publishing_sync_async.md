# Comparing Message Publishing with BlockingConnection and SelectConnection

For those doing simple, non-asynchronous programming, `pika.adapters.blocking_connection.BlockingConnection` proves to be the easiest way to get up and running with Pika to publish messages.

In the following example, a connection is made to RabbitMQ listening to port *5672* on *localhost* using the username *guest* and password *guest* and virtual host */*. Once connected, a channel is opened and a message is published to the *test_exchange* exchange using the *test_routing_key* routing key. The BasicProperties value passed in sets the message to delivery mode *1* (non-persisted) with a content-type of *text/plain*. Once the message is published, the connection is closed:

```python
--8<-- "examples/comparing_publishing_sync.py"
```

In contrast, using `pika.adapters.select_connection.SelectConnection` and the other asynchronous adapters is more complicated and less pythonic, but when used with other asynchronous services can have tremendous performance improvements. In the following code example, all of the same parameters and values are used as were used in the previous example:

```python
--8<-- "examples/comparing_publishing_async.py"
```
