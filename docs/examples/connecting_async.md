# Connecting to RabbitMQ with Callback-Passing Style

When you connect to RabbitMQ with an asynchronous adapter, you are writing event oriented code. The connection adapter will block on the IOLoop that is watching to see when pika should read data from and write data to RabbitMQ. Because you're now blocking on the IOLoop, you will receive callback notifications when specific events happen.

## Example Code
In the example, there are five steps that take place:

1. Setup the connection to RabbitMQ
2. Start the IOLoop
3. Once connected, the on_open method will be called by Pika with a handle to
   the connection. In this method, a new channel will be opened on the connection.
4. Once the channel is opened, you can do your other actions, whether they be
   publishing messages, consuming messages or other RabbitMQ related activities.
   Here that means declaring the exchange to publish to.
5. Once the exchange is declared, publish, then close the connection. Closing it
   does not stop the IOLoop, so on_close does that and the process exits.

```python
--8<-- "examples/connecting_async.py"
```
