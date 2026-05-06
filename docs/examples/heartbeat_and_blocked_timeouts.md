# Ensuring well-behaved connection with heartbeat and blocked-connection timeouts

This example demonstrates explicit setting of heartbeat and blocked connection timeouts.

Starting with RabbitMQ 3.5.5, the broker's default heartbeat timeout decreased from 580 seconds to 60 seconds. As a result, applications that perform lengthy processing in the same thread that also runs their Pika connection may experience unexpected dropped connections due to heartbeat timeout. Here, we specify an explicit lower bound for heartbeat timeout.

When RabbitMQ broker is running out of certain resources, such as memory and disk space, it may block connections that are performing resource-consuming operations, such as publishing messages. Once a connection is blocked, RabbitMQ stops reading from that connection's socket, so no commands from the client will get through to the broker on that connection until the broker unblocks it. A blocked connection may last for an indefinite period of time, stalling the connection and possibly resulting in a hang (e.g., in BlockingConnection) until the connection is unblocked. Blocked Connection Timeout is intended to interrupt (i.e., drop) a connection that has been blocked longer than the given timeout value.

Example of configuring heartbeat and blocked-connection timeouts:

```python
--8<-- "examples/heartbeat_and_blocked_timeouts.py"
```
