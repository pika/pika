# Using the Blocking Connection with connection recovery with multiple hosts

<a id="example_blocking_basic_consume_recover_multiple_hosts"></a>

RabbitMQ nodes can be [clustered](http://www.rabbitmq.com/clustering.html).
In the absence of failure clients can connect to any node and perform any operation.
In case a node fails, stops, or becomes unavailable, clients should be able to
connect to another node and continue.

To simplify reconnection to a different node, connection recovery mechanism
should be combined with connection configuration that specifies multiple hosts.

The BlockingConnection adapter relies on exception handling to check for
connection errors:

```python
--8<-- "examples/blocking_consume_recover_multiple_hosts.py"
```
Generic operation retry libraries such as [retry](https://github.com/invl/retry)
can prove useful.

To run the following example, install the library first with `pip install retry`.

In this example the `retry` decorator is used to set up recovery with delay:

```python
--8<-- "examples/blocking_consume_recover_multiple_hosts_retry.py"
```
