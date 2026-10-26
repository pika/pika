# Pika Examples

## Prerequisites

All examples require a running RabbitMQ broker. Start one with Docker:

```bash
hatch run rabbitmq
```

This starts a RabbitMQ container with the management plugin on ports 5672 (AMQP) and 15672 (HTTP management UI, guest/guest).

If you intend to run the TLS examples too, start `rabbitmq-tls` instead and skip this one. It publishes 5672 and 15672 as well, so it serves every example, and both scripts name the container `pika-rabbitmq`, so running the second while the first is up fails with a name conflict.

## Running examples

Use the `examples` hatch environment, which includes all optional dependencies (Twisted, Tornado, etc.):

```bash
hatch run examples:run blocking_consume.py
```

Or run directly with Python if you only need pika's core dependencies:

```bash
python examples/blocking_consume.py
```

## Checking every example

CI runs every example on each pull request. To do the same, start the broker CI uses in place of `rabbitmq-tls`:

```bash
docker rm -f pika-rabbitmq
PIKA_CI_EXAMPLES=true .ci/linux/gha-setup.sh
hatch run examples:check
```

Between examples it deletes every queue, and every exchange the broker did not create, so point it at a throwaway broker. Pass example file names to run only those, against any broker the examples themselves need.

An example has to exit 0 by itself, unless it is listed in `SERVES` in `.ci/run_examples.py`; then it has to keep running until Ctrl-C and stop without a traceback.

## TLS examples

The TLS examples connect to port 5671 with the test certificates from `tests/certs/`, so they need a broker with a TLS listener using those same certificates. That is what `.ci/linux/rabbitmq.conf` configures, and `rabbitmq-tls` starts a container with it:

```bash
# Stop the plain broker first if you started one; both use this container name.
docker rm -f pika-rabbitmq
hatch run rabbitmq-tls
```

`tls_mutual_authentication_twisted.py` needs more than that, because it authenticates by client certificate rather than by password. Its docstring lists what the broker also has to have: the `rabbitmq_auth_mechanism_ssl` plugin, `EXTERNAL` among `auth_mechanisms`, and a user named for the certificate's distinguished name. The broker under "Checking every example" has all three.

## Queue durability and RabbitMQ 4.3

The examples declare queues that are either durable or exclusive. This is not incidental, and it is why several of them carry a `durable=True` that points here.

RabbitMQ 4.3 and later refuse a queue that is both non-durable and non-exclusive, because the `transient_nonexcl_queues` deprecated feature moved to denied-by-default. pika's `queue_declare` still defaults to `durable=False`, so the naive `queue_declare('name')` now fails. It fails hard: the broker answers 541 and closes the **connection**, not just the channel.

```text
Feature `transient_nonexcl_queues` is deprecated.
By default, this feature is not permitted anymore.
```

4.3 is where that default changed, not 4.0, so a queue declaration that works against an older 4.x broker can still fail here. The same declare was measured against the `rabbitmq:4.N-management-alpine` images:

| broker | `queue_declare('q')`, no flags |
|---|---|
| 4.0 | accepted |
| 4.1 | accepted |
| 4.2.9 | accepted |
| 4.3.6 | refused, 541 |

Either flag is enough on its own. `durable=True` is the usual answer; `exclusive=True` suits a queue that should disappear with its connection, and `blocking_consume_recover_multiple_hosts_retry.py` uses it to stay non-durable on purpose.

## Examples

### Blocking connection

| File | Description |
|------|-------------|
| `blocking_basic_get.py` | Synchronous get of a single message with `basic_get` |
| `blocking_consume.py` | Blocking consumer with `basic_consume` and manual ack |
| `blocking_consumer_generator.py` | Using `BlockingChannel.consume` as a generator |
| `blocking_delivery_confirmations.py` | Publisher confirms with `BlockingConnection` |
| `blocking_publish_mandatory.py` | Mandatory publishing with unroutable message handling |
| `blocking_consume_recover_multiple_hosts.py` | Connection recovery across multiple brokers |
| `blocking_consume_recover_multiple_hosts_retry.py` | Multi-host recovery using the `retry` library |
| `blocking_consume_tls.py` | Blocking consumer with TLS |
| `comparing_publishing_sync.py` | Simple synchronous publish |

### Asynchronous adapters

| File | Description |
|------|-------------|
| `asynchronous_consumer_example.py` | Full-featured async consumer with reconnection logic |
| `asynchronous_publisher_example.py` | Full-featured async publisher with reconnection logic |
| `asyncio_consumer_example.py` | Async consumer using `AsyncioConnection` |
| `comparing_publishing_async.py` | Simple async publish with `SelectConnection` |
| `connecting_async.py` | Minimal async connection and publish |
| `confirmation.py` | Publisher confirms with `SelectConnection` |
| `consume.py` | Async consumer with exchange/queue setup |
| `heartbeat_and_blocked_timeouts.py` | Configuring heartbeat and blocked connection timeouts |
| `tornado_consumer.py` | Async consumer using `TornadoConnection` |
| `twisted_service.py` | RPC service using Twisted (`twistd -ny twisted_service.py`) |

### Thread-safe connection

| File | Description |
|------|-------------|
| `basic_consumer_threaded.py` | Consuming with `Connection` worker threads |
| `basic_publisher_threaded.py` | Publishing from multiple threads with `Connection` |
| `publisher_confirms_threaded.py` | Publisher confirms with `Connection` (ack/nack on a worker thread) |
| `long_running_publisher.py` | Background publisher thread with `BlockingConnection` |
| `long_running_publisher_threaded.py` | Long-running publisher with `Connection` (no `process_data_events` loop) |
| `consumer_queued.py` | Threaded consumer buffering messages for batch processing |
| `consumer_simple.py` | Simple blocking consumer with multiple exchanges |
| `producer.py` | Blocking publisher for multiple exchanges |

### TLS

| File | Description |
|------|-------------|
| `tls_mutual_authentication.py` | Mutual TLS using test certificates from `tests/certs/` |
| `tls_mutual_authentication_twisted.py` | Mutual TLS with Twisted adapter |
| `blocking_consume_tls.py` | Blocking consumer over TLS, where the certificate does not name the address dialed |

### Other

| File | Description |
|------|-------------|
| `direct_reply_to.py` | RPC using RabbitMQ's direct reply-to feature |
| `publish.py` | Publish with exchange declaration and delivery confirmation |
| `using_urlparameters.py` | Constructing AMQP URLs with `ssl_options` (connects over TLS, so it needs the broker from the TLS section) |
| `async_as_callback.py` | Asyncio integration with pydantic models and msgpack serialization |
