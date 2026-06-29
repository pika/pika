# Pika Examples

## Prerequisites

All examples require a running RabbitMQ broker. Start one with Docker:

```bash
hatch run rabbitmq
```

This starts a RabbitMQ container with the management plugin on ports 5672 (AMQP) and 15672 (HTTP management UI, guest/guest).

## Running examples

Use the `examples` hatch environment, which includes all optional dependencies (Twisted, Tornado, etc.):

```bash
hatch run examples:run blocking_consume.py
```

Or run directly with Python if you only need pika's core dependencies:

```bash
python examples/blocking_consume.py
```

## TLS examples

The TLS mutual authentication examples use test certificates from `tests/certs/`. To run them, you need a RabbitMQ broker configured for TLS. See the [RabbitMQ TLS documentation](https://www.rabbitmq.com/ssl.html) for setup instructions.

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
| `basic_consumer_threaded.py` | Consuming with `ThreadSafeConnection` worker threads |
| `basic_publisher_threaded.py` | Publishing from multiple threads with `ThreadSafeConnection` |
| `publisher_confirms_threaded.py` | Publisher confirms with `ThreadSafeConnection` (ack/nack on a worker thread) |
| `long_running_publisher.py` | Background publisher thread with `BlockingConnection` |
| `long_running_publisher_threaded.py` | Long-running publisher with `ThreadSafeConnection` (no `process_data_events` loop) |
| `consumer_queued.py` | Threaded consumer buffering messages for batch processing |
| `consumer_simple.py` | Simple blocking consumer with multiple exchanges |
| `producer.py` | Blocking publisher for multiple exchanges |

### TLS

| File | Description |
|------|-------------|
| `tls_server_authentication.py` | TLS with server certificate verification |
| `tls_mutual_authentication.py` | Mutual TLS using test certificates from `tests/certs/` |
| `tls_mutual_authentication_twisted.py` | Mutual TLS with Twisted adapter |
| `blocking_consume_tls.py` | Blocking consumer over TLS |

### Other

| File | Description |
|------|-------------|
| `direct_reply_to.py` | RPC using RabbitMQ's direct reply-to feature |
| `publish.py` | Publish with exchange declaration and delivery confirmation |
| `using_urlparameters.py` | Constructing AMQP URLs with `ssl_options` |
| `async_as_callback.py` | Asyncio integration with pydantic models and msgpack serialization |
