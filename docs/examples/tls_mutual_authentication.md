# TLS parameters example

This example demonstrates a TLS session with RabbitMQ using mutual authentication (server and client authentication).

See [the RabbitMQ TLS/SSL documentation](https://www.rabbitmq.com/ssl.html) for certificate generation and RabbitMQ TLS configuration. Please note that the [RabbitMQ TLS (x509 certificate) authentication mechanism](https://github.com/rabbitmq/rabbitmq-auth-mechanism-ssl) must be enabled for these examples to work.

`examples/tls_mutual_authentication.py`:

```python
--8<-- "examples/tls_mutual_authentication.py"
```
rabbitmq.config:

```text
# Enable AMQPS
listeners.ssl.default = 5671
ssl_options.cacertfile = PIKA_DIR/testdata/certs/ca_certificate.pem
ssl_options.certfile = PIKA_DIR/testdata/certs/server_certificate.pem
ssl_options.keyfile = PIKA_DIR/testdata/certs/server_key.pem
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true

# Enable HTTPS
management.listener.port = 15671
management.listener.ssl = true
management.listener.ssl_opts.cacertfile = PIKA_DIR/testdata/certs/ca_certificate.pem
management.listener.ssl_opts.certfile = PIKA_DIR/testdata/certs/server_certificate.pem
management.listener.ssl_opts.keyfile = PIKA_DIR/testdata/certs/server_key.pem


```
To perform mutual authentication with a Twisted connection:

```python
--8<-- "examples/tls_mutual_authentication_twisted.py"
```
