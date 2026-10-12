# TLS parameters example
This example demonstrates a TLS session with RabbitMQ using server authentication.

See [TLS Support](https://www.rabbitmq.com/ssl.html) for certificate creation and RabbitMQ TLS configuration instructions.

`examples/tls_server_authentication.py`:

```python
--8<-- "examples/tls_server_authentication.py"
```
The example reads the certificates in `tests/certs/`, which are the ones the repository's own CI configuration gives the broker, so it runs against a broker started with that configuration:

```bash
hatch run rabbitmq-tls
```

That configuration, `.ci/linux/rabbitmq.conf`, included here in full so it cannot drift from the file:

```text
--8<-- ".ci/linux/rabbitmq.conf"
```

Note `ssl_options.fail_if_no_peer_cert = true`. It requires every TLS client to present a certificate chain, so against this broker even a server-authentication example has to load a client certificate, which is why the code above does. Server authentication on its own, verifying the broker and presenting nothing, needs `fail_if_no_peer_cert = false`; set that and the `load_cert_chain` call becomes unnecessary.

To use your own certificates instead, generate a set with [tls-gen](https://github.com/rabbitmq/tls-gen) and point both the config above and the example at them. See [configure](https://www.rabbitmq.com/configure.html) for where `rabbitmq.conf` lives on your system.
