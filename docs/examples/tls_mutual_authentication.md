# TLS parameters example

This example demonstrates a TLS session with RabbitMQ using mutual authentication (server and client authentication).

See [the RabbitMQ TLS/SSL documentation](https://www.rabbitmq.com/ssl.html) for certificate generation and RabbitMQ TLS configuration.

Both examples read the certificates in `tests/certs/`, which are the ones the repository's own CI configuration gives the broker, so they run against a broker started with that configuration:

```bash
hatch run rabbitmq-tls
```

`examples/tls_mutual_authentication.py`:

```python
--8<-- "examples/tls_mutual_authentication.py"
```

That configuration, `.ci/linux/rabbitmq.conf`, included here in full so it cannot drift from the file:

```text
--8<-- ".ci/linux/rabbitmq.conf"
```

The example above authenticates as *guest* over PLAIN and needs nothing more than that. Authenticating as the client certificate instead is a separate feature: it needs the [RabbitMQ TLS (x509 certificate) authentication mechanism](https://github.com/rabbitmq/rabbitmq-auth-mechanism-ssl) enabled, `EXTERNAL` among the broker's `auth_mechanisms`, and a user named for the certificate's distinguished name. The Twisted example below does that, and its docstring spells out the requirements:

```python
--8<-- "examples/tls_mutual_authentication_twisted.py"
```
