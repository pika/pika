# TLS parameters example
This examples demonstrates a TLS session with RabbitMQ using server authentication.

Note the use of `ssl.PROTOCOL_TLSv1_2`. The recent versions of RabbitMQ disable older versions of SSL due to security vulnerabilities.

See [TLS Support](https://www.rabbitmq.com/ssl.html) for certificate creation and rabbitmq SSL configuration instructions.

`examples/tls_server_authentication.py`:

```python
--8<-- "examples/tls_server_authentication.py"
```
rabbitmq.conf:

```text
%% In this example, both the client and RabbitMQ server are assumed to be running on the same machine
%% with a self-signed set of certificates generated using https://github.com/rabbitmq/tls-gen.
%%
%% To find out the default rabbitmq.conf location, see https://www.rabbitmq.com/configure.html.
%%
%% The contents of the example config file are for demonstration purposes only.
%% See https://www.rabbitmq.com/ssl.html to learn how to use TLS for client connections in RabbitMQ.
%%
%% The example below allows clients without a certificate to connect
%% but performs peer verification on those that present a certificate chain.

listeners.ssl.default = 5671

ssl_options.cacertfile = /Users/me/tls-gen/basic/result/ca_certificate.pem
ssl_options.certfile = /Users/me/tls-gen/basic/result/server_certificate.pem
ssl_options.keyfile = /Users/me/tls-gen/basic/result/server_key.pem
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = false
```
