TLS parameters example
=============================
This examples demonstrates a TLS session with RabbitMQ using server authentication.

It was tested against RabbitMQ 3.6.10, using Python 3.6.1 and pre-release Pika `0.11.0`

Note the use of `ssl.PROTOCOL_TLSv1_2`. The recent versions of RabbitMQ disable older versions of
SSL due to security vulnerabilities.

See https://www.rabbitmq.com/ssl.html for certificate creation and rabbitmq SSL configuration instructions.


tls_example.py::

    import ssl
    import pika
    import logging

    logging.basicConfig(level=logging.INFO)

    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations('/Users/me/tls-gen/basic/testca/cacert.pem')

    cp = pika.ConnectionParameters(ssl_options=pika.SSLOptions(context))

    conn = pika.BlockingConnection(cp)
    ch = conn.channel()
    print(ch.queue_declare("sslq"))
    ch.publish("", "sslq", "abc")
    print(ch.basic_get("sslq"))


rabbitmq.conf::

    %% In this example, both the client and RabbitMQ server are assumed to be running on the same machine
    %% with a self-signed set of certificates generated using https://github.com/michaelklishin/tls-gen.
    %%
    %% To find out the default rabbitmq.conf location, see https://www.rabbitmq.com/configure.html.
    %%
    %% The contents of the example config file are for demonstration purposes only.
    %% See https://www.rabbitmq.com/ssl.html to learn how to use TLS for client connections in RabbitMQ.
    %%
    %% The example below allows clients without a certificate to connect
    %% but performs peer verification on those that present a certificate chain.

    listeners.ssl.default = 5671

    ssl_options.cacertfile = /Users/me/tls-gen/basic//ca_certificate.pem
    ssl_options.certfile = /Users/me/tls-gen/basic//server_certificate.pem
    ssl_options.keyfile = /Users/me/tls-gen/basic/server_key.pem
    ssl_options.verify = verify_peer
    ssl_options.fail_if_no_peer_cert = false
