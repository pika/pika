TLS parameters example
=============================
This examples demonstrates a TLS session with RabbitMQ using server authentication.

It was tested against RabbitMQ 3.6.10, using Python 3.6.1 and pre-release Pika `0.11.0`

Note the use of `ssl_version=ssl.PROTOCOL_TLSv1`. The recent versions of RabbitMQ disable older versions of
SSL due to security vulnerabilities.

See https://www.rabbitmq.com/ssl.html for certificate creation and rabbitmq SSL configuration instructions.


tls_example.py::

    import ssl
    import pika
    import logging

    logging.basicConfig(level=logging.INFO)

    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations('/Users/me/tls-gen/basic/testca/cacert.pem')

    cp = pika.ConnectionParameters(ssl_options=pika.SSLOptions(context))

    conn = pika.BlockingConnection(cp)
    ch = conn.channel()
    print(ch.queue_declare("sslq"))
    ch.publish("", "sslq", "abc")
    print(ch.basic_get("sslq"))


rabbitmq.config::

    %% Both the client and rabbitmq server were running on the same machine, a MacBookPro laptop.
    %%
    %% rabbitmq.config was created in its default location for OS X: /usr/local/etc/rabbitmq/rabbitmq.config.
    %%
    %% The contents of the example rabbitmq.config are for demonstration purposes only. See https://www.rabbitmq.com/ssl.html for instructions about creating the test certificates and the contents of rabbitmq.config.
    %%
    %% Note that the {fail_if_no_peer_cert,false} option, states that RabbitMQ should accept clients that don't have a certificate to send to the broker, but through the {verify,verify_peer} option, we state that if the client does send a certificate to the broker, the broker must be able to establish a chain of trust to it.

    [
      {rabbit,
        [
          {ssl_listeners, [{"127.0.0.1", 5671}]},

          %% Configuring SSL.
          %% See http://www.rabbitmq.com/ssl.html for full documentation.
          %%
          {ssl_options, [{cacertfile,           "/Users/me/tls-gen/basic/testca/cacert.pem"},
                         {certfile,             "/Users/me/tls-gen/basic/server/cert.pem"},
                         {keyfile,              "/Users/me/tls-gen/basic/server/key.pem"},
                         {verify,               verify_peer},
                         {fail_if_no_peer_cert, false}]}
        ]
      }
    ].
