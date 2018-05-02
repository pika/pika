TLS parameters example
======================

This examples demonstrates a TLS session with RabbitMQ using mutual authentication. It was tested against RabbitMQ 3.7.4, using Python 3.6.5 and Pika 1.0.0b1.

See https://www.rabbitmq.com/ssl.html for certificate generation and RabbitMQ TLS configuration.

tls_example.py::

    import logging
    import pika
    import ssl

    logging.basicConfig(level=logging.INFO)
    context = ssl.create_default_context(
        cafile="/Users/me/tls-gen/basic/testca/cacert.pem")
    context.load_cert_chain("/Users/me/tls-gen/basic/client/cert.pem",
                            "/Users/me/tls-gen/basic/client/key.pem")
    server_hostname = "example.com"
    ssl_options = pika.SSLOptions(context=context,
                                  server_hostname=server_hostname)
    conn_params = pika.ConnectionParameters(ssl_options=ssl_options)
    
    with pika.BlockingConnection(conn_params) as conn:
        ch = conn.channel()
        print(ch.queue_declare("foobar"))
        ch.publish("", "foobar", "Hello, world!")
        print(ch.basic_get("foobar"))

rabbitmq.config::

    # Enable A.M.Q.P.S.
    listeners.ssl.default = 5671
    ssl_options.cacertfile = /Users/me/tls-gen/basic/testca/cacert.pem
    ssl_options.certfile = /Users/me/tls-gen/basic/server/cert.pem
    ssl_options.keyfile = /Users/me/tls-gen/basic/server/key.pem
    ssl_options.verify = verify_peer
    ssl_options.fail_if_no_peer_cert = true

    # Enable H.T.T.P.S.
    management.listener.port = 15671
    management.listener.ssl = true
    management.listener.ssl_opts.cacertfile = /Users/me/tls-gen/basic/testca/cacert.pem
    management.listener.ssl_opts.certfile = /Users/me/tls-gen/basic/server/cert.pem
    management.listener.ssl_opts.keyfile = /Users/me/tls-gen/basic/server/key.pem
