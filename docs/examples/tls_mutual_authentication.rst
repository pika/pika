TLS parameters example
======================

This example demonstrates a TLS session with RabbitMQ using mutual authentication (server and client authentication). It was tested against RabbitMQ 3.7.4, using Python 3.6.5 and Pika 1.0.0b1.

See https://www.rabbitmq.com/ssl.html for certificate generation and RabbitMQ TLS configuration.

tls_example.py::

    import logging
    import pika
    import ssl

    logging.basicConfig(level=logging.INFO)
    context = ssl.create_default_context(
        cafile="PIKA_DIR/testdata/certs/ca_certificate.pem")
    context.load_cert_chain("PIKA_DIR/testdata/certs/client_certificate.pem",
                            "PIKA_DIR/testdata/certs/client_key.pem")
    ssl_options = pika.SSLOptions(context, "localhost")
    conn_params = pika.ConnectionParameters(port=5671,
                                            ssl_options=ssl_options)
    
    with pika.BlockingConnection(conn_params) as conn:
        ch = conn.channel()
        ch.queue_declare("foobar")
        ch.publish("", "foobar", "Hello, world!")
        print(ch.basic_get("foobar"))

rabbitmq.config::

    # Enable A.M.Q.P.S.
    listeners.ssl.default = 5671
    ssl_options.cacertfile = PIKA_DIR/testdata/certs/ca_certificate.pem
    ssl_options.certfile = PIKA_DIR/testdata/certs/server_certificate.pem
    ssl_options.keyfile = PIKA_DIR/testdata/certs/server_key.pem
    ssl_options.verify = verify_peer
    ssl_options.fail_if_no_peer_cert = true

    # Enable H.T.T.P.S.
    management.listener.port = 15671
    management.listener.ssl = true
    management.listener.ssl_opts.cacertfile = PIKA_DIR/testdata/certs/ca_certificate.pem
    management.listener.ssl_opts.certfile = PIKA_DIR/testdata/certs/server_certificate.pem
    management.listener.ssl_opts.keyfile = PIKA_DIR/testdata/certs/server_key.pem
