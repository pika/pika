TLS parameters example
======================

This example demonstrates a TLS session with RabbitMQ using mutual authentication (server and client authentication). It was tested against RabbitMQ 3.7.4, using Python 3.6.5 and Pika 1.0.0.

See `the RabbitMQ TLS/SSL documentation <https://www.rabbitmq.com/ssl.html>`_ for certificate generation and RabbitMQ TLS configuration. Please note that the `RabbitMQ TLS (x509 certificate) authentication mechanism <https://github.com/rabbitmq/rabbitmq-auth-mechanism-ssl>`_ must be enabled for these examples to work.

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
        ch.basic_publish("", "foobar", "Hello, world!")
        print(ch.basic_get("foobar"))

rabbitmq.config::

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


To perform mutual authentication with a Twisted connection::

    from pika import ConnectionParameters
    from pika.adapters import twisted_connection
    from pika.credentials import ExternalCredentials

    from twisted.internet import defer, protocol, ssl, reactor

    @defer.inlineCallbacks
    def publish(connection):
        channel = yield connection.channel()
        yield channel.basic_publish(
            exchange='amq.topic',
            routing_key='hello.world',
            body='Hello World!',
        )
        print("published")

    # Load the CA certificate to validate the server's identity
    with open("PIKA_DIR/testdata/certs/ca_certificate.pem") as fd:
        ca_cert = ssl.Certificate.loadPEM(fd.read())

    # Load the client certificate and key to authenticate with the server
    with open("PIKA_DIR/testdata/certs/client_key.pem") as fd:
        client_key = fd.read()
    with open("PIKA_DIR/testdata/certs/client_certificate.pem"") as fd:
        client_cert = fd.read()
    client_keypair = ssl.PrivateCertificate.loadPEM(client_key + client_cert)

    context_factory = ssl.optionsForClientTLS(
        "localhost",
        trustRoot=ca_cert,
        clientCertificate=client_keypair,
    )
    params = ConnectionParameters(credentials=ExternalCredentials())
    cc = protocol.ClientCreator(
        reactor, twisted_connection.TwistedProtocolConnection, params)
    deferred = cc.connectSSL("localhost", 5671, context_factory)
    deferred.addCallback(lambda protocol: protocol.ready)
    deferred.addCallback(publish)
    reactor.run()
