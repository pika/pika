TLS parameters example
=============================
This examples demonstrates a TLS session with RabbitMQ using mutual authentication.

It was tested against RabbitMQ 3.6.0, using pre-release pika v 0.10.1.

Note the use of `ssl_version=ssl.PROTOCOL_TLSv1`. The recent verions of RabbitMQ disable older versions of
SSL due to security vulnerabilities.

See https://www.rabbitmq.com/ssl.html for certificate creation and rabbitmq SSL configuration instructions.


tls_example.py::

    import ssl
    import pika


    cp = pika.ConnectionParameters(
        ssl=True,
        ssl_options=dict(
            ssl_version=ssl.PROTOCOL_TLSv1,
            ca_certs="/Users/me/rabbitmqcert/testca/cacert.pem",
            keyfile="/Users/me/rabbitmqcert/client/key.pem",
            certfile="/Users/me/rabbitmqcert/client/cert.pem",
            cert_reqs=ssl.CERT_REQUIRED))

    conn = pika.BlockingConnection(cp)
    ch = conn.channel()
    ch.queue_declare("sslq")
    Out[10]: <METHOD(['channel_number=1', 'frame_type=1', "method=<Queue.DeclareOk(['consumer_count=0', 'message_count=0', 'queue=sslq'])>"])>

    ch.publish("", "sslq", "abc")
    ch.basic_get("sslq")
    Out[17]:
    (<Basic.GetOk(['delivery_tag=1', 'exchange=', 'message_count=0', 'redelivered=False', 'routing_key=sslq'])>,
     <BasicProperties>,
     'abc')


rabbitmq.config::

    %% Both the client and rabbitmq server were running on the same machine, a MacBookPro laptop.
    %%
    %% rabbitmq.config was created in its default location for OS X: /usr/local/etc/rabbitmq/rabbitmq.config.
    %%
    %% The contents of the example rabbitmq.config are for demonstration purposes only. See https://www.rabbitmq.com/ssl.html for instructions about creating the test certificates and the contents of rabbitmq.config.


    [
      {rabbit,
        [
          {ssl_listeners, [{"127.0.0.1", 5671}]},

          %% Configuring SSL.
          %% See http://www.rabbitmq.com/ssl.html for full documentation.
          %%
          {ssl_options, [{cacertfile,           "/Users/me/rabbitmqcert/testca/cacert.pem"},
                         {certfile,             "/Users/me/rabbitmqcert/server/cert.pem"},
                         {keyfile,              "/Users/me/rabbitmqcert/server/key.pem"},
                         {verify,               verify_peer},
                         {fail_if_no_peer_cert, true}]}
        ]
      }
    ].
