"""
Consuming over TLS when the broker's certificate does not name the address you dial.

This is the ordinary situation with a hosted broker: you connect to an IP address or a CNAME and the
certificate names something else, so hostname verification fails even though the certificate is
valid and signed by a CA you trust. The fix is to keep verification on and tell the context which
name to check, through `SSLOptions(context, server_hostname=...)`, rather than to switch
verification off.

`tests/certs/server_certificate.pem` names `shostakovich` and `localhost` and no IP address, so
dialing 127.0.0.1 reproduces the failure locally. Measured against this broker:

- dial 127.0.0.1 and verify that name: `SSLCertVerificationError`, IP address mismatch
- dial 127.0.0.1 and verify `localhost`: connects
- dial 127.0.0.1 and verify `bogus.example.com`: `SSLCertVerificationError`, hostname mismatch
- dial 127.0.0.1 with `context.check_hostname = False`: connects
- the same, trusting only the system CAs: `SSLCertVerificationError`, self-signed in chain

The second is what this example does. The third row is why "verification is still on" is a
measurement rather than a hope: a name the certificate does not carry is still refused. The fifth is
the same for the CA: `check_hostname = False` drops the name check and leaves `verify_mode` at
`CERT_REQUIRED`, so an untrusted chain is still refused. That asymmetry is the reason to prefer
either of them over `verify_mode = CERT_NONE`, which accepts anything at all.

The fourth is the blunter fallback: it accepts any name the CA has signed, which is sometimes all
you can do, but reach for `server_hostname` first.

A client certificate is loaded as well, because `.ci/linux/rabbitmq.conf` sets
`ssl_options.fail_if_no_peer_cert` and so this broker requires one from every TLS client. That is a
property of the broker, not of the problem shown here.
"""

import ssl
from pathlib import Path

import pika

_certs = Path(__file__).resolve().parent.parent / 'tests' / 'certs'

context = ssl.create_default_context(cafile=str(_certs / 'ca_certificate.pem'))
context.verify_mode = ssl.CERT_REQUIRED
context.load_cert_chain(str(_certs / 'client_certificate.pem'),
                        str(_certs / 'client_key.pem'))

parameters = pika.ConnectionParameters(
    # An address the certificate does not name.
    host='127.0.0.1',
    port=5671,
    heartbeat=150,
    # Verify against the name the certificate actually carries. Passing
    # '127.0.0.1' here instead, or letting it default to `host`, fails.
    ssl_options=pika.SSLOptions(context, server_hostname='localhost'),
    virtual_host='/',
    channel_max=10,
    credentials=pika.PlainCredentials('guest', 'guest'),
    client_properties={
        'connection_name': 'Pika connection with TLS, channel_max'
    })


def on_message(channel, method_frame, _header_frame, body):
    print(method_frame.delivery_tag)
    print(body)
    print()
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# `durable=True` is required by RabbitMQ 4.3; see examples/README.md.
channel.queue_declare(queue='test', durable=True)

# Publish one message so there is something for `on_message` to receive.
channel.basic_publish(exchange='', routing_key='test', body=b'Hello World!')

channel.basic_consume('test', on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
