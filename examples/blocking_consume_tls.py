import ssl
from pathlib import Path

import pika

# The certificates the test suite uses, so this runs against the broker the
# repository's own CI configuration starts. See `.ci/linux/rabbitmq.conf`,
# which sets `ssl_options.fail_if_no_peer_cert`, so a client certificate is
# not optional here.
_certs = Path(__file__).resolve().parent.parent / 'tests' / 'certs'

context = ssl.create_default_context(cafile=str(_certs / 'ca_certificate.pem'))
context.verify_mode = ssl.CERT_REQUIRED
context.load_cert_chain(str(_certs / 'client_certificate.pem'),
                        str(_certs / 'client_key.pem'))

parameters = pika.ConnectionParameters(
    host='localhost',
    port=5671,
    heartbeat=150,
    ssl_options=pika.SSLOptions(context, 'localhost'),
    virtual_host='/',
    channel_max=10,
    credentials=pika.PlainCredentials('guest', 'guest'),
    client_properties={
        'connection_name': 'Pika connection with TLS, channel_max'
    })


def on_message(channel, method_frame, header_frame, body):
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
