import logging
import ssl
from pathlib import Path

import pika

logging.basicConfig(level=logging.INFO)

_certs = Path(__file__).resolve().parent.parent / 'tests' / 'certs'

context = ssl.create_default_context(cafile=str(_certs / 'ca_certificate.pem'))
context.verify_mode = ssl.CERT_REQUIRED
context.load_cert_chain(str(_certs / 'client_certificate.pem'),
                        str(_certs / 'client_key.pem'))
ssl_options = pika.SSLOptions(context, "localhost")
conn_params = pika.ConnectionParameters(port=5671, ssl_options=ssl_options)

with pika.BlockingConnection(conn_params) as conn:
    ch = conn.channel()
    ch.queue_declare("foobar")
    ch.basic_publish("", "foobar", b"Hello, world!")
    print(ch.basic_get("foobar"))
