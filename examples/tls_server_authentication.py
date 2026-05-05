import ssl
import pika
import logging

logging.basicConfig(level=logging.INFO)

context = ssl.create_default_context(
    cafile="/Users/me/tls-gen/basic/result/ca_certificate.pem")
context.verify_mode = ssl.CERT_REQUIRED
context.load_cert_chain("/Users/me/tls-gen/result/client_certificate.pem",
                        "/Users/me/tls-gen/result/client_key.pem")
ssl_options = pika.SSLOptions(context, "localhost")
conn_params = pika.ConnectionParameters(port=5671, ssl_options=ssl_options)

with pika.BlockingConnection(conn_params) as conn:
    ch = conn.channel()
    print(ch.queue_declare("sslq"))
    ch.publish("", "sslq", "abc")
    print(ch.basic_get("sslq"))
