import pika
import ssl

context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.check_hostname = False
context.verify_mode=False

parameters = pika.ConnectionParameters(
    host="serverhostname.com",
    port=5671,
    heartbeat=150,
    ssl_options=pika.SSLOptions(context),
    virtual_host="rwgvqgbl",
    channel_max=10,
    credentials=pika.PlainCredentials('rwgvqgbl', 'password'),
    client_properties={'connection_name':'Pika connection with TLS, channel_max'})

def on_message(channel, method_frame, header_frame, body):
    print(method_frame.delivery_tag)
    print(body)
    print()
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='test')
channel.basic_consume('test', on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
