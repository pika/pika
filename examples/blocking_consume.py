import pika


def on_message(channel, method_frame, header_frame, body):
    print(method_frame.delivery_tag)
    print(body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


connection = pika.BlockingConnection()
channel = connection.channel()
channel.basic_consume('test', on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
