# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of simple consumer. Acks each message as it arrives.
"""
import sys
import pika

# Import all adapters for easier experimentation
from pika.adapters import *

pika.log.setup(color=True)

connection = None
channel = None


def on_connected(connection):
    global channel
    pika.log.info("demo_receive: Connected to RabbitMQ")
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    channel = channel_
    pika.log.info("demo_receive: Received our Channel")
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False,
                          callback=on_queue_declared)


def on_queue_declared(frame):
    pika.log.info("demo_receive: Queue Declared")
    channel.basic_consume(handle_delivery, queue='test')


def handle_delivery(channel, method_frame, header_frame, body):
    pika.log.info("Basic.Deliver %s delivery-tag %i: %s",
                  header_frame.content_type,
                  method_frame.delivery_tag,
                  body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

if __name__ == '__main__':
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    parameters = pika.ConnectionParameters(host)
    connection = SelectConnection(parameters, on_connected)
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
