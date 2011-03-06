# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of simple producer, creates one message and exits.
"""
import sys
import pika
import time

pika.log.setup(color=True)

connection = None
channel = None

# Import all adapters for easier experimentation
from pika.adapters import *

message_id = 1
SEND_QTY = 10


def on_connected(connection):
    print "demo_send: Connected to RabbitMQ"
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    channel = channel_
    print "demo_send: Received our Channel"
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False,
                          callback=on_queue_declared)


def send_message(id):
    global channel
    message = "Hello World #%i" % id
    print"Sending: %s" % message
    channel.basic_publish(exchange='',
                          routing_key="test",
                          body=message,
                          properties=pika.BasicProperties(
                            timestamp=time.time(),
                            user_id='guest',
                            content_type="text/plain",
                            delivery_mode=1))


def on_delivered(frame):
    global message_id

    print "Received delivery confirmation: %r" % frame

    message_id += 1
    if message_id > SEND_QTY:
        connection.close()
    send_message(message_id)


def on_queue_declared(frame):
    print "demo_send: Queue Declared"
    channel.confirm_delivery(on_delivered)
    send_message(1)


if __name__ == '__main__':
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    parameters = pika.ConnectionParameters(host)
    connection = SelectConnection(parameters, on_connected)
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
