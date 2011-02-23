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


def on_connected(connection):
    pika.log.info("demo_send: Connected to RabbitMQ")
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    channel = channel_
    pika.log.info("demo_send: Received our Channel")
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False,
                          callback=on_queue_declared)


def on_queue_declared(frame):
    pika.log.info("demo_send: Queue Declared")
    for x in xrange(0, 10):
        message = "Hello World #%i: %.8f" % (x, time.time())
        pika.log.info("Sending: %s" % message)
        channel.basic_publish(exchange='',
                              routing_key="test",
                              body=message,
                              properties=pika.BasicProperties(
                                content_type="text/plain",
                                delivery_mode=1))

    # Close our connection
    connection.close()

if __name__ == '__main__':
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    parameters = pika.ConnectionParameters(host)
    connection = SelectConnection(parameters, on_connected)
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
