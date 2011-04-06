#!/usr/bin/env python
# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of the use of basic_get. NOT RECOMMENDED for fast consuming - use
basic_consume instead if at all possible!
"""
import sys

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

from pika.adapters import SelectConnection
from pika.connection import ConnectionParameters

# We use these to hold our connection & channel
connection = None
channel = None


def on_connected(connection):
    global channel
    print "demo_get: Connected to RabbitMQ"
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    print "demo_get: Received our Channel"
    channel = channel_
    channel.queue_declare(queue="test",
                          durable=True,
                          exclusive=False,
                          auto_delete=False,
                          callback=on_queue_declared)


def on_queue_declared(frame):
    print "demo_get: Queue Declared"
    connection.add_timeout(1, basic_get)


def basic_get():
    print "demo_get: Invoking Basic.Get"
    channel.basic_get(callback=handle_delivery, queue="test")
    connection.add_timeout(1, basic_get)


def handle_delivery(channel, method_frame, header_frame, body):
    print "demo_get: Basic.GetOk %s delivery-tag %i: %s" %\
          (header_frame.content_type,
           method_frame.delivery_tag,
           body)

    # Ack the message
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


if __name__ == '__main__':

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    connection = SelectConnection(ConnectionParameters(host),
                                  on_connected)
    # Loop until CTRL-C
    try:
        # Start our blocking loop
        connection.ioloop.start()

    except KeyboardInterrupt:

        # Close the connection
        connection.close()

        # Loop until the connection is closed
        connection.ioloop.start()
