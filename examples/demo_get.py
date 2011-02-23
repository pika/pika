#!/usr/bin/env python
# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of the use of basic_get. NOT RECOMMENDED - use
basic_consume instead if at all possible!
"""
import pika
import sys

# Import all adapters for easier experimentation
from pika.adapters import *

pika.log.setup(color=True)

connection = None
channel = None


def on_connected(connection):
    global channel
    logging.info("demo_get: Connected to RabbitMQ")
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    logging.info("demo_get: Received our Channel")
    channel = channel_
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False,
                          callback=on_queue_declared)


def on_queue_declared(frame):
    logging.info("demo_get: Queue Declared")
    connection.add_timeout(1, basic_get)


def basic_get():
    logging.info("Invoking Basic.Get")
    channel.basic_get(callback=handle_delivery, queue="test")
    connection.add_timeout(1, basic_get)


def handle_delivery(channel, method_frame, header_frame, body):
    pika.log.info("Basic.GetOk %s delivery-tag %i: %s",
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
