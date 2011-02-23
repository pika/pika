# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Example of simple consumer. Acks each message as it arrives.
"""

import pika
import sys
import time

# Import all adapters for easier experimentation
from pika.adapters import *

pika.log.setup(color=True)

channel = None
connection = None
count = 0
last_count = 0
start_time = None


def on_connected(connection):
    global channel
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
    global start_time
    pika.log.info("demo_send: Queue Declared")
    start_time = time.time()
    channel.basic_consume(handle_delivery, queue='test', no_ack=True)


def handle_delivery(channel, method, header, body):
    global count, last_count, start_time
    count += 1
    if count % 1000 == 0:
        now = time.time()
        duration = now - start_time
        sent = count - last_count
        rate = sent / duration
        last_count = count
        start_time = now
        pika.log.info("timed_receive: %i Messages Received, %.4f per second",
                      count, rate)


if __name__ == '__main__':
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    parameters = pika.ConnectionParameters(host)
    connection = SelectConnection(parameters, on_connected)
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
