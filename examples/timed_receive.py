# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Example of simple consumer. Acks each message as it arrives, timing the
receive rate
"""

import sys
import time

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

from pika.adapters import SelectConnection
from pika.connection import ConnectionParameters

# We use these to hold our connection & channel
connection = None
channel = None

count = 0
last_count = 0
start_time = None


def on_connected(connection):
    global channel
    print "timed_receive: Connected to RabbitMQ"
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    channel = channel_
    print "timed_receive: Received our Channel"
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False,
                          callback=on_queue_declared)


def on_queue_declared(frame):
    global start_time
    print "timed_receive: Queue Declared"
    start_time = time.time()
    channel.basic_consume(handle_delivery, queue='test', no_ack=True)


def handle_delivery(channel, method, header, body):
    global count, last_count, start_time
    count += 1
    if not count % 1000:
        now = time.time()
        duration = now - start_time
        sent = count - last_count
        rate = sent / duration
        last_count = count
        start_time = now
        print "timed_receive: %i Messages Received, %.4f per second" %\
              (count, rate)


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
