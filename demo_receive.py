# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of simple consumer. Acks each message as it arrives.
"""
import logging
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
    print "demo_receive: Connected to RabbitMQ"
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    channel = channel_
    print "demo_receive: Received our Channel"
    channel.queue_declare(queue="test",
                          durable=True,
                          exclusive=False,
                          auto_delete=False,
                          callback=on_queue_declared)


def on_queue_declared(frame):
    print "demo_receive: Queue Declared"
    channel.basic_consume(handle_delivery, queue='test')


def handle_delivery(channel, method_frame, header_frame, body):

    print method_frame.delivery_tag
    print body
    print

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(levelname) -10s %(asctime)s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s')

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    connection = SelectConnection(ConnectionParameters(host,
                                                       port=5672,
                                                       heartbeat_interval=30,
                                                       ssl=False),
                                   on_connected,
                                   True)
    # Loop until CTRL-C
    try:
        # Start our blocking loop
        connection.ioloop.start()

    except KeyboardInterrupt:

        # Close the connection
        connection.close()

        # Loop until the conneciton is closed
        connection.ioloop.start()
