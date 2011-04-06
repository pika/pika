# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of simple publisher using SSL, sends 10 messages and exits
"""
import sys
import time

from os.path import exists

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

from pika.adapters import SelectConnection
from pika.connection import ConnectionParameters
from pika import BasicProperties

# We use these to hold our connection & channel
connection = None
channel = None


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


def on_queue_declared(frame):
    print "demo_send: Queue Declared"
    for x in xrange(0, 10):
        message = "Hello World #%i: %.8f" % (x, time.time())

        # Create properties with when we sent the message, the app_id
        # user we connected with, a content type and non persisted messages
        properties = BasicProperties(timestamp=time.time(),
                                     app_id=__file__,
                                     user_id='guest',
                                     content_type="text/plain",
                                     delivery_mode=1)

        # Send the message
        channel.basic_publish(exchange='',
                              routing_key="test",
                              body=message,
                              properties=properties)

        print "demo_send: Sent %s" % message

    # Close our connection
    print "demo_send: Closing"
    connection.close()


if __name__ == '__main__':

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    connection = SelectConnection(ConnectionParameters(host, 5671),
                                  on_connected,
                                  ssl=True)
    # Loop until CTRL-C
    try:
        # Start our blocking loop
        connection.ioloop.start()

    except KeyboardInterrupt:

        # Close the connection
        connection.close()

        # Loop until the connection is closed
        connection.ioloop.start()
