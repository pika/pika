# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of publisher that waits for messages to be confirmed before delivering
the next message. This makes use of the new Confirm.Select functionality in
RabbitMQ 2.3.1
"""
import sys
import time

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

from pika import BasicProperties
from pika.connection import ConnectionParameters
from pika.adapters import SelectConnection

# We use these to hold our connection & channel
connection = None
channel = None

# Our message counter
message_id = 1

# Our acked counter
acked      = 0

# Send up to this many messages
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
    print 'demo_send: Sending "%s"' % message
    channel.basic_publish(exchange='',
                          routing_key="test",
                          body=message,
                          properties=BasicProperties(timestamp=time.time(),
                                                     app_id=__file__,
                                                     user_id='guest',
                                                     content_type="text/plain",
                                                     delivery_mode=1))


def on_delivered(frame):
    global message_id, acked

    if frame.method.NAME == 'Confirm.SelectOk':
        print "demo_send: Confirm.SelectOk Received"
        send_message(message_id)
        return

    success = frame.method.NAME == 'Basic.Ack'

    for dtag in range(acked+1, frame.method.delivery_tag+1):
        print "demo_send: Received delivery confirmation for message_id %d, success: %s" %(dtag, success)
    acked = frame.method.delivery_tag

    message_id += 1
    if message_id > SEND_QTY:
        connection.close()
    send_message(message_id)


def on_queue_declared(frame):
    print "demo_send: Queue Declared"
    channel.confirm_delivery(on_delivered)


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
