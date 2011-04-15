# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of simple publisher using SSL, sends 10 messages and exits
"""
from ssl import CERT_REQUIRED, PROTOCOL_SSLv3
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
    # Setup empty ssl options
    ssl_options = {}

    # Uncomment this to test client certs, change to your cert paths
    # Uses certs as generated from http://www.rabbitmq.com/ssl.html
    ssl_options = {"ca_certs": "/etc/rabbitmq/new/server/chain.pem",
                   "certfile": "/etc/rabbitmq/new/client/cert.pem",
                   "keyfile": "/etc/rabbitmq/new/client/key.pem",
                   "cert_reqs": CERT_REQUIRED}

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    parameters = ConnectionParameters(host, 5671,
                                      ssl=True, ssl_options=ssl_options)
    connection = SelectConnection(parameters, on_connected)
    # Loop until CTRL-C
    try:
        # Start our blocking loop
        connection.ioloop.start()

    except KeyboardInterrupt:

        # Close the connection
        connection.close()

        # Loop until the connection is closed
        connection.ioloop.start()
