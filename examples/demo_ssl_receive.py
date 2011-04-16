# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of simple consumer using SSL. Acks each message as it arrives.
"""
from ssl import CERT_REQUIRED, PROTOCOL_SSLv3
import sys

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

from pika.adapters import SelectConnection
from pika.connection import ConnectionParameters
import pika.log as log

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
    print "Basic.Deliver %s delivery-tag %i: %s" %\
          (header_frame.content_type,
           method_frame.delivery_tag,
           body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


if __name__ == '__main__':

    # Setup pika logging for internal info
    log.setup(log.INFO, True)

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
                                      ssl=True,
                                      ssl_options=ssl_options)
    connection = SelectConnection(parameters, on_connected)
    # Loop until CTRL-C
    try:
        # Start our blocking loop
        connection.ioloop.start()

    except KeyboardInterrupt:

        # Close the connection
        connection.close()

        # Loop until the conneciton is closed
        connection.ioloop.start()
