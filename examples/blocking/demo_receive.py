# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

'''
Example of simple consumer. Acks each message as it arrives.
'''
# Import all adapters for easier experimentation
import sys

from pika.adapters import BlockingConnection
from pika.connection import ConnectionParameters

def handle_delivery(channel, method_frame, header_frame, body):
    # Receive the data in 3 frames from RabbitMQ
    print "demo_receive: Basic.Deliver %s delivery-tag %i: %s" % \
          (header_frame.content_type,
           method_frame.delivery_tag,
           body)

    # Acknowledge the message
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

if __name__ == '__main__':

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    connection = BlockingConnection(ConnectionParameters(host))

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test",
                          durable=True,
                          exclusive=False,
                          auto_delete=False)

    # Add a queue to consume
    consumer_tag = channel.basic_consume(handle_delivery, queue='test')

    # Start consuming, block until keyboard interrupt
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
