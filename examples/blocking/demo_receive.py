# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

'''
Example of simple consumer. Acks each message as it arrives.
'''
# Import all adapters for easier experimentation
import pika
import sys

from pika.adapters import BlockingConnection

pika.log.setup(color=True)


def handle_delivery(channel, method_frame, header_frame, body):
    # Receive the data in 3 frames from RabbitMQ
    pika.log.info("Basic.Deliver %s delivery-tag %i: %s",
                  header_frame.content_type,
                  method_frame.delivery_tag,
                  body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

if __name__ == '__main__':
    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    parameters = pika.ConnectionParameters(host)
    connection = BlockingConnection(parameters)

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False)

    # We're stuck looping here since this is a blocking adapter
    channel.basic_consume(handle_delivery, queue='test')
