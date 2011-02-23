# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

'''
Example of the use of basic_get. NOT RECOMMENDED - use
basic_consume instead if at all possible!
'''
import sys
import pika
import time

from pika.adapters import BlockingConnection

pika.log.setup(color=True)

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

    # Initialize our timers and loop until external influence stops us
    while connection.is_open:

        # Call basic get which returns the 3 frame types
        method_frame, header_frame, body = channel.basic_get(queue="test")

        # It can be empty if the queue is empty so don't do anything
        if method_frame.NAME == 'Basic.GetEmpty':
            pika.log.info("Empty Basic.Get Response (Basic.GetEmpty)")

        # We have data
        else:
            pika.log.info("Basic.GetOk %s delivery-tag %i: %s",
                          header_frame.content_type,
                          method_frame.delivery_tag,
                          body)

            # Acknowledge the receipt of the data
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        # No need to pound rabbit, sleep for a while. If you want messages as
        # fast as you can get them, use Basic.Consume
        time.sleep(1)
