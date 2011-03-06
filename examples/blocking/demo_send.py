# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Example of simple publisher, loop and send messages as fast as we can
"""

import sys
import time

from pika.adapters import BlockingConnection
from pika.connection import ConnectionParameters
from pika import BasicProperties

if __name__ == '__main__':
    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    connection = BlockingConnection(ConnectionParameters(host))

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False)

    # Initialize our timers and loop until external influence stops us
    count = 0
    start_time = time.time()
    while True:
        try:
            # Construct a message and send it
            message = "BlockingConnection.channel.basic_publish #%i" % count
            channel.basic_publish(exchange='',
                                  routing_key="test",
                                  body=message,
                                  properties=BasicProperties(timestamp=\
                                                             time.time(),
                                                             app_id=__file__,
                                                             user_id='guest',
                                                             content_type=\
                                                             "text/plain",
                                                             delivery_mode=1))
            count += 1
            if count % 1000 == 0:
                duration = time.time() - start_time
                print "%i Messages Sent: %.8f per second" % (count,
                                                             count / duration)

        # Close when someone presses CTRL-C
        except KeyboardInterrupt:
            break

    print "CTRL-C Received, closing"
    connection.close()
