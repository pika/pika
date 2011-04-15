# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Send a message to a non-existent queue with the mandatory flag and confirm
that it is returned via Basic.Return
"""
from time import time
import support
import support.tools

from pika.adapters import BlockingConnection
from pika.exceptions import AMQPChannelError
from pika.spec import BasicProperties


def test_blocking_invalid_exchange():

    # Connect to RabbitMQ
    connection = BlockingConnection(support.PARAMETERS)

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    queue_name = support.tools.test_queue_name('blocking_send_get')
    channel.queue_declare(queue=queue_name,
                          durable=False,
                          exclusive=True,
                          auto_delete=True)

    message = 'test_blocking_send:%.4f' % time()
    try:
        channel.basic_publish(exchange="invalid-exchange",
                              routing_key=queue_name,
                              body=message,
                              mandatory=True,
                              properties=BasicProperties(
                                      content_type="text/plain",
                                      delivery_mode=1))

        while True:
            channel.transport.connection.process_data_events()

    except AMQPChannelError, err:
        if err[0] != 404:
            assert False, "Did not receive a Channel.Close"
    connection.close()
