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
from pika.spec import BasicProperties


def test_blocking_send_get():

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
    channel.basic_publish(routing_key=queue_name,
                          exchange="",
                          body=message,
                          properties=BasicProperties(
                                  content_type="text/plain",
                                  delivery_mode=1))

    # Loop while we try to get the message we sent
    message_in = channel.basic_get(queue=queue_name)

    # Close the connection
    connection.close()

    # Only check the body
    if message_in[2] != message:
        assert False, "Did not receive the same message back"
