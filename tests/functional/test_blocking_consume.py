# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Send a message to a non-existent queue with the mandatory flag and confirm
that it is returned via Basic.Return
"""
import nose
import os
import sys
import time
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import pika
from pika.adapters import BlockingConnection
import support.tools

HOST = 'localhost'
PORT = 5672
MESSAGES = 10


def test_blocking_consume():

    parameters = pika.ConnectionParameters(host=HOST, port=PORT)
    connection = BlockingConnection(parameters)

    # Open the channel
    channel = connection.channel()

    # Declare the exchange
    exchange_name = support.tools.test_queue_name('blocking_exchange')
    frame = channel.exchange_declare(exchange=exchange_name,
                                     type="direct",
                                     auto_delete="true")
    if not isinstance(frame.method, pika.spec.Exchange.DeclareOk):
        assert False, \
        "Did not receive Exchange.DeclareOk from channel.exchange_declare"

    # Declare the queue
    queue_name = support.tools.test_queue_name('blocking_consume')
    frame = channel.queue_declare(queue=queue_name,
                                  durable=False,
                                  exclusive=True,
                                  auto_delete=True)

    if not isinstance(frame.method, pika.spec.Queue.DeclareOk):
        assert False, \
        "Did not receive Queue.DeclareOk from channel.queue_declare"

    routing_key = "%s.%s" % (exchange_name, queue_name)
    frame = channel.queue_bind(queue=queue_name,
                               exchange=exchange_name,
                               routing_key=routing_key)
    if not isinstance(frame.method, pika.spec.Queue.BindOk):
        assert False, \
        "Did not receive Queue.BindOk from channel.queue_bind"

    _sent = []
    _received = []

    @pika.log.method_call
    def _on_message(channel, method, header, body):
        _received.append(body)
        if len(_received) == MESSAGES:
            channel.stop_consuming()
        if start < time.time() - 2:
            assert False, "Test timed out"

    for x in xrange(0, MESSAGES):
        message = 'test_blocking_send:%i:%.4f' % (x, time.time())
        _sent.append(message)
        channel.basic_publish(exchange=exchange_name,
                              routing_key=routing_key,
                              body=message,
                              properties=pika.BasicProperties(
                                content_type="text/plain",
                                delivery_mode=1))

    # Loop while we get messages (for 2 seconds)
    start = time.time()

    # This is blocking
    channel.basic_consume(consumer=_on_message, queue=queue_name, no_ack=True)
    connection.close()

    # Check our results
    if len(_sent) != MESSAGES:
        assert False, "We did not send the expected qty of messages: %i" %\
                      len(_sent)
    if len(_received) != MESSAGES:
        assert False, "Did not receive the expected qty of messages: %i" %\
                      len(_received)
    for message in _received:
        if message not in _sent:
            assert False, 'Received a message we did not send.'
    for message in _sent:
        if message not in _received:
            assert False, 'Sent a message we did not receive.'

if __name__ == "__main__":
    #pika.log.setup(pika.log.DEBUG, color=True)
    #test_blocking_consume()
    nose.runmodule()
