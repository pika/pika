# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
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
