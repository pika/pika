#!/usr/bin/env python
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

'''
Example of simple consumer. Acks each message as it arrives.
'''

import logging
import sys
import pika

logging.basicConfig(level=logging.DEBUG)

conn = None

def handle_connection_state_change(conn, is_connected):
    if not is_connected:
        print 'Was disconnected from server: %s' % conn.close_text
        sys.exit(1)

def handle_delivery(channel, method, header, body):

    print "method=%r" % (method,)
    print "header=%r" % (header,)
    print "  body=%r" % (body,)
    channel.basic_ack(delivery_tag = method.delivery_tag)

def on_connected():
    logging.info("demo_send: Connected to RabbitMQ")

    conn.add_state_change_handler(handle_connection_state_change)
    conn.channel(on_channel)


def on_closed():
    # We've been called by the Connection object to let us know we're done
    logging.info("demo_send: Connection Closed")

def on_channel(channel):

    logging.info("demo_send: Received our Channel")

    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False)

    channel.basic_consume(handle_delivery, queue='test')

parameters = pika.ConnectionParameters((len(sys.argv) > 1) and \
                                       sys.argv[1] or \
                                       '127.0.0.1')
conn = pika.asyncore_adapter.AsyncoreConnection(parameters,
                                                open_callback=on_connected,
                                                close_callback=on_closed)
pika.asyncore_adapter.loop()
