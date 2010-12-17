# fast_receive.py
# Copyright (C) 2010 Tony Garnock-Jones
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
'''
Overly-simple example program useful for getting a rough idea of how
many messages per second pika can deliver via basic_consume. Many,
many caveats apply.
'''
import sys
import pika
import asyncore
import time

conn = pika.AsyncoreConnection(pika.ConnectionParameters(
        (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1',
        credentials = pika.PlainCredentials('guest', 'guest'),
        heartbeat = 10))

def handle_connection_state_change(conn, is_connected):
    if not is_connected:
        print 'Was disconnected from server:', conn.connection_close
        sys.exit(1)
conn.addStateChangeHandler(handle_connection_state_change)

print 'Connected to %r' % (conn.server_properties,)

ch = conn.channel()
qname = ch.queue_declare(queue='', durable=False, exclusive=True, auto_delete=True).queue
print 'My queue is %s' % (qname,)
ch.queue_bind(exchange='amq.direct', queue=qname, routing_key='test queue')

count = 0
start_time = time.time()
def handle_delivery(ch, method, header, body):
    global count, start_time
    count = count + 1
    if count % 1000 == 0:
        now = time.time()
        print count, 1000 / (now - start_time)
        start_time = now
    ch.basic_ack(delivery_tag = method.delivery_tag)
    conn.flush_outbound()

ch.basic_consume(handle_delivery, queue = qname)
pika.asyncore_loop()
print 'Close reason:', conn.connection_close
