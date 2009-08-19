#!/usr/bin/env python
'''
Example of simple consumer, waits one message, replies an ack and exits.
'''

import rabbitmq
import asyncore

conn = rabbitmq.Connection('127.0.0.1', 
                        credentials=rabbitmq.PlainCredentials('guest', 'guest'))

ch = conn.channel()
ch.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False)


def handle_delivery(method, header, body):
    print "method=%r" % (method,)
    print "header=%r" % (header,)
    print "  body=%r" % (body,)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    conn.close()

tag = ch.basic_consume(handle_delivery, queue = 'test')

asyncore.loop()

