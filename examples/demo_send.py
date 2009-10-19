#!/usr/bin/env python
'''
Example of simple producer, creates one message and exits.
'''

import rabbitmq
import asyncore

conn = rabbitmq.AsyncoreConnection('127.0.0.1', 
                                   credentials=rabbitmq.PlainCredentials('guest', 'guest'))

ch = conn.channel()
ch.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False)

ch.basic_publish(exchange='',
                routing_key="test",
                body="Hello World!",
                properties=rabbitmq.BasicProperties(
                            content_type = "text/plain",
                            delivery_mode = 2, # persistent
                             ))

conn.close()
asyncore.loop()

