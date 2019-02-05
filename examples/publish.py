# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import logging
import pika

logging.basicConfig(level=logging.DEBUG)

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.exchange_declare(
    exchange="test_exchange",
    exchange_type="direct",
    passive=False,
    durable=True,
    auto_delete=False)

print("Sending message to create a queue")
channel.basic_publish(
    'test_exchange', 'standard_key', 'queue:group',
    pika.BasicProperties(content_type='text/plain', delivery_mode=1))

connection.sleep(5)

print("Sending text message to group")
channel.basic_publish(
    'test_exchange', 'group_key', 'Message to group_key',
    pika.BasicProperties(content_type='text/plain', delivery_mode=1))

connection.sleep(5)

print("Sending text message")
channel.basic_publish(
    'test_exchange', 'standard_key', 'Message to standard_key',
    pika.BasicProperties(content_type='text/plain', delivery_mode=1))

connection.close()
