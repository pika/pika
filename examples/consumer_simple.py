# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import json
import logging
import pika

print('pika version: %s' % pika.__version__)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

main_channel = connection.channel()
consumer_channel = connection.channel()
bind_channel = connection.channel()

main_channel.exchange_declare(exchange='com.micex.sten', exchange_type='direct')
main_channel.exchange_declare(
    exchange='com.micex.lasttrades', exchange_type='direct')

queue = main_channel.queue_declare('', exclusive=True).method.queue
queue_tickers = main_channel.queue_declare('', exclusive=True).method.queue

main_channel.queue_bind(
    exchange='com.micex.sten', queue=queue, routing_key='order.stop.create')


def hello():
    print('Hello world')


connection.call_later(5, hello)


def callback(_ch, _method, _properties, body):
    body = json.loads(body)['order.stop.create']

    ticker = None
    if 'ticker' in body['data']['params']['condition']:
        ticker = body['data']['params']['condition']['ticker']
    if not ticker:
        return

    print('got ticker %s, gonna bind it...' % ticker)
    bind_channel.queue_bind(
        exchange='com.micex.lasttrades',
        queue=queue_tickers,
        routing_key=str(ticker))
    print('ticker %s binded ok' % ticker)


logging.basicConfig(level=logging.INFO)

# Note: consuming with automatic acknowledgements has its risks
#       and used here for simplicity.
#       See https://www.rabbitmq.com/confirms.html.
consumer_channel.basic_consume(queue, callback, auto_ack=True)

try:
    consumer_channel.start_consuming()
finally:
    connection.close()
