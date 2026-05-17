import json
import threading

import pika
from pika.exchange_type import ExchangeType

body_buffer = []
lock = threading.Lock()

print(f'pika version: {pika.__version__}')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

main_channel = connection.channel()
consumer_channel = connection.channel()
bind_channel = connection.channel()

main_channel.exchange_declare(exchange='com.micex.sten',
                              exchange_type=ExchangeType.direct)
main_channel.exchange_declare(exchange='com.micex.lasttrades',
                              exchange_type=ExchangeType.direct)

queue = main_channel.queue_declare('', exclusive=True).method.queue
queue_tickers = main_channel.queue_declare('', exclusive=True).method.queue

main_channel.queue_bind(exchange='com.micex.sten',
                        queue=queue,
                        routing_key='order.stop.create')


def process_buffer():
    if not lock.acquire(False):
        print('locked!')
        return
    try:
        while body_buffer:
            body = body_buffer.pop(0)

            ticker = None
            if 'ticker' in body['data']['params']['condition']:
                ticker = body['data']['params']['condition']['ticker']
            if not ticker:
                continue

            print(f'got ticker {ticker}, gonna bind it...')
            bind_channel.queue_bind(exchange='com.micex.lasttrades',
                                    queue=queue_tickers,
                                    routing_key=str(ticker))
            print(f'ticker {ticker} binded ok')
    finally:
        lock.release()


def callback(_ch, _method, _properties, body):
    body = json.loads(body)['order.stop.create']
    body_buffer.append(body)
    process_buffer()


# Note: consuming with automatic acknowledgements has its risks
#       and used here for simplicity.
#       See https://www.rabbitmq.com/confirms.html.
consumer_channel.basic_consume(queue, callback, auto_ack=True)

try:
    consumer_channel.start_consuming()
finally:
    connection.close()
