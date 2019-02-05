# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import json
import random
import pika

print('pika version: %s' % pika.__version__)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
main_channel = connection.channel()

main_channel.exchange_declare(exchange='com.micex.sten', exchange_type='direct')
main_channel.exchange_declare(
    exchange='com.micex.lasttrades', exchange_type='direct')

tickers = {
    'MXSE.EQBR.LKOH': (1933, 1940),
    'MXSE.EQBR.MSNG': (1.35, 1.45),
    'MXSE.EQBR.SBER': (90, 92),
    'MXSE.EQNE.GAZP': (156, 162),
    'MXSE.EQNE.PLZL': (1025, 1040),
    'MXSE.EQNL.VTBR': (0.05, 0.06)
}


def getticker():
    return list(tickers.keys())[random.randrange(0, len(tickers) - 1)]


_COUNT_ = 10

for i in range(0, _COUNT_):
    ticker = getticker()
    msg = {
        'order.stop.create': {
            'data': {
                'params': {
                    'condition': {
                        'ticker': ticker
                    }
                }
            }
        }
    }
    main_channel.basic_publish(
        exchange='com.micex.sten',
        routing_key='order.stop.create',
        body=json.dumps(msg),
        properties=pika.BasicProperties(content_type='application/json'))
    print('send ticker %s' % ticker)

connection.close()
