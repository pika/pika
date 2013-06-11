#!/usr/bin/python
# -*- coding: utf-8 -*-

import pika    
import json
import random

print(('pika version: %s') % pika.__version__)

connection   = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
main_channel = connection.channel()  

if pika.__version__=='0.9.5':
    main_channel.exchange_declare(exchange='com.micex.sten',       type='direct')
    main_channel.exchange_declare(exchange='com.micex.lasttrades', type='direct')
else:
    main_channel.exchange_declare(exchange='com.micex.sten',       exchange_type='direct')
    main_channel.exchange_declare(exchange='com.micex.lasttrades', exchange_type='direct')

tickers = {}
tickers['MXSE.EQBR.LKOH'] = (1933,1940)
tickers['MXSE.EQBR.MSNG'] = (1.35,1.45)
tickers['MXSE.EQBR.SBER'] = (90,92)
tickers['MXSE.EQNE.GAZP'] = (156,162)
tickers['MXSE.EQNE.PLZL'] = (1025,1040)
tickers['MXSE.EQNL.VTBR'] = (0.05,0.06)
def getticker(): return list(tickers.keys())[random.randrange(0,len(tickers)-1)]

_COUNT_ = 10

for i in range(0,_COUNT_):
    ticker = getticker()
    msg = {'order.stop.create':{'data':{'params':{'condition':{'ticker':ticker}}}}}
    main_channel.basic_publish(exchange='com.micex.sten', 
                               routing_key='order.stop.create', 
                               body=json.dumps(msg),
                               properties=pika.BasicProperties(content_type='application/json')
                              )                          
    print('send ticker %s' %  ticker)                         

connection.close()
