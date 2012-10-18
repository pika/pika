# -*- encoding: utf-8 -*-
"""
Example of simple publisher, loop and send messages as fast as we can

"""
import logging
from os import path
import sys
import time
sys.path.insert(0, '.')
if path.exists('../../pika'):
    sys.path.insert(0, '../../pika')


logging.basicConfig(level=logging.DEBUG)
if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG, format='%(levelname) -10s %(asctime)s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s')
    host = (len(sys.argv) > 1) and sys.argv[1] or 'localhost'
    import pika

    parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/test')
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.confirm_delivery()
    if not channel.basic_publish(exchange='test',
                                 routing_key='test',
                                 body='Test Message'):
        raise ValueError('Error publishing')
    connection.close()
