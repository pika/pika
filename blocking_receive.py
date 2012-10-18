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

def on_message(channel, method_frame, header_frame, body):
    print method_frame.delivery_tag
    print body
    print
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


logging.basicConfig(level=logging.DEBUG)
if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG, format='%(levelname) -10s %(asctime)s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s')
    host = (len(sys.argv) > 1) and sys.argv[1] or 'localhost'
    import pika
    connection = pika.BlockingConnection()
    channel = connection.channel()

    response = channel.basic_get('test')
    print response

    connection.close()
