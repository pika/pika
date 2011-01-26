# Async pika support

import sys
sys.path.append('..')
sys.path.append('../..')
import nose
import os
import pika
import time


@nose.tools.nottest
def queue_name():
    return 'test-queue-%i' % os.getpid()


@nose.tools.nottest
def connect(connection_type, host, port, callback):
    parameters = pika.ConnectionParameters(host, port)
    return connection_type(parameters, callback)


@nose.tools.nottest
def channel(connection, callback):
    connection.channel(callback)


@nose.tools.nottest
def queue_declare(channel, queue, callback):
    channel.queue_declare(queue=queue,
                          durable=True,
                          exclusive=False,
                          auto_delete=True,
                          callback=callback)


@nose.tools.nottest
def send_test_message(channel, queue):

    message = 'test-message:%.8f' % time.time()
    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=message,
                          properties=pika.BasicProperties(
                              content_type="text/plain",
                              delivery_mode=1))
    return message


@nose.tools.nottest
def is_connected(connection):
    return connection.is_open
