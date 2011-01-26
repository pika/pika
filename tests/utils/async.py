# Async pika testing support

import sys
sys.path.append('..')
sys.path.append('../..')

import os
import pika
import time


class AsyncPattern(object):

    def __init__(self):
        self.connection = None
        self.channel = None
        self._queue = self._queue_name()
        self._timeout = False

    def _queue_name(self):
        return 'test-%s-%i' % (self.__class__.__name__, os.getpid())

    def _connect(self, connection_type, host, port):
        parameters = pika.ConnectionParameters(host, port)
        return connection_type(parameters, self._on_connected)

    def _on_connected(self, connection):
        self.connection.channel(self._on_channel)

    def _on_channel(self, channel):
        assert False, "_on_channel no _extended"

    def _queue_declare(self):
        self.channel.queue_declare(queue=self._queue,
                                   durable=True,
                                   exclusive=False,
                                   auto_delete=True,
                                   callback=self._on_queue_declared)
    def _on_queue_declared(self, frame):
        assert False, "_on_queue_declared not extended"

    def _send_message(self):

        message = 'test-message-%s: %.8f' % (self.__class__.__name__,
                                             time.time())
        self.channel.basic_publish(exchange='',
                              routing_key=self._queue,
                              body=message,
                              properties=pika.BasicProperties(
                                  content_type="text/plain",
                                  delivery_mode=1))
        return message

    @property
    def _is_connected(connection):
        return self.connection.is_open

    def _on_timeout(self):
        self._timeout = True
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.close()

    def _on_closed(self, frame):
        self.connection.ioloop.stop()
