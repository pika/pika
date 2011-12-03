# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

# Async pika testing support

import sys
sys.path.append('..')

import os
import pika
import random
import time

timeout_id = None


def test_queue_name(keyword):
    uid = random.randint(0, 1000) + os.getpid()
    return 'test-%s-%i' % (keyword, uid)


# These methods are scoped to the AsyncPattern class
def timeout(method):
    def _timeout(self, *args, **kwargs):
        global timeout_id
        timeout_id = self.connection.add_timeout(time.time() + 2,
                                                 self._on_timeout)
        return method(self, *args, **kwargs)
    return _timeout


def timeout_cancel(method):
    def _timeout(self, *args, **kwargs):
        global timeout_id
        self.connection.remove_timeout(timeout_id)
        del(timeout_id)
        return method(self, *args, **kwargs)
    return _timeout


class AsyncPattern(object):

    def __init__(self):
        self.connection = None
        self.channel = None
        self._queue = test_queue_name(self.__class__.__name__)
        self._timeout = False

    def _connect(self, connection_type, parameters):
        return connection_type(parameters, self._on_connected)

    def _on_connected(self, connection):
        self.connection.channel(self._on_channel)

    def _on_channel(self, channel):
        assert False, "_on_channel no _extended"

    def _queue_declare(self):
        self.channel.queue_declare(queue=self._queue,
                                   durable=False,
                                   exclusive=True,
                                   auto_delete=True,
                                   callback=self._on_queue_declared)

    def _on_queue_declared(self, frame):
        assert False, "_on_queue_declared not extended"

    def _send_message(self, exchange='', mandatory=False, immediate=False):
        message = 'test-message-%s: %.8f' % (self.__class__.__name__,
                                             time.time())
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=self._queue,
                                   body=message,
                                   properties=pika.BasicProperties(
                                       content_type="text/plain",
                                       delivery_mode=1),
                                   mandatory=mandatory,
                                   immediate=immediate)
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
