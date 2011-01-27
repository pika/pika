#!/usr/bin/env python
"""
Connection Test

First test to make sure all async adapters can connect properly
"""
import sys
sys.path.append('..')
sys.path.append('../..')

import nose
import pika
import pika.adapters as adapters

from pika.adapters.tornado_connection import TornadoConnection

HOST = 'localhost'
PORT = 5672

class TestAdapters(object):

    def __init__(self):
        self.connection = None

    @nose.tools.timed(2)
    def test_kqueue_connection(self):
        self._set_select_poller('kqueue')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'KQueuePoller':
            assert False
        if not self.connected:
            assert False
        pass

    def _connect(self, connection_type):
        if self.connection:
            del self.connection
        self.connected = False
        parameters = pika.ConnectionParameters(HOST, PORT)
        return connection_type(parameters, self._on_connected)

    def _on_connected(self, connection):
        self.connected = self.connection.is_open
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.close()

    def _on_closed(self, frame):
        self.connection.ioloop.stop()

    def _set_select_poller(self, type):
        adapters.select_connection.SELECT_TYPE = type

if __name__ == "__main__":
    nose.runmodule()
