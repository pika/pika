#!/usr/bin/env python
"""
Connection Test

First test to make sure all async adapters can connect properly
"""
import sys
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
    def test_asyncore_connection(self):
        self.connection = self._connect(adapters.AsyncoreConnection)
        self.connection.ioloop.start()
        if not self.connected:
            assert False
        pass

    @nose.tools.timed(2)
    def test_epoll_connection(self):
        self._set_select_poller('epoll')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'EPollPoller':
            assert False
        if not self.connected:
            assert False
        pass

    @nose.tools.timed(2)
    def test_poll_connection(self):
        self._set_select_poller('poll')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'PollPoller':
            assert False
        if not self.connected:
            assert False
        pass

    @nose.tools.timed(2)
    def test_select_connection(self):
        self._set_select_poller('select')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'SelectPoller':
            assert False
        if not self.connected:
            assert False
        pass

    @nose.tools.timed(2)
    def test_tornado_connection(self):
        self.connection = self._connect(TornadoConnection)
        self.connection.ioloop.start()
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
    import logging
    logging.basicConfig(level=logging.DEBUG)
    x = TestAdapters()
    x.test_asyncore_connection()
    x.test_epoll_connection()
    x.test_select_connection()
    x.test_poll_connection()
    x.test_tornado_connection()
