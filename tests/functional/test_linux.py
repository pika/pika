# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Connection Test

First test to make sure epoll and poll async adapters can connect properly
"""
import nose
import platform
import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import pika
import pika.adapters as adapters

from pika.adapters.tornado_connection import TornadoConnection

HOST = 'localhost'
PORT = 5672


class TestAdapters(object):

    def __init__(self):
        self.connection = None

    @nose.tools.timed(2)
    def test_epoll_connection(self):
        # EPoll is 2.6+
        if float('.'.join(platform.python_version().split('.')[:-1])) < 2.6:
            raise nose.SkipTest
        self._set_select_poller('epoll')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'EPollPoller':
            assert False, "Not EPollPoller"
        if not self.connected:
            assert False, "Not Connected"
        pass

    @nose.tools.timed(2)
    def test_poll_connection(self):
        self._set_select_poller('poll')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'PollPoller':
            assert False, "Not PollPoller"
        if not self.connected:
            assert False, "Not Connected"
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
