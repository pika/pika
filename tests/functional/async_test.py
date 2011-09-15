# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Connection Test

First test to make sure all async adapters can connect properly
"""
import nose
import select
import support

import pika.adapters as adapters
from pika.adapters.tornado_connection import IOLoop as tornado_ioloop


class TestAdapters(object):

    def __init__(self):
        self.connection = None
        self._timeout = False

    @nose.tools.timed(2)
    def test_asyncore_connection(self):
        self.connection = self._connect(adapters.AsyncoreConnection)
        self.connection.ioloop.start()
        if not self.connected:
            assert False, "Not Connected"
        pass

    @nose.tools.timed(2)
    def test_select_connection(self):
        self._set_select_poller('select')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'SelectPoller':
            assert False, "Not SelectPoller"
        if not self.connected:
            assert False, "Not Connected"
        pass

    @nose.tools.timed(2)
    def test_tornado_connection(self):
        # Tornado is 2.5+ only
        if support.PYTHON_VERSION < 2.5 or not tornado_ioloop:
            raise nose.SkipTest
        self.connection = self._connect(adapters.TornadoConnection)
        self.connection.ioloop.start()
        if not self.connected:
            assert False, "Not Connected"
        pass

    @nose.tools.timed(2)
    def test_epoll_connection(self):
        # EPoll is 2.6+ and linux only
        if not hasattr(select, 'epoll'):
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
        if not hasattr(select, 'poll') or not hasattr(select.poll, 'modify'):
            raise nose.SkipTest
        self._set_select_poller('poll')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'PollPoller':
            assert False, "Not PollPoller"
        if not self.connected:
            assert False, "Not Connected"
        pass

    @nose.tools.timed(2)
    def test_kqueue_connection(self):
        if not hasattr(select, 'kqueue'):
            raise nose.SkipTest
        self._set_select_poller('kqueue')
        self.connection = self._connect(adapters.SelectConnection)
        self.connection.ioloop.start()
        if self.connection.ioloop.poller_type != 'KQueuePoller':
            assert False, "Not KQueuePoller"
        if not self.connected:
            assert False, "Not Connected"
        pass

    @nose.tools.timed(2)
    def test_twisted_connection(self):
        try:
            from pika.adapters import TwistedConnection
        except ImportError:
            raise nose.SkipTest
        self.connection = self._connect(TwistedConnection)
        self.connection.ioloop.start()
        if not self.connected:
            assert False, "Not Connected"
        pass

    def _connect(self, connection_type):
        if self.connection:
            del self.connection
        self.connected = False
        return connection_type(support.PARAMETERS, self._on_connected)

    def _on_connected(self, connection):
        self.connected = self.connection.is_open
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.close()

    def _on_closed(self, frame):
        self.connection.ioloop.stop()

    def _set_select_poller(self, type):
        adapters.select_connection.SELECT_TYPE = type
