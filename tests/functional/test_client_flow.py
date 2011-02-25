# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Test for being able to toggle Channel.Flow on the server which will become
more explicitly supported in AMQP 1-0
"""
import nose
import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import support.tools as tools
from pika.adapters import SelectConnection

HOST = 'localhost'
PORT = 5672


class TestAsyncClientFlow(tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_flow(self):
        self.connection = self._connect(SelectConnection, HOST, PORT)
        self.connection.ioloop.start()
        pass

    def _on_channel(self, channel):
        self.channel = channel
        self._queue_declare()

    def _on_queue_declared(self, frame):
        self._channel_flow_test_1()

    @nose.tools.nottest
    def _channel_flow_test_1(self):
        # Confirm we can turn it on
        self.channel.flow(self._channel_flow_test_1_response, True)

    @nose.tools.nottest
    def _channel_flow_test_1_response(self, active):
        if not active:
            assert False, "Test #1: Channel flow did not turn on"
        self.connection.add_timeout(0.5, self._channel_flow_test_2)

    @nose.tools.nottest
    def _channel_flow_test_2(self):
        # Confirm we can turn it off
        self.channel.flow(self._channel_flow_test_2_response, False)

    @nose.tools.nottest
    def _channel_flow_test_2_response(self, active):
        if active:
            assert False, "Test #2: Channel flow did not turn off"

        def _on_closed(frame):
            self.connection.add_on_close_callback(self._on_closed)
            self.connection.close()

        self.connection.add_on_close_callback(_on_closed)
        self.connection.close()


if __name__ == "__main__":
    nose.runmodule()
