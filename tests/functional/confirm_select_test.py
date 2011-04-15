# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Tests the use of confirm.select
"""

__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__date__ = '2011-04-15'
import nose
import support
import support.tools
from pika.adapters import SelectConnection

MESSAGES_TO_TEST = 10

class TestPublisherConfirms(support.tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_publisher_confirms(self):
        self.received = 0
        self.connection = self._connect(SelectConnection, support.PARAMETERS)
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.ioloop.start()
        if self.received != MESSAGES_TO_TEST:
            assert False, 'Confirm count did not match.'

    @nose.tools.nottest
    def _on_channel(self, channel):
        self.channel = channel
        self._queue_declare()

    @nose.tools.nottest
    def _on_queue_declared(self, frame):

        self.channel.confirm_delivery(callback=self._on_basic_ack)
        for x in xrange(0, MESSAGES_TO_TEST):
            self._send_message()

    @support.tools.timeout
    @nose.tools.nottest
    def _on_basic_ack(self, frame):
        self.received += 1
        if self.received == MESSAGES_TO_TEST:
            self.connection.close()
