# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Send a message , get it with Basic.Get, reject it with Basic.Reject and then
get it again to confirm you get the same message as last time.
"""
import nose
import support
import support.tools
import time
from pika.adapters import SelectConnection


class TestSendGetRejectGet(support.tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_send_and_get(self):
        self.confirm = list()
        self.connection = self._connect(SelectConnection, support.PARAMETERS)
        self.connection.ioloop.start()
        if self._timeout:
            assert False, "Test timed out"
        if len(self.confirm) < 3:
            assert False, "Did not retrieve both messages"
        if self.confirm[0] != self.confirm[1] != self.confirm[3]:
            assert False, 'Messages did not match.'
        pass

    def _on_channel(self, channel):
        self.channel = channel
        self._queue_declare()

    @support.tools.timeout
    def _on_queue_declared(self, frame):
        test_message = self._send_message()
        self.confirm.append(test_message)
        self.channel.basic_get(callback=self._check_first_message,
                               queue=self._queue)

    @support.tools.timeout_cancel
    def _check_first_message(self, channel_number, method, header, body):
        self.channel.basic_reject(method.delivery_tag)
        self.confirm.append(body)
        self.connection.add_timeout(time.time() + .2, self._get_second_message)

    @support.tools.timeout
    def _get_second_message(self):
        self.channel.basic_get(callback=self._check_second_message,
                               queue=self._queue)

    @support.tools.timeout_cancel
    @support.tools.timeout
    def _check_second_message(self, channel_number, method, header, body):
        self.confirm.append(body)
        self.channel.basic_ack(method.delivery_tag)
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.close()
