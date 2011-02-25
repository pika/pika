# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Send a message to a non-existent queue with the mandatory flag and confirm
that it is returned via Basic.Return
"""
import nose
import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import support.tools as tools
from pika.adapters import SelectConnection
from config import HOST, PORT


class TestAsyncSendReturn(tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_send_and_return(self):
        self.confirmed = False
        self.connection = self._connect(SelectConnection, HOST, PORT)
        self.connection.ioloop.start()
        if not self.confirmed:
            assert False, 'Messages did not match.'
        pass

    def _on_channel(self, channel):
        self.channel = channel

        def _on_basic_return(method, header, body):
            self.confirmed = (body == test_message)
            self.connection.add_on_close_callback(self._on_closed)
            self.connection.close()

        self.channel.add_on_return_callback(_on_basic_return)
        test_message = self._send_message(mandatory=True)


if __name__ == "__main__":
    nose.runmodule()
