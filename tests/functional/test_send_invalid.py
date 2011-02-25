# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Send a message to an invalid exchange which will yield a channel.Close call
from the AMQP broker. Validate this process happens correctly.
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


class TestAsyncSendInvalid(tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_send_invalid(self):
        self.confirmed = False
        self.connection = self._connect(SelectConnection, HOST, PORT)
        self.connection.ioloop.start()
        if not self.confirmed:
            assert False, 'Did not receive the remote channel close callback.'
        pass

    def _on_channel(self, channel):
        self.channel = channel
        # Catch the remote close which should happen when we send
        self.channel.add_on_close_callback(self._on_remote_close)
        self._send_message(exchange='undeclared-exchange')

    def _on_remote_close(self, reason_code, reason_text):
        if reason_code == 404:
            self.confirmed = True
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.close()

if __name__ == "__main__":
    nose.runmodule()
