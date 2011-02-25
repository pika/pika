# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Send a message and confirm you can retrieve it with Basic.Get
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


class TestAsyncSendGet(tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_send_and_get(self):
        self.confirmed = False
        self.connection = self._connect(SelectConnection, HOST, PORT)
        self.connection.ioloop.start()
        if not self.confirmed:
            assert False, 'Messages did not match.'
        pass

    def _on_channel(self, channel):
        self.channel = channel
        self._queue_declare()

    def _on_queue_declared(self, frame):
        test_message = self._send_message()

        def check_message(channel_number, method, header, body):
            self.confirmed = (body == test_message)
            self.connection.add_on_close_callback(self._on_closed)
            self.connection.close()

        self.channel.basic_get(callback=check_message, queue=self._queue)

if __name__ == "__main__":
    nose.runmodule()
