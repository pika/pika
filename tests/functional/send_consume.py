#!/usr/bin/env python
"""
Send n messages and confirm you can retrieve them with Basic.Consume
"""
import sys
sys.path.append("../..")

import nose
import support.async as async
from pika.adapters import SelectConnection

channel = None
confirmed = False
connection = None
queue = None

HOST = 'localhost'
MESSAGES = 10
PORT = 5672

class TestSendConsume(async.AsyncPattern):

    def __init__(self):
        async.AsyncPattern.__init__(self)
        self._sent = list()
        self._received = list()

    @nose.tools.timed(2)
    def test_send_and_consume(self):
        self.connection = self._connect(SelectConnection, HOST, PORT)
        self.connection.ioloop.start()
        if self._timeout:
            assert False, "Test timed out"
        if len(self._sent) != MESSAGES:
            assert False, "We did not send the expected qty of messages: %i" %\
                          len(self._sent)
        if len(self._received) != MESSAGES:
            assert False, "Did not receive the expected qty of messages: %i" %\
                          len(self._received)
        for message in self._received:
            if message not in self._sent:
                print message
                print self._sent
                print self._received
                assert False, 'Received a message we did not send.'
        for message in self._sent:
            if message not in self._received:
                assert False, 'Sent a message we did not receive.'
        pass

    def _on_channel(self, channel):
        self.channel = channel
        self._queue_declare()

    @async.timeout
    def _on_queue_declared(self, frame):
        for x in xrange(0, MESSAGES):
            self._sent.append(self._send_message())
        self.channel.basic_consume(self._on_message, queue=self._queue)

    def _on_message(self, channel_number, method, header, body):
        self._received.append(body)
        if len(self._received) == MESSAGES:
            self.connection.add_on_close_callback(self._on_closed)
            self.connection.close()

if __name__ == "__main__":
    nose.runmodule()
