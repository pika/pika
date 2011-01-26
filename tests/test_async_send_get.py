#!/usr/bin/env python
"""
Send a message and confirm you can retrieve it with Basic.Get
Test Steps:

1) Connect to broker    - start_test
2) Open Channel         - on_connected
3) Delcare Queue        - on_channel_open
4) Send test message    - on_queue_declared
5) Call basic get       - on_queue_declared
6) Validate that sent message and basic get body are the same - check_message
"""
import utils.async as async
import nose
from pika.adapters import SelectConnection

channel = None
confirmed = False
connection = None
queue = None

ADAPTER = SelectConnection
HOST = 'localhost'
PORT = 5672

class TestAsyncSendGet(object):

    def __init__(self):
        self.confirmed = False
        self.queue = async.queue_name()

    @nose.tools.timed(2)
    def test_send_and_get(self):
        self.connection = async.connect(ADAPTER, HOST, PORT,
                                        self._on_connected)
        self.connection.ioloop.start()
        if not self.confirmed:
            assert False, 'Messages did not match.'
        pass

    def _on_connected(self, connection):
        async.channel(self.connection, self._on_channel_open)

    def _on_channel_open(self, channel):
        self.channel = channel
        async.queue_declare(self.channel, self.queue, self.on_queue_declared)

    def on_queue_declared(self, frame):
        test_message = async.send_test_message(self.channel, self.queue)
        def check_message(channel_number, method, header, body):
            self.confirmed = (body == test_message)
            self.connection.ioloop.stop()
        self.channel.basic_get(callback=check_message, queue=self.queue)
