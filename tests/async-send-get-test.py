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


@nose.tools.timed(2)
def start_test():
    global confirmed, connection
    confirmed = False
    connection = async.connect(ADAPTER, HOST, PORT, on_connected)
    connection.ioloop.start()
    if not confirmed:
        assert False
    pass

@nose.tools.nottest
def on_connected(connection):
    global connected
    connected = connection.is_open()
    if connected:
        async.channel(connection, on_channel_open)


@nose.tools.nottest
def on_channel_open(channel_):
    global channel, queue
    channel = channel_
    queue = async.queue_name()
    async.queue_declare(channel, queue, on_queue_declared)


@nose.tools.nottest
def on_queue_declared(frame):
    global channel, queue

    test_message = async.send_test_message(channel, queue)

    def check_message(channel_number, method, header, body):
        global connection, confirmed
        if body == test_message:
            confirmed = True
        connection.ioloop.stop()

    channel.basic_get(callback=check_message, queue=queue)
