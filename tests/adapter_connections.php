#!/usr/bin/env python
"""
Connection Test

First test to make sure all adapters can connect and disconnect properly
"""
import nose
import pika
import pika.adapters
from pika.adapters.tornado_connection import TornadoConnection

connected = False  # Global connected flag, resets in each test

HOST = 'localhost'
PORT = 5672

""" Hanging
@nose.tools.timed(2)
def test_asyncore_connection():
    global connected
    connected = False
    pika.adapters.select_connection.SELECT_TYPE = 'epoll'
    parameters = pika.ConnectionParameters(HOST, PORT)
    connection = pika.adapters.AsyncoreConnection(parameters,
                                                  on_connected)
    connection.ioloop.start()
    if not connected:
        assert False
    else:
        pass
"""

@nose.tools.timed(2)
def test_select_epoll_connection():
    global connected
    connected = False
    pika.adapters.select_connection.SELECT_TYPE = 'epoll'
    parameters = pika.ConnectionParameters(HOST, PORT)
    connection = pika.adapters.SelectConnection(parameters,
                                                on_connected)
    connection.ioloop.start()
    if not connected:
        assert False
    else:
        pass

@nose.tools.timed(2)
def test_select_poll_connection():
    global connected
    connected = False
    pika.adapters.select_connection.SELECT_TYPE = 'poll'

    print pika.adapters.select_connection.SELECT_TYPE

    parameters = pika.ConnectionParameters(HOST, PORT)
    connection = pika.adapters.SelectConnection(parameters,
                                                on_connected)
    connection.ioloop.start()
    if not connected:
        assert False
    else:
        pass

@nose.tools.timed(2)
def test_select_select_connection():
    global connected
    connected = False
    pika.adapters.select_connection.SELECT_TYPE = 'select'
    parameters = pika.ConnectionParameters(HOST, PORT)
    connection = pika.adapters.SelectConnection(parameters,
                                                on_connected)
    connection.ioloop.start()
    if not connected:
        assert False
    else:
        pass

@nose.tools.timed(2)
def test_select_kqueue_connection():
    global connected
    connected = False
    pika.adapters.select_connection.SELECT_TYPE = 'kqueue'
    parameters = pika.ConnectionParameters(HOST, PORT)
    connection = pika.adapters.SelectConnection(parameters,
                                                on_connected)
    connection.ioloop.start()
    if not connected:
        assert False
    else:
        pass

@nose.tools.timed(2)
def test_select_tornado_connection():
    global connected
    connected = False
    pika.adapters.select_connection.SELECT_TYPE = 'epoll'
    parameters = pika.ConnectionParameters(HOST, PORT)
    connection = TornadoConnection(parameters, on_connected)
    connection.ioloop.start()
    if not connected:
        assert False
    else:
        pass


@nose.tools.nottest
def on_connected(connection):
    global connected
    connected = connection.is_open()
    connection.ioloop.stop()
