# -*- coding: utf8 -*-
"""
Tests for pika.adapters.blocking_connection.BlockingConnection

"""
import errno
import socket
import struct
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver

import threading

try:
    from unittest import mock
    patch = mock.patch
except ImportError:
    import mock
    from mock import patch
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika
import pika.adapters.base_connection
import pika.connection
import pika.exceptions
from pika.adapters import blocking_connection


class BlockingConnectionMockTemplate(blocking_connection.BlockingConnection):
    pass


class SelectConnectionTemplate(blocking_connection.SelectConnection):
    is_closed = False
    is_closing = False
    is_open = True
    outbound_buffer = []
    _channels = dict()


class BlockingConnectionTests(unittest.TestCase):
    """TODO: test properties"""

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_constructor(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        select_connection_class_mock.assert_called_once_with(
            parameters='params',
            on_open_callback=mock.ANY,
            on_open_error_callback=mock.ANY,
            on_close_callback=mock.ANY,
            stop_ioloop_on_close=mock.ANY)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._opened_result.set_value_once(
            select_connection_class_mock.return_value)

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection._process_io_for_connection_setup()

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup_fails_with_open_error(
            self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        exc_value = pika.exceptions.AMQPConnectionError('failed')
        connection._open_error_result.set_value_once(
            select_connection_class_mock.return_value, exc_value)

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
                connection._process_io_for_connection_setup()

            self.assertEqual(cm.exception, exc_value)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._opened_result.set_value_once(
            select_connection_class_mock.return_value)

        connection._flush_output(lambda: False, lambda: True)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output_user_initiated_close(self,
                                               select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = True
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value,
            200, 'success')

        connection._flush_output(lambda: False, lambda: True)

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output_server_initiated_error_close(
            self,
            select_connection_class_mock):

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = False
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value, 404, 'not found')

        with self.assertRaises(pika.exceptions.ConnectionClosed) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (404, 'not found'))

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False, outbound_buffer=[])
    def test_flush_output_server_initiated_no_error_close(self,
                                                       select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = False
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value,
            200, 'ok')

        with self.assertRaises(pika.exceptions.ConnectionClosed) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(
            cm.exception.args,
            ())

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_close(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._impl._channels = {1: mock.Mock()}

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

        select_connection_class_mock.return_value.close.assert_called_once_with(
            200, 'text')

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    @patch.object(blocking_connection, 'BlockingChannel',
                  spec_set=blocking_connection.BlockingChannel)
    def test_channel(self, blocking_channel_class_mock,
                     select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            channel = connection.channel()

    @patch.object(blocking_connection, 'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_sleep(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection.sleep(0.00001)


HAS_SO_LINGER = hasattr(socket, 'SO_LINGER')


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):

    def log(self, m):
        self.server.log(m)

    TO_RECEIVE = b''.join([
        b'AMQP\x00\x00\t\x01',
        b'\x01\x00\x00\x00\x00\x01"\x00\n\x00\x0b\x00\x00\x00\xfe\x07versionS\x00\x00\x00\x060.10.0\x08platformS\x00\x00\x00\x0cPython 3.4.3\x07productS\x00\x00\x00\x1aPika Python Client Library\x0ccapabilitiesF\x00\x00\x00o\x12connection.blockedt\x01\nbasic.nackt\x01\x12publisher_confirmst\x01\x16consumer_cancel_notifyt\x01\x1cauthentication_failure_closet\x01\x0binformationS\x00\x00\x00\x18See http://pika.rtfd.org\x05PLAIN\x00\x00\x00\x0c\x00guest\x00guest\x05en_US\xce',
        b'\x01\x00\x00\x00\x00\x00\x0c\x00\n\x00\x1f\x00\x00\x00\x02\x00\x00\x02D\xce',
        b'\x01\x00\x00\x00\x00\x00\x08\x00\n\x00(\x01/\x00\x01\xce'
    ])

    TO_SEND = [
        b"\x01\x00\x00\x00\x00\x01\xc3\x00\n\x00\n\x00\t\x00\x00\x01\x9e\x0ccapabilitiesF\x00\x00\x00\xb5\x12publisher_confirmst\x01\x1aexchange_exchange_bindingst\x01\nbasic.nackt\x01\x16consumer_cancel_notifyt\x01\x12connection.blockedt\x01\x13consumer_prioritiest\x01\x1cauthentication_failure_closet\x01\x10per_consumer_qost\x01\x0ccluster_nameS\x00\x00\x00\nrabbit@taf\tcopyrightS\x00\x00\x00'Copyright (C) 2007-2014 GoPivotal, Inc.\x0binformationS\x00\x00\x005Licensed under the MPL.  See http://www.rabbitmq.com/\x08platformS\x00\x00\x00\nErlang/OTP\x07productS\x00\x00\x00\x08RabbitMQ\x07versionS\x00\x00\x00\x053.5.3\x00\x00\x00\x0eAMQPLAIN PLAIN\x00\x00\x00\x05en_US\xce",
        b'\x01\x00\x00\x00\x00\x00\x0c\x00\n\x00\x1e\x00\x00\x00\x02\x00\x00\x02D\xce',
        b'\x01\x00\x00\x00\x00\x00\x05\x00\n\x00)\x00\xce'
    ]

    def __init__(self, *args, **kwargs):
        self.already_received = b''
        socketserver.BaseRequestHandler.__init__(self, *args, **kwargs)

    def recv(self, n):
        if len(self.already_received) >= len(self.TO_RECEIVE):
            # important: some packets may arrive combined,
            # so we have to count what we already have received,
            # in order to not try to receive more than what we expect to..
            # which otherwise would make us to possibly block forever
            return
        self.log('reading %s ..' % n)
        data = self.request.recv(n)
        self.log('received = %r' % data)
        self.already_received += data

    def send(self, data):
        self.log('sending %r' % data)
        self.request.send(data)
        self.log('sent ok')

    def handle(self):
        self.log('handling new connection..')
        server = self.server
        self.recv(1024)
        for d in self.TO_SEND:
            self.send(d)
            self.recv(1024)
        self.log("waiting ev1")
        server.ev1.wait()
        self.log("got ev1")

        if HAS_SO_LINGER:
            # the following setsockopt should make the trick to have
            # the connection get a RST packet at the tcp level..
            l_onoff = 1
            l_linger = 0
            self.request.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                    struct.pack('ii', l_onoff, l_linger))
        self.log('closing connection ..')
        self.request.close()  # ..once we close it here.
        server.connections_received += 1
        server.ev2.set()
        self.log('ev2 set')


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        verbose = kwargs.pop('verbose', 0)
        socketserver.TCPServer.__init__(self, *args, **kwargs)
        self.ev1 = threading.Event()
        self.ev2 = threading.Event()
        self.connections_received = 0
        self.verbose = verbose

    def log(self, *args):
        if self.verbose:
            new_args = [
                '%s> %s' % (threading.current_thread().name, args[0])]
            new_args.extend(args[1:])
            print(''.join(map(str, new_args)))


class TestChannelCreationOnBrokenConnection(unittest.TestCase):

    def test_that_it_raise_closed_connection(self):

        expected_error = errno.ECONNRESET

        tcp_server = ThreadedTCPServer(('127.0.0.1', 0), ThreadedTCPRequestHandler, verbose=1)
        _, port = tcp_server.server_address

        log = tcp_server.log

        log("port=", port)

        server_thread = threading.Thread(target=tcp_server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        try:

            params = pika.connection.ConnectionParameters(host='127.0.0.1', port=port)

            # initiate a connection:
            log('initiating connection..')
            conn = blocking_connection.BlockingConnection(parameters=params)
            log("connection done")
            tcp_server.ev1.set()
            log("set ev1 and wait ev2")
            tcp_server.ev2.wait()
            log("got ev2")

            if not HAS_SO_LINGER:
                # without SO_LINGER we can't be sure that we'll get a RST a the tcp-level.
                # but we can still trigger/force manually the right error

                class _Proxy(object):
                    def __init__(self, obj):
                        self._obj = obj

                    def __getattribute__(self, item):
                        if item in ('_obj', 'send'):
                            return object.__getattribute__(self, item)
                        return getattr(self._obj, item)

                    def send(self, *args):
                        raise socket.error(expected_error, "douh")

                conn._impl.socket = _Proxy(conn._impl.socket)

            log('now creating new channel ..')
            with mock.patch.object(
                    pika.adapters.base_connection,
                    'LOGGER') as logger_mock:
                with self.assertRaises(pika.exceptions.ConnectionClosed):
                    conn.channel()

            self.assertEqual(1, tcp_server.connections_received)
            logger_mock.error.assert_called_with("Socket Error: %s", expected_error)
        finally:
            tcp_server.shutdown()
            tcp_server.server_close()
