import socket
import unittest
from unittest import mock

import pika._utils


class UtilsTests(unittest.TestCase):

    def test_get_linux_version_normal(self):
        self.assertEqual(pika._utils.get_linux_version('4.11.0-2-amd64'),
                         (4, 11, 0))

    def test_get_linux_version_short(self):
        self.assertEqual(pika._utils.get_linux_version('4.11.0'), (4, 11, 0))

    def test_get_linux_version_gcp(self):
        self.assertEqual(pika._utils.get_linux_version('4.4.64+'), (4, 4, 64))

    def test_to_digit(self):
        self.assertEqual(pika._utils.to_digit('64'), 64)

    def test_to_digit_with_plus_sign(self):
        self.assertEqual(pika._utils.to_digit('64+'), 64)

    def test_to_digit_with_dot(self):
        self.assertEqual(pika._utils.to_digit('64.'), 64)


class NonblockingSocketpairTests(unittest.TestCase):

    def test_returns_a_connected_nonblocking_pair(self):
        ssock, csock = pika._utils.nonblocking_socketpair()
        self.addCleanup(ssock.close)
        self.addCleanup(csock.close)
        self.assertEqual(ssock.getpeername(), csock.getsockname())
        self.assertEqual(ssock.gettimeout(), 0.0)
        self.assertEqual(csock.gettimeout(), 0.0)

    def test_rejects_a_connection_that_reached_the_listener_first(self):
        intruders = []
        clients = []
        real_connect = socket.socket.connect

        def connect_after_an_intruder(sock, address):
            # The intruder's connection is queued first, so accept() returns it.
            intruder = socket.socket(sock.family, sock.type)
            self.addCleanup(intruder.close)
            real_connect(intruder, address)
            intruders.append(intruder)
            clients.append(sock)
            return real_connect(sock, address)

        patcher = mock.patch.object(socket.socket, 'connect',
                                    connect_after_an_intruder)
        patcher.start()
        self.addCleanup(patcher.stop)
        with self.assertRaisesRegex(ConnectionError,
                                    'Unexpected peer connection'):
            pika._utils.nonblocking_socketpair()
        self.assertEqual(len(intruders), 1,
                         'the test no longer interposes a connection')
        self.assertEqual(clients[0].fileno(), -1, 'client socket left open')
