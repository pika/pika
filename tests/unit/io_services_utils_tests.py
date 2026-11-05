"""Tests for `pika.adapters.utils.io_services_utils` transports."""

import errno
import socket
import unittest
from unittest import mock

from pika.adapters.utils import io_services_utils


class PlaintextTransportReadableTests(unittest.TestCase):

    def setUp(self):
        self.local, self.peer = socket.socketpair()
        self.local.setblocking(False)
        self.addCleanup(self.local.close)
        self.addCleanup(self.peer.close)
        self.protocol = mock.Mock()
        # As the real protocol does, so an idle read mistaken for EOF closes.
        self.protocol.eof_received.return_value = False
        self.transport = self.make_transport(self.local)

    def make_transport(self, sock):
        nbio = mock.Mock()
        # Run deferred callbacks at once, so connection_lost is observable.
        nbio.add_callback_threadsafe.side_effect = lambda callback: callback()
        return io_services_utils._AsyncPlaintextTransport(
            sock, self.protocol, nbio)

    def test_data_then_would_block_keeps_the_connection_open(self):
        self.peer.sendall(b'frame')
        self.transport._on_socket_readable()
        self.protocol.data_received.assert_called_once_with(b'frame')
        self.protocol.connection_lost.assert_not_called()

    def test_would_block_with_no_data_keeps_the_connection_open(self):
        self.transport._on_socket_readable()
        self.protocol.data_received.assert_not_called()
        self.protocol.connection_lost.assert_not_called()

    def test_try_again_error_from_data_received_aborts_the_connection(self):
        error = BlockingIOError(errno.EAGAIN, 'raised by a callback')
        self.protocol.data_received.side_effect = error
        self.peer.sendall(b'frame')
        self.transport._on_socket_readable()
        self.protocol.connection_lost.assert_called_once_with(error)

    def test_other_error_from_recv_aborts_the_connection(self):
        error = ConnectionResetError(errno.ECONNRESET, 'reset by peer')
        sock = mock.Mock(spec=socket.socket)
        sock.recv.side_effect = error
        self.make_transport(sock)._on_socket_readable()
        self.protocol.connection_lost.assert_called_once_with(error)


if __name__ == '__main__':
    unittest.main()
