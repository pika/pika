"""Tests for `pika.adapters.utils.io_services_utils` transports."""

import errno
import logging
import socket
import ssl
import unittest
from unittest import mock

import pika
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


class TransportFailureIsLoggedOnceTests(unittest.TestCase):

    def readable_failure_records(self, transport_class, sock):
        protocol = mock.Mock()
        protocol.data_received.side_effect = RuntimeError('from data_received')
        nbio = mock.Mock()
        transport = transport_class(sock, protocol, nbio)
        nbio.add_callback_threadsafe.side_effect = lambda callback: callback()
        if not sock.recv.side_effect:
            sock.recv.return_value = b'frame'
        with self.assertLogs('pika', level='ERROR') as logs:
            transport._on_socket_readable()
        protocol.connection_lost.assert_called_once()
        return logs.records

    def assert_one_traceback(self, records):
        self.assertEqual(len(records), 1, [r.getMessage() for r in records])
        formatted = logging.Formatter().format(records[0])
        self.assertEqual(formatted.count('Traceback (most recent call last)'),
                         1, formatted)

    def test_plaintext_data_received_failure(self):
        sock = mock.Mock(spec=socket.socket)
        self.assert_one_traceback(
            self.readable_failure_records(
                io_services_utils._AsyncPlaintextTransport, sock))

    def test_tls_data_received_failure(self):
        sock = mock.Mock(spec=ssl.SSLSocket)
        self.assert_one_traceback(
            self.readable_failure_records(io_services_utils._AsyncSSLTransport,
                                          sock))

    def test_tls_recv_failure(self):
        sock = mock.Mock(spec=ssl.SSLSocket)
        sock.recv.side_effect = ssl.SSLError(1, 'decryption failed')
        self.assert_one_traceback(
            self.readable_failure_records(io_services_utils._AsyncSSLTransport,
                                          sock))

    def writable_failure_records(self, transport_class, sock):
        sock.getsockopt.return_value = 2626560
        protocol = mock.Mock()
        nbio = mock.Mock()
        transport = transport_class(sock, protocol, nbio)
        nbio.add_callback_threadsafe.side_effect = lambda callback: callback()
        transport.write(b'frame')
        with self.assertLogs('pika', level='ERROR') as logs:
            transport._on_socket_writable()
        # The send's own error, so a failure earlier in the write path cannot stand in for it.
        protocol.connection_lost.assert_called_once_with(sock.send.side_effect)
        return logs.records

    def test_plaintext_send_failure(self):
        sock = mock.Mock(spec=socket.socket)
        sock.send.side_effect = ConnectionResetError(errno.ECONNRESET, 'reset')
        self.assert_one_traceback(
            self.writable_failure_records(
                io_services_utils._AsyncPlaintextTransport, sock))

    def test_tls_send_failure(self):
        sock = mock.Mock(spec=ssl.SSLSocket)
        sock.send.side_effect = ssl.SSLError(1, 'bad record mac')
        self.assert_one_traceback(
            self.writable_failure_records(io_services_utils._AsyncSSLTransport,
                                          sock))


class TransportWriteBatchTests(unittest.TestCase):

    # What a default Linux loopback socket reports, so the 64 KiB cap applies.
    DEFAULT_SNDBUF = 2626560

    def make_transport(self, transport_class, sock_spec):
        self.sock = mock.Mock(spec=sock_spec)
        self.sock.getsockopt.return_value = self.DEFAULT_SNDBUF
        self.protocol = mock.Mock()
        self.nbio = mock.Mock()
        return transport_class(self.sock, self.protocol, self.nbio)

    def test_plaintext_batches_adjacent_small_writes(self):
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket)
        for chunk in (b'one', b'two', b'three'):
            transport.write(chunk)

        self.sock.send.side_effect = lambda data: len(data)
        transport._on_socket_writable()

        self.sock.send.assert_called_once_with(b'onetwothree')
        self.assertEqual(transport.get_write_buffer_size(), 0)

    def test_batch_size_is_bounded_for_large_writes(self):
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket)
        chunks = [b'a' * 40000, b'b' * 40000, b'c' * 40000]
        for chunk in chunks:
            transport.write(chunk)

        sent = []

        def send(data):
            sent.append(data)
            return len(data)

        self.sock.send.side_effect = send
        transport._on_socket_writable()

        self.assertEqual(b''.join(sent), b''.join(chunks))
        self.assertTrue(
            all(
                len(chunk) <= transport._MAX_COALESCED_WRITE_BYTES
                for chunk in sent))

    def test_batches_are_capped_at_half_the_reported_send_buffer(self):
        """Linux reports twice the SO_SNDBUF that was set; that set value is the cap (#1864)."""
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket)
        self.sock.getsockopt.return_value = 8192
        chunks = [bytes([65 + i]) * 1000 for i in range(10)]
        for chunk in chunks:
            transport.write(chunk)
        sent = []
        self.sock.send.side_effect = lambda data: sent.append(data) or len(data)

        transport._on_socket_writable()

        self.sock.getsockopt.assert_called_with(socket.SOL_SOCKET,
                                                socket.SO_SNDBUF)
        self.assertEqual(b''.join(sent), b''.join(chunks))
        self.assertEqual([len(data) for data in sent], [4000, 4000, 2000])

    def test_the_cap_follows_the_send_buffer_as_it_changes(self):
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket)
        self.sock.send.side_effect = lambda data: len(data)
        for sndbuf, expected in ((4096, 2), (self.DEFAULT_SNDBUF, 1)):
            with self.subTest(sndbuf=sndbuf):
                self.sock.send.reset_mock()
                self.sock.getsockopt.return_value = sndbuf
                transport.write(b'a' * 1500)
                transport.write(b'b' * 1500)
                transport._on_socket_writable()
                self.assertEqual(self.sock.send.call_count, expected)

    def test_a_batch_may_fill_the_cap_but_not_exceed_it(self):
        cap = io_services_utils._AsyncTransportBase._MAX_COALESCED_WRITE_BYTES
        for tail, expected_sends in ((b'b', 1), (b'bb', 2)):
            with self.subTest(tail=tail):
                transport = self.make_transport(
                    io_services_utils._AsyncPlaintextTransport, socket.socket)
                transport.write(b'a' * (cap - 1))
                transport.write(tail)
                self.sock.send.side_effect = lambda data: len(data)
                transport._on_socket_writable()
                self.assertEqual(self.sock.send.call_count, expected_sends)

    def test_legal_large_frame_is_sent_without_copying_or_splitting(self):
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket)
        legal_frame = pika.frame.Body(1, b'x' * (128 * 1024 - 8)).marshal()
        transport.write(legal_frame)
        self.sock.send.return_value = len(legal_frame)

        transport._on_socket_writable()

        self.sock.send.assert_called_once()
        self.assertIs(self.sock.send.call_args[0][0], legal_frame)
        self.assertEqual(transport.get_write_buffer_size(), 0)

    def test_partial_send_and_would_block_preserve_order(self):
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket)
        transport.write(b'first')
        transport.write(b'second')
        attempts = []

        def send(data):
            attempts.append(data)
            if len(attempts) == 1:
                return 4
            if len(attempts) == 2:
                raise BlockingIOError(errno.EWOULDBLOCK, 'would block')
            return len(data)

        self.sock.send.side_effect = send
        transport._on_socket_writable()
        self.assertEqual(transport.get_write_buffer_size(), 7)
        transport._on_socket_writable()

        self.assertEqual(attempts[0], b'firstsecond')
        self.assertEqual(attempts[1], b'tsecond')
        self.assertEqual(b''.join((attempts[0][:4], attempts[2])),
                         b'firstsecond')
        self.assertEqual(transport.get_write_buffer_size(), 0)

    def test_tls_want_retry_keeps_its_data_when_new_output_is_queued(self):
        for want in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE):
            with self.subTest(want=want):
                transport = self.make_transport(
                    io_services_utils._AsyncSSLTransport, ssl.SSLSocket)
                transport.write(b'first')
                transport.write(b'second')
                attempts = []

                def send(data, attempts=attempts, want=want):
                    attempts.append(data)
                    if len(attempts) == 1:
                        raise ssl.SSLError(want, 'retry')
                    return len(data)

                self.sock.send.side_effect = send
                transport._on_socket_writable()
                self.assertEqual(transport.get_write_buffer_size(), 11)
                transport.write(b'new')
                transport.write(b'more')
                self.assertEqual(transport.get_write_buffer_size(), 18)

                if want == ssl.SSL_ERROR_WANT_READ:
                    transport._on_socket_readable()
                else:
                    transport._on_socket_writable()

                # The retry repeats its buffer; batching then resumes.
                self.assertEqual(attempts,
                                 [b'firstsecond', b'firstsecond', b'newmore'])
                self.assertEqual(transport.get_write_buffer_size(), 0)


if __name__ == '__main__':
    unittest.main()
