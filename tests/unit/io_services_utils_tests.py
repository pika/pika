"""Tests for `pika.adapters.utils.io_services_utils` transports."""

import asyncio
import errno
import logging
import socket
import ssl
import unittest
from unittest import mock

import pika.frame
from pika.adapters.utils import io_services_utils, nbio_interface


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


class StreamSocket:
    """
    Stand-in for a socket whose receive buffer always holds `stream`.

    Each `recv(n)` returns up to `n` bytes, as a real socket does, and raises "would block" once
    `stream` is exhausted. Pass `endless=True` for a peer that never stops sending.
    """

    def __init__(self, stream=b'', endless=False):
        self._stream = stream
        self._endless = endless
        self._position = 0

    def recv(self, size):
        if self._endless:
            chunk = bytes((self._position + i) % 251 for i in range(size))
        else:
            chunk = self._stream[self._position:self._position + size]
            if not chunk:
                raise BlockingIOError(errno.EAGAIN, 'would block')
        self._position += len(chunk)
        return chunk

    def expected(self, length):
        """Return the first `length` bytes the peer has sent."""
        if self._endless:
            return bytes(i % 251 for i in range(length))
        return self._stream[:length]


class ReadTurnTests(unittest.TestCase):
    """The read turn takes few receive calls and stays within its budget."""

    def make_transport(self, transport_class, spec, stream):
        self.sock = mock.Mock(spec=spec)
        stream_socket = StreamSocket(**stream)
        self.sock.recv.side_effect = stream_socket.recv
        self.stream_socket = stream_socket
        self.protocol = mock.Mock()
        self.protocol.eof_received.return_value = False
        self.nbio = mock.Mock()
        return transport_class(self.sock, self.protocol, self.nbio)

    def delivered(self):
        return b''.join(
            call[0][0] for call in self.protocol.data_received.call_args_list)

    def assert_delivered_in_order(self, length):
        self.assertEqual(self.delivered(), self.stream_socket.expected(length))

    def test_full_budget_takes_at_most_two_receive_calls(self):
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket,
            {'endless': True})
        transport._on_socket_readable()
        self.assertLessEqual(self.sock.recv.call_count, 2)
        self.assertLessEqual(self.protocol.data_received.call_count, 2)

    def test_many_frames_in_one_read_are_delivered_in_one_dispatch(self):
        stream = pika.frame.Heartbeat().marshal() * 5000
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket,
            {'stream': stream})
        transport._on_socket_readable()
        self.protocol.data_received.assert_called_once_with(stream)

    def test_fragmented_frame_stream_is_forwarded_in_order(self):
        # A body frame that straddles the receive boundary, between
        # heartbeats, to the end of the stream.
        heartbeat = pika.frame.Heartbeat().marshal()
        body = pika.frame.Body(1, b'x' * 100000).marshal()
        stream = heartbeat * 10 + body + heartbeat * 10
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket,
            {'stream': stream})
        transport._on_socket_readable()
        self.assertLessEqual(self.sock.recv.call_count, 3)
        self.assertEqual(self.protocol.data_received.call_count, 2)
        self.assert_delivered_in_order(len(stream))

    def test_plaintext_turn_is_bounded_then_resumes_on_readiness(self):
        transport = self.make_transport(
            io_services_utils._AsyncPlaintextTransport, socket.socket,
            {'endless': True})
        budget = transport._MAX_CONSUME_BYTES
        loop = asyncio.SelectorEventLoop()
        self.addCleanup(loop.close)
        completion = loop.create_future()
        ready_bytes = []

        def ready_work():
            ready_bytes.append(len(self.delivered()))
            loop.call_soon(dispatch_registered_readiness)

        def dispatch_registered_readiness():
            registered_reader()
            completion.set_result(None)

        def receive_data(data):
            if self.protocol.data_received.call_count == 1:
                loop.call_soon(ready_work)

        self.protocol.data_received.side_effect = receive_data
        registered_reader = self.nbio.set_reader.call_args[0][1]
        loop.call_soon(registered_reader)
        loop.run_until_complete(asyncio.wait_for(completion, timeout=1))

        self.assertEqual(ready_bytes, [budget])
        self.assertEqual(len(self.delivered()), 2 * budget)
        self.nbio.remove_reader.assert_not_called()
        self.protocol.connection_lost.assert_not_called()
        self.assert_delivered_in_order(2 * budget)

    def test_tls_turn_is_bounded_then_resumes_from_buffered_data(self):
        transport = self.make_transport(io_services_utils._AsyncSSLTransport,
                                        ssl.SSLSocket, {'endless': True})
        budget = transport._MAX_CONSUME_BYTES
        self.nbio.add_callback_threadsafe.reset_mock()
        loop = asyncio.SelectorEventLoop()
        self.addCleanup(loop.close)
        completion = loop.create_future()
        ready_bytes = []

        def ready_work():
            ready_bytes.append(len(self.delivered()))

        def receive_data(data):
            if self.protocol.data_received.call_count == 1:
                loop.call_soon(ready_work)
            if len(self.delivered()) >= 2 * budget:
                completion.set_result(None)

        scheduled_callbacks = []

        def schedule_callback(callback):
            scheduled_callbacks.append(callback)
            if len(scheduled_callbacks) <= 2:
                loop.call_soon(callback)

        self.nbio.add_callback_threadsafe.side_effect = schedule_callback
        self.protocol.data_received.side_effect = receive_data
        self.nbio.add_callback_threadsafe(transport._on_socket_readable)
        loop.run_until_complete(asyncio.wait_for(completion, timeout=1))

        self.assertEqual(ready_bytes, [budget])
        self.assertEqual(len(self.delivered()), 2 * budget)
        self.assertEqual(len(scheduled_callbacks), 3)
        self.protocol.connection_lost.assert_not_called()

    def test_tls_full_budget_takes_one_receive_call_per_record(self):
        transport = self.make_transport(io_services_utils._AsyncSSLTransport,
                                        ssl.SSLSocket, {'endless': True})
        # `SSLSocket.recv` returns at most one 16 KiB TLS record per call.
        self.sock.recv.side_effect = lambda size: b'x' * min(size, 16 * 1024)
        transport._on_socket_readable()
        self.assertEqual(self.sock.recv.call_count, 7)
        self.assertEqual(len(self.delivered()), transport._MAX_CONSUME_BYTES)


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


class WriteFlowControlTests(unittest.TestCase):
    """The transport pauses and resumes writing at its watermarks (#1859)."""

    HIGH = io_services_utils._AsyncTransportBase._WRITE_HIGH_WATER_BYTES
    LOW = io_services_utils._AsyncTransportBase._WRITE_LOW_WATER_BYTES

    def make_transport(self, cls=io_services_utils._AsyncPlaintextTransport):
        self.protocol = mock.Mock()
        self.sock = mock.Mock(spec=ssl.SSLSocket if cls is io_services_utils.
                              _AsyncSSLTransport else socket.socket)
        self.sock.fileno.return_value = 99
        self.sock.getsockopt.return_value = 2626560  # a default send buffer (#1864)
        return cls(self.sock, self.protocol, mock.Mock())

    def test_pause_writing_is_called_once_past_the_high_water_mark(self):
        for cls in (io_services_utils._AsyncPlaintextTransport,
                    io_services_utils._AsyncSSLTransport):
            with self.subTest(transport=cls.__name__):
                transport = self.make_transport(cls)
                transport.write(b'x' * self.HIGH)
                self.protocol.pause_writing.assert_not_called()
                transport.write(b'x')
                self.protocol.pause_writing.assert_called_once_with()
                transport.write(b'x' * self.HIGH)
                self.protocol.pause_writing.assert_called_once_with()

    def test_resume_writing_is_called_once_drained_to_the_low_water_mark(self):
        transport = self.make_transport()
        transport.write(b'x' * (self.HIGH + 1))
        backlog = self.HIGH + 1
        # Send one byte too few to reach the low-water mark, then the rest.
        self.sock.send.side_effect = [
            backlog - self.LOW - 1, 1,
            BlockingIOError(errno.EAGAIN, 'full')
        ]
        with self.assertRaises(BlockingIOError):
            transport._produce()
        self.protocol.resume_writing.assert_called_once_with()
        self.assertEqual(transport.get_write_buffer_size(), self.LOW)

    def test_resume_writing_is_not_called_above_the_low_water_mark(self):
        transport = self.make_transport()
        transport.write(b'x' * (self.HIGH + 1))
        self.sock.send.side_effect = [
            self.HIGH - self.LOW,
            BlockingIOError(errno.EAGAIN, 'full')
        ]
        with self.assertRaises(BlockingIOError):
            transport._produce()
        self.assertEqual(transport.get_write_buffer_size(), self.LOW + 1)
        self.protocol.resume_writing.assert_not_called()

    def test_writing_pauses_again_after_a_resume(self):
        transport = self.make_transport()
        transport.write(b'x' * (self.HIGH + 1))
        self.sock.send.side_effect = [self.HIGH + 1]
        transport._produce()
        self.protocol.resume_writing.assert_called_once_with()
        transport.write(b'x' * (self.HIGH + 1))
        self.assertEqual(self.protocol.pause_writing.call_count, 2)

    def test_resume_writing_is_not_called_without_a_pause(self):
        transport = self.make_transport()
        transport.write(b'x' * 10)
        self.sock.send.side_effect = [10]
        transport._produce()
        self.protocol.resume_writing.assert_not_called()

    def test_stream_protocol_defaults_do_nothing(self):

        class Protocol(nbio_interface.AbstractStreamProtocol):

            def connection_made(self, transport):
                pass

            def connection_lost(self, error):
                pass

            def eof_received(self):
                return None

            def data_received(self, data):
                pass

        self.assertIsNone(Protocol().pause_writing())
        self.assertIsNone(Protocol().resume_writing())


if __name__ == '__main__':
    unittest.main()
