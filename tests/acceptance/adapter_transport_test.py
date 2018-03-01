"""Adapter transport test

"""
import collections
from datetime import datetime
import errno
import functools
import logging
import os
import socket
import unittest

from forward_server import ForwardServer

from pika.adapters import adapter_transport
from pika.adapters.ioloop_interface import PollEvents
from pika.adapters import select_connection
import pika.compat


# Disable warning about access to protected member
# pylint: disable=W0212

# Disable warning Attribute defined outside __init__
# pylint: disable=W0201

# Disable warning Missing docstring
# pylint: disable=C0111

# Disable warning Too many public methods
# pylint: disable=R0904

# Disable warning Invalid variable name
# pylint: disable=C0103


LOGGER = logging.getLogger(__name__)

PARAMS_URL_TEMPLATE = (
    'amqp://guest:guest@127.0.0.1:%(port)s/%%2f?socket_timeout=1')
DEFAULT_URL = PARAMS_URL_TEMPLATE % {'port': 5672}
DEFAULT_PARAMS = pika.URLParameters(DEFAULT_URL)
DEFAULT_TIMEOUT = 15


def setUpModule():
    logging.basicConfig(level=logging.DEBUG)


class TransportTestCaseBase(unittest.TestCase):
    """Base class for Transport test cases

    """

    DEFAULT_TEST_TIMEOUT = 15

    def create_ioloop_with_timeout(self):
        """Create IOLoop with test timeout and schedule cleanup to close it

        """
        ioloop = select_connection.IOLoop()
        self.addCleanup(ioloop.close)

        def _on_test_timeout():
            """Called when test times out"""
            LOGGER.info('%s TIMED OUT (%s)', datetime.utcnow(), self)
            self.fail('Test timed out')

        ioloop.add_timeout(self.DEFAULT_TEST_TIMEOUT, _on_test_timeout)

        return ioloop


    def create_non_blocking_tcp_socket(self):
        """Create a TCP stream socket and schedule cleanup to close it

        """
        sock = socket.socket()
        sock.setblocking(False)
        self.addCleanup(sock.close)
        return sock

    @staticmethod
    def safe_connect_nonblocking_socket(sock, addr_pair):
        """Initiate socket connection, suppressing EINPROGRESS/EWOULDBLOCK
        :param socket.socket sock

        """
        try:
            sock.connect(addr_pair)
        except pika.compat.SOCKET_ERROR as error:
            # EINPROGRESS for posix and EWOULDBLOCK for windows
            if error.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK,):
                raise

    def get_dead_socket_address(self):
        """

        :return: socket address pair (ip-addr, port) that is guaranteed to
            refuse connection

        """
        s1, s2 = pika.compat._nonblocking_socketpair()
        s2.close()
        self.addCleanup(s1.close)
        return s1.getsockname()  # pylint: disable=E1101


class PlainTransportTestCase(TransportTestCaseBase):

    def test_constructor_without_on_connected_callback(self):
        sock = self.create_non_blocking_tcp_socket()
        transport = adapter_transport.PlainTransport(sock)
        self.assertEqual(transport.poll_which_events(), 0)

    def test_constructor_with_on_connected_callback(self):
        sock = self.create_non_blocking_tcp_socket()
        transport = adapter_transport.PlainTransport(sock, lambda t: None)
        self.assertEqual(transport.poll_which_events(), PollEvents.WRITE)

    def test_connection_reported_via_on_connected_callback(self):
        ioloop = self.create_ioloop_with_timeout()

        sock = self.create_non_blocking_tcp_socket()

        # Run echo server
        with ForwardServer(remote_addr=None) as echo:
            # Initiate socket connection
            self.safe_connect_nonblocking_socket(sock, echo.server_address)

            on_connected_called = []

            def on_connected(transport):
                on_connected_called.append(transport)
                ioloop.stop()

            transport = adapter_transport.PlainTransport(sock, on_connected)

            def handle_socket_events(_fd, in_events):
                transport.handle_events(in_events)
                events = transport.poll_which_events()
                self.assertEqual(events,
                                 0 if on_connected_called else PollEvents.WRITE)
                ioloop.update_handler(sock.fileno(), events)

            # Add IO event watcher for our socket
            events = transport.poll_which_events()
            self.assertEqual(events, PollEvents.WRITE)
            ioloop.add_handler(sock.fileno(), handle_socket_events, events)

            # Start event loop; it should get stopped by on_connected
            ioloop.start()

            # Check
            self.assertEqual(len(on_connected_called), 1)
            self.assertIs(on_connected_called[0], transport)
            self.assertEqual(transport.poll_which_events(), 0)

    def test_connection_establishment_failed_while_waiting_for_connection(self):

        ioloop = self.create_ioloop_with_timeout()

        sock = self.create_non_blocking_tcp_socket()

        self.safe_connect_nonblocking_socket(sock,
                                             self.get_dead_socket_address())

        on_connected_called = []
        def on_connected(_transport):
            on_connected_called.append(1)
            self.fail("Unexpected on_connected callback.")

        transport = adapter_transport.PlainTransport(sock, on_connected)

        handle_socket_events_called = []
        def handle_socket_events(_fd, in_events):
            expected_events = PollEvents.WRITE
            self.assertEqual(in_events & expected_events, expected_events)
            with self.assertRaises(pika.compat.SOCKET_ERROR) as cm:
                transport.handle_events(in_events)
            self.assertEqual(cm.exception.errno, errno.ECONNREFUSED)
            self.assertIsNone(transport.on_connected_callback)
            self.assertEqual(transport.poll_which_events(), 0)
            ioloop.stop()
            handle_socket_events_called.append(1)

        # Add IO event watcher for our socket
        events = transport.poll_which_events()
        self.assertEqual(events, PollEvents.WRITE)
        ioloop.add_handler(sock.fileno(), handle_socket_events, events)

        # Start event loop; it should get stopped by handle_socket_events
        ioloop.start()

        # Check
        self.assertEqual(on_connected_called, [])
        self.assertEqual(handle_socket_events_called, [1])

    def test_data_exchange_after_on_connected_callback(self):
        original_data = tuple(
            os.urandom(1000) for _ in pika.compat.xrange(1000))
        original_data_length = sum(len(s) for s in original_data)
        tx_buffers = collections.deque(original_data)
        rx_buffers = []

        ioloop = self.create_ioloop_with_timeout()

        sock = self.create_non_blocking_tcp_socket()

        # Run echo server
        with ForwardServer(remote_addr=None) as echo:
            # Initiate socket connection
            self.safe_connect_nonblocking_socket(sock, echo.server_address)

            def rx_sink(data):
                rx_buffers.append(data)
                current_rx_length = sum(len(s) for s in rx_buffers)
                self.assertLessEqual(current_rx_length, original_data_length)
                if current_rx_length == original_data_length:
                    ioloop.stop()

            def begin_transporting(transport):
                LOGGER.debug("%s: In begin_transporting", self)
                self.assertEqual(transport.poll_which_events(), 0)
                transport.begin_transporting(tx_buffers=tx_buffers,
                                             rx_sink=rx_sink,
                                             max_rx_bytes=4096)
                self.assertEqual(transport.poll_which_events(),
                                 PollEvents.READ | PollEvents.WRITE)

                ioloop.update_handler(sock.fileno(),
                                      transport.poll_which_events())

            def on_connected(transport):
                LOGGER.debug("%s: In on_connected", self)
                ioloop.add_callback_threadsafe(
                    functools.partial(begin_transporting, transport))

            transport = adapter_transport.PlainTransport(sock, on_connected)

            def handle_socket_events(_fd, in_events):
                transport.handle_events(in_events)
                events = transport.poll_which_events()
                ioloop.update_handler(sock.fileno(), events)

            # Add IO event watcher for our socket
            events = transport.poll_which_events()
            self.assertEqual(events, PollEvents.WRITE)
            ioloop.add_handler(sock.fileno(), handle_socket_events, events)

            # Start event loop; it should get stopped by rx_sink
            ioloop.start()

            # Check
            self.assertEqual(len(tx_buffers), 0)
            self.assertEqual(sum(len(s) for s in rx_buffers),
                             original_data_length)
            self.assertEqual(b''.join(rx_buffers), b''.join(original_data))
            self.assertEqual(transport.poll_which_events(), PollEvents.READ)

    def test_data_exchange_sans_on_connected_callback(self):
        original_data = tuple(
            os.urandom(1000) for _ in pika.compat.xrange(1000))
        original_data_length = sum(len(s) for s in original_data)
        tx_buffers = collections.deque(original_data)
        rx_buffers = []

        ioloop = self.create_ioloop_with_timeout()

        sock = self.create_non_blocking_tcp_socket()

        # Run echo server
        with ForwardServer(remote_addr=None) as echo:
            # Initiate socket connection
            self.safe_connect_nonblocking_socket(sock, echo.server_address)

            def rx_sink(data):
                rx_buffers.append(data)
                current_rx_length = sum(len(s) for s in rx_buffers)
                self.assertLessEqual(current_rx_length, original_data_length)
                if current_rx_length == original_data_length:
                    ioloop.stop()

            transport = adapter_transport.PlainTransport(sock)

            self.assertEqual(transport.poll_which_events(), 0)
            transport.begin_transporting(tx_buffers=tx_buffers,
                                         rx_sink=rx_sink,
                                         max_rx_bytes=4096)
            self.assertEqual(transport.poll_which_events(),
                             PollEvents.READ | PollEvents.WRITE)

            # Add IO event watcher for our socket
            def handle_socket_events(_fd, in_events):
                transport.handle_events(in_events)
                ioloop.update_handler(sock.fileno(),
                                      transport.poll_which_events())

            ioloop.add_handler(sock.fileno(),
                               handle_socket_events,
                               transport.poll_which_events())

            # Start event loop; it should get stopped by rx_sink
            ioloop.start()

            # Check
            self.assertEqual(len(tx_buffers), 0)
            self.assertEqual(sum(len(s) for s in rx_buffers),
                             original_data_length)
            self.assertEqual(b''.join(rx_buffers), b''.join(original_data))
            self.assertEqual(transport.poll_which_events(), PollEvents.READ)
