"""Adapter transport test

"""
from __future__ import print_function

import collections
from datetime import datetime
import errno
import functools
import logging
import os
import platform
import socket
import sys
import unittest

import mock

from .forward_server import ForwardServer

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


def _trace_stderr(fmt, *args):
    """Format and output the text to stderr"""
    print((fmt % args) + "\n", end="", file=sys.stderr)


def _fd_events_to_str(events):
    str_events = '{}: '.format(events)

    if events & PollEvents.READ:
        str_events += "RD."
    if events & PollEvents.WRITE:
        str_events += "WR."
    if events & PollEvents.ERROR:
        str_events += "ERR."

    remainig_events = events & ~(PollEvents.READ |
                                 PollEvents.WRITE |
                                 PollEvents.ERROR)
    if remainig_events:
        str_events += '+{}'.format(bin(remainig_events))

    return str_events


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


    def create_nonblocking_tcp_socket(self):
        """Create a TCP stream socket and schedule cleanup to close it

        """
        sock = socket.socket()
        sock.setblocking(False)
        self.addCleanup(sock.close)
        return sock

    def create_nonblocking_socketpair(self):
        """Creates a non-blocking socket pair and schedules cleanup to close
        them

        :returns: two-tuple of connected non-blocking sockets

        """
        pair = pika.compat._nonblocking_socketpair()
        self.addCleanup(pair[0].close)
        self.addCleanup(pair[1].close)
        return pair

    def create_blocking_socketpair(self):
        """Creates a blocking socket pair and schedules cleanup to close
        them

        :returns: two-tuple of connected non-blocking sockets

        """
        pair = self.create_nonblocking_socketpair()
        pair[0].setblocking(True)  # pylint: disable=E1101
        pair[1].setblocking(True)
        return pair

    @staticmethod
    def safe_connect_nonblocking_socket(sock, addr_pair):
        """Initiate socket connection, suppressing EINPROGRESS/EWOULDBLOCK
        :param socket.socket sock
        :param addr_pair: two tuple of address string and port integer
        """
        try:
            sock.connect(addr_pair)
        except pika.compat.SOCKET_ERROR as error:
            # EINPROGRESS for posix and EWOULDBLOCK for windows
            if error.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK,):
                raise

    def get_dead_socket_address(self):
        """

        :return: socket address pair (ip-addr, port) that will refuse connection

        """
        s1, s2 = pika.compat._nonblocking_socketpair()
        s2.close()
        self.addCleanup(s1.close)
        return s1.getsockname()  # pylint: disable=E1101


class PlainTransportTestCase(TransportTestCaseBase):
    """PlainTransport tests

    """

    def test_constructor_without_on_connected_callback(self):
        sock = self.create_nonblocking_tcp_socket()
        transport = adapter_transport.PlainTransport(sock)
        self.assertEqual(transport.poll_which_events(), 0)

    def test_constructor_with_on_connected_callback(self):
        sock = self.create_nonblocking_tcp_socket()
        transport = adapter_transport.PlainTransport(sock, lambda t: None)
        self.assertEqual(transport.poll_which_events(),
                         PollEvents.WRITE | PollEvents.ERROR)

    def test_connection_reported_via_on_connected_callback(self):
        ioloop = self.create_ioloop_with_timeout()

        sock = self.create_nonblocking_tcp_socket()

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
            self.assertEqual(events, PollEvents.WRITE | PollEvents.ERROR)
            ioloop.add_handler(sock.fileno(), handle_socket_events, events)

            # Start event loop; it should get stopped by on_connected
            ioloop.start()

            # Check
            self.assertEqual(len(on_connected_called), 1)
            self.assertIs(on_connected_called[0], transport)
            self.assertEqual(transport.poll_which_events(), 0)

    def test_connection_establishment_failed_while_waiting_for_connection(self):

        ioloop = self.create_ioloop_with_timeout()

        sock = self.create_nonblocking_tcp_socket()

        self.safe_connect_nonblocking_socket(sock,
                                             self.get_dead_socket_address())

        on_connected_called = []
        def on_connected(_transport):
            on_connected_called.append(1)
            self.fail("Unexpected on_connected callback.")

        transport = adapter_transport.PlainTransport(sock, on_connected)

        handle_socket_events_called = []
        def handle_socket_events(_fd, in_events):
            # NOTE: Unlike POSIX, Windows select doesn't indicate as
            # readable/writable a socket that failed to connect - it reflects the
            # failure only via exceptfds.
            if platform.system() == 'Windows':
                expected_events = PollEvents.ERROR
            else:
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
        self.assertEqual(events, PollEvents.WRITE | PollEvents.ERROR)
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

        sock = self.create_nonblocking_tcp_socket()

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
                                 PollEvents.READ | PollEvents.WRITE |
                                 PollEvents.ERROR)

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
            self.assertEqual(events, PollEvents.WRITE | PollEvents.ERROR)
            ioloop.add_handler(sock.fileno(), handle_socket_events, events)

            # Start event loop; it should get stopped by rx_sink
            ioloop.start()

            # Check
            self.assertEqual(len(tx_buffers), 0)
            self.assertEqual(sum(len(s) for s in rx_buffers),
                             original_data_length)
            self.assertEqual(b''.join(rx_buffers), b''.join(original_data))
            self.assertEqual(transport.poll_which_events(),
                             PollEvents.READ | PollEvents.ERROR)

    def test_data_exchange_sans_on_connected_callback(self):
        original_data = tuple(
            os.urandom(1000) for _ in pika.compat.xrange(1000))
        original_data_length = sum(len(s) for s in original_data)
        tx_buffers = collections.deque(original_data)
        rx_buffers = []

        ioloop = self.create_ioloop_with_timeout()

        sock = self.create_nonblocking_tcp_socket()

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
                             PollEvents.READ | PollEvents.WRITE |
                             PollEvents.ERROR)

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
            self.assertEqual(transport.poll_which_events(),
                             PollEvents.READ | PollEvents.ERROR)


class SSLTransportTestCase(TransportTestCaseBase):
    """SSLTransport tests

    """

    def test_constructor_without_on_connected_callback(self):
        sock = self.create_nonblocking_tcp_socket()
        transport = adapter_transport.SSLTransport(sock)
        self.assertEqual(transport.poll_which_events(), 0)

    def test_constructor_with_on_connected_callback(self):
        sock = self.create_nonblocking_tcp_socket()
        transport = adapter_transport.SSLTransport(sock, lambda t: None)
        self.assertEqual(transport.poll_which_events(),
                         PollEvents.WRITE | PollEvents.ERROR)


class PlainSocketDiagnostics(TransportTestCaseBase):
    """This test suite outputs diagnostic information usful for debugging the
    IOLoop poller's fd watcher

    """

    def _which_events_are_set_with_varying_eventmasks(self,
                                                      sock,
                                                      requested_eventmasks,
                                                      msg_prefix):
        """Common logic for which_events_are_set_* tests. Runs the event loop
        while varying eventmasks at each socket event callback

        :param ioloop:
        :param sock:
        :param requested_eventmasks: a mutable list of eventmasks to apply after
                                     each socket event callback
        :param msg_prefix: Message prefix to apply when printing watched vs.
                           indicated events.
        """
        ioloop = self.create_ioloop_with_timeout()

        def handle_socket_events(_fd, in_events):
            socket_error = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            socket_error = 0 if socket_error == 0 else '{} ({})'.format(
                socket_error,
                os.strerror(socket_error))

            _trace_stderr('[%s] %s: watching=%s; indicated=%s; sockerr=%s',
                          ioloop._poller.__class__.__name__,
                          msg_prefix,
                          _fd_events_to_str(requested_eventmasks[0]),
                          _fd_events_to_str(in_events),
                          socket_error)

            # NOTE PollEvents.ERROR may be added automatically by some pollers
            #      without being requested.
            self.assertTrue(
                in_events & (requested_eventmasks[0] | PollEvents.ERROR),
                'watching={}; indicated={}'.format(
                    _fd_events_to_str(requested_eventmasks[0]),
                    _fd_events_to_str(in_events)))

            requested_eventmasks.pop(0)

            if requested_eventmasks:
                ioloop.update_handler(sock.fileno(), requested_eventmasks[0])
            else:
                ioloop.stop()

        ioloop.add_handler(sock.fileno(),
                           handle_socket_events,
                           requested_eventmasks[0])
        ioloop.start()


    def test_which_events_are_set_when_failed_to_connect(self):

        msg_prefix = 'ZZZ Failed to connect'

        sock = self.create_nonblocking_tcp_socket()

        self.safe_connect_nonblocking_socket(sock,
                                             self.get_dead_socket_address())
        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        # NOTE: Unlike POSIX, Windows select doesn't indicate as
        # readable/writable a socket that failed to connect - it reflects the
        # failure only via exceptfds.
        if platform.system() == 'Windows':
            _trace_stderr(
                '%s: setting `PollEvents.ERROR` to all event filters on '
                'Windows, because its `select()` does not indicate a socket '
                'that failed to connect as readable or writable.', msg_prefix)
            for i in pika.compat.xrange(len(requested_eventmasks)):
                requested_eventmasks[i] |= PollEvents.ERROR

        self._which_events_are_set_with_varying_eventmasks(
            sock=sock,
            requested_eventmasks=requested_eventmasks,
            msg_prefix=msg_prefix)

    def test_which_events_are_set_after_remote_end_closes(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.close()

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='ZZZ Remote closed')

    def test_which_events_are_set_after_remote_end_closes_with_pending_data(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.send(b'abc')
        s2.close()

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='ZZZ Remote closed with pending data')

    def test_which_events_are_set_after_remote_shuts_rd(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.shutdown(socket.SHUT_RD)

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.WRITE
        ]

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='ZZZ Remote shut RD')

    def test_which_events_are_set_after_remote_shuts_wr(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.shutdown(socket.SHUT_WR)

        requested_eventmasks = [
            (PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR),
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='ZZZ Remote shut WR')

    def test_which_events_are_set_after_remote_shuts_wr_with_pending_data(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.send(b'abc')
        s2.shutdown(socket.SHUT_WR)

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='ZZZ Remote shut WR with pending data')

    def test_which_events_are_set_after_remote_shuts_rdwr(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.shutdown(socket.SHUT_RDWR)

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='ZZZ Remote shut RDWR')

    def test_which_events_are_set_after_local_shuts_rd(self):

        msg_prefix = 'ZZZ Local shut RD'

        s1, _s2 = self.create_blocking_socketpair()

        s1.shutdown(socket.SHUT_RD)  # pylint: disable=E1101

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        # NOTE: Unlike POSIX, Windows select doesn't indicate as readable socket
        #  that was shut down locally with SHUT_RD.
        if platform.system() == 'Windows':
            _trace_stderr(
                '%s: removing check for solo PollEvents.READ on Windows, '
                'because its `select()` does not indicate a socket shut '
                'locally with SHUT_RD as readable.', msg_prefix)
            requested_eventmasks.remove(PollEvents.READ)

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix=msg_prefix)

    def test_which_events_are_set_after_local_shuts_wr(self):

        s1, _s2 = self.create_blocking_socketpair()

        s1.shutdown(socket.SHUT_WR)  # pylint: disable=E1101

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.WRITE
        ]

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='ZZZ Local shut WR')

    def test_which_events_are_set_after_local_shuts_rdwr(self):

        msg_prefix = 'ZZZ Local shut RDWR'

        s1, _s2 = self.create_blocking_socketpair()

        s1.shutdown(socket.SHUT_RDWR)  # pylint: disable=E1101

        requested_eventmasks = [
            PollEvents.READ | PollEvents.WRITE | PollEvents.ERROR,
            PollEvents.READ | PollEvents.WRITE,
            PollEvents.READ,
            PollEvents.WRITE
        ]

        # NOTE: Unlike POSIX, Windows select doesn't indicate as readable socket
        #  that was shut down locally with SHUT_RDWR.
        if platform.system() == 'Windows':
            _trace_stderr(
                '%s: removing check for solo PollEvents.READ on Windows, '
                'because its `select()` does not indicate a socket shut '
                'locally with SHUT_RDWR as readable.', msg_prefix)
            requested_eventmasks.remove(PollEvents.READ)

        self._which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix=msg_prefix)


@mock.patch.multiple(select_connection, SELECT_TYPE='select')
class SelectPollerPlainSocketDiagnostics(PlainSocketDiagnostics):
    """Runs `PlainSocketDiagnostics` tests with forced use of SelectPoller"""
    pass
