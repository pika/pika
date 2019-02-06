# -*- coding: utf-8 -*-
"""
Tests for SelectConnection IOLoops

"""

from __future__ import print_function

import errno
import datetime
import functools
import logging
import os
import platform
import select
import signal
import socket
import sys
import time
import threading
import unittest

try:
    from unittest import mock
except ImportError:
    import mock

import pika
from pika import compat
from pika.adapters import select_connection


# protected-access
# pylint: disable=W0212

# missing-docstring
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103

# attribute-defined-outside-init
# pylint: disable=W0201


LOGGER = logging.getLogger(__name__)

EPOLL_SUPPORTED = hasattr(select, 'epoll')
POLL_SUPPORTED = hasattr(select, 'poll') and hasattr(select.poll(), 'modify')
KQUEUE_SUPPORTED = hasattr(select, 'kqueue')

POLLIN = getattr(select, 'POLLIN', 0) or 1
POLLOUT = getattr(select, 'POLLOUT', 0) or 4
POLLERR = getattr(select, 'POLLERR', 0) or 8

POLLHUP = getattr(select, 'POLLHUP', 0) or 16
POLLNVAL = getattr(select, 'POLLNVAL', 0) or 32
POLLPRI = getattr(select, 'POLLPRI', 0) or 2


def _trace_stderr(fmt, *args):
    """Format and output the text to stderr"""
    print((fmt % args) + "\n", end="", file=sys.stderr)


def _fd_events_to_str(events):
    str_events = '{}: '.format(events)

    if events & POLLIN:
        str_events += "In."
    if events & POLLOUT:
        str_events += "Out."
    if events & POLLERR:
        str_events += "Err."
    if events & POLLHUP:
        str_events += "Hup."
    if events & POLLNVAL:
        str_events += "Inval."
    if events & POLLPRI:
        str_events += "Pri."


    remainig_events = events & ~(POLLIN |
                                 POLLOUT |
                                 POLLERR |
                                 POLLHUP |
                                 POLLNVAL |
                                 POLLPRI)
    if remainig_events:
        str_events += '+{}'.format(bin(remainig_events))

    return str_events


class IOLoopBaseTest(unittest.TestCase):
    SELECT_POLLER = None
    TIMEOUT = 1.5

    def setUp(self):
        select_type_patch = mock.patch.multiple(
            select_connection, SELECT_TYPE=self.SELECT_POLLER)
        select_type_patch.start()
        self.addCleanup(select_type_patch.stop)

        self.ioloop = select_connection.IOLoop()
        self.addCleanup(setattr, self, 'ioloop', None)
        self.addCleanup(self.ioloop.close)

        activate_poller_patch = mock.patch.object(
            self.ioloop._poller,
            'activate_poller',
            wraps=self.ioloop._poller.activate_poller)
        activate_poller_patch.start()
        self.addCleanup(activate_poller_patch.stop)

        deactivate_poller_patch = mock.patch.object(
            self.ioloop._poller,
            'deactivate_poller',
            wraps=self.ioloop._poller.deactivate_poller)
        deactivate_poller_patch.start()
        self.addCleanup(deactivate_poller_patch.stop)

    def shortDescription(self):
        method_desc = super(IOLoopBaseTest, self).shortDescription()
        return '%s (%s)' % (method_desc, self.SELECT_POLLER)

    def start(self):
        """Setup timeout handler for detecting 'no-activity'
        and start polling.

        """
        fail_timer = self.ioloop.call_later(self.TIMEOUT, self.on_timeout)
        self.addCleanup(self.ioloop.remove_timeout, fail_timer)
        self.ioloop.start()

        self.ioloop._poller.activate_poller.assert_called_once_with()  # pylint: disable=E1101
        self.ioloop._poller.deactivate_poller.assert_called_once_with()  # pylint: disable=E1101

    def on_timeout(self):
        """Called when stuck waiting for connection to close"""
        self.ioloop.stop()  # force the ioloop to stop
        self.fail('Test timed out')


class IOLoopCloseClosesSubordinateObjectsTestSelect(IOLoopBaseTest):
    """ Test ioloop being closed """
    SELECT_POLLER = 'select'

    def start_test(self):
        with mock.patch.multiple(self.ioloop,
                                 _timer=mock.DEFAULT,
                                 _poller=mock.DEFAULT,
                                 _callbacks=mock.DEFAULT) as mocks:
            self.ioloop.close()
            mocks['_timer'].close.assert_called_once_with()
            mocks['_poller'].close.assert_called_once_with()
            self.assertEqual(self.ioloop._callbacks, [])


class IOLoopCloseAfterStartReturnsTest(IOLoopBaseTest):
    """ Test IOLoop.close() after normal return from start(). """
    SELECT_POLLER = 'select'

    def start_test(self):
        self.ioloop.stop() # so start will terminate quickly
        self.start()
        self.ioloop.close()
        self.assertEqual(self.ioloop._callbacks, [])


class IOLoopStartReentrancyNotAllowedTestSelect(IOLoopBaseTest):
    """ Test calling IOLoop.start() while arleady in start() raises exception. """
    SELECT_POLLER = 'select'

    def start_test(self):
        callback_completed = []

        def call_close_from_callback():
            with self.assertRaises(RuntimeError) as cm:
                self.ioloop.start()

            self.assertEqual(cm.exception.args[0],
                             'IOLoop is not reentrant and is already running')
            self.ioloop.stop()
            callback_completed.append(1)

        self.ioloop.add_callback_threadsafe(call_close_from_callback)
        self.start()
        self.assertEqual(callback_completed, [1])


class IOLoopCloseBeforeStartReturnsTestSelect(IOLoopBaseTest):
    """ Test calling IOLoop.close() before return from start() raises exception. """
    SELECT_POLLER = 'select'

    def start_test(self):
        callback_completed = []

        def call_close_from_callback():
            with self.assertRaises(AssertionError) as cm:
                self.ioloop.close()

            self.assertEqual(cm.exception.args[0],
                             'Cannot call close() before start() unwinds.')
            self.ioloop.stop()
            callback_completed.append(1)

        self.ioloop.add_callback_threadsafe(call_close_from_callback)
        self.start()
        self.assertEqual(callback_completed, [1])


class IOLoopThreadStopTestSelect(IOLoopBaseTest):
    """ Test ioloop being stopped by another Thread. """
    SELECT_POLLER = 'select'

    def start_test(self):
        """Starts a thread that stops ioloop after a while and start polling"""
        timer = threading.Timer(
            0.1,
            lambda: self.ioloop.add_callback_threadsafe(self.ioloop.stop))
        self.addCleanup(timer.cancel)
        timer.start()
        self.start() # NOTE: Normal return from `start()` constitutes success


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
class IOLoopThreadStopTestPoll(IOLoopThreadStopTestSelect):
    """Same as IOLoopThreadStopTestSelect but uses 'poll' syscall."""
    SELECT_POLLER = 'poll'


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
class IOLoopThreadStopTestEPoll(IOLoopThreadStopTestSelect):
    """Same as IOLoopThreadStopTestSelect but uses 'epoll' syscall."""
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
class IOLoopThreadStopTestKqueue(IOLoopThreadStopTestSelect):
    """Same as IOLoopThreadStopTestSelect but uses 'kqueue' syscall."""
    SELECT_POLLER = 'kqueue'


class IOLoopAddCallbackAfterCloseDoesNotRaiseTestSelect(IOLoopBaseTest):
    """ Test ioloop add_callback_threadsafe() after ioloop close doesn't raise exception. """
    SELECT_POLLER = 'select'

    def start_test(self):
        # Simulate closing after start returns
        self.ioloop.stop()  # so that start() returns ASAP
        self.start() # NOTE: Normal return from `start()` constitutes success
        self.ioloop.close()

        # Expect: add_callback_threadsafe() won't raise after ioloop.close()
        self.ioloop.add_callback_threadsafe(lambda: None)


# TODO FUTURE - fix this flaky test
@unittest.skipIf(platform.python_implementation() == 'PyPy', 'test is flaky on PyPy')
class IOLoopTimerTestSelect(IOLoopBaseTest):
    """Set a bunch of very short timers to fire in reverse order and check
    that they fire in order of time, not

    """
    NUM_TIMERS = 5
    TIMER_INTERVAL = 0.25
    SELECT_POLLER = 'select'

    def set_timers(self):
        """Set timers that timers that fires in succession with the specified
        interval.

        """
        self.timer_stack = list()
        for i in range(self.NUM_TIMERS, 0, -1):
            deadline = i * self.TIMER_INTERVAL
            self.ioloop.call_later(
                deadline, functools.partial(self.on_timer, i))
            self.timer_stack.append(i)

    def start_test(self):
        """Set timers and start ioloop."""
        self.set_timers()
        self.start()

    def on_timer(self, val):
        """A timeout handler that verifies that the given parameter matches
        what is expected.

        """
        self.assertEqual(val, self.timer_stack.pop())
        if not self.timer_stack:
            self.ioloop.stop()

    def test_normal(self):
        """Setup 5 timeout handlers and observe them get invoked one by one."""
        self.start_test()

    def test_timer_for_deleting_itself(self):
        """Verifies that an attempt to delete a timeout within the
        corresponding handler generates no exceptions.

        """
        self.timer_stack = list()
        handle_holder = []
        self.timer_got_fired = False
        self.handle = self.ioloop.call_later(
            0.1, functools.partial(
                self._on_timer_delete_itself, handle_holder))
        handle_holder.append(self.handle)
        self.start()
        self.assertTrue(self.timer_got_called)

    def _on_timer_delete_itself(self, handle_holder):
        """A timeout handler that tries to remove itself."""
        self.assertEqual(self.handle, handle_holder.pop())
        # This removal here should not raise exception by itself nor
        # in the caller SelectPoller._process_timeouts().
        self.timer_got_called = True
        self.ioloop.remove_timeout(self.handle)
        self.ioloop.stop()

    def test_timer_delete_another(self):
        """Verifies that an attempt by a timeout handler to delete another,
        that  is ready to run, cancels the execution of the latter without
        generating an exception. This should pose no issues.

        """
        holder_for_target_timer = []
        self.ioloop.call_later(
            0.01, functools.partial(
                self._on_timer_delete_another, holder_for_target_timer))
        timer_2 = self.ioloop.call_later(0.02, self._on_timer_no_call)
        holder_for_target_timer.append(timer_2)
        time.sleep(0.03)  # so that timer_1 and timer_2 fires at the same time.
        self.start()
        self.assertTrue(self.deleted_another_timer)
        self.assertTrue(self.concluded)

    def _on_timer_delete_another(self, holder):
        """A timeout handler that tries to remove another timeout handler
        that is ready to run. This should pose no issues.

        """
        target_timer = holder[0]
        self.ioloop.remove_timeout(target_timer)
        self.deleted_another_timer = True

        def _on_timer_conclude():
            """A timeout handler that is called to verify outcome of calling
            or not calling of previously set handlers.

            """
            self.concluded = True
            self.assertTrue(self.deleted_another_timer)
            self.assertIsNone(target_timer.callback)
            self.ioloop.stop()

        self.ioloop.call_later(0.01, _on_timer_conclude)

    def _on_timer_no_call(self):
        """A timeout handler that is used when it's assumed not be called."""
        self.fail('deleted timer callback was called.')


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
class IOLoopTimerTestPoll(IOLoopTimerTestSelect):
    """Same as IOLoopTimerTestSelect but uses 'poll' syscall"""
    SELECT_POLLER = 'poll'


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
class IOLoopTimerTestEPoll(IOLoopTimerTestSelect):
    """Same as IOLoopTimerTestSelect but uses 'epoll' syscall"""
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
class IOLoopTimerTestKqueue(IOLoopTimerTestSelect):
    """Same as IOLoopTimerTestSelect but uses 'kqueue' syscall"""
    SELECT_POLLER = 'kqueue'


class IOLoopSleepTimerTestSelect(IOLoopTimerTestSelect):
    """Sleep until all the timers should have passed and check they still
        fire in deadline order"""

    def start_test(self):
        """ Setup timers, sleep and start polling """
        self.set_timers()
        time.sleep(self.NUM_TIMERS * self.TIMER_INTERVAL)
        self.start()


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
class IOLoopSleepTimerTestPoll(IOLoopSleepTimerTestSelect):
    """Same as IOLoopSleepTimerTestSelect but uses 'poll' syscall"""
    SELECT_POLLER = 'poll'


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
class IOLoopSleepTimerTestEPoll(IOLoopSleepTimerTestSelect):
    """Same as IOLoopSleepTimerTestSelect but uses 'epoll' syscall"""
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
class IOLoopSleepTimerTestKqueue(IOLoopSleepTimerTestSelect):
    """Same as IOLoopSleepTimerTestSelect but uses 'kqueue' syscall"""
    SELECT_POLLER = 'kqueue'


class IOLoopSocketBaseSelect(IOLoopBaseTest):
    """A base class for setting up a communicating pair of sockets."""
    SELECT_POLLER = 'select'
    READ_SIZE = 1024

    def save_sock(self, sock):
        """Store 'sock' in self.sock_map and return the fileno."""
        fd_ = sock.fileno()
        self.sock_map[fd_] = sock
        return fd_

    def setUp(self):
        super(IOLoopSocketBaseSelect, self).setUp()
        self.sock_map = dict()
        self.create_accept_socket()

    def tearDown(self):
        for fd_ in self.sock_map:
            self.ioloop.remove_handler(fd_)
            self.sock_map[fd_].close()
        super(IOLoopSocketBaseSelect, self).tearDown()

    def create_accept_socket(self):
        """Create a socket and setup 'accept' handler"""
        listen_sock = socket.socket()
        listen_sock.setblocking(0)
        listen_sock.bind(('localhost', 0))
        listen_sock.listen(1)
        fd_ = self.save_sock(listen_sock)
        self.listen_addr = listen_sock.getsockname()
        self.ioloop.add_handler(fd_, self.do_accept, self.ioloop.READ)

    def create_write_socket(self, on_connected):
        """ Create a pair of socket and setup 'connected' handler """
        write_sock = socket.socket()
        write_sock.setblocking(0)
        err = write_sock.connect_ex(self.listen_addr)
        # NOTE we get errno.EWOULDBLOCK 10035 on Windows
        self.assertIn(err, (errno.EINPROGRESS, errno.EWOULDBLOCK))
        fd_ = self.save_sock(write_sock)
        self.ioloop.add_handler(fd_, on_connected, self.ioloop.WRITE)
        return write_sock

    def do_accept(self, fd_, events):
        """ Create socket from the given fd_ and setup 'read' handler """
        self.assertEqual(events, self.ioloop.READ)
        listen_sock = self.sock_map[fd_]
        read_sock, _ = listen_sock.accept()
        fd_ = self.save_sock(read_sock)
        self.ioloop.add_handler(fd_, self.do_read, self.ioloop.READ)

    def connected(self, _fd, _events):
        """ Create socket from given _fd and respond to 'connected'.
            Implemenation is subclass's responsibility. """
        self.fail("IOLoopSocketBase.connected not extended")

    def do_read(self, fd_, events):
        """ read from fd and check the received content """
        self.assertEqual(events, self.ioloop.READ)
        # NOTE Use socket.recv instead of os.read for Windows compatibility
        self.verify_message(self.sock_map[fd_].recv(self.READ_SIZE))

    def verify_message(self, _msg):
        """ See if 'msg' matches what is expected. This is a stub.
            Real implementation is subclass's responsibility """
        self.fail("IOLoopSocketBase.verify_message not extended")

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        # force the ioloop to stop
        self.ioloop.stop()
        self.fail('Test timed out')


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
class IOLoopSocketBasePoll(IOLoopSocketBaseSelect):
    """Same as IOLoopSocketBaseSelect but uses 'poll' syscall"""
    SELECT_POLLER = 'poll'


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
class IOLoopSocketBaseEPoll(IOLoopSocketBaseSelect):
    """Same as IOLoopSocketBaseSelect but uses 'epoll' syscall"""
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
class IOLoopSocketBaseKqueue(IOLoopSocketBaseSelect):
    """ Same as IOLoopSocketBaseSelect but uses 'kqueue' syscall """
    SELECT_POLLER = 'kqueue'


class IOLoopSimpleMessageTestCaseSelect(IOLoopSocketBaseSelect):
    """Test read/write by creating a pair of sockets, writing to one
    end and reading from the other

    """
    def start(self):
        """Create a pair of sockets and poll"""
        self.create_write_socket(self.connected)
        super(IOLoopSimpleMessageTestCaseSelect, self).start()

    def connected(self, fd, events):
        """Respond to 'connected' event by writing to the write-side."""
        self.assertEqual(events, self.ioloop.WRITE)
        # NOTE Use socket.send instead of os.write for Windows compatibility
        self.sock_map[fd].send(b'X')
        self.ioloop.update_handler(fd, 0)

    def verify_message(self, msg):
        """Make sure we get what is expected and stop polling """
        self.assertEqual(msg, b'X')
        self.ioloop.stop()

    def start_test(self):
        """Simple message Test"""
        self.start()


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
class IOLoopSimpleMessageTestCasetPoll(IOLoopSimpleMessageTestCaseSelect):
    """Same as IOLoopSimpleMessageTestCaseSelect but uses 'poll' syscall"""
    SELECT_POLLER = 'poll'


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
class IOLoopSimpleMessageTestCasetEPoll(IOLoopSimpleMessageTestCaseSelect):
    """Same as IOLoopSimpleMessageTestCaseSelect but uses 'epoll' syscall"""
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
class IOLoopSimpleMessageTestCasetKqueue(IOLoopSimpleMessageTestCaseSelect):
    """Same as IOLoopSimpleMessageTestCaseSelect but uses 'kqueue' syscall"""
    SELECT_POLLER = 'kqueue'


class IOLoopEintrTestCaseSelect(IOLoopBaseTest):
    """ Tests if EINTR is properly caught and polling gets resumed. """
    SELECT_POLLER = 'select'
    MSG_CONTENT = b'hello'

    @staticmethod
    def signal_handler(signum, interrupted_stack):
        """A signal handler that gets called in response to
           os.kill(signal.SIGUSR1)."""
        pass

    def _eintr_read_handler(self, fileno, events):
        """Read from within poll loop that gets receives eintr error."""
        self.assertEqual(events, self.ioloop.READ)

        sock = socket.fromfd(
            os.dup(fileno), socket.AF_INET, socket.SOCK_STREAM)
        self.addCleanup(sock.close)

        mesg = sock.recv(256)
        self.assertEqual(mesg, self.MSG_CONTENT)
        self.poller.stop()
        self._eintr_read_handler_is_called = True

    def _eintr_test_fail(self):
        """This function gets called when eintr-test failed to get
           _eintr_read_handler called."""
        self.poller.stop()
        self.fail('Eintr-test timed out')

    @unittest.skipUnless(compat.HAVE_SIGNAL,
                         "This platform doesn't support posix signals")
    @mock.patch('pika.adapters.select_connection._is_resumable')
    def test_eintr(
            self,
            is_resumable_mock,
            is_resumable_raw=pika.adapters.select_connection._is_resumable):
        """Test that poll() is properly restarted after receiving EINTR error.
           Class of an exception raised to signal the error differs in one
           implementation of polling mechanism and another."""
        is_resumable_mock.side_effect = is_resumable_raw

        timer = select_connection._Timer()
        self.poller = self.ioloop._get_poller(timer.get_remaining_interval,
                                              timer.process_timeouts)
        self.addCleanup(self.poller.close)

        sockpair = self.poller._get_interrupt_pair()
        self.addCleanup(sockpair[0].close)
        self.addCleanup(sockpair[1].close)

        self._eintr_read_handler_is_called = False
        self.poller.add_handler(sockpair[0].fileno(), self._eintr_read_handler,
                                self.ioloop.READ)

        self.ioloop.call_later(self.TIMEOUT, self._eintr_test_fail)

        original_signal_handler = \
            signal.signal(signal.SIGUSR1, self.signal_handler)
        self.addCleanup(signal.signal, signal.SIGUSR1, original_signal_handler)

        tmr_k = threading.Timer(0.1,
                                lambda: os.kill(os.getpid(), signal.SIGUSR1))
        self.addCleanup(tmr_k.cancel)
        tmr_w = threading.Timer(0.2,
                                lambda: sockpair[1].send(self.MSG_CONTENT))
        self.addCleanup(tmr_w.cancel)
        tmr_k.start()
        tmr_w.start()
        self.poller.start()
        self.assertTrue(self._eintr_read_handler_is_called)
        if pika.compat.EINTR_IS_EXPOSED:
            self.assertEqual(is_resumable_mock.call_count, 1)
        else:
            self.assertEqual(is_resumable_mock.call_count, 0)


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
class IOLoopEintrTestCasePoll(IOLoopEintrTestCaseSelect):
    """Same as IOLoopEintrTestCaseSelect but uses poll syscall"""
    SELECT_POLLER = 'poll'


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
class IOLoopEintrTestCaseEPoll(IOLoopEintrTestCaseSelect):
    """Same as IOLoopEINTRrTestCaseSelect but uses epoll syscall"""
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
class IOLoopEintrTestCaseKqueue(IOLoopEintrTestCaseSelect):
    """Same as IOLoopEINTRTestCaseSelect but uses kqueue syscall"""
    SELECT_POLLER = 'kqueue'


class SelectPollerTestPollWithoutSockets(unittest.TestCase):
    def start_test(self):
        timer = select_connection._Timer()
        poller = select_connection.SelectPoller(
            get_wait_seconds=timer.get_remaining_interval,
            process_timeouts=timer.process_timeouts)
        self.addCleanup(poller.close)

        timer_call_container = []
        timer.call_later(0.00001, lambda: timer_call_container.append(1))
        poller.poll()

        delay = poller._get_wait_seconds()
        self.assertIsNotNone(delay)
        deadline = pika.compat.time_now() + delay

        while True:
            poller._process_timeouts()

            if pika.compat.time_now() < deadline:
                self.assertEqual(timer_call_container, [])
            else:
                # One last time in case deadline reached after previous
                # processing cycle
                poller._process_timeouts()
                break

        self.assertEqual(timer_call_container, [1])


class PollerTestCaseSelect(unittest.TestCase):
    SELECT_POLLER = 'select'

    def setUp(self):
        select_type_patch = mock.patch.multiple(
            select_connection, SELECT_TYPE=self.SELECT_POLLER)
        select_type_patch.start()
        self.addCleanup(select_type_patch.stop)

        timer = select_connection._Timer()
        self.addCleanup(timer.close)
        self.poller = select_connection.IOLoop._get_poller(
            timer.get_remaining_interval,
            timer.process_timeouts)
        self.addCleanup(self.poller.close)

    def test_poller_close(self):
        self.poller.close()
        self.assertIsNone(self.poller._r_interrupt)
        self.assertIsNone(self.poller._w_interrupt)
        self.assertIsNone(self.poller._fd_handlers)
        self.assertIsNone(self.poller._fd_events)
        self.assertIsNone(self.poller._processing_fd_event_map)


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
class PollerTestCasePoll(PollerTestCaseSelect):
    """Same as PollerTestCaseSelect but uses poll syscall"""
    SELECT_POLLER = 'poll'


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
class PollerTestCaseEPoll(PollerTestCaseSelect):
    """Same as PollerTestCaseSelect but uses epoll syscall"""
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
class PollerTestCaseKqueue(PollerTestCaseSelect):
    """Same as PollerTestCaseSelect but uses kqueue syscall"""
    SELECT_POLLER = 'kqueue'


class DefaultPollerSocketEventsTestCase(unittest.TestCase):
    """This test suite outputs diagnostic information usful for debugging the
    IOLoop poller's fd watcher

    """

    DEFAULT_TEST_TIMEOUT = 15

    IOLOOP_CLS = select_connection.IOLoop

    READ = IOLOOP_CLS.READ
    WRITE = IOLOOP_CLS.WRITE
    ERROR = IOLOOP_CLS.ERROR

    def create_ioloop_with_timeout(self):
        """Create IOLoop with test timeout and schedule cleanup to close it

        """
        ioloop = select_connection.IOLoop()
        self.addCleanup(ioloop.close)

        def _on_test_timeout():
            """Called when test times out"""
            LOGGER.info('%s TIMED OUT (%s)', datetime.datetime.utcnow(), self)
            self.fail('Test timed out')

        ioloop.call_later(self.DEFAULT_TEST_TIMEOUT, _on_test_timeout)

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

    def which_events_are_set_with_varying_eventmasks(self,
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

            # NOTE ERROR may be added automatically by some pollers
            #      without being requested.
            self.assertTrue(
                in_events & (requested_eventmasks[0] | self.ERROR),
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

        msg_prefix = '@ Failed to connect'

        sock = self.create_nonblocking_tcp_socket()

        self.safe_connect_nonblocking_socket(sock,
                                             self.get_dead_socket_address())
        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.READ | self.WRITE,
            self.READ,
            self.WRITE | self.ERROR
        ]

        # NOTE: on OS X, we just get POLLHUP when requesting WRITE in this case
        # with PollPoller. It's documented in `man poll` on OS X as mutually
        # exclusive with POLLOUT; so it looks like PollPoller on OS X needs to
        # translate POLLHUP TO POLLERR and we need to request ERROR just in
        # case.

        # NOTE: Unlike POSIX, Windows select doesn't indicate as
        # readable/writable a socket that failed to connect - it reflects the
        # failure only via exceptfds.
        if platform.system() == 'Windows':
            _trace_stderr(
                '%s: setting `ERROR` to all event filters on '
                'Windows, because its `select()` does not indicate a socket '
                'that failed to connect as readable or writable.', msg_prefix)
            for i in pika.compat.xrange(len(requested_eventmasks)):
                requested_eventmasks[i] |= self.ERROR

        self.which_events_are_set_with_varying_eventmasks(
            sock=sock,
            requested_eventmasks=requested_eventmasks,
            msg_prefix=msg_prefix)

    def test_which_events_are_set_after_remote_end_closes(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.close()

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.READ | self.WRITE,
            self.READ,
            self.WRITE
        ]

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='@ Remote closed')

    def test_which_events_are_set_after_remote_end_closes_with_pending_data(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.send(b'abc')
        s2.close()

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.READ | self.WRITE,
            self.READ,
            self.WRITE
        ]

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='@ Remote closed with pending data')

    def test_which_events_are_set_after_remote_shuts_rd(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.shutdown(socket.SHUT_RD)

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.WRITE
        ]

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='@ Remote shut RD')

    def test_which_events_are_set_after_remote_shuts_wr(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.shutdown(socket.SHUT_WR)

        requested_eventmasks = [
            (self.READ | self.WRITE | self.ERROR),
            self.READ | self.WRITE,
            self.READ,
            self.WRITE
        ]

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='@ Remote shut WR')

    def test_which_events_are_set_after_remote_shuts_wr_with_pending_data(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.send(b'abc')
        s2.shutdown(socket.SHUT_WR)

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.READ | self.WRITE,
            self.READ,
            self.WRITE
        ]

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='@ Remote shut WR with pending data')

    def test_which_events_are_set_after_remote_shuts_rdwr(self):

        s1, s2 = self.create_blocking_socketpair()

        s2.shutdown(socket.SHUT_RDWR)

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.READ | self.WRITE,
            self.READ,
            self.WRITE
        ]

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='@ Remote shut RDWR')

    def test_which_events_are_set_after_local_shuts_rd(self):

        msg_prefix = '@ Local shut RD'

        s1, _s2 = self.create_blocking_socketpair()

        s1.shutdown(socket.SHUT_RD)  # pylint: disable=E1101

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.READ | self.WRITE,
            self.READ,
            self.WRITE
        ]

        # NOTE: Unlike POSIX, Windows select doesn't indicate as readable socket
        #  that was shut down locally with SHUT_RD.
        if platform.system() == 'Windows':
            _trace_stderr(
                '%s: removing check for solo READ on Windows, '
                'because its `select()` does not indicate a socket shut '
                'locally with SHUT_RD as readable.', msg_prefix)
            requested_eventmasks.remove(self.READ)

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix=msg_prefix)

    def test_which_events_are_set_after_local_shuts_wr(self):

        s1, _s2 = self.create_blocking_socketpair()

        s1.shutdown(socket.SHUT_WR)  # pylint: disable=E1101

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.WRITE | self.ERROR
        ]

        # NOTE: on OS X, we just get POLLHUP when requesting WRITE in this case
        # with PollPoller. It's documented in `man poll` on OS X as mutually
        # exclusive with POLLOUT; so it looks like PollPoller on OS X needs to
        # translate POLLHUP TO POLLERR and we need to request ERROR just in
        # case.

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix='@ Local shut WR')

    def test_which_events_are_set_after_local_shuts_rdwr(self):

        msg_prefix = '@ Local shut RDWR'

        s1, _s2 = self.create_blocking_socketpair()

        s1.shutdown(socket.SHUT_RDWR)  # pylint: disable=E1101

        requested_eventmasks = [
            self.READ | self.WRITE | self.ERROR,
            self.READ | self.WRITE,
            self.READ,
            self.WRITE | self.ERROR
        ]
        # NOTE: on OS X, we just get POLLHUP when requesting WRITE in this case
        # with PollPoller. It's documented in `man poll` on OS X as mutually
        # exclusive with POLLOUT; so it looks like PollPoller on OS X needs to
        # translate POLLHUP TO POLLERR and we need to request ERROR just in
        # case.

        # NOTE: Unlike POSIX, Windows select doesn't indicate as readable socket
        #  that was shut down locally with SHUT_RDWR.
        if platform.system() == 'Windows':
            _trace_stderr(
                '%s: removing check for solo READ on Windows, '
                'because its `select()` does not indicate a socket shut '
                'locally with SHUT_RDWR as readable.', msg_prefix)
            requested_eventmasks.remove(self.READ)

        self.which_events_are_set_with_varying_eventmasks(
            sock=s1,
            requested_eventmasks=requested_eventmasks,
            msg_prefix=msg_prefix)


@mock.patch.multiple(select_connection, SELECT_TYPE='select')
class SelectPollerSocketEventsTestCase(DefaultPollerSocketEventsTestCase):
    """Runs `DefaultPollerSocketEventsTestCase` tests with forced use of
    SelectPoller
    """
    pass


@unittest.skipIf(not POLL_SUPPORTED, 'poll not supported')
@mock.patch.multiple(select_connection, SELECT_TYPE='poll')
class PollPollerSocketEventsTestCase(DefaultPollerSocketEventsTestCase):
    """Same as DefaultPollerSocketEventsTestCase but uses poll syscall"""
    pass


@unittest.skipIf(not EPOLL_SUPPORTED, 'epoll not supported')
@mock.patch.multiple(select_connection, SELECT_TYPE='epoll')
class EpollPollerSocketEventsTestCase(DefaultPollerSocketEventsTestCase):
    """Same as DefaultPollerSocketEventsTestCase but uses epoll syscall"""
    pass


@unittest.skipIf(not KQUEUE_SUPPORTED, 'kqueue not supported')
@mock.patch.multiple(select_connection, SELECT_TYPE='kqueue')
class KqueuePollerSocketEventsTestCase(DefaultPollerSocketEventsTestCase):
    """Same as DefaultPollerSocketEventsTestCase but uses kqueue syscall"""
    pass
