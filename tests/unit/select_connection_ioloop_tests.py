# -*- coding: utf-8 -*-
"""
Tests for SelectConnection IOLoops

"""
# Disable warnings about initialization of members outside of __init__
# pylint: disable=W0201

# Disable warnings about too many public methods as they are in base classes
# pylint: disable=R0904

# Disable pylint warnings concerning access to protected members
# pylint: disable=W0212

import select

try:
    #pylint: disable=F0401
    import unittest2 as unittest
except ImportError:
    #pylint: disable=W0404
    import unittest

try:
    #pylint: disable=F0401,E0611
    from unittest import mock
except ImportError:
    #pylint: disable=W0404
    import mock

import errno
import os
import signal
import socket
import time
import threading

import pika
from pika.adapters import select_connection
from pika.adapters.select_connection import READ, WRITE
from functools import partial

class IOLoopBaseTest(unittest.TestCase):
    ''' Base for test classes in this uit. '''

    SELECT_POLLER = None
    TIMEOUT = 1.0

    def setUp(self):
        select_type_patch = mock.patch.multiple(select_connection,
                                                SELECT_TYPE=self.SELECT_POLLER)
        select_type_patch.start()
        self.addCleanup(select_type_patch.stop)

        self.ioloop = select_connection.IOLoop()
        self.addCleanup(setattr, self, 'ioloop', None)

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
        return "%s (%s)" % (method_desc, self.SELECT_POLLER)

    def start(self):
        ''' Setup timeout handler for detecting 'no-activity'
            and start polling. '''
        fail_timer = self.ioloop.add_timeout(self.TIMEOUT, self.on_timeout)
        self.addCleanup(self.ioloop.remove_timeout, fail_timer)
        self.ioloop.start()

        self.ioloop._poller.activate_poller.assert_called_once_with()
        self.ioloop._poller.deactivate_poller.assert_called_once_with()

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        # force the ioloop to stop
        self.ioloop.stop()
        self.fail('Test timed out')


class IOLoopThreadStopTestSelect(IOLoopBaseTest):
    ''' Test ioloop being stopped by another Thread. '''
    SELECT_POLLER = 'select'
    def start_test(self):
        ''' Starts a thread that stops ioloop after a while
            and start polling '''
        timer = threading.Timer(0.1, self.ioloop.stop)
        self.addCleanup(timer.cancel)
        timer.start()
        self.start()


@unittest.skipIf(
    not hasattr(select, 'poll') or
    not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
class IOLoopThreadStopTestPoll(IOLoopThreadStopTestSelect):
    ''' Same as IOLoopThreadStopTestSelect but uses 'poll' syscall. '''
    SELECT_POLLER = 'poll'


@unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
class IOLoopThreadStopTestEPoll(IOLoopThreadStopTestSelect):
    ''' Same as IOLoopThreadStopTestSelect but uses 'epoll' syscall. '''
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
class IOLoopThreadStopTestKqueue(IOLoopThreadStopTestSelect):
    ''' Same as IOLoopThreadStopTestSelect but uses 'kqueue' syscall. '''
    SELECT_POLLER = 'kqueue'


class IOLoopTimerTestSelect(IOLoopBaseTest):
    """ Set a bunch of very short timers to fire in reverse order and check
        that they fire in order of time, not
    """
    NUM_TIMERS = 5
    TIMER_INTERVAL = 0.02
    SELECT_POLLER = 'select'

    def set_timers(self):
        """Set timers that timers that fires in succession with the sepecified
        interval."""
        self.timer_stack = list()
        for i in range(self.NUM_TIMERS, 0, -1):
            deadline = i * self.TIMER_INTERVAL
            self.ioloop.add_timeout(deadline, partial(self.on_timer, i))
            self.timer_stack.append(i)

    def start_test(self):
        """Set timers and start ioloop."""
        self.set_timers()
        self.start()

    def on_timer(self, val):
        """A timeout handler that verifies that the given parameter matches
        what is expected."""
        self.assertEqual(val, self.timer_stack.pop())
        if not self.timer_stack:
            self.ioloop.stop()

    def test_normal(self):
        """Setup 5 timeout handlers and observe them get invoked one by one."""
        self.start_test()

    def test_timer_for_deleting_itself(self):
        """Verifies that an attempt to delete a timeout within the
        corresponding handler generates no exceptions."""
        self.timer_stack = list()
        handle_holder = []
        self.timer_got_fired = False
        self.handle = self.ioloop.add_timeout(
            0.1, partial(self._on_timer_delete_itself, handle_holder))
        handle_holder.append(self.handle)
        self.start()
        self.assertTrue(self.timer_got_called)

    def _on_timer_delete_itself(self, handle_holder):
        """ A timeout hanlder that tries to remove itself. """
        self.assertEqual(self.handle, handle_holder.pop())
        # This removal here should not raise exception by itself nor
        # in the caller SelectPoller.process_timeouts().
        self.timer_got_called = True
        self.ioloop.remove_timeout(self.handle)
        self.ioloop.stop()

    def test_timer_delete_another(self):
        """Verifies that an attempt by a timeout handler to delete another,
        that  is ready to run, cancels the execution of the latter without
        generating an exception. This should pose no issues."""
        holder_for_target_timer = []
        self.ioloop.add_timeout(
            0.01, partial(self._on_timer_delete_another,
                          holder_for_target_timer))
        timer_2 = self.ioloop.add_timeout(
            0.02, self._on_timer_no_call)
        holder_for_target_timer.append(timer_2)
        time.sleep(0.03) # so that timer_1 and timer_2 fires at the same time.
        self.start()
        self.assertTrue(self.deleted_another_timer)
        self.assertTrue(self.concluded)

    def _on_timer_delete_another(self, holder):
        """A timeout handler that tries to remove another timeout handler
        that is ready to run. This should pose no issues."""
        target_timer = holder[0]
        self.ioloop.remove_timeout(target_timer)
        self.deleted_another_timer = True

        def _on_timer_conclude():
            """A timeout handler that is called to verify outcome of calling
            or not calling of previously set handlers."""
            self.concluded = True
            self.assertTrue(self.deleted_another_timer)
            self.assertNotIn(target_timer, self.ioloop._poller._timeouts)
            self.ioloop.stop()

        self.ioloop.add_timeout(0.01, _on_timer_conclude)

    def _on_timer_no_call(self):
        """A timeout handler that is used when it's assumed not be called."""
        self.fail('deleted timer callback was called.')


@unittest.skipIf(
    not hasattr(select, 'poll') or
    not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
class IOLoopTimerTestPoll(IOLoopTimerTestSelect):
    ''' Same as IOLoopTimerTestSelect but uses 'poll' syscall '''
    SELECT_POLLER = 'poll'


@unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
class IOLoopTimerTestEPoll(IOLoopTimerTestSelect):
    ''' Same as IOLoopTimerTestSelect but uses 'epoll' syscall '''
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
class IOLoopTimerTestKqueue(IOLoopTimerTestSelect):
    ''' Same as IOLoopTimerTestSelect but uses 'kqueue' syscall '''
    SELECT_POLLER = 'kqueue'


class IOLoopSleepTimerTestSelect(IOLoopTimerTestSelect):
    """Sleep until all the timers should have passed and check they still
        fire in deadline order"""

    def start_test(self):
        ''' Setup timers, sleep and start polling '''
        self.set_timers()
        time.sleep(self.NUM_TIMERS * self.TIMER_INTERVAL)
        self.start()


@unittest.skipIf(
    not hasattr(select, 'poll') or
    not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
class IOLoopSleepTimerTestPoll(IOLoopSleepTimerTestSelect):
    ''' Same as IOLoopSleepTimerTestSelect but uses 'poll' syscall '''
    SELECT_POLLER = 'poll'


@unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
class IOLoopSleepTimerTestEPoll(IOLoopSleepTimerTestSelect):
    ''' Same as IOLoopSleepTimerTestSelect but uses 'epoll' syscall '''
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
class IOLoopSleepTimerTestKqueue(IOLoopSleepTimerTestSelect):
    ''' Same as IOLoopSleepTimerTestSelect but uses 'kqueue' syscall '''
    SELECT_POLLER = 'kqueue'


class IOLoopSocketBaseSelect(IOLoopBaseTest):
    ''' A base class for setting up a communicating pair of sockets. '''
    SELECT_POLLER = 'select'
    READ_SIZE = 1024

    def save_sock(self, sock):
        ''' Store 'sock' in self.sock_map and return the fileno.'''
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
        ''' Create a socket and setup 'accept' handler '''
        listen_sock = socket.socket()
        listen_sock.setblocking(0)
        listen_sock.bind(('localhost', 0))
        listen_sock.listen(1)
        fd_ = self.save_sock(listen_sock)
        self.listen_addr = listen_sock.getsockname()
        self.ioloop.add_handler(fd_, self.do_accept, READ)

    def create_write_socket(self, on_connected):
        ''' Create a pair of socket and setup 'connected' handler '''
        write_sock = socket.socket()
        write_sock.setblocking(0)
        err = write_sock.connect_ex(self.listen_addr)
        # NOTE we get errno.EWOULDBLOCK 10035 on Windows
        self.assertIn(err, (errno.EINPROGRESS, errno.EWOULDBLOCK))
        fd_ = self.save_sock(write_sock)
        self.ioloop.add_handler(fd_, on_connected, WRITE)
        return write_sock

    def do_accept(self, fd_, events): # pylint: disable=W0613
        ''' Create socket from the given fd_ and setup 'read' handler '''
        self.assertEqual(events, READ)
        listen_sock = self.sock_map[fd_]
        read_sock, _ = listen_sock.accept()
        fd_ = self.save_sock(read_sock)
        self.ioloop.add_handler(fd_, self.do_read, READ)

    def connected(self, _fd, _events): # pylint: disable=W0613,R0201
        ''' Create socket from given _fd and respond to 'connected'.
            Implemenation is subclass's responsibility. '''
        self.fail("IOLoopSocketBase.connected not extended")

    def do_read(self, fd_, events): # pylint: disable=W0613
        ''' read from fd and check the received content '''
        self.assertEqual(events, READ)
        # NOTE Use socket.recv instead of os.read for Windows compatibility
        self.verify_message(self.sock_map[fd_].recv(self.READ_SIZE))

    def verify_message(self, _msg): # pylint: disable=W0613,R0201
        ''' See if 'msg' matches what is expected. This is a stub.
            Real implementation is subclass's responsibility '''
        self.fail("IOLoopSocketBase.verify_message not extended")

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        # force the ioloop to stop
        self.ioloop.stop()
        self.fail('Test timed out')


@unittest.skipIf(
    not hasattr(select, 'poll') or
    not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
class IOLoopSocketBasePoll(IOLoopSocketBaseSelect):
    ''' Same as IOLoopSocketBaseSelect but uses 'poll' syscall '''
    SELECT_POLLER = 'poll'


@unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
class IOLoopSocketBaseEPoll(IOLoopSocketBaseSelect):
    ''' Same as IOLoopSocketBaseSelect but uses 'epoll' syscall '''
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
class IOLoopSocketBaseKqueue(IOLoopSocketBaseSelect):
    ''' Same as IOLoopSocketBaseSelect but uses 'kqueue' syscall '''
    SELECT_POLLER = 'kqueue'


class IOLoopSimpleMessageTestCaseSelect(IOLoopSocketBaseSelect):
    ''' Test read/write by creating a pair of sockets, writing to one
        end and reading from the other '''
    def start(self):
        ''' Create a pair of sockets and poll '''
        self.create_write_socket(self.connected)
        super(IOLoopSimpleMessageTestCaseSelect, self).start()

    def connected(self, fd, events):
        ''' Respond to 'connected' event by writing to the write-side. '''
        self.assertEqual(events, WRITE)
        # NOTE Use socket.send instead of os.write for Windows compatibility
        self.sock_map[fd].send(b'X')
        self.ioloop.update_handler(fd, 0)

    def verify_message(self, msg):
        ''' Make sure we get what is expected and stop polling '''
        self.assertEqual(msg, b'X')
        self.ioloop.stop()

    def start_test(self):
        ''' Simple message Test'''
        self.start()


@unittest.skipIf(
    not hasattr(select, 'poll') or
    not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
class IOLoopSimpleMessageTestCasetPoll(IOLoopSimpleMessageTestCaseSelect):
    ''' Same as IOLoopSimpleMessageTestCaseSelect but uses 'poll' syscall '''
    SELECT_POLLER = 'poll'


@unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
class IOLoopSimpleMessageTestCasetEPoll(IOLoopSimpleMessageTestCaseSelect):
    ''' Same as IOLoopSimpleMessageTestCaseSelect but uses 'epoll' syscall '''
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
class IOLoopSimpleMessageTestCasetKqueue(IOLoopSimpleMessageTestCaseSelect):
    ''' Same as IOLoopSimpleMessageTestCaseSelect but uses 'kqueue' syscall '''
    SELECT_POLLER = 'kqueue'


class IOLoopEintrTestCaseSelect(IOLoopBaseTest):
    ''' Tests if EINTR is properly caught and polling gets resumed. '''
    SELECT_POLLER = 'select'
    MSG_CONTENT = b'hello'

    @staticmethod
    def signal_handler(signum, interrupted_stack):
        '''A signal handler that gets called in response to
           os.kill(signal.SIGUSR1).'''
        pass

    def _eintr_read_handler(self, fileno, events):
        '''Read from within poll loop that gets receives eintr error.'''
        self.assertEqual(events, READ)

        sock = socket.fromfd(os.dup(fileno), socket.AF_INET, socket.SOCK_STREAM)
        self.addCleanup(sock.close)

        mesg = sock.recv(256)
        self.assertEqual(mesg, self.MSG_CONTENT)
        self.poller.stop()
        self._eintr_read_handler_is_called = True

    def _eintr_test_fail(self):
        '''This function gets called when eintr-test failed to get
           _eintr_read_handler called.'''
        self.poller.stop()
        self.fail('Eintr-test timed out')

    @unittest.skipUnless(pika.compat.HAVE_SIGNAL,
                         "This platform doesn't support posix signals")
    @mock.patch('pika.adapters.select_connection._is_resumable')
    def test_eintr(self, is_resumable_mock, is_resumable_raw=pika.adapters
                   .select_connection._is_resumable): #pylint: disable=W0212
        '''Test that poll() is properly restarted after receiving EINTR error.
           Class of an exception raised to signal the error differs in one
           implementation of polling mechanism and another.'''
        is_resumable_mock.side_effect = is_resumable_raw

        self.poller = self.ioloop._get_poller() #pylint: disable=W0212

        sockpair = self.poller._get_interrupt_pair()
        self.addCleanup(sockpair[0].close)
        self.addCleanup(sockpair[1].close)

        self._eintr_read_handler_is_called = False
        self.poller.add_handler(sockpair[0].fileno(), self._eintr_read_handler,
                                READ)

        self.poller.add_timeout(self.TIMEOUT, self._eintr_test_fail)

        original_signal_handler = \
            signal.signal(signal.SIGUSR1, self.signal_handler)
        self.addCleanup(signal.signal, signal.SIGUSR1, original_signal_handler)

        tmr_k = threading.Timer(
            0.1, lambda: os.kill(os.getpid(), signal.SIGUSR1))
        self.addCleanup(tmr_k.cancel)
        tmr_w = threading.Timer(
            0.2, lambda: sockpair[1].send(self.MSG_CONTENT))
        self.addCleanup(tmr_w.cancel)
        tmr_k.start()
        tmr_w.start()
        self.poller.start()
        self.assertTrue(self._eintr_read_handler_is_called)
        if pika.compat.EINTR_IS_EXPOSED:
            self.assertEqual(is_resumable_mock.call_count, 1)
        else:
            self.assertEqual(is_resumable_mock.call_count, 0)


@unittest.skipIf(
    not hasattr(select, 'poll') or
    not hasattr(select.poll(), 'modify'), "poll not supported")  # pylint: disable=E1101
class IOLoopEintrTestCasePoll(IOLoopEintrTestCaseSelect):
    ''' Same as IOLoopEintrTestCaseSelect but uses poll syscall '''
    SELECT_POLLER = 'poll'


@unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
class IOLoopEintrTestCaseEPoll(IOLoopEintrTestCaseSelect):
    ''' Same as IOLoopEINTRrTestCaseSelect but uses epoll syscall '''
    SELECT_POLLER = 'epoll'


@unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
class IOLoopEintrTestCaseKqueue(IOLoopEintrTestCaseSelect):
    ''' Same as IOLoopEINTRTestCaseSelect but uses kqueue syscall '''
    SELECT_POLLER = 'kqueue'


class SelectPollerTestPollWithoutSockets(unittest.TestCase):

    def start_test(self):
        poller = select_connection.SelectPoller()

        timer_call_container = []
        timer = poller.add_timeout(0.00001,
                                   lambda: timer_call_container.append(1))
        poller.poll()

        deadline = poller._next_timeout

        while True:
            poller.process_timeouts()

            if time.time() < deadline:
                self.assertEqual(timer_call_container, [])
            else:
                # One last time in case deadline reached after previous
                # processing cycle
                poller.process_timeouts()
                break

        self.assertEqual(timer_call_container, [1])
