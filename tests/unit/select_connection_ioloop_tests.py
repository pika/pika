# -*- coding: utf-8 -*-
"""
Tests for SelectConnection IOLoops

"""
# Disable warnings about initialization of members outside of __init__
# pylint: disable=W0201
# Disable warnings about too many public methods as they are in base classes
# pylint: disable=R0904

import logging
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import os
import socket
import errno
import time
import threading

import pika
from pika.adapters import select_connection
from pika.adapters.select_connection import READ, WRITE, ERROR
from functools import partial

class IOLoopBaseTest(unittest.TestCase):

    SELECT_POLLER = None
    TIMEOUT = 1.0

    def setUp(self):
        select_connection.SELECT_TYPE = self.SELECT_POLLER
        self.ioloop = select_connection.IOLoop()

    def tearDown(self):
        self.ioloop.remove_timeout(self.fail_timer)
        self.ioloop = None

    def start(self):
        self.fail_timer = self.ioloop.add_timeout(self.TIMEOUT, self.on_timeout)
        self.ioloop.start()

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        # force the ioloop to stop
        self.ioloop.stop()
        raise AssertionError('Test timed out')


class IOLoopThreadStopTestSelect(IOLoopBaseTest):
    SELECT_POLLER = 'select'
    def start_test(self):
        t = threading.Timer(0.1, self.ioloop.stop)
        t.start()
        self.start()

class IOLoopThreadStopTestPoll(IOLoopThreadStopTestSelect):
    SELECT_POLLER = 'poll'

class IOLoopThreadStopTestEPoll(IOLoopThreadStopTestSelect):
    SELECT_POLLER = 'epoll'

class IOLoopThreadStopTestKqueue(IOLoopThreadStopTestSelect):
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
            self.assertNotIn(target_timer,
                             getattr(self.ioloop, '_timeouts'))
            self.ioloop.stop()

        self.ioloop.add_timeout(0.01, _on_timer_conclude)

    def _on_timer_no_call(self):
        """A timeout handler that is used when it's assumed not be called."""
        self.fail('deleted timer callback was called.')


class IOLoopTimerTestPoll(IOLoopTimerTestSelect):
    SELECT_POLLER = 'poll'

class IOLoopTimerTestEPoll(IOLoopTimerTestSelect):
    SELECT_POLLER = 'epoll'

class IOLoopTimerTestKqueue(IOLoopTimerTestSelect):
    SELECT_POLLER = 'kqueue'


class IOLoopSleepTimerTestSelect(IOLoopTimerTestSelect):
    """Sleep until all the timers should have passed and check they still
        fire in deadline order"""

    def start_test(self):
        self.set_timers()
        time.sleep(self.NUM_TIMERS * self.TIMER_INTERVAL)
        self.start()


class IOLoopSleepTimerTestPoll(IOLoopSleepTimerTestSelect):
    SELECT_POLLER = 'poll'

class IOLoopSleepTimerTestEPoll(IOLoopSleepTimerTestSelect):
    SELECT_POLLER = 'epoll'

class IOLoopSleepTimerTestKqueue(IOLoopSleepTimerTestSelect):
    SELECT_POLLER = 'kqueue'

class IOLoopSocketBaseSelect(IOLoopBaseTest):

    SELECT_POLLER = 'select'
    READ_SIZE = 1024

    def save_sock(self, sock):
        fd = sock.fileno()
        self.sock_map[fd] = sock
        return fd

    def setUp(self):
        super(IOLoopSocketBaseSelect, self).setUp()
        self.sock_map = dict()
        self.create_accept_socket()

    def tearDown(self):
        for fd in self.sock_map:
            self.ioloop.remove_handler(fd)
            self.sock_map[fd].close()
        super(IOLoopSocketBaseSelect, self).tearDown()


    def create_accept_socket(self):
        listen_sock = socket.socket()
        listen_sock.setblocking(0)
        listen_sock.bind(('localhost', 0))
        listen_sock.listen(1)
        fd = self.save_sock(listen_sock)
        self.listen_addr = listen_sock.getsockname()
        self.ioloop.add_handler(fd, self.do_accept, READ)

    def create_write_socket(self, on_connected):
        write_sock = socket.socket()
        write_sock.setblocking(0)
        err = write_sock.connect_ex(self.listen_addr)
        self.assertEqual(err, errno.EINPROGRESS)
        fd = self.save_sock(write_sock)
        self.ioloop.add_handler(fd, on_connected, WRITE)
        return write_sock

    def do_accept(self, fd, events, write_only):
        self.assertEqual(events, READ)
        listen_sock = self.sock_map[fd]
        read_sock, _ = listen_sock.accept()
        fd = self.save_sock(read_sock)
        self.ioloop.add_handler(fd, self.do_read, READ)

    def connected(self, fd, events, write_only):
        raise AssertionError("IOLoopSocketBase.connected not extended")

    def do_read(self, fd, events, write_only):
        self.assertEqual(events, READ)
        self.verify_message(os.read(fd, self.READ_SIZE))

    def verify_message(self, msg):
        raise AssertionError("IOLoopSocketBase.verify_message not extended")

    def on_timeout(self):
        """called when stuck waiting for connection to close"""
        # force the ioloop to stop
        self.ioloop.stop()
        raise AssertionError('Test timed out')

class IOLoopSocketBasePoll(IOLoopSocketBaseSelect):
    SELECT_POLLER = 'poll'

class IOLoopSocketBaseEPoll(IOLoopSocketBaseSelect):
    SELECT_POLLER = 'epoll'

class IOLoopSocketBaseKqueue(IOLoopSocketBaseSelect):
    SELECT_POLLER = 'kqueue'


class IOLoopSimpleMessageTestCaseSelect(IOLoopSocketBaseSelect):

    def start(self):
        self.create_write_socket(self.connected)
        super(IOLoopSimpleMessageTestCaseSelect, self).start()

    def connected(self, fd, events, write_only):
        self.assertEqual(events, WRITE)
        logging.debug("Writing to %d message: %s", fd, 'X')
        os.write(fd, b'X')
        self.ioloop.update_handler(fd, 0)

    def verify_message(self, msg):
        self.assertEqual(msg, b'X')
        self.ioloop.stop()

    def start_test(self):
        self.start()

class IOLoopSimpleMessageTestCasetPoll(IOLoopSimpleMessageTestCaseSelect):
    SELECT_POLLER = 'poll'

class IOLoopSimpleMessageTestCasetEPoll(IOLoopSimpleMessageTestCaseSelect):
    SELECT_POLLER = 'epoll'

class IOLoopSimpleMessageTestCasetKqueue(IOLoopSimpleMessageTestCaseSelect):
    SELECT_POLLER = 'kqueue'
