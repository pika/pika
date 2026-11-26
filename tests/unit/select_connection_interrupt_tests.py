"""
Unit tests for the poller's interrupt socket pair.

`_read_interrupt` non-blocking errno handling is pika/pika#1314. On Windows, a non-blocking `recv`
against an empty socket raises `BlockingIOError` with `errno.EWOULDBLOCK` (`WSAEWOULDBLOCK`, 10035)
rather than `errno.EAGAIN` (11). The interrupt handler must swallow both; otherwise a spurious wake
or a drained buffer kills the poller thread on Windows.

The #1314 test uses a real `SelectPoller` and a real non-blocking socket pair, so it exercises
whatever errno the OS actually raises. On Linux both symbols share a value and the test is a no-op
guard; on Windows it fails against the pre-fix code and passes against the fix.

Re-entering `wake_threadsafe` from a signal handler is pika/pika#1863.
"""

import select
import threading
import unittest

from pika.adapters import select_connection


class ReadInterruptHandlesEmptySocket(unittest.TestCase):
    """
    `_read_interrupt` must return cleanly when the interrupt fd is empty.

    The interrupt socket pair created by `_PollerBase.__init__` is non-blocking, so calling
    `_read_interrupt` against an empty read side triggers an OS-native "would block" error. The
    handler is expected to recognize it and return without propagating.
    """

    def setUp(self):
        self._poller = select_connection.SelectPoller(
            get_wait_seconds=lambda: 0, process_timeouts=lambda: None)

    def tearDown(self):
        self._poller.close()

    def test_empty_read_does_not_raise(self):
        # Interrupt socket is created empty by __init__, so this directly
        # exercises the would-block path. No prior read is needed.
        self._poller._read_interrupt(None, None)


class _ReentrantSocket:
    """A socket stand-in whose `send` or `close` re-enters the poller, as a signal handler would."""

    def __init__(self, sock, poller, reenter_on):
        self._sock = sock
        self._poller = poller
        self._reenter_on = reenter_on
        self.sent = []
        self.errors = []
        self.saw_cleared = None

    def fileno(self):
        return self._sock.fileno()

    def _reenter(self):
        self.saw_cleared = self._poller._w_interrupt is None
        try:
            self._poller.wake_threadsafe()
        except Exception as error:
            self.errors.append(error)

    def send(self, data):
        self.sent.append(data)
        if self._reenter_on == 'send' and len(self.sent) == 1:
            self._reenter()
        return self._sock.send(data)

    def close(self):
        if self._reenter_on == 'close':
            self._reenter()
        self._sock.close()


class WakeReentryTests(unittest.TestCase):
    """A signal handler re-enters the wakeup mutex on the thread it interrupted (#1863)."""

    def setUp(self):
        self._poller = select_connection.SelectPoller(
            get_wait_seconds=lambda: 0, process_timeouts=lambda: None)

    def _run_bounded(self, target):
        errors = []

        def run():
            try:
                target()
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive(), 'the wakeup mutex deadlocked')
        self.assertEqual(errors, [])

    def test_a_wakeup_reentered_during_a_wakeup_sends_both(self):
        sock = _ReentrantSocket(self._poller._w_interrupt, self._poller, 'send')
        self._poller._w_interrupt = sock
        self._run_bounded(self._poller.wake_threadsafe)
        # Registered after the bounded run, so a deadlock fails the assertion
        # there rather than hanging in cleanup on the mutex the thread holds.
        self.addCleanup(self._poller.close)
        self.assertEqual(sock.sent, [b'X', b'X'])
        self.assertEqual(sock.errors, [])

    def test_a_wakeup_reentered_during_close_finds_the_sockets_cleared(self):
        sock = _ReentrantSocket(self._poller._r_interrupt, self._poller,
                                'close')
        writer = _ReentrantSocket(self._poller._w_interrupt, self._poller, None)
        self._poller._r_interrupt = sock
        self._poller._w_interrupt = writer
        self._run_bounded(self._poller.close)
        self.assertIs(sock.saw_cleared, True,
                      'the wakeup could still reach a closing socket')
        self.assertEqual(sock.errors, [])

    def test_a_close_reentered_during_a_send_ends_the_wakeup_quietly(self):
        poller = self._poller
        real = poller._w_interrupt

        class _ClosingSocket:

            def fileno(self):
                return real.fileno()

            def send(self, data):
                poller.close()
                return real.send(data)

            def close(self):
                real.close()

        poller._w_interrupt = _ClosingSocket()
        self._run_bounded(poller.wake_threadsafe)
        self.assertIsNone(poller._w_interrupt)

    def test_a_running_poller_thread_wakes_without_the_mutex(self):
        held, release = threading.Event(), threading.Event()

        def hold():
            with self._poller._waking_mutex:
                held.set()
                release.wait(timeout=10)

        def wake_as_poller_thread():
            self._poller._thread = threading.current_thread()
            self._poller._running = True
            try:
                self._poller.wake_threadsafe()
            finally:
                self._poller._running = False

        holder = threading.Thread(target=hold, daemon=True)
        holder.start()
        self.assertTrue(held.wait(timeout=5))
        try:
            self._run_bounded(wake_as_poller_thread)
        finally:
            release.set()
            holder.join(timeout=5)
        self.addCleanup(self._poller.close)
        # Windows' interrupt pair is loopback TCP, which can deliver late.
        readable, _, _ = select.select([self._poller._r_interrupt], [], [], 10)
        self.assertTrue(readable)
        self.assertEqual(self._poller._r_interrupt.recv(16), b'X')

    def test_start_records_the_thread_it_runs_on(self):
        seen = []

        def process_timeouts():
            seen.append(self._poller._thread is threading.current_thread())
            self._poller.stop()

        self._poller._process_timeouts = process_timeouts
        self._run_bounded(self._poller.start)
        self.addCleanup(self._poller.close)
        self.assertEqual(seen, [True])


if __name__ == '__main__':
    unittest.main()
