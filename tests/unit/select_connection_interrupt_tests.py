"""Unit test for `_PollerBase._read_interrupt` non-blocking errno handling.

Regression coverage for pika/pika#1314. On Windows, a non-blocking `recv`
against an empty socket raises `BlockingIOError` with `errno.EWOULDBLOCK`
(`WSAEWOULDBLOCK`, 10035) rather than `errno.EAGAIN` (11). The interrupt
handler must swallow both; otherwise a spurious wake or a drained buffer
kills the poller thread on Windows.

This test uses a real `SelectPoller` and a real non-blocking socket pair,
so it exercises whatever errno the OS actually raises. On Linux both
symbols share a value and the test is a no-op guard; on Windows it fails
against the pre-fix code and passes against the fix.

"""

import unittest

from pika.adapters import select_connection

# protected-access
# pylint: disable=W0212


class ReadInterruptHandlesEmptySocket(unittest.TestCase):
    """`_read_interrupt` must return cleanly when the interrupt fd is empty.

    The interrupt socket pair created by `_PollerBase.__init__` is
    non-blocking, so calling `_read_interrupt` against an empty read side
    triggers an OS-native "would block" error. The handler is expected to
    recognize it and return without propagating.
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


if __name__ == '__main__':
    unittest.main()
