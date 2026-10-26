"""Tests for `tests.misc.forward_server.ForwardServer`."""

import unittest
from unittest import mock

from tests.misc.forward_server import ForwardServer


class StopTests(unittest.TestCase):

    def test_stop_kills_a_subprocess_that_ignores_terminate(self):
        server = ForwardServer(None)
        subproc = mock.Mock()
        subproc.is_alive.side_effect = [True, False]
        subproc.exitcode = -9
        server._subproc = subproc

        server.stop()

        subproc.terminate.assert_called_once_with()
        subproc.kill.assert_called_once_with()
        self.assertIsNone(server._subproc)


if __name__ == '__main__':
    unittest.main()
