"""Tests for .ci/run_examples.py."""

import contextlib
import os
import pathlib
import signal
import sys
import textwrap
import threading
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / '.ci'))

import run_examples

#: Long enough for the child interpreter to install its SIGINT handler first, which a signal
#: arriving earlier would preempt by killing it outright.
SETTLE = 2.0


def run(source, serves=False, **timings):
    return run_examples.run([sys.executable, '-c', source], serves, **timings)


class RunTests(unittest.TestCase):

    def setUp(self):
        # Run as `hatch run` does, with SIGINT ignored, which a child inherits unless the runner
        # restores it. Only the main thread may change that, and xdist does not always test there.
        if (sys.platform != 'win32' and
                threading.current_thread() is threading.main_thread()):
            previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
            self.addCleanup(signal.signal, signal.SIGINT, previous)

    def test_an_example_that_exits_zero_passes_and_both_streams_are_kept(self):
        reason, output = run(
            'import sys; print("out"); sys.stderr.write("err\\n")')
        self.assertIsNone(reason)
        self.assertIn('out', output)
        self.assertIn('err', output)

    def test_a_nonzero_status_fails(self):
        self.assertEqual(run('raise SystemExit(3)')[0], 'exited 3')

    def test_a_hang_fails_at_the_deadline(self):
        reason, _ = run('import time; time.sleep(60)', deadline=1)
        self.assertEqual(reason, 'still running after 1s')

    def test_a_traceback_fails_an_example_that_exits_zero(self):
        reason, _ = run('print("Traceback (most recent call last):")')
        self.assertIn('Traceback', reason)

    def test_a_server_that_exits_before_being_interrupted_fails(self):
        """The shape of an example that hits a declare error and returns quietly."""
        reason, _ = run('pass', serves=True, settle=10)
        self.assertEqual(reason, 'exited 0 before it was interrupted')

    @unittest.skipIf(sys.platform == 'win32',
                     'Windows cannot send SIGINT to a child')
    def test_a_server_that_stops_cleanly_on_sigint_passes(self):
        source = textwrap.dedent("""
            import time
            try:
                time.sleep(60)
            except KeyboardInterrupt:
                print('stopped')
        """)
        reason, output = run(source, serves=True, settle=SETTLE, grace=10)
        self.assertIsNone(reason)
        self.assertIn('stopped', output)

    @unittest.skipIf(sys.platform == 'win32',
                     'Windows cannot send SIGINT to a child')
    def test_a_server_that_does_not_handle_sigint_fails(self):
        reason, _ = run('import time; time.sleep(60)',
                        serves=True,
                        settle=SETTLE,
                        grace=10)
        # It must fail for the unhandled KeyboardInterrupt (a traceback on 3.8+, exit 1 on 3.7),
        # not because the child never received SIGINT.
        self.assertIsNotNone(reason)
        self.assertNotIn('still running', reason)

    def test_a_server_that_never_connects_fails(self):
        reason, _ = run('import time; time.sleep(60)',
                        serves=True,
                        settle=0.5,
                        connected=lambda: False,
                        connect_wait=1)
        self.assertEqual(reason, 'never connected to the broker')

    def test_serves_names_only_existing_examples(self):
        present = {path.name for path in run_examples.EXAMPLES_DIR.glob('*.py')}
        self.assertLessEqual(run_examples.SERVES, present)


class MainTests(unittest.TestCase):

    @mock.patch.object(run_examples, '_reset')
    @mock.patch.object(run_examples, 'run', return_value=(None, 'did it'))
    def test_a_pass_prints_its_output_in_a_collapsed_group_on_github(
            self, _run, _reset):
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}))
            printed = stack.enter_context(mock.patch('builtins.print'))
            self.assertEqual(0, run_examples.main(['consume.py']))
        text = '\n'.join(str(c[0][0]) for c in printed.call_args_list)
        self.assertIn('::group::consume.py output\ndid it\n::endgroup::', text)


class ResetTests(unittest.TestCase):

    @mock.patch.object(run_examples.time, 'sleep')
    @mock.patch.object(run_examples, '_api')
    def test_a_queue_locked_by_a_closing_connection_is_retried(
            self, api, _sleep):
        locked = urllib.error.HTTPError('url', 400, 'Bad Request', None, None)
        # Listing, refused delete, then a fresh listing without the queue.
        api.side_effect = [[{'name': 'standard'}], locked, [], []]
        run_examples._reset()
        self.assertEqual(api.call_count, 4)

    @mock.patch.object(run_examples.time, 'sleep')
    @mock.patch.object(run_examples, '_api')
    def test_an_error_that_outlasts_the_retries_is_raised(self, api, _sleep):
        api.side_effect = urllib.error.HTTPError('url', 401, 'Unauthorized',
                                                 None, None)
        self.assertRaises(urllib.error.HTTPError, run_examples._reset, 3)
        self.assertEqual(api.call_count, 3)


if __name__ == '__main__':
    unittest.main()
