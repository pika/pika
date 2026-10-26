"""Tests for .ci/run_examples.py."""

import pathlib
import sys
import textwrap
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
        self.assertIsNotNone(reason)

    def test_serves_names_only_existing_examples(self):
        present = {path.name for path in run_examples.EXAMPLES_DIR.glob('*.py')}
        self.assertLessEqual(run_examples.SERVES, present)


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
