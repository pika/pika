"""
Tests for threaded_test_wrapper.py

"""
from __future__ import print_function

import sys
import threading
import time
import unittest

try:
    from unittest import mock
except ImportError:
    import mock

import pika.compat

from .. import threaded_test_wrapper
from ..threaded_test_wrapper import (_ThreadedTestWrapper,
                                     run_in_thread_with_timeout)

# Suppress invalid-name, since our test names are descriptive and quite long
# pylint: disable=C0103

# Suppress missing-docstring to allow test method names to be printed by our the
# test runner
# pylint: disable=C0111


class ThreadedTestWrapperSelfChecks(unittest.TestCase):
    """Tests for threaded_test_wrapper.py.

    """

    def start(self):
        """Each of the tests in this test case patches this method to run its
        own test

        """
        raise NotImplementedError

    def test_propagation_of_failure_from_test_execution_thread(self):
        class SelfCheckExceptionHandling(Exception):
            pass

        caller_thread_id = threading.current_thread().ident

        @run_in_thread_with_timeout
        def my_errant_function(*_args, **_kwargs):
            if threading.current_thread().ident != caller_thread_id:
                raise SelfCheckExceptionHandling()

        # Suppress error output by redirecting to stringio_stderr
        stringio_stderr = pika.compat.StringIO()
        try:
            with mock.patch.object(_ThreadedTestWrapper, '_stderr',
                                   stringio_stderr):
                with self.assertRaises(AssertionError) as exc_ctx:
                    my_errant_function()

            self.assertIn('raise SelfCheckExceptionHandling()',
                          exc_ctx.exception.args[0])
            expected_tail = 'SelfCheckExceptionHandling\n'
            self.assertEqual(exc_ctx.exception.args[0][-len(expected_tail):],
                             expected_tail)

            self.assertIn('raise SelfCheckExceptionHandling()',
                          stringio_stderr.getvalue())
            self.assertEqual(stringio_stderr.getvalue()[-len(expected_tail):],
                             expected_tail)
        except Exception:
            try:
                print('This stderr was captured from our thread wrapper:\n',
                      stringio_stderr.getvalue(),
                      file=sys.stderr)
            except Exception:  # pylint: disable=W0703
                pass

            raise

    def test_handling_of_test_execution_thread_timeout(self):
        # Suppress error output by redirecting to our stringio_stderr object
        stringio_stderr = pika.compat.StringIO()

        @run_in_thread_with_timeout
        def my_sleeper(*_args, **_kwargs):
            time.sleep(1.1)

        # Redirect _ThreadedTestWrapper error output to our StringIO instance
        with mock.patch.object(_ThreadedTestWrapper, '_stderr',
                               stringio_stderr):
            # Patch DEFAULT_TEST_TIMEOUT to much smaller value than sleep in
            # my_start()
            with mock.patch.object(threaded_test_wrapper,
                                   'DEFAULT_TEST_TIMEOUT',
                                   0.01):
                # Redirect start() call from thread to our own my_start()
                with self.assertRaises(AssertionError) as exc_ctx:
                    my_sleeper()

        self.assertEqual(len(stringio_stderr.getvalue()), 0)
        self.assertIn('The test timed out.', exc_ctx.exception.args[0])

    def test_integrity_of_args_and_return_value(self):
        args_bucket = []
        kwargs_bucket = []
        value_to_return = dict()

        @run_in_thread_with_timeout
        def my_guinea_pig(*args, **kwargs):
            args_bucket.append(args)
            kwargs_bucket.append(kwargs)
            return value_to_return

        arg0 = dict()
        arg1 = tuple()

        kwarg0 = list()

        result = my_guinea_pig(arg0, arg1, kwarg0=kwarg0)

        self.assertIs(result, value_to_return)

        args_ut = args_bucket[0]
        self.assertEqual(len(args_ut), 2, repr(args_ut))
        self.assertIs(args_ut[0], arg0)
        self.assertIs(args_ut[1], arg1)

        kwargs_ut = kwargs_bucket[0]
        self.assertEqual(len(kwargs_ut), 1, repr(kwargs_ut))
        self.assertIn('kwarg0', kwargs_ut, repr(kwargs_ut))
        self.assertIs(kwargs_ut['kwarg0'], kwarg0)


    def test_skip_test_is_passed_through(self):

        @run_in_thread_with_timeout
        def my_test_skipper():
            raise unittest.SkipTest('I SKIP')

        with self.assertRaises(unittest.SkipTest) as ctx:
            my_test_skipper()

        self.assertEqual(ctx.exception.args[0], 'I SKIP')
