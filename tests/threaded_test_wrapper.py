"""
Implements run_in_thread_with_timeout decorator for running tests that might
deadlock.

"""

from __future__ import print_function

import functools
import os
import sys
import threading
import traceback
import unittest


MODULE_PID = os.getpid()

DEFAULT_TEST_TIMEOUT = 15


def create_run_in_thread_decorator(test_timeout=None):
    """Create a decorator that will run the decorated method in a thread via
    `_ThreadedTestWrapper` and return the value that is returned by the
    given function, unless it exits with exception or times out, in which
    case AssertionError will be raised

    :param int | float | None test_timeout: maximum number of seconds to wait
        for test to complete. If None, `DEFAULT_TEST_TIMEOUT` will be used.
        NOTE: we handle default this way to facilitate patching of the timeout
        in our self-tests.
    :return: decorator
    """

    def run_in_thread_with_timeout_decorator(fun):
        """Create a wrapper that will run the decorated method in a thread via
        `_ThreadedTestWrapper` and return the value that is returned by the
        given function, unless it exits with exception or times out, in which
        case AssertionError will be raised

        :param fun: function to run in thread
        :return: wrapper function
        """

        @functools.wraps(fun)
        def run_in_thread_with_timeout_wrapper(*args, **kwargs):
            """

            :param args: positional args to pass to wrapped function
            :param kwargs: keyword args to pass to wrapped function
            :return: value returned by the function, unless it exits with
                exception or times out
            :raises AssertionError: if wrapped function exits with exception or
                times out
            """
            runner = _ThreadedTestWrapper(
                functools.partial(fun, *args, **kwargs),
                test_timeout)
            return runner.kick_off()

        return run_in_thread_with_timeout_wrapper

    return run_in_thread_with_timeout_decorator


run_in_thread_with_timeout = create_run_in_thread_decorator()  # pylint: disable=C0103


class _ThreadedTestWrapper(object):
    """Runs user's function in a thread. Then wait on the
    thread to terminate up to the given `test_timeout` seconds, raising
    `AssertionError` if user's function exits with exception or times out.

    """
    # We use the saved member when printing to facilitate patching by our
    # self-tests
    _stderr = sys.stderr

    def __init__(self, fun, test_timeout):
        """
        :param callable fun: the function to run in thread, no args.
        :param int | float test_timeout: maximum number of seconds to wait for
            thread to exit.

        """
        self._fun = fun
        if test_timeout is None:
            # NOTE: we handle default here to facilitate patching of
            # DEFAULT_TEST_TIMEOUT in our self-tests
            self._test_timeout = DEFAULT_TEST_TIMEOUT
        else:
            self._test_timeout = test_timeout

        # Save possibly-patched class-level _stderr value in instance so in case
        # user's function times out and later exits with exception, our
        # exception handler in `_thread_entry` won't inadvertently output to the
        # wrong object.
        self._stderr = self._stderr

        self._fun_result = None  # result returned by function being run
        self._exc_info = None

    def kick_off(self):
        """Run user's function in a thread. Then wait on the
        thread to terminate up to self._test_timeout seconds, raising
        `AssertionError` if user's function exits with exception or times out.

        :return: the value returned by function if function exited without
            exception and didn't time out
        :raises AssertionError: if user's function timed out or exited with
            exception.
        """
        try:
            runner = threading.Thread(target=self._thread_entry)
            # `daemon = True` so that the script won't wait for thread's exit
            runner.daemon = True
            runner.start()
            runner.join(self._test_timeout)

            if runner.is_alive():
                raise AssertionError('The test timed out.')

            if self._exc_info is not None:
                if isinstance(self._exc_info[1], unittest.SkipTest):
                    raise self._exc_info[1]

                # Fail the test because the thread running the test's start()
                # failed
                raise AssertionError(self._exc_info_to_str(self._exc_info))

            return self._fun_result
        finally:
            # Facilitate garbage collection
            self._exc_info = None
            self._fun = None

    def _thread_entry(self):
        """Our test-execution thread entry point that calls the test's `start()`
        method.

        Here, we catch all exceptions from `start()`, save the `exc_info` for
        processing by `_kick_off()`, and print the stack trace to `sys.stderr`.
        """
        try:
            self._fun_result = self._fun()
        except:  # pylint: disable=W0702
            self._exc_info = sys.exc_info()
            del self._fun_result  # to force exception on inadvertent access
            if not isinstance(self._exc_info[1], unittest.SkipTest):
                print(
                    'ERROR start() of test {} failed:\n{}'.format(
                        self,
                        self._exc_info_to_str(self._exc_info)),
                    end='',
                    file=self._stderr)

    @staticmethod
    def _exc_info_to_str(exc_info):
        """Convenience method for converting the value returned by
        `sys.exc_info()` to a string.

        :param tuple exc_info: Value returned by `sys.exc_info()`.
        :return: A string representation of the given `exc_info`.
        :rtype: str
        """
        return ''.join(traceback.format_exception(*exc_info))
