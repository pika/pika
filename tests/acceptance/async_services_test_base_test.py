"""
Tests for async_services_test_base.py

"""
import sys
import time

try:
    from unittest import mock
except ImportError:
    import mock

import pika.compat

from .async_services_test_base import AsyncServicesTestBase

# Suppress invalid-name, since our test names are descriptive and quite long
# pylint: disable=C0103

# Suppress missing-docstring to allow test method names to be printed by our the
# test runner
# pylint: disable=C0111


class AsyncServicesTestBaseSelfChecks(AsyncServicesTestBase):
    """ Base class for tests that contains stub test methods for each
    `AbstractAsyncServices` adaptation.

    """
    def start(self):
        """ Each of the tests in this test case patches this method to run its
        own test

        """
        raise NotImplementedError

    def test_propagation_of_failure_from_test_execution_thread(self):
        class SelfCheckExceptionHandling(Exception):
            pass

        def my_start(*_args, **_kwargs):
            raise SelfCheckExceptionHandling()

        # Suppress error output by redirecting to stringio_stderr
        stringio_stderr = pika.compat.StringIO()
        try:
            with mock.patch.object(self, '_stderr', stringio_stderr):
                # Redirect start() call from thread to our own my_start()
                with mock.patch.object(self, 'start', my_start):
                    with self.assertRaises(AssertionError) as exc_ctx:
                        self._kick_off(None, None)

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
        stderr_patch = mock.patch.object(self, '_stderr', stringio_stderr)
        # NOTE: We don't unpatch it because this test expects the thread to
        # linger slightly longer than the test and this patch only affects
        # this test method's instance anyway
        stderr_patch.start()

        def my_start(*_args, **_kwargs):
            time.sleep(1.1)

        # Patch TEST_TIMEOUT to much smaller value than sleep in my_start()
        with mock.patch.object(self, 'TEST_TIMEOUT', 0.01):
            # Redirect start() call from thread to our own my_start()
            with mock.patch.object(self, 'start', my_start):
                with self.assertRaises(AssertionError) as exc_ctx:
                    self._kick_off(None, None)

        self.assertEqual(len(stringio_stderr.getvalue()), 0)
        self.assertIn('The test timed out.', exc_ctx.exception.args[0])
