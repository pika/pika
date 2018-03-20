"""
Base classes for testing async_interface.AbstractAsyncServices adaptations

Import the following:

AsyncServicesTestBaseSelfChecks - to cause self-check tests to run once
AsyncServicesTestBase - contains test workflow logic
AsyncServicesTestStubs - contains stub test methods for each adaptation

Implement each test as a class that subclasses from both AsyncServicesTestBase
and AsyncServicesTestBase and implement the instance method `start()` method
as the entry point of your test (`AsyncServicesTestBase` calls your `start()`
method to run your test).

Example:

```
from .async_services_test_base import (AsyncServicesTestBase,
                                       AsyncServicesTestStubs)

class TestGetNativeIOLoop(AsyncServicesTestBase,
                          AsyncServicesTestStubs):

    def start(self):
        native_loop = self.create_async().get_native_ioloop()
        self.assertIsNotNone(self._native_loop)
        self.assertIs(native_loop, self._native_loop)
```

"""

from __future__ import print_function

import logging
import os
import sys
import threading
import traceback
import unittest

# invalid-name
# pylint: disable=C0103

g_module_pid = os.getpid()


class AsyncServicesTestBase(unittest.TestCase):
    """ Base test case class that contains test workflow logic.

    """
    TEST_TIMEOUT = 15  # timeout for each individual test entry point

    def setUp(self):
        # Provisioned by _kick_off()
        self._async_factory = None
        self._native_loop = None
        self._use_ssl = None

        self.logger = logging.getLogger(self.__class__.__module__ + '.' +
                                        self.__class__.__name__)
        self._exc_info = None

        # We use the saved member when printing to facilitate patching by our
        # self-tests
        self._stderr = sys.stderr

    def start(self):
        """ Subclasses must override to run the test. This method is called
        from a thread.

        """
        raise NotImplementedError

    def create_async(self):
        """Create the configured AbstractAsyncServices adaptation and schedule
        it to be closed automatically when the test terminates.

        :rtype: pika.adapters.async_interface.AbstractAsyncServices

        """
        async_svc = self._async_factory()
        self.addCleanup(async_svc.close)

        return async_svc

    def _thread_entry(self):
        """Our test-execution thread entry point that calls the test's `start()`
        method.

        Here, we catch all exceptions from `start()`, save the `exc_info` for
        processing by `_kick_off()`, and print the stack trace to `sys.stderr`.
        """
        try:
            self.start()
        except:  # pylint: disable=W0702
            self._exc_info = sys.exc_info()
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

    def _kick_off(self, async_factory, native_loop, use_ssl=False):
        """ Kick off the current test in a thread using `self.start()` as its
        entry point. Then wait on the thread to terminate up to
        `self.TEST_TIMEOUT` seconds, failing the test if it doesn't.

        :param async_interface.AbstractAsyncServices _() async_factory: function
            to call to create an instance of `AbstractAsyncServices` adaptation.
        :param native_loop: native loop implementation instance
        :param bool use_ssl: Whether to test with SSL instead of Plaintext
            transport. Defaults to Plaintext.
        """
        self._async_factory = async_factory
        self._native_loop = native_loop
        self._use_ssl = use_ssl
        # runner = threading.Thread(target=self._thread_entry, args=(self,))
        runner = threading.Thread(target=self._thread_entry)
        runner.daemon = True  # so that the script won't wait for thread's exit
        runner.start()
        runner.join(self.TEST_TIMEOUT)

        self.assertFalse(runner.is_alive(), 'The test timed out.')

        if self._exc_info is not None:
            # Fail the test because the thread running the test's start() failed
            self.fail(self._exc_info_to_str(self._exc_info))



class AsyncServicesTestStubs(object):
    """Provides a stub test method for each combination of parameters we wish to
    test

    """
    def _kick_off(self, *args, **kwargs):
        """We expect users to also include `AsyncServicesTestBase` as a base
        class that defines this method.

        See `AsyncServicesTestBase._kick_off()` for details.

        """
        raise NotImplementedError('Add AsyncServicesTestBase as base to your '
                                  'test class to get the implementation of '
                                  'this method.')

    def test_with_select_async_services(self):
        """Test entry point for `select_connection.IOLoop`-based async services
        implementation.

        """
        from pika.adapters.select_connection import IOLoop
        from pika.adapters.selector_ioloop_adapter import (
            SelectorAsyncServicesAdapter)
        native_loop = IOLoop()
        self._kick_off(
            async_factory=lambda: SelectorAsyncServicesAdapter(native_loop),
            native_loop=native_loop)

    def test_with_tornado_async_services(self):
        """Test entry point for `tornado.ioloop.IOLoop`-based async services
        implementation.

        """
        from tornado.ioloop import IOLoop
        from pika.adapters.selector_ioloop_adapter import (
            SelectorAsyncServicesAdapter)

        native_loop = IOLoop()
        self._kick_off(
            async_factory=lambda: SelectorAsyncServicesAdapter(native_loop),
            native_loop=native_loop)

    def test_with_twisted_async_services(self):
        """Test entry point for `twisted.reactor`-based async services
        implementation.

        """
        # Twisted testing requires a test runner that isolates each test in an
        # individual process, such as `py.test` with the `--boxed` option provided
        # by the `pytest-xdist` module.

        if os.getpid() == g_module_pid:
            raise unittest.SkipTest(
                'Cannot run twisted tests in the main process '
                'because twisted reactor is not restartable.')

        from twisted.internet import reactor
        from pika.adapters.twisted_connection import (
            _TwistedAsyncServicesAdapter)

        # There doesn't seem to be a way to created a custom instance of a
        # twisted reactor, so we use the singleton
        self._kick_off(
            async_factory=lambda: _TwistedAsyncServicesAdapter(reactor),
            native_loop=reactor)

    @unittest.skipIf(sys.version_info < (3, 4), "Asyncio available for Python 3.4+")
    def test_with_asyncio_async_services(self):
        """Test entry point for `asyncio` event loop-based async services
        implementation.

        """
        import asyncio
        from pika.adapters.asyncio_connection import (
            _AsyncioAsyncServicesAdapter)

        native_loop = asyncio.new_event_loop()
        self._kick_off(
            async_factory=lambda: _AsyncioAsyncServicesAdapter(native_loop),
            native_loop=native_loop)
