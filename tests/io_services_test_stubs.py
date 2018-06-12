"""
Test stubs for running tests against all supported adaptations of
nbio_interface.AbstractIOServices and variations such as without SSL and
with SSL.

Usage example:

```
import unittest

from ..io_services_test_stubs import IOServicesTestStubs


class TestGetNativeIOLoop(unittest.TestCase,
                          IOServicesTestStubs):

    def start(self):
        native_loop = self.create_nbio().get_native_ioloop()
        self.assertIsNotNone(self._native_loop)
        self.assertIs(native_loop, self._native_loop)
```

"""

import sys
import unittest

from .threaded_test_wrapper import run_in_thread_with_timeout

# Suppress missing-docstring to allow test method names to be printed by our the
# test runner
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103


class IOServicesTestStubs(object):
    """Provides a stub test method for each combination of parameters we wish to
    test

    """
    # Overridden by framework-specific test methods
    _nbio_factory = None
    _native_loop = None
    _use_ssl = None

    def start(self):
        """ Subclasses must override to run the test. This method is called
        from a thread.

        """
        raise NotImplementedError

    def create_nbio(self):
        """Create the configured AbstractIOServices adaptation and schedule
        it to be closed automatically when the test terminates.

        :param unittest.TestCase self:
        :rtype: pika.adapters.utils.nbio_interface.AbstractIOServices

        """
        nbio = self._nbio_factory()
        self.addCleanup(nbio.close)  # pylint: disable=E1101
        return nbio

    def _run_start(self, nbio_factory, native_loop, use_ssl=False):
        """Called by framework-specific test stubs to initialize test paramters
        and execute the `self.start()` method.

        :param nbio_interface.AbstractIOServices _() nbio_factory: function
            to call to create an instance of `AbstractIOServices` adaptation.
        :param native_loop: native loop implementation instance
        :param bool use_ssl: Whether to test with SSL instead of Plaintext
            transport. Defaults to Plaintext.
        """
        self._nbio_factory = nbio_factory
        self._native_loop = native_loop
        self._use_ssl = use_ssl

        self.start()

    # Suppress missing-docstring to allow test method names to be printed by our
    # test runner
    # pylint: disable=C0111

    @run_in_thread_with_timeout
    def test_with_select_connection_io_services(self):
        # Test entry point for `select_connection.IOLoop`-based async services
        # implementation.

        from pika.adapters.select_connection import IOLoop
        from pika.adapters.utils.selector_ioloop_adapter import (
            SelectorIOServicesAdapter)
        native_loop = IOLoop()
        self._run_start(
            nbio_factory=lambda: SelectorIOServicesAdapter(native_loop),
            native_loop=native_loop)

    @run_in_thread_with_timeout
    def test_with_tornado_io_services(self):
        # Test entry point for `tornado.ioloop.IOLoop`-based async services
        # implementation.

        from tornado.ioloop import IOLoop
        from pika.adapters.utils.selector_ioloop_adapter import (
            SelectorIOServicesAdapter)

        native_loop = IOLoop()
        self._run_start(
            nbio_factory=lambda: SelectorIOServicesAdapter(native_loop),
            native_loop=native_loop)

    @unittest.skipIf(sys.version_info < (3, 4),
                     "Asyncio is available only with Python 3.4+")
    @run_in_thread_with_timeout
    def test_with_asyncio_io_services(self):
        # Test entry point for `asyncio` event loop-based io services
        # implementation.

        import asyncio
        from pika.adapters.asyncio_connection import (
            _AsyncioIOServicesAdapter)

        native_loop = asyncio.new_event_loop()
        self._run_start(
            nbio_factory=lambda: _AsyncioIOServicesAdapter(native_loop),
            native_loop=native_loop)
