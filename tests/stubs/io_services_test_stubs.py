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

import contextlib
import logging

from pika.adapters.utils import nbio_interface
from tests.wrappers.threaded_test_wrapper import run_in_thread_with_timeout

LOGGER = logging.getLogger(__name__)


class IOServicesTestStubs:
    """Provides a stub test method for each combination of parameters we wish to test."""

    # Overridden by framework-specific test methods
    _nbio_factory = None
    _native_loop = None
    _use_ssl = None

    def start(self):
        """
        Subclasses must override to run the test.

        This method is called from a thread.
        """
        raise NotImplementedError

    def create_nbio(self) -> nbio_interface.AbstractIOServices:
        """
        Create the configured AbstractIOServices adaptation and schedule it to be closed
        automatically when the test terminates.

        :param unittest.TestCase self:
        """
        nbio = self._nbio_factory()
        self.addCleanup(self._close_nbio, nbio)
        return nbio

    @staticmethod
    def _close_nbio(nbio: nbio_interface.AbstractIOServices) -> None:
        """
        Close the given I/O services instance, tolerating a still-running loop.

        Cleanups run on the main thread, while `start()` runs in a thread that
        `run_in_thread_with_timeout` abandons when the test times out. The loop is then still
        running, and closing it fails: asyncio and tornado raise `RuntimeError: Cannot close a
        running event loop`, and `select_connection` asserts `Cannot call close() before start()
        unwinds.`. Reported from cleanup, either one buries the `AssertionError: The test timed
        out.` that says why the test actually failed.

        So request a stop first, which an abandoned loop honors from its own thread, and warn rather
        than raise if the close still does not take. There is only one attempt to make: tornado
        unregisters its I/O loop before closing the asyncio loop underneath it, so a close that
        fails part way leaves every later attempt raising `KeyError`. An unclosed loop is what a
        timed-out test already left behind before this method existed.

        :param nbio: the instance to close.
        """
        with contextlib.suppress(Exception):
            # Honored only by a loop that is still running, i.e. an abandoned one
            nbio.add_callback_threadsafe(nbio.stop)

        try:
            nbio.close()
        except Exception as error:
            LOGGER.warning(
                'Could not close %r: %r. Its test most likely timed out, '
                'leaving the I/O loop running.', nbio, error)

    def _run_start(self, nbio_factory, native_loop, use_ssl=False):
        """
        Called by framework-specific test stubs to initialize test parameters and execute the
        `self.start()` method.

        :param nbio_interface.AbstractIOServices _() nbio_factory: function to call to create an
            instance of `AbstractIOServices` adaptation.
        :param native_loop: native loop implementation instance
        :param bool use_ssl: Whether to test with SSL instead of Plaintext transport. Defaults to
            Plaintext.
        """
        self._nbio_factory = nbio_factory
        self._native_loop = native_loop
        self._use_ssl = use_ssl

        self.start()

    @run_in_thread_with_timeout
    def test_with_select_connection_io_services(self):
        # Test entry point for `select_connection.IOLoop`-based async services
        # implementation.

        from pika.adapters.select_connection import IOLoop
        from pika.adapters.utils.selector_ioloop_adapter import SelectorIOServicesAdapter

        native_loop = IOLoop()
        self._run_start(
            nbio_factory=lambda: SelectorIOServicesAdapter(native_loop),
            native_loop=native_loop)

    @run_in_thread_with_timeout
    def test_with_tornado_io_services(self):
        # Test entry point for `tornado.ioloop.IOLoop`-based async services
        # implementation.

        from tornado.ioloop import IOLoop

        from pika.adapters.utils.selector_ioloop_adapter import SelectorIOServicesAdapter

        native_loop = IOLoop()
        self._run_start(
            nbio_factory=lambda: SelectorIOServicesAdapter(native_loop),
            native_loop=native_loop)

    @run_in_thread_with_timeout
    def test_with_asyncio_io_services(self):
        # Test entry point for `asyncio` event loop-based io services
        # implementation.

        from pika.adapters.asyncio_connection import _AsyncioIOServicesAdapter
        from tests.base.asyncio_loop import new_pika_asyncio_loop

        native_loop = new_pika_asyncio_loop()
        self._run_start(
            nbio_factory=lambda: _AsyncioIOServicesAdapter(native_loop),
            native_loop=native_loop)
