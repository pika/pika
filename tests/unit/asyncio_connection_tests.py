"""
Tests for pika.adapters.asyncio_connection
"""

import asyncio
import threading
import unittest

from pika.adapters.asyncio_connection import _AsyncioIOServicesAdapter


# missing-docstring
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103


class AsyncioIOServicesAdapterLoopInitTests(unittest.TestCase):
    """Tests for _AsyncioIOServicesAdapter loop initialisation logic."""

    def test_explicit_loop_is_used_as_is(self):
        loop = asyncio.new_event_loop()
        try:
            adapter = _AsyncioIOServicesAdapter(loop)
            self.assertIs(adapter.get_native_ioloop(), loop)
        finally:
            loop.close()

    def test_no_loop_uses_running_loop_when_available(self):
        results = []

        async def _run():
            adapter = _AsyncioIOServicesAdapter()
            results.append(adapter.get_native_ioloop())

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_run())
            self.assertIs(results[0], loop)
        finally:
            loop.close()

    def test_no_loop_creates_new_loop_on_main_thread_sync_context(self):
        # Simulates the Python 3.14 breakage: a plain synchronous call with no
        # running event loop (e.g. top-level script before any loop is started).
        adapter = _AsyncioIOServicesAdapter()
        loop = adapter.get_native_ioloop()
        try:
            self.assertIsInstance(loop, asyncio.AbstractEventLoop)
        finally:
            loop.close()

    def test_no_loop_creates_new_loop_when_none_running(self):
        results = []
        errors = []

        def _thread_target():
            try:
                adapter = _AsyncioIOServicesAdapter()
                results.append(adapter.get_native_ioloop())
                adapter.get_native_ioloop().close()
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        t = threading.Thread(target=_thread_target)
        t.start()
        t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], asyncio.AbstractEventLoop)
