"""
Tests of async_interface.AbstractAsyncServices adaptations
"""

import functools
import time

try:
    from unittest import mock
except ImportError:
    import mock

# NOTE: We import AsyncServicesTestBaseSelfChecks to make sure that self-checks
# will run
from .async_services_test_base import (AsyncServicesTestBase,
                                       AsyncServicesTestStubs,
                                       AsyncServicesTestBaseSelfChecks)

# Make pylin't unused-import check happy
AsyncServicesTestBaseSelfChecks = AsyncServicesTestBaseSelfChecks


class TestGetNativeIOLoop(AsyncServicesTestBase,
                          AsyncServicesTestStubs):

    def start(self):
        native_loop = self.create_async().get_native_ioloop()
        self.assertIsNotNone(self._native_loop)
        self.assertIs(native_loop, self._native_loop)


class TestRunWithStopFromThreadsafeCallback(AsyncServicesTestBase,
                                            AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()

        bucket = []

        def callback():
            loop.stop()
            bucket.append('I was called')

        loop.add_callback_threadsafe(callback)
        loop.run()

        self.assertEqual(len(bucket), 1)


class TestCallLater(AsyncServicesTestBase,
                    AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()

        bucket = []

        def callback():
            loop.stop()
            bucket.append('I was here')

        start_time = time.time()
        loop.call_later(0.1, callback)
        loop.run()
        self.assertGreaterEqual(time.time() - start_time, 0.1)
        self.assertEqual(len(bucket), 1)
