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
                                       AsyncServicesTestStubs)


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

        self.assertEqual(bucket, ['I was called'])


class TestCallLaterDoesNotCallAheadOfTime(AsyncServicesTestBase,
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
        self.assertEqual(bucket, ['I was here'])


class TestCallLaterCancelReturnsNone(AsyncServicesTestBase,
                                     AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        self.assertIsNone(loop.call_later(0, lambda: None).cancel())


class TestCallLaterCancelTwiceFromOwnCallback(AsyncServicesTestBase,
                                              AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        bucket = []

        def callback():
            timer.cancel()
            timer.cancel()
            loop.stop()
            bucket.append('I was here')

        timer = loop.call_later(0.1, callback)
        loop.run()
        self.assertEqual(bucket, ['I was here'])


class TestCallLaterCallInOrder(AsyncServicesTestBase,
                               AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        bucket = []

        loop.call_later(0.3, lambda: bucket.append(3) or loop.stop())
        loop.call_later(0, lambda: bucket.append(1))
        loop.call_later(0.15, lambda: bucket.append(2))
        loop.run()
        self.assertEqual(bucket, [1, 2, 3])


class TestCallLaterCancelledDoesNotCallBack(AsyncServicesTestBase,
                                            AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        bucket = []

        timer1 = loop.call_later(0, lambda: bucket.append(1))
        timer1.cancel()
        loop.call_later(0.15, lambda: bucket.append(2) or loop.stop())
        loop.run()
        self.assertEqual(bucket, [2])
