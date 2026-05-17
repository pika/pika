"""
Test for io_services_test_stubs.py
"""
from __future__ import annotations

import pika._utils

try:
    import asyncio
except ImportError:
    asyncio = None

import threading
import unittest
from typing import ClassVar

import tornado.ioloop

from pika.adapters import select_connection
from tests.stubs.io_services_test_stubs import IOServicesTestStubs

if asyncio is not None:
    if pika._utils.ON_WINDOWS:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
else:
    loop = None

# Tornado does some magic that substitutes the class dynamically
_TORNADO_IO_LOOP = tornado.ioloop.IOLoop()
_TORNADO_IOLOOP_CLASS = _TORNADO_IO_LOOP.__class__
_TORNADO_IO_LOOP.close()
del _TORNADO_IO_LOOP

_SUPPORTED_LOOP_CLASSES = {
    select_connection.IOLoop,
    _TORNADO_IOLOOP_CLASS,
}

if asyncio is not None:
    assert loop is not None
    _SUPPORTED_LOOP_CLASSES.add(loop.__class__)
    loop.close()


class TestStartCalledFromOtherThreadAndWithVaryingNativeLoops(
        unittest.TestCase, IOServicesTestStubs):

    _native_loop_classes: ClassVar[set[type]] = set()

    @classmethod
    def setUpClass(cls):
        cls._native_loop_classes = set()

    # AssertionError: Expected these 3 native I/O loop classes from IOServicesTestStubs:
    # {<class 'asyncio.windows_events.ProactorEventLoop'>, <class 'tornado.platform.asyncio.AsyncIOLoop'>, <class 'pika.adapters.select_connection.IOLoop'>}
    # but got these 3:
    # {<class 'asyncio.windows_events._WindowsSelectorEventLoop'>, <class 'tornado.platform.asyncio.AsyncIOLoop'>, <class 'pika.adapters.select_connection.IOLoop'>}
    @classmethod
    def tearDownClass(cls):
        # Now check against what was made available to us by
        # IOServicesTestStubs
        if cls._native_loop_classes != _SUPPORTED_LOOP_CLASSES:
            raise AssertionError(
                f'Expected these {len(_SUPPORTED_LOOP_CLASSES)} native I/O loop classes from '
                f'IOServicesTestStubs: {_SUPPORTED_LOOP_CLASSES!r}, but got these {len(cls._native_loop_classes)}: {cls._native_loop_classes!r}'
            )

    def setUp(self):
        self._runner_thread_id = threading.current_thread().ident

    def start(self):
        nbio = self.create_nbio()
        native_loop = nbio.get_native_ioloop()
        self.assertIsNotNone(self._native_loop)
        self.assertIs(native_loop, self._native_loop)

        self._native_loop_classes.add(native_loop.__class__)

        # Check that we're called from a different thread than the one that
        # set up this test.
        self.assertNotEqual(threading.current_thread().ident,
                            self._runner_thread_id)

        # And make sure the loop actually works using this rudimentary test
        nbio.add_callback_threadsafe(nbio.stop)
        nbio.run()
