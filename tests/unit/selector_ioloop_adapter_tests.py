"""Tests for pika.adapters.utils.selector_ioloop_adapter.AbstractSelectorIOLoop."""

import inspect
import sys
import unittest

import pika._utils
from pika.adapters import select_connection
from pika.adapters.utils.selector_ioloop_adapter import AbstractSelectorIOLoop

try:
    import tornado.ioloop
    HAS_TORNADO = True
except ImportError:
    HAS_TORNADO = False

try:
    from pika.adapters.gevent_connection import _GeventSelectorIOLoop
    HAS_GEVENT = True
except ImportError:
    _GeventSelectorIOLoop = None
    HAS_GEVENT = False

FLAG_NAMES = ('READ', 'WRITE', 'ERROR')

# Derived from the protocol itself rather than hard-coded, so a method added to
# the interface is immediately required of every loop checked below.
PROTOCOL_METHOD_NAMES = tuple(
    sorted(name for name, value in vars(AbstractSelectorIOLoop).items()
           if not name.startswith('_') and callable(value)))


class _StructuralLoop:
    """Implements the whole interface without deriving from it, as tornado's `IOLoop` does."""

    READ = 1
    WRITE = 2
    ERROR = 4

    def close(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def call_later(self, delay, callback):
        pass

    def remove_timeout(self, timeout_handle):
        pass

    def add_callback(self, callback):
        pass

    def add_handler(self, fd, handler, events):
        pass

    def update_handler(self, fd, events):
        pass

    def remove_handler(self, fd):
        pass


class _IncompleteLoop:
    """Carries the flags but none of the methods."""

    READ = 1
    WRITE = 2
    ERROR = 4


class _ConformanceChecks:
    """
    Checks that a loop class satisfies `AbstractSelectorIOLoop`.

    Mixed into a `unittest.TestCase` per loop class. Everything is checked against the class rather
    than an instance so that no loop, hub, or file descriptor is created.
    """

    loop_class = None

    def test_flags_are_int_attributes(self):
        # The protocol declares READ/WRITE/ERROR as variable members, which a
        # read-only property does not satisfy. `getattr_static` is what
        # distinguishes the two: for a property it returns the property object
        # rather than the value.
        for name in FLAG_NAMES:
            value = inspect.getattr_static(self.loop_class, name)
            self.assertIsInstance(value, int, f'{name} is not a plain int')

    def test_implements_every_protocol_method(self):
        self.assertTrue(PROTOCOL_METHOD_NAMES)
        for name in PROTOCOL_METHOD_NAMES:
            self.assertTrue(callable(getattr(self.loop_class, name, None)),
                            f'{name} is missing or not callable')


class SelectConnectionIOLoopConformanceTests(_ConformanceChecks,
                                             unittest.TestCase):
    loop_class = select_connection.IOLoop


@unittest.skipUnless(HAS_GEVENT, 'gevent not installed')
@unittest.skipIf(pika._utils.ON_WINDOWS, 'Windows not supported')
class GeventSelectorIOLoopConformanceTests(_ConformanceChecks,
                                           unittest.TestCase):
    loop_class = _GeventSelectorIOLoop


@unittest.skipUnless(HAS_TORNADO, 'tornado not installed')
class TornadoIOLoopConformanceTests(_ConformanceChecks, unittest.TestCase):
    """
    Tornado's `IOLoop` is the reason the interface is structural.

    It implements every member without deriving from the protocol, which is what allowed the two
    `[arg-type]` suppressions in `tornado_connection.py` to be removed.
    """

    loop_class = tornado.ioloop.IOLoop if HAS_TORNADO else None

    def test_does_not_derive_from_the_protocol(self):
        self.assertNotIn(AbstractSelectorIOLoop, self.loop_class.__mro__)


class AbstractSelectorIOLoopTests(unittest.TestCase):

    def test_flags_are_declared_as_variable_members(self):
        # Declaring the flags as properties instead would let a loop expose them
        # as read-only properties, but pyright then rejects the plain `int` class
        # attributes both in-tree loops use. A property declaration puts a
        # property object in the class dict; an annotation does not.
        self.assertEqual(set(AbstractSelectorIOLoop.__annotations__),
                         set(FLAG_NAMES))
        for name in FLAG_NAMES:
            self.assertNotIn(name, vars(AbstractSelectorIOLoop))

    def test_carries_no_abstract_methods(self):
        # `typing._ProtocolMeta` derives from `abc.ABCMeta`, so an
        # `@abc.abstractmethod` here would block construction of an incomplete
        # subclass on 3.8 and later while doing nothing at all on 3.7, where the
        # protocol falls back to `object`.
        self.assertEqual(
            getattr(AbstractSelectorIOLoop, '__abstractmethods__', frozenset()),
            frozenset())

    @unittest.skipIf(sys.version_info < (3, 8),
                     '`Protocol` falls back to a nominal base class on 3.7')
    def test_isinstance_accepts_a_loop_that_does_not_derive_from_it(self):
        loop = _StructuralLoop()
        self.assertNotIn(AbstractSelectorIOLoop, type(loop).__mro__)
        self.assertIsInstance(loop, AbstractSelectorIOLoop)

    @unittest.skipIf(sys.version_info < (3, 8),
                     '`Protocol` falls back to a nominal base class on 3.7')
    def test_isinstance_rejects_an_incomplete_loop(self):
        self.assertNotIsInstance(_IncompleteLoop(), AbstractSelectorIOLoop)
