"""
Tests that adapters slated for removal in Pika 2.0 emit a DeprecationWarning
on instantiation. See https://github.com/pika/pika/issues/1608.

"""
import sys
import unittest
import warnings
from unittest import mock

from pika.adapters import blocking_connection

try:
    import tornado  # noqa: F401
    HAS_TORNADO = True
except ImportError:
    HAS_TORNADO = False

try:
    import gevent  # noqa: F401
    HAS_GEVENT = True
except ImportError:
    HAS_GEVENT = False

try:
    import twisted  # noqa: F401
    HAS_TWISTED = True
except ImportError:
    HAS_TWISTED = False


class DeprecationTestCase(unittest.TestCase):
    """Base class with a helper to instantiate an adapter and assert that it
    emits a single, well-formed DeprecationWarning.
    """

    def assert_deprecated(self, adapter_name, factory):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            factory()

        deprecations = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        self.assertEqual(
            len(deprecations), 1,
            f'{adapter_name} should emit exactly one DeprecationWarning')

        message = str(deprecations[0].message)
        self.assertIn(adapter_name, message)
        self.assertIn('Pika 2.0', message)
        self.assertIn('ThreadSafeConnection', message)


class BlockingConnectionDeprecationTests(DeprecationTestCase):

    def test_init_emits_deprecation_warning(self):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection'):
            self.assert_deprecated(
                'BlockingConnection',
                lambda: blocking_connection.BlockingConnection('params'))


@unittest.skipUnless(HAS_TORNADO, 'tornado not installed')
class TornadoConnectionDeprecationTests(DeprecationTestCase):

    def test_init_emits_deprecation_warning(self):
        from pika.adapters import tornado_connection
        with mock.patch('pika.adapters.base_connection.BaseConnection.__init__',
                        return_value=None):
            self.assert_deprecated('TornadoConnection',
                                   tornado_connection.TornadoConnection)


@unittest.skipUnless(HAS_GEVENT, 'gevent not installed')
@unittest.skipIf(sys.platform == 'win32',
                 'GeventConnection is not supported on Windows')
class GeventConnectionDeprecationTests(DeprecationTestCase):

    def test_init_emits_deprecation_warning(self):
        from pika.adapters import gevent_connection
        with mock.patch('pika.adapters.base_connection.BaseConnection.__init__',
                        return_value=None):
            self.assert_deprecated('GeventConnection',
                                   gevent_connection.GeventConnection)


@unittest.skipUnless(HAS_TWISTED, 'twisted not installed')
class TwistedProtocolConnectionDeprecationTests(DeprecationTestCase):

    def test_init_emits_deprecation_warning(self):
        from pika.adapters import twisted_connection
        self.assert_deprecated('TwistedProtocolConnection',
                               twisted_connection.TwistedProtocolConnection)


if __name__ == '__main__':
    unittest.main()
