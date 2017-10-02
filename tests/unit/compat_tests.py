try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import compat


class UtilsTests(unittest.TestCase):

    def test_get_linux_version_normal(self):
        self.assertEqual(compat.get_linux_version("4.11.0-2-amd64"), (4, 11, 0))

    def test_get_linux_version_short(self):
        self.assertEqual(compat.get_linux_version("4.11.0"), (4, 11, 0))
