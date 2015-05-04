"""
Tests for pika.base_connection.BaseConnection

"""
try:
    import mock
except:
    from unittest import mock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika.adapters import base_connection


class BaseConnectionTests(unittest.TestCase):

    def test_should_raise_value_exception_with_no_params_func_instead(self):

        def foo():
            return True

        self.assertRaises(ValueError, base_connection.BaseConnection, foo)
