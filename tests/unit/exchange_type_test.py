"""
Tests for pika.exchange_type

"""
import unittest

from pika.exchange_type import ExchangeType


# missing-docstring
# pylint: disable=C0111

# invalid-name - our tests use long, descriptive names
# pylint: disable=C0103


class ExchangeTypeTests(unittest.TestCase):
    def test_exchange_type_direct(self):
        self.assertEqual(ExchangeType.direct.value, 'direct')

    def test_exchange_type_fanout(self):
        self.assertEqual(ExchangeType.fanout.value, 'fanout')
