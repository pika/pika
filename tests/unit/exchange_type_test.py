"""Tests for pika.exchange_type."""

import unittest

from pika.exchange_type import ExchangeType


class ExchangeTypeTests(unittest.TestCase):

    def test_exchange_type_direct(self):
        self.assertEqual(ExchangeType.direct.value, 'direct')

    def test_exchange_type_fanout(self):
        self.assertEqual(ExchangeType.fanout.value, 'fanout')

    def test_exchange_type_headers(self):
        self.assertEqual(ExchangeType.headers.value, 'headers')

    def test_exchange_type_topic(self):
        self.assertEqual(ExchangeType.topic.value, 'topic')
