# -*- coding: utf8 -*-
"""
Tests for pika.spec

"""
import unittest

from pika import spec


class BasicPropertiesTests(unittest.TestCase):
    def test_equality(self):
        a = spec.BasicProperties(content_type='text/plain')
        self.assertEqual(a, a)
        self.assertNotEqual(a, None)

        b = spec.BasicProperties()
        self.assertNotEqual(a, b)
        b.content_type = 'text/plain'
        self.assertEqual(a, b)

        a.correlation_id = 'abc123'
        self.assertNotEqual(a, b)

        b.correlation_id = 'abc123'
        self.assertEqual(a, b)
