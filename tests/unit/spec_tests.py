"""Tests for pika.spec."""

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

    def test_headers_repr(self):
        hdr = 'timestamp_in_ms'
        v = 912598613
        h = {hdr: v}
        p = spec.BasicProperties(content_type='text/plain', headers=h)
        self.assertEqual(repr(p.headers[hdr]), '912598613')
