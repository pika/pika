"""
Tests for pika.callback

"""
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import amqp_object


class AMQPObjectTests(unittest.TestCase):

    def test_base_name(self):
        self.assertEqual(amqp_object.AMQPObject().NAME, 'AMQPObject')

    def test_repr_no_items(self):
        obj = amqp_object.AMQPObject()
        self.assertEqual(repr(obj), '<AMQPObject>')

    def test_repr_items(self):
        obj = amqp_object.AMQPObject()
        setattr(obj, 'foo', 'bar')
        setattr(obj, 'baz', 'qux')
        self.assertEqual(repr(obj), "<AMQPObject(['foo=bar', 'baz=qux'])>")


class ClassTests(unittest.TestCase):

    def test_base_name(self):
        self.assertEqual(amqp_object.Class().NAME, 'Unextended Class')

class MethodTests(unittest.TestCase):

    def test_base_name(self):
        self.assertEqual(amqp_object.Method().NAME, 'Unextended Method')

    def test_set_content_body(self):
        properties = amqp_object.Properties()
        body = 'This is a test'
        obj = amqp_object.Method()
        obj._set_content(properties, body)
        self.assertEqual(obj._body, body)

    def test_set_content_properties(self):
        properties = amqp_object.Properties()
        body = 'This is a test'
        obj = amqp_object.Method()
        obj._set_content(properties, body)
        self.assertEqual(obj._properties, properties)

    def test_get_body(self):
        properties = amqp_object.Properties()
        body = 'This is a test'
        obj = amqp_object.Method()
        obj._set_content(properties, body)
        self.assertEqual(obj.get_body(), body)

    def test_get_properties(self):
        properties = amqp_object.Properties()
        body = 'This is a test'
        obj = amqp_object.Method()
        obj._set_content(properties, body)
        self.assertEqual(obj.get_properties(), properties)

class PropertiesTests(unittest.TestCase):

    def test_base_name(self):
        self.assertEqual(amqp_object.Properties().NAME, 'Unextended Properties')
