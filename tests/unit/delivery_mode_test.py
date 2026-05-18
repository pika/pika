"""
Tests for pika.delivery_mode

"""
import unittest

from pika.delivery_mode import DeliveryMode


class DeliveryModeTests(unittest.TestCase):

    def test_delivery_mode_transient(self):
        self.assertEqual(DeliveryMode.Transient.value, 1)

    def test_delivery_mode_persistent(self):
        self.assertEqual(DeliveryMode.Persistent.value, 2)
