import unittest

import pika._utils


class UtilsTests(unittest.TestCase):

    def test_get_linux_version_normal(self):
        self.assertEqual(pika._utils.get_linux_version("4.11.0-2-amd64"),
                         (4, 11, 0))

    def test_get_linux_version_short(self):
        self.assertEqual(pika._utils.get_linux_version("4.11.0"), (4, 11, 0))

    def test_get_linux_version_gcp(self):
        self.assertEqual(pika._utils.get_linux_version("4.4.64+"), (4, 4, 64))

    def test_to_digit(self):
        self.assertEqual(pika._utils.to_digit("64"), 64)

    def test_to_digit_with_plus_sign(self):
        self.assertEqual(pika._utils.to_digit("64+"), 64)

    def test_to_digit_with_dot(self):
        self.assertEqual(pika._utils.to_digit("64."), 64)
