"""
Tests for pika.credentials

"""
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import credentials
from pika import spec

class PlainCredentialsTests(unittest.TestCase):

    CREDENTIALS = 'guest', 'guest'
    def test_response_for(self):
        obj = credentials.PlainCredentials(*self.CREDENTIALS)
        start = spec.Connection.Start()
        self.assertEqual(obj.response_for(start),
                         ('PLAIN', '\x00guest\x00guest'))

    def test_erase_response_for_no_mechanism_match(self):
        obj = credentials.PlainCredentials(*self.CREDENTIALS)
        start = spec.Connection.Start()
        start.mechanisms = 'FOO BAR BAZ'
        self.assertEqual(obj.response_for(start), (None, None))

    def test_erase_credentials_false(self):
        obj = credentials.PlainCredentials(*self.CREDENTIALS)
        obj.erase_credentials()
        self.assertEqual((obj.username, obj.password), self.CREDENTIALS)

    def test_erase_credentials_true(self):
        obj = credentials.PlainCredentials(self.CREDENTIALS[0],
                                           self.CREDENTIALS[1],
                                           True)
        obj.erase_credentials()
        self.assertEqual((obj.username, obj.password), (None, None))


class ExternalCredentialsTest(unittest.TestCase):

    def test_response_for(self):
        obj = credentials.ExternalCredentials()
        start = spec.Connection.Start()
        start.mechanisms = 'PLAIN EXTERNAL'
        self.assertEqual(obj.response_for(start), ('EXTERNAL', ''))

    def test_erase_response_for_no_mechanism_match(self):
        obj = credentials.ExternalCredentials()
        start = spec.Connection.Start()
        start.mechanisms = 'FOO BAR BAZ'
        self.assertEqual(obj.response_for(start), (None, None))

    def test_erase_credentials(self):
        with mock.patch('pika.credentials.LOGGER', autospec=True) as logger:
            obj = credentials.ExternalCredentials()
            obj.erase_credentials()
            logger.debug.assert_called_once_with('Not supported by this '
                                                 'Credentials type')
