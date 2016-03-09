"""
Tests for pika.credentials

"""
try:
    import mock
except ImportError:
    from unittest import mock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import credentials
from pika import spec


class PlainCredentialsTests(unittest.TestCase):

    CREDENTIALS = 'guest', 'guest'

    def test_eq(self):
        self.assertEqual(
            credentials.PlainCredentials('u', 'p'),
            credentials.PlainCredentials('u', 'p'))

        self.assertEqual(
            credentials.PlainCredentials('u', 'p', True),
            credentials.PlainCredentials('u', 'p', True))

        self.assertEqual(
            credentials.PlainCredentials('u', 'p', False),
            credentials.PlainCredentials('u', 'p', False))

    def test_ne(self):
        self.assertNotEqual(
            credentials.PlainCredentials('uu', 'p', False),
            credentials.PlainCredentials('u', 'p', False))

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'p', False),
            credentials.PlainCredentials('uu', 'p', False))

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'pp', False),
            credentials.PlainCredentials('u', 'p', False))

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'p', False),
            credentials.PlainCredentials('u', 'pp', False))

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'p', True),
            credentials.PlainCredentials('u', 'p', False))

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'p', False),
            credentials.PlainCredentials('u', 'p', True))

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'p', False),
            dict(username='u', password='p', erase_on_connect=False))

        self.assertNotEqual(
            dict(username='u', password='p', erase_on_connect=False),
            credentials.PlainCredentials('u', 'p', False))

        class ImprovedPlainCredentials(credentials.PlainCredentials):
            def __init__(self, *args, **kwargs):
                super(ImprovedPlainCredentials, self).__init__(*args, **kwargs)
                self.extra = 'e'

            def __eq__(self, other):
                return (isinstance(other, ImprovedPlainCredentials) and
                        self.extra == other.extra and
                        super(ImprovedPlainCredentials, self).__eq__(other))

            def __ne__(self, other):
                return not self == other

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'p'),
            ImprovedPlainCredentials('u', 'p'))

        self.assertNotEqual(
            ImprovedPlainCredentials('u', 'p'),
            credentials.PlainCredentials('u', 'p'))

    def test_response_for(self):
        obj = credentials.PlainCredentials(*self.CREDENTIALS)
        start = spec.Connection.Start()
        self.assertEqual(obj.response_for(start),
                         ('PLAIN', b'\x00guest\x00guest'))

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
                                           self.CREDENTIALS[1], True)
        obj.erase_credentials()
        self.assertEqual((obj.username, obj.password), (None, None))


class ExternalCredentialsTest(unittest.TestCase):

    def test_eq(self):
        self.assertEqual(
            credentials.ExternalCredentials(),
            credentials.ExternalCredentials())

    def test_ne(self):
        cr1 = credentials.ExternalCredentials()
        cr2 = credentials.ExternalCredentials()
        cr2.erase_on_connect = not cr2.erase_on_connect

        self.assertNotEqual(cr1, cr2)
        self.assertNotEqual(cr2, cr1)

        cred = credentials.ExternalCredentials()
        self.assertNotEqual(
            cred,
            dict(erase_on_connect=cred.erase_on_connect))

        self.assertNotEqual(
            dict(erase_on_connect=cred.erase_on_connect),
            cred)

        class ImprovedExternalCredentials(credentials.ExternalCredentials):
            def __init__(self, *args, **kwargs):
                super(ImprovedExternalCredentials, self).__init__(*args, **kwargs)
                self.extra = 'e'

            def __eq__(self, other):
                return (isinstance(other, ImprovedExternalCredentials) and
                        self.extra == other.extra and
                        super(ImprovedExternalCredentials, self).__eq__(other))

            def __ne__(self, other):
                return not self == other

        self.assertNotEqual(
            credentials.ExternalCredentials(),
            ImprovedExternalCredentials())

        self.assertNotEqual(
            ImprovedExternalCredentials(),
            credentials.ExternalCredentials())

    def test_response_for(self):
        obj = credentials.ExternalCredentials()
        start = spec.Connection.Start()
        start.mechanisms = 'PLAIN EXTERNAL'
        self.assertEqual(obj.response_for(start), ('EXTERNAL', b''))

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
