"""
Tests for pika.credentials

"""
import unittest

import mock

from pika import credentials, spec


# pylint: disable=C0111,W0212,C0103

class ChildPlainCredentials(credentials.PlainCredentials):

    def __init__(self, *args, **kwargs):
        super(ChildPlainCredentials, self).__init__(*args, **kwargs)
        self.extra = 'e'

    def __eq__(self, other):
        if isinstance(other, ChildPlainCredentials):
            return self.extra == other.extra and super(
                ChildPlainCredentials, self).__eq__(other)
        return NotImplemented


class ChildExternalCredentials(credentials.ExternalCredentials):

    def __init__(self, *args, **kwargs):
        super(ChildExternalCredentials, self).__init__(*args, **kwargs)
        self.extra = 'e'

    def __eq__(self, other):
        if isinstance(other, ChildExternalCredentials):
            return self.extra == other.extra and super(
                ChildExternalCredentials, self).__eq__(other)
        return NotImplemented


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

        self.assertEqual(
            credentials.PlainCredentials('u', 'p', False),
            ChildPlainCredentials('u', 'p', False))

        self.assertEqual(
            ChildPlainCredentials('u', 'p', False),
            credentials.PlainCredentials('u', 'p', False))

        class Foreign(object):

            def __eq__(self, other):
                return 'foobar'

        self.assertEqual(
            credentials.PlainCredentials('u', 'p', False) == Foreign(),
            'foobar')
        self.assertEqual(
            Foreign() == credentials.PlainCredentials('u', 'p', False),
            'foobar')

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
            credentials.PlainCredentials('uu', 'p', False),
            ChildPlainCredentials('u', 'p', False))

        self.assertNotEqual(
            ChildPlainCredentials('u', 'pp', False),
            credentials.PlainCredentials('u', 'p', False))

        self.assertNotEqual(
            credentials.PlainCredentials('u', 'p', False),
            dict(username='u', password='p', erase_on_connect=False))

        self.assertNotEqual(
            dict(username='u', password='p', erase_on_connect=False),
            credentials.PlainCredentials('u', 'p', False))

        class Foreign(object):

            def __ne__(self, other):
                return 'foobar'

        self.assertEqual(
            credentials.PlainCredentials('u', 'p', False) != Foreign(),
            'foobar')
        self.assertEqual(
            Foreign() != credentials.PlainCredentials('u', 'p', False),
            'foobar')

    def test_response_for(self):
        cred = credentials.PlainCredentials(*self.CREDENTIALS)
        start = spec.Connection.Start()
        self.assertEqual(
            cred.response_for(start), ('PLAIN', b'\x00guest\x00guest'))

    def test_erase_response_for_no_mechanism_match(self):
        cred = credentials.PlainCredentials(*self.CREDENTIALS)
        start = spec.Connection.Start()
        start.mechanisms = 'FOO BAR BAZ'
        self.assertEqual(cred.response_for(start), (None, None))

    def test_erase_credentials_false(self):
        cred = credentials.PlainCredentials(*self.CREDENTIALS)
        cred.erase_credentials()
        self.assertEqual((cred.username, cred.password), self.CREDENTIALS)

    def test_erase_credentials_true(self):
        cred = credentials.PlainCredentials(self.CREDENTIALS[0],
                                            self.CREDENTIALS[1], True)
        cred.erase_credentials()
        self.assertEqual((cred.username, cred.password), (None, None))


class ExternalCredentialsTest(unittest.TestCase):

    def test_eq(self):
        cred_1 = credentials.ExternalCredentials()
        cred_2 = credentials.ExternalCredentials()
        cred_3 = ChildExternalCredentials()

        self.assertEqual(cred_1, cred_2)
        self.assertEqual(cred_2, cred_1)

        cred_1.erase_on_connect = True
        cred_2.erase_on_connect = True
        self.assertEqual(cred_1, cred_2)
        self.assertEqual(cred_2, cred_1)

        cred_1.erase_on_connect = False
        cred_2.erase_on_connect = False
        self.assertEqual(cred_1, cred_2)
        self.assertEqual(cred_2, cred_1)

        cred_1.erase_on_connect = False
        cred_3.erase_on_connect = False
        self.assertEqual(cred_1, cred_3)
        self.assertEqual(cred_3, cred_1)

        class Foreign(object):

            def __eq__(self, other):
                return 'foobar'

        self.assertEqual(
            credentials.ExternalCredentials() == Foreign(),
            'foobar')
        self.assertEqual(
            Foreign() == credentials.ExternalCredentials(),
            'foobar')

    def test_ne(self):
        cred_1 = credentials.ExternalCredentials()
        cred_2 = credentials.ExternalCredentials()
        cred_3 = ChildExternalCredentials()

        cred_1.erase_on_connect = False
        cred_2.erase_on_connect = True
        self.assertNotEqual(cred_1, cred_2)
        self.assertNotEqual(cred_2, cred_1)

        cred_1.erase_on_connect = False
        cred_3.erase_on_connect = True
        self.assertNotEqual(cred_1, cred_3)
        self.assertNotEqual(cred_3, cred_1)

        self.assertNotEqual(cred_1, dict(erase_on_connect=False))
        self.assertNotEqual(dict(erase_on_connect=False), cred_1)

        class Foreign(object):

            def __ne__(self, other):
                return 'foobar'

        self.assertEqual(
            credentials.ExternalCredentials() != Foreign(),
            'foobar')
        self.assertEqual(
            Foreign() != credentials.ExternalCredentials(),
            'foobar')

    def test_response_for(self):
        cred = credentials.ExternalCredentials()
        start = spec.Connection.Start()
        start.mechanisms = 'PLAIN EXTERNAL'
        self.assertEqual(cred.response_for(start), ('EXTERNAL', b''))

    def test_erase_response_for_no_mechanism_match(self):
        cred = credentials.ExternalCredentials()
        start = spec.Connection.Start()
        start.mechanisms = 'FOO BAR BAZ'
        self.assertEqual(cred.response_for(start), (None, None))

    # pylint: disable=R0201
    def test_erase_credentials(self):
        with mock.patch('pika.credentials.LOGGER', autospec=True) as logger:
            cred = credentials.ExternalCredentials()
            cred.erase_credentials()
            logger.debug.assert_called_once_with('Not supported by this '
                                                 'Credentials type')
