"""
Tests for pika.exceptions

"""
import unittest

from pika import exceptions


# missing-docstring
# pylint: disable=C0111

# invalid-name - our tests use long, descriptive names
# pylint: disable=C0103


class ExceptionTests(unittest.TestCase):
    def test_amqp_connection_error_one_param_repr(self):
        self.assertEqual(
            repr(exceptions.AMQPConnectionError(10)),
            'AMQPConnectionError: (10,)')

    def test_amqp_connection_error_two_params_repr(self):
        self.assertEqual(
            repr(exceptions.AMQPConnectionError(1, 'Test')),
            'AMQPConnectionError: (1) Test')

    def test_authentication_error_repr(self):
        self.assertEqual(
            repr(exceptions.AuthenticationError('PLAIN')),
            'AuthenticationError: Server and client could not negotiate use of '
            'the PLAIN authentication mechanism')

    def test_body_too_long_error_repr(self):
        self.assertEqual(
            repr(exceptions.BodyTooLongError(100, 50)),
            'BodyTooLongError: Received too many bytes for a message delivery: '
            'Received 100, expected 50')

    def test_channel_closed_properties_positional_args(self):
        exc = exceptions.ChannelClosed(9, 'args abcd')
        self.assertEqual(exc.reply_code, 9)
        self.assertEqual(exc.reply_text, 'args abcd')

    def test_channel_closed_properties_kwargs(self):
        exc = exceptions.ChannelClosed(reply_code=9, reply_text='kwargs abcd')
        self.assertEqual(exc.reply_code, 9)
        self.assertEqual(exc.reply_text, 'kwargs abcd')

    def test_channel_closed_repr(self):
        exc = exceptions.ChannelClosed(200, 'all is swell')

        self.assertEqual(repr(exc), "ChannelClosed: (200) 'all is swell'")
