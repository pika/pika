"""
Tests for pika.exceptions

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import exceptions


class ExceptionTests(unittest.TestCase):

    def test_amqp_connection_error_one_param_repr(self):
        self.assertEqual(repr(exceptions.AMQPConnectionError(10)),
                         "No connection could be opened after 10 connection attempts")

    def test_amqp_connection_error_two_params_repr(self):
        self.assertEqual(repr(exceptions.AMQPConnectionError(1, 'Test')),
                         "1: Test")

    def test_authentication_error_repr(self):
        self.assertEqual(repr(exceptions.AuthenticationError('PLAIN')),
                         'Server and client could not negotiate use of the '
                         'PLAIN authentication mechanism')

    def test_body_too_long_error_repr(self):
        self.assertEqual(repr(exceptions.BodyTooLongError(100, 50)),
                         'Received too many bytes for a message delivery: '
                         'Received 100, expected 50' )

    def test_invalid_minimum_frame_size_repr(self):
        self.assertEqual(repr(exceptions.InvalidMinimumFrameSize()),
                         'AMQP Minimum Frame Size is 4096 Bytes')

    def test_invalid_maximum_frame_size_repr(self):
        self.assertEqual(repr(exceptions.InvalidMaximumFrameSize()),
                         'AMQP Maximum Frame Size is 131072 Bytes')
