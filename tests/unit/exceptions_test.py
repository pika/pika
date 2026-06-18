"""Tests for pika.exceptions."""

import unittest
from unittest.mock import MagicMock

from pika import exceptions


class ExceptionTests(unittest.TestCase):

    def test_amqp_connection_error_one_param_repr(self):
        self.assertEqual(repr(exceptions.AMQPConnectionError(10)),
                         'AMQPConnectionError: (10,)')

    def test_amqp_connection_error_two_params_repr(self):
        self.assertEqual(repr(exceptions.AMQPConnectionError(1, 'Test')),
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

    def test_amqp_error_repr(self):
        self.assertEqual(
            repr(exceptions.AMQPError('something went wrong')),
            "AMQPError: An unspecified AMQP error has occurred; ('something went wrong',)"
        )

    def test_incompatible_protocol_error_repr(self):
        self.assertEqual(
            repr(exceptions.IncompatibleProtocolError('bad')),
            "IncompatibleProtocolError: The protocol returned by the server is not supported: ('bad',)"
        )

    def test_probable_authentication_error_repr(self):
        self.assertEqual(
            repr(exceptions.ProbableAuthenticationError('x')),
            "ProbableAuthenticationError: Client was disconnected at a connection stage indicating a "
            "probable authentication error: ('x',)")

    def test_probable_access_denied_error_repr(self):
        self.assertEqual(
            repr(exceptions.ProbableAccessDeniedError('x')),
            "ProbableAccessDeniedError: Client was disconnected at a connection stage indicating a "
            "probable denial of access to the specified virtual host: ('x',)")

    def test_no_free_channels_repr(self):
        self.assertEqual(
            repr(exceptions.NoFreeChannels()),
            'NoFreeChannels: The connection has run out of free channels')

    def test_connection_wrong_state_error_repr_no_args(self):
        self.assertEqual(
            repr(exceptions.ConnectionWrongStateError()),
            'ConnectionWrongStateError: The connection is in wrong state for the requested operation.'
        )

    def test_connection_wrong_state_error_repr_with_args(self):
        exc = exceptions.ConnectionWrongStateError('details')
        self.assertEqual(repr(exc), "ConnectionWrongStateError: ('details',)")

    def test_amqp_channel_error_repr(self):
        self.assertEqual(repr(exceptions.AMQPChannelError('oops')),
                         "AMQPChannelError: ('oops',)")

    def test_duplicate_consumer_tag_repr(self):
        self.assertEqual(
            repr(exceptions.DuplicateConsumerTag('my-tag')),
            'DuplicateConsumerTag: The consumer tag specified already exists for this channel: my-tag'
        )

    def test_consumer_cancelled_repr(self):
        self.assertEqual(repr(exceptions.ConsumerCancelled()),
                         'ConsumerCancelled: Server cancelled consumer')

    def test_unroutable_error_repr(self):
        msgs = [MagicMock(), MagicMock()]
        exc = exceptions.UnroutableError(msgs)
        self.assertEqual(
            repr(exc),
            'UnroutableError: 2 unroutable messages returned by broker')

    def test_unroutable_error_messages(self):
        msgs = [MagicMock(), MagicMock()]
        exc = exceptions.UnroutableError(msgs)
        self.assertEqual(exc.messages, msgs)

    def test_nack_error_repr(self):
        msgs = [MagicMock(), MagicMock(), MagicMock()]
        exc = exceptions.NackError(msgs)
        self.assertEqual(repr(exc),
                         'NackError: 3 unroutable messages returned by broker')

    def test_nack_error_messages(self):
        msgs = [MagicMock(), MagicMock()]
        exc = exceptions.NackError(msgs)
        self.assertEqual(exc.messages, msgs)

    def test_invalid_channel_number_repr(self):
        self.assertEqual(
            repr(exceptions.InvalidChannelNumber(99)),
            'InvalidChannelNumber: An invalid channel number has been specified: 99'
        )

    def test_protocol_syntax_error_repr(self):
        self.assertEqual(
            repr(exceptions.ProtocolSyntaxError()),
            'ProtocolSyntaxError: An unspecified protocol syntax error occurred'
        )

    def test_unexpected_frame_error_repr(self):
        self.assertEqual(
            repr(exceptions.UnexpectedFrameError('bad-frame')),
            "UnexpectedFrameError: Received a frame out of sequence: 'bad-frame'"
        )

    def test_protocol_version_mismatch_repr(self):
        self.assertEqual(
            repr(exceptions.ProtocolVersionMismatch('0-9-1', '0-10')),
            "ProtocolVersionMismatch: Protocol versions did not match: '0-9-1' vs '0-10'"
        )

    def test_invalid_frame_error_repr(self):
        self.assertEqual(repr(exceptions.InvalidFrameError('frame')),
                         "InvalidFrameError: Invalid frame received: 'frame'")

    def test_invalid_field_type_exception_repr(self):
        self.assertEqual(repr(exceptions.InvalidFieldTypeException('z')),
                         'InvalidFieldTypeException: Unsupported field kind z')

    def test_unsupported_amqp_field_exception_repr(self):
        self.assertEqual(
            repr(exceptions.UnsupportedAMQPFieldException('kind', 42)),
            'UnsupportedAMQPFieldException: Unsupported field kind <class \'int\'>'
        )

    def test_channel_error_repr(self):
        self.assertEqual(
            repr(exceptions.ChannelError()),
            'ChannelError: An unspecified error occurred with the Channel')

    def test_short_string_too_long_repr(self):
        self.assertEqual(
            repr(exceptions.ShortStringTooLong('toolong')),
            'ShortStringTooLong: AMQP Short String can contain up to 255 bytes: toolong'
        )

    def test_duplicate_get_ok_callback_repr(self):
        self.assertEqual(
            repr(exceptions.DuplicateGetOkCallback()),
            'DuplicateGetOkCallback: basic_get can only be called again after the callback for '
            'the previous basic_get is executed')
