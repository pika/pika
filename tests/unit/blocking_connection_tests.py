"""Tests for pika.adapters.blocking_connection.BlockingConnection."""

import sys
import unittest
from unittest import mock
from unittest.mock import patch

import pika
import pika.channel
import pika.exceptions
from pika.adapters import blocking_connection
from pika.adapters.utils import nbio_interface


class BlockingConnectionMockTemplate(blocking_connection.BlockingConnection):
    pass


class SelectConnectionTemplate(
        blocking_connection.select_connection.SelectConnection):
    # is_closed/is_closing/is_open/ioloop (properties) and
    # _get_write_buffer_size (method) are inherited and already part of the
    # mock spec. Only the instance attributes below need declaring at class
    # level so spec_set exposes them.
    _channels = {}  # noqa: RUF012 -- throwaway mock-spec template, not real state
    _transport = None


class BlockingConnectionTests(unittest.TestCase):
    """Tests for BlockingConnection."""

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_constructor(self, _select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection') as _create_connection_mock:
            connection = blocking_connection.BlockingConnection('params')

        _create_connection_mock.assert_called_once_with('params', None)

        impl_mock = _create_connection_mock.return_value
        impl_mock.add_on_close_callback.assert_called_once_with(
            connection._closed_result.set_value_once)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup(self,
                                             select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection'):
            connection = blocking_connection.BlockingConnection('params')

        mock_connection = select_connection_class_mock.return_value
        with mock.patch.object(
                select_connection_class_mock,
                'create_connection',
                side_effect=lambda configs, on_done, custom_ioloop:
            [on_done(mock_connection),
             custom_ioloop.close(), None][2]):
            result = connection._create_connection(
                None, select_connection_class_mock)

            self.assertIs(result, mock_connection)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup_fails_with_open_error(
            self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection'):
            connection = blocking_connection.BlockingConnection('params')

        exc_value = pika.exceptions.AMQPConnectionError('failed')
        with mock.patch.object(select_connection_class_mock,
                               'create_connection',
                               side_effect=lambda configs, on_done,
                               custom_ioloop: on_done(exc_value)):
            with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
                connection._create_connection(None,
                                              select_connection_class_mock)

            self.assertIs(cm.exception, exc_value)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output(self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        get_buffer_size_mock = mock.Mock(
            name='_get_write_buffer_size',
            side_effect=[100, 50, 0],
            spec=nbio_interface.AbstractStreamTransport.get_write_buffer_size)

        transport_mock = mock.NonCallableMock(
            spec_set=nbio_interface.AbstractStreamTransport)

        connection._impl._transport = transport_mock
        connection._impl._get_write_buffer_size = get_buffer_size_mock

        connection._flush_output(lambda: False, lambda: True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output_user_initiated_close(self,
                                               select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        original_exc = pika.exceptions.ConnectionClosedByClient(200, 'success')
        connection._closed_result.set_value_once(impl_mock, original_exc)

        connection._flush_output(lambda: False, lambda: True)

        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output_server_initiated_error_close(
            self, select_connection_class_mock):

        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        original_exc = pika.exceptions.ConnectionClosedByBroker(
            404, 'not found')
        connection._closed_result.set_value_once(impl_mock, original_exc)

        with self.assertRaises(pika.exceptions.ConnectionClosedByBroker) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (404, 'not found'))

        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output_server_initiated_no_error_close(
            self, select_connection_class_mock):

        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        original_exc = pika.exceptions.ConnectionClosedByBroker(200, 'ok')
        connection._closed_result.set_value_once(impl_mock, original_exc)

        impl_mock.is_closed = False
        with self.assertRaises(pika.exceptions.ConnectionClosed) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (200, 'ok'))

        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_close(self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        impl_channel_mock = mock.Mock()
        connection._impl._channels = {1: impl_channel_mock}

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

        impl_channel_mock._get_cookie.return_value.close.assert_called_once_with(
            200, 'text')
        select_connection_class_mock.return_value.close.assert_called_once_with(
            200, 'text')

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_close_with_channel_closed_exception(self,
                                                 select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        channel1_mock = mock.Mock(is_open=True,
                                  close=mock.Mock(
                                      side_effect=pika.exceptions.ChannelClosed(
                                          -1, 'Just because'),
                                      spec_set=pika.channel.Channel.close),
                                  spec_set=blocking_connection.BlockingChannel)

        channel2_mock = mock.Mock(is_open=True,
                                  spec_set=blocking_connection.BlockingChannel)

        connection._impl._channels = {
            1:
                mock.Mock(_get_cookie=mock.Mock(
                    return_value=channel1_mock,
                    spec_set=pika.channel.Channel._get_cookie),
                          spec_set=pika.channel.Channel),
            2:
                mock.Mock(_get_cookie=mock.Mock(
                    return_value=channel2_mock,
                    spec_set=pika.channel.Channel._get_cookie),
                          spec_set=pika.channel.Channel)
        }

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

            channel1_mock.close.assert_called_once_with(200, 'text')
            channel2_mock.close.assert_called_once_with(200, 'text')
        impl_mock.close.assert_called_once_with(200, 'text')

    @unittest.skipIf(sys.version_info < (3, 8),
                     "mock args differ on 3.7 and earlier")
    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_update_secret(self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False
        impl_mock.is_open = True
        impl_mock._transport = None

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(blocking_connection._CallbackResult,
                               'is_ready',
                               return_value=True):
            connection.update_secret("new_secret", "reason")

        if sys.version_info < (3, 8):
            args_0 = select_connection_class_mock.return_value.update_secret.call_args[
                0]
            args_1 = select_connection_class_mock.return_value.update_secret.call_args[
                1]
            args_len = len(select_connection_class_mock.return_value.
                           update_secret.call_args)
        else:
            args_0 = select_connection_class_mock.return_value.update_secret.call_args.args[
                0]
            args_1 = select_connection_class_mock.return_value.update_secret.call_args.args[
                1]
            args_len = len(select_connection_class_mock.return_value.
                           update_secret.call_args.args)

        self.assertEqual(args_0, "new_secret")
        self.assertEqual(args_1, "reason")
        self.assertEqual(args_len, 3)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    @patch.object(blocking_connection,
                  'BlockingChannel',
                  spec_set=blocking_connection.BlockingChannel)
    def test_channel(self, blocking_channel_class_mock,
                     select_connection_class_mock):

        impl_mock = select_connection_class_mock.return_value

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection.channel()

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_sleep(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection'):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection.sleep(0.00001)

    def test_connection_blocked_evt(self):
        blocked_buffer = []
        frame = pika.frame.Method(0, pika.spec.Connection.Blocked('reason'))
        evt = blocking_connection._ConnectionBlockedEvt(blocked_buffer.append,
                                                        frame)
        repr(evt)
        evt.dispatch()
        self.assertEqual(len(blocked_buffer), 1)
        self.assertIs(blocked_buffer[0], frame)

    def test_connection_unblocked_evt(self):
        unblocked_buffer = []
        frame = pika.frame.Method(0, pika.spec.Connection.Unblocked())
        evt = blocking_connection._ConnectionUnblockedEvt(
            unblocked_buffer.append, frame)
        repr(evt)
        evt.dispatch()
        self.assertEqual(len(unblocked_buffer), 1)
        self.assertIs(unblocked_buffer[0], frame)

    @patch.object(blocking_connection.select_connection,
                  "SelectConnection",
                  spec_set=SelectConnectionTemplate)
    def test_process_data_events_raises_on_channel_closed_by_broker(
            self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               "_create_connection",
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection("params")

        exc = pika.exceptions.ChannelClosedByBroker(406, "consumer_timeout")

        impl_channel_mock = mock.Mock()
        impl_channel_mock.channel_number = 1
        channel = blocking_connection.BlockingChannel(impl_channel_mock,
                                                      connection)

        with mock.patch.object(channel, "_cleanup"):
            channel._on_channel_closed(impl_channel_mock, exc)

        with mock.patch.object(connection, "_flush_output"):
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker) as cm:
                connection.process_data_events(time_limit=0)

            self.assertEqual(cm.exception.reply_code, 406)
            self.assertEqual(cm.exception.reply_text, "consumer_timeout")

    @patch.object(blocking_connection.select_connection,
                  "SelectConnection",
                  spec_set=SelectConnectionTemplate)
    def test_process_data_events_raises_after_full_channel_cleanup(
            self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False
        impl_mock._channels = {}

        with mock.patch.object(blocking_connection.BlockingConnection,
                               "_create_connection",
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection("params")

        exc = pika.exceptions.ChannelClosedByBroker(406, "consumer_timeout")

        impl_channel_mock = mock.Mock()
        impl_channel_mock.channel_number = 1
        channel = blocking_connection.BlockingChannel(impl_channel_mock,
                                                      connection)

        with mock.patch.object(channel, "_cleanup"):
            channel._on_channel_closed(impl_channel_mock, exc)

        impl_mock._channels.pop(1, None)
        impl_channel_mock._cookie = None

        with mock.patch.object(connection, "_flush_output"):
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker) as cm:
                connection.process_data_events(time_limit=0)

            self.assertEqual(cm.exception.reply_code, 406)
            self.assertEqual(cm.exception.reply_text, "consumer_timeout")

    @staticmethod
    def _make_open_connection(select_connection_class_mock):
        """Create a BlockingConnection and return it with its impl mock.

        Set attributes on the returned mock (not connection._impl) so pyright
        sees a MagicMock rather than the real SelectConnection properties.
        """
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            return blocking_connection.BlockingConnection('params'), impl_mock

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_is_closed_delegates_to_impl(self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.is_closed = True
        self.assertIs(connection.is_closed, True)
        impl_mock.is_closed = False
        self.assertIs(connection.is_closed, False)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_is_open_delegates_to_impl(self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.is_open = True
        self.assertIs(connection.is_open, True)
        impl_mock.is_open = False
        self.assertIs(connection.is_open, False)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_basic_nack_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.basic_nack = True
        self.assertIs(connection.basic_nack_supported, True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_consumer_cancel_notify_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.consumer_cancel_notify = True
        self.assertIs(connection.consumer_cancel_notify_supported, True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_exchange_exchange_bindings_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.exchange_exchange_bindings = True
        self.assertIs(connection.exchange_exchange_bindings_supported, True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_publisher_confirms_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.publisher_confirms = True
        self.assertIs(connection.publisher_confirms_supported, True)
