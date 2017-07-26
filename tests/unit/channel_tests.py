"""
Tests for pika.channel.Channel

"""

# Disable pylint warning about Access to a protected member
# pylint: disable=W0212

# Disable pylint messages concerning missing docstrings and invalid method names
# pylint: disable=C0111,C0103

import collections
import logging
import sys
import warnings

try:
    import mock
except ImportError:
    from unittest import mock  # pylint: disable=E0611


try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import channel
from pika import connection
from pika import exceptions
from pika import frame
from pika import spec


class ConnectionTemplate(connection.Connection):
    """Template for using as mock spec_set for the pika Connection class. It
    defines members accessed by the code under test that would be defined in the
    base class's constructor.
    """
    callbacks = None

    # Suppress pylint warnings about specific abstract methods not being
    # overridden
    _adapter_connect = connection.Connection._adapter_connect
    _adapter_disconnect = connection.Connection._adapter_disconnect
    _flush_outbound = connection.Connection._flush_outbound
    add_timeout = connection.Connection.add_timeout
    remove_timeout = connection.Connection.remove_timeout


class ChannelTests(unittest.TestCase):

    @staticmethod
    @mock.patch('pika.connection.Connection', autospec=ConnectionTemplate)
    def _create_connection(connection_class_mock=None):
        return connection_class_mock()

    def setUp(self):
        self.connection = self._create_connection()
        self._on_openok_callback = mock.Mock()
        self.obj = channel.Channel(self.connection, 1, self._on_openok_callback)
        warnings.resetwarnings()

    def tearDown(self):
        del self.connection
        del self._on_openok_callback
        del self.obj
        warnings.resetwarnings()

    def test_init_invalid_channel_number(self):
        self.assertRaises(exceptions.InvalidChannelNumber, channel.Channel,
                          'Foo', self.connection, lambda *args: None)

    def test_init_channel_number(self):
        self.assertEqual(self.obj.channel_number, 1)

    def test_init_callbacks(self):
        self.assertEqual(self.obj.callbacks, self.connection.callbacks)

    def test_init_connection(self):
        self.assertEqual(self.obj.connection, self.connection)

    def test_init_content_frame_assembler(self):
        self.assertIsInstance(self.obj._content_assembler,
                              channel.ContentFrameAssembler)

    def test_init_blocked(self):
        self.assertIsInstance(self.obj._blocked, collections.deque)

    def test_init_blocking(self):
        self.assertEqual(self.obj._blocking, None)

    def test_init_on_flowok_callback(self):
        self.assertEqual(self.obj._on_flowok_callback, None)

    def test_init_has_on_flow_callback(self):
        self.assertEqual(self.obj._has_on_flow_callback, False)

    def test_init_on_openok_callback(self):
        self.assertEqual(self.obj._on_openok_callback, self._on_openok_callback)

    def test_init_state(self):
        self.assertEqual(self.obj._state, channel.Channel.CLOSED)

    def test_init_cancelled(self):
        self.assertIsInstance(self.obj._cancelled, set)

    def test_init_consumers(self):
        self.assertEqual(self.obj._consumers, dict())

    def test_init_flow(self):
        self.assertEqual(self.obj.flow_active, True)

    def test_init_on_getok_callback(self):
        self.assertEqual(self.obj._on_getok_callback, None)

    def test_add_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_callback(mock_callback, [spec.Basic.Qos])
        self.connection.callbacks.add.assert_called_once_with(
            self.obj.channel_number, spec.Basic.Qos, mock_callback, True)

    def test_add_callback_multiple_replies(self):
        mock_callback = mock.Mock()
        self.obj.add_callback(mock_callback, [spec.Basic.Qos, spec.Basic.QosOk])
        calls = [mock.call(self.obj.channel_number, spec.Basic.Qos,
                           mock_callback, True),
                 mock.call(self.obj.channel_number, spec.Basic.QosOk,
                           mock_callback, True)]
        self.connection.callbacks.add.assert_has_calls(calls)

    def test_add_on_cancel_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_cancel_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(
            self.obj.channel_number, spec.Basic.Cancel, mock_callback, False)

    def test_add_on_close_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_close_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(
            self.obj.channel_number, '_on_channel_close', mock_callback, False,
            self.obj)

    def test_add_on_flow_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_flow_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(
            self.obj.channel_number, spec.Channel.Flow, mock_callback, False)

    def test_add_on_return_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_return_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(
            self.obj.channel_number, '_on_return', mock_callback, False)

    def test_basic_ack_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_ack)

    @mock.patch('pika.spec.Basic.Ack')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_ack_calls_send_method(self, send_method, _unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj.basic_ack(1, False)
        send_method.assert_called_once_with(spec.Basic.Ack(1, False))

    def test_basic_cancel_asynch(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._consumers['ctag0'] = logging.debug
        self.obj._rpc = mock.Mock(wraps=self.obj._rpc)
        self.obj.basic_cancel(consumer_tag='ctag0', nowait=True)

        self.assertTrue(self.obj._rpc.called)
        self.assertFalse(self.obj.callbacks.add.called)

    def test_basic_cancel_asynch_with_user_callback_raises_value_error(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        callback_mock = mock.Mock()
        self.obj._consumers[consumer_tag] = callback_mock
        self.assertRaises(ValueError, self.obj.basic_cancel, callback_mock,
                          consumer_tag,
                          nowait=True)

    @mock.patch('pika.channel.Channel._validate_channel_and_callback')
    def test_basic_cancel_calls_validate(self, validate):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        callback_mock = mock.Mock()
        self.obj._consumers[consumer_tag] = mock.Mock()

        self.obj.basic_cancel(callback_mock, consumer_tag)

        validate.assert_called_once_with(callback_mock)

    def test_basic_cancel_synch(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        callback_mock = mock.Mock()
        self.obj._consumers[consumer_tag] = mock.Mock()

        self.obj.basic_cancel(callback_mock, consumer_tag)

        # Verify consumer tag added to the cancelled list
        self.assertListEqual(list(self.obj._cancelled), [consumer_tag])
        # Verify user completion callback registered
        self.obj.callbacks.add.assert_any_call(
            self.obj.channel_number,
            spec.Basic.CancelOk,
            callback_mock)
        # Verify Channel._on_cancelok callback registered
        self.obj.callbacks.add.assert_any_call(
            self.obj.channel_number,
            spec.Basic.CancelOk,
            self.obj._on_cancelok,
            arguments={'consumer_tag': 'ctag0'})
        # Verify Channel._on_synchronous_complete callback registered
        self.obj.callbacks.add.assert_any_call(
            self.obj.channel_number,
            spec.Basic.CancelOk,
            self.obj._on_synchronous_complete,
            arguments={'consumer_tag': 'ctag0'})

    def test_basic_cancel_synch_no_user_callback_raises_value_error(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._consumers['ctag0'] = mock.Mock()

        with self.assertRaises(ValueError):
            self.obj.basic_cancel(consumer_tag='ctag0')

        # Verify arg error detected raised without making state changes
        self.assertIn('ctag0', self.obj._consumers)
        self.assertEqual(len(self.obj._cancelled), 0)

    def test_basic_cancel_then_close(self):
        self.obj._set_state(self.obj.OPEN)
        callback_mock = mock.Mock()
        consumer_tag = 'ctag0'
        self.obj._consumers[consumer_tag] = mock.Mock()
        self.obj.basic_cancel(callback_mock, consumer_tag)
        try:
            self.obj.close()
        except exceptions.ChannelClosed:
            self.fail('unable to cancel consumers as channel is closing')
        self.assertTrue(self.obj.is_closing)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_cancel_unknown_consumer_tag(self, rpc):
        self.obj._set_state(self.obj.OPEN)
        callback_mock = mock.Mock()
        consumer_tag = 'ctag0'
        self.obj.basic_cancel(callback_mock, consumer_tag)
        self.assertFalse(rpc.called)

    def test_basic_consume_channel_closed(self):
        mock_callback = mock.Mock()
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_consume,
                          mock_callback, 'test-queue')

    @mock.patch('pika.channel.Channel._validate_channel_and_callback')
    def test_basic_consume_calls_validate(self, validate):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        validate.assert_called_once_with(mock_callback)

    def test_basic_consume_consumer_tag(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = 'ctag1.'
        mock_callback = mock.Mock()
        self.assertEqual(
            self.obj.basic_consume(mock_callback, 'test-queue')[:6],
            expectation)

    def test_basic_consume_consumer_tag_cancelled_full(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = 'ctag1.'
        mock_callback = mock.Mock()
        for ctag in ['ctag1.%i' % ii for ii in range(11)]:
            self.obj._cancelled.add(ctag)
        self.assertEqual(
            self.obj.basic_consume(mock_callback, 'test-queue')[:6],
            expectation)

    def test_basic_consume_consumer_tag_in_consumers(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue',
                               consumer_tag=consumer_tag)
        self.assertIn(consumer_tag, self.obj._consumers)

    def test_basic_consume_duplicate_consumer_tag_raises(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj._consumers[consumer_tag] = logging.debug
        self.assertRaises(exceptions.DuplicateConsumerTag,
                          self.obj.basic_consume, mock_callback, 'test-queue',
                          False, False, consumer_tag)

    def test_basic_consume_consumers_callback_value(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue',
                               consumer_tag=consumer_tag)
        self.assertEqual(self.obj._consumers[consumer_tag], mock_callback)

    @mock.patch('pika.spec.Basic.Consume')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_consumers_rpc_called(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue',
                               consumer_tag=consumer_tag)
        expectation = spec.Basic.Consume(queue='test-queue',
                                         consumer_tag=consumer_tag,
                                         no_ack=False,
                                         exclusive=False)
        rpc.assert_called_once_with(expectation, self.obj._on_eventok,
                                    [(spec.Basic.ConsumeOk,
                                      {'consumer_tag': consumer_tag})])

    @mock.patch('pika.channel.Channel._validate_channel_and_callback')
    def test_basic_get_calls_validate(self, validate):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_get(mock_callback, 'test-queue')
        validate.assert_called_once_with(mock_callback)

    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_get_callback(self, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_get(mock_callback, 'test-queue')
        self.assertEqual(self.obj._on_getok_callback, mock_callback)

    @mock.patch('pika.spec.Basic.Get')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_get_send_method_called(self, send_method, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_get(mock_callback, 'test-queue', False)
        send_method.assert_called_once_with(spec.Basic.Get(queue='test-queue',
                                                           no_ack=False))

    def test_basic_nack_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_nack, 0,
                          False, True)

    @mock.patch('pika.spec.Basic.Nack')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_nack_send_method_request(self, send_method, _unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj.basic_nack(1, False, True)
        send_method.assert_called_once_with(spec.Basic.Nack(1, False, True))

    def test_basic_publish_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_publish,
                          'foo', 'bar', 'baz')

    @mock.patch('pika.channel.LOGGER')
    @mock.patch('pika.spec.Basic.Publish')
    @mock.patch('pika.channel.Channel._send_method')
    def test_immediate_called_logger_warning(self, _send_method, _unused,
                                             logger):
        self.obj._set_state(self.obj.OPEN)
        exchange = 'basic_publish_test'
        routing_key = 'routing-key-fun'
        body = b'This is my body'
        properties = spec.BasicProperties(content_type='text/plain')
        mandatory = False
        immediate = True
        self.obj.basic_publish(exchange, routing_key, body, properties,
                               mandatory, immediate)
        logger.warning.assert_called_once_with('The immediate flag is '
                                               'deprecated in RabbitMQ')

    @mock.patch('pika.spec.Basic.Publish')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_publish_send_method_request(self, send_method, _unused):
        self.obj._set_state(self.obj.OPEN)
        exchange = 'basic_publish_test'
        routing_key = 'routing-key-fun'
        body = b'This is my body'
        properties = spec.BasicProperties(content_type='text/plain')
        mandatory = False
        immediate = False
        self.obj.basic_publish(exchange, routing_key, body, properties,
                               mandatory, immediate)
        send_method.assert_called_once_with(
            spec.Basic.Publish(exchange=exchange,
                               routing_key=routing_key,
                               mandatory=mandatory,
                               immediate=immediate), (properties, body))

    def test_basic_qos_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_qos, 0,
                          False, True)

    @mock.patch('pika.spec.Basic.Qos')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_qos_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_qos(mock_callback, 10, 20, False)
        rpc.assert_called_once_with(spec.Basic.Qos(10, 20, False),
                                    mock_callback,
                                    [spec.Basic.QosOk])

    def test_basic_reject_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_reject, 1,
                          False)

    @mock.patch('pika.spec.Basic.Reject')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_reject_send_method_request_with_int_tag(self,
                                                           send_method,
                                                           _unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj.basic_reject(1, True)
        send_method.assert_called_once_with(spec.Basic.Reject(1, True))

    def test_basic_reject_spec_with_int_tag(self):
        decoded = spec.Basic.Reject()
        decoded.decode(b''.join(spec.Basic.Reject(1, True).encode()))

        self.assertEqual(decoded.delivery_tag, 1)
        self.assertIs(decoded.requeue, True)

    @mock.patch('pika.spec.Basic.Reject')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_reject_send_method_request_with_long_tag(self,
                                                            send_method,
                                                            _unused):
        self.obj._set_state(self.obj.OPEN)

        # NOTE: we use `sys.maxsize` for compatibility with python 3, which
        # doesn't have `sys.maxint`
        self.obj.basic_reject(sys.maxsize, True)
        send_method.assert_called_once_with(spec.Basic.Reject(sys.maxsize,
                                                              True))

    def test_basic_reject_spec_with_long_tag(self):
        # NOTE: we use `sys.maxsize` for compatibility with python 3, which
        # doesn't have `sys.maxint`
        decoded = spec.Basic.Reject()
        decoded.decode(b''.join(spec.Basic.Reject(sys.maxsize, True).encode()))

        self.assertEqual(decoded.delivery_tag, sys.maxsize)
        self.assertIs(decoded.requeue, True)

    def test_basic_recover_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_qos, 0,
                          False, True)

    @mock.patch('pika.spec.Basic.Recover')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_recover_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_recover(mock_callback, True)
        rpc.assert_called_once_with(spec.Basic.Recover(True),
                                    mock_callback, [spec.Basic.RecoverOk])

    def test_close_in_closing_state_raises_already_closing(self):
        self.obj._set_state(self.obj.CLOSING)
        self.assertRaises(exceptions.ChannelAlreadyClosing, self.obj.close)
        self.assertTrue(self.obj.is_closing)

    def test_close_in_closed_state_raises_channel_error_and_stays_closed(self):
        self.assertTrue(self.obj.is_closed)
        self.assertRaises(exceptions.ChannelClosed, self.obj.close)
        self.assertTrue(self.obj.is_closed)

    @mock.patch('pika.spec.Channel.Close')
    def test_close_in_opening_state(self, _unused):
        self.obj._set_state(self.obj.OPENING)
        self.obj._rpc = mock.Mock(wraps=self.obj._rpc)
        self.obj._cleanup = mock.Mock(wraps=self.obj._cleanup)

        # close() called by user
        self.obj.close(200, 'Got to go')

        self.obj._rpc.assert_called_once_with(
            spec.Channel.Close(200, 'Got to go', 0, 0),
            self.obj._on_closeok, [spec.Channel.CloseOk])

        self.assertEqual(self.obj._closing_code_and_text, (0, ''))
        self.assertEqual(self.obj._state, self.obj.CLOSING)

        # OpenOk method from broker
        self.obj._on_openok(
            frame.Method(self.obj.channel_number,
                         spec.Channel.OpenOk(self.obj.channel_number)))
        self.assertEqual(self.obj._state, self.obj.CLOSING)
        self.assertEqual(self.obj.callbacks.process.call_count, 0)

        # CloseOk method from broker
        self.obj._on_closeok(frame.Method(self.obj.channel_number,
                                          spec.Channel.CloseOk()))
        self.assertEqual(self.obj._state, self.obj.CLOSED)

        self.obj.callbacks.process.assert_any_call(
            self.obj.channel_number,
            '_on_channel_close',
            self.obj,
            self.obj,
            0, '')

        self.assertEqual(self.obj._cleanup.call_count, 1)

    def test_close_in_open_state_transitions_to_closing(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj.close()
        self.assertEqual(self.obj._state, channel.Channel.CLOSING)

    def test_close_basic_cancel_called(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._consumers['abc'] = None
        with mock.patch.object(self.obj, 'basic_cancel') as basic_cancel:
            self.obj.close()
            # this is actually not necessary but Pika currently cancels
            # every consumer before closing the channel
            basic_cancel.assert_called_once_with(consumer_tag='abc', nowait=True)

    def test_confirm_delivery_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.confirm_delivery)

    def test_confirm_delivery_raises_method_not_implemented_for_confirms(self):
        self.obj._set_state(self.obj.OPEN)
        # Since connection is a mock.Mock, overwrite the method def with False
        self.obj.connection.publisher_confirms = False
        self.assertRaises(exceptions.MethodNotImplemented,
                          self.obj.confirm_delivery, logging.debug)

    def test_confirm_delivery_raises_method_not_implemented_for_nack(self):
        self.obj._set_state(self.obj.OPEN)
        # Since connection is a mock.Mock, overwrite the method def with False
        self.obj.connection.basic_nack = False
        self.assertRaises(exceptions.MethodNotImplemented,
                          self.obj.confirm_delivery, logging.debug)

    def test_confirm_delivery_async(self):
        self.obj._set_state(self.obj.OPEN)
        user_ack_nack_callback = mock.Mock()
        self.obj.confirm_delivery(callback=user_ack_nack_callback, nowait=True)

        self.assertEqual(self.obj.callbacks.add.call_count, 2)
        self.obj.callbacks.add.assert_any_call(
            self.obj.channel_number,
            spec.Basic.Ack,
            user_ack_nack_callback,
            False)
        self.obj.callbacks.add.assert_any_call(
            self.obj.channel_number,
            spec.Basic.Nack,
            user_ack_nack_callback,
            False)

    def test_confirm_delivery_callback_without_nowait_selectok(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = [self.obj.channel_number, spec.Confirm.SelectOk,
                       self.obj._on_selectok]
        self.obj.confirm_delivery(logging.debug)
        self.obj.callbacks.add.assert_called_with(*expectation, arguments=None)

    def test_confirm_delivery_callback_basic_ack(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = (self.obj.channel_number, spec.Basic.Ack, logging.debug,
                       False)
        self.obj.confirm_delivery(logging.debug)
        self.obj.callbacks.add.assert_any_call(*expectation)

    def test_confirm_delivery_callback_basic_nack(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = (self.obj.channel_number, spec.Basic.Nack, logging.debug,
                       False)
        self.obj.confirm_delivery(logging.debug)
        self.obj.callbacks.add.assert_any_call(*expectation)

    def test_confirm_delivery_no_callback_callback_call_count(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj.confirm_delivery()
        expectation = [mock.call(*[self.obj.channel_number,
                                   spec.Confirm.SelectOk,
                                   self.obj._on_synchronous_complete],
                                 arguments=None),
                       mock.call(*[self.obj.channel_number,
                                   spec.Confirm.SelectOk,
                                   self.obj._on_selectok,],
                                 arguments=None)]
        self.assertEqual(self.obj.callbacks.add.call_args_list, expectation)

    def test_confirm_delivery_no_callback_no_basic_ack_callback(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = [self.obj.channel_number, spec.Basic.Ack, None, False]
        self.obj.confirm_delivery()
        self.assertNotIn(mock.call(*expectation),
                         self.obj.callbacks.add.call_args_list)

    def test_confirm_delivery_no_callback_no_basic_nack_callback(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = [self.obj.channel_number, spec.Basic.Nack, None, False]
        self.obj.confirm_delivery()
        self.assertNotIn(mock.call(*expectation),
                         self.obj.callbacks.add.call_args_list)

    def test_consumer_tags(self):
        self.assertListEqual(self.obj.consumer_tags, list(self.obj._consumers.keys()))

    def test_exchange_bind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.exchange_bind,
                          None, 'foo', 'bar', 'baz')

    def test_exchange_bind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.exchange_bind, 'callback', 'foo',
                          'bar', 'baz')

    @mock.patch('pika.spec.Exchange.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_bind_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_bind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(spec.Exchange.Bind(0, 'foo', 'bar', 'baz'),
                                    mock_callback, [spec.Exchange.BindOk])

    @mock.patch('pika.spec.Exchange.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_bind_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_bind(mock_callback, 'foo', 'bar', 'baz', nowait=True)
        rpc.assert_called_once_with(spec.Exchange.Bind(0, 'foo', 'bar', 'baz'),
                                    mock_callback, [])

    def test_exchange_declare_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.exchange_declare,
                          exchange='foo')

    def test_exchange_declare_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.exchange_declare, 'callback',
                          'foo')

    @mock.patch('pika.spec.Exchange.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_declare_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_declare(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Exchange.Declare(0, 'foo'),
                                    mock_callback, [spec.Exchange.DeclareOk])

    @mock.patch('pika.spec.Exchange.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_declare_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_declare(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Exchange.Declare(0, 'foo'),
                                    mock_callback, [])

    def test_exchange_delete_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.exchange_delete,
                          exchange='foo')

    def test_exchange_delete_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.exchange_delete, 'callback',
                          'foo')

    @mock.patch('pika.spec.Exchange.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_delete_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_delete(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Exchange.Delete(0, 'foo'),
                                    mock_callback, [spec.Exchange.DeleteOk])

    @mock.patch('pika.spec.Exchange.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_delete_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_delete(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Exchange.Delete(0, 'foo'),
                                    mock_callback, [])

    def test_exchange_unbind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.exchange_unbind,
                          None, 'foo', 'bar', 'baz')

    def test_exchange_unbind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.exchange_unbind, 'callback',
                          'foo', 'bar', 'baz')

    @mock.patch('pika.spec.Exchange.Unbind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_unbind_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_unbind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(
            spec.Exchange.Unbind(0, 'foo', 'bar', 'baz'), mock_callback,
            [spec.Exchange.UnbindOk])

    @mock.patch('pika.spec.Exchange.Unbind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_unbind_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_unbind(mock_callback, 'foo', 'bar', 'baz',
                                 nowait=True)
        rpc.assert_called_once_with(
            spec.Exchange.Unbind(0, 'foo', 'bar', 'baz'), mock_callback, [])

    def test_flow_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.flow, 'foo', True)

    def test_flow_raises_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.flow, 'foo', True)

    @mock.patch('pika.spec.Channel.Flow')
    @mock.patch('pika.channel.Channel._rpc')
    def test_flow_on_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.flow(mock_callback, True)
        rpc.assert_called_once_with(spec.Channel.Flow(True),
                                    self.obj._on_flowok, [spec.Channel.FlowOk])

    @mock.patch('pika.spec.Channel.Flow')
    @mock.patch('pika.channel.Channel._rpc')
    def test_flow_off_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.flow(mock_callback, False)
        rpc.assert_called_once_with(spec.Channel.Flow(False),
                                    self.obj._on_flowok, [spec.Channel.FlowOk])

    @mock.patch('pika.channel.Channel._rpc')
    def test_flow_on_flowok_callback(self, _rpc):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.flow(mock_callback, True)
        self.assertEqual(self.obj._on_flowok_callback, mock_callback)

    def test_is_closed_true(self):
        self.obj._set_state(self.obj.CLOSED)
        self.assertTrue(self.obj.is_closed)

    def test_is_closed_false(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertFalse(self.obj.is_closed)

    def test_is_closing_true(self):
        self.obj._set_state(self.obj.CLOSING)
        self.assertTrue(self.obj.is_closing)

    def test_is_closing_false(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertFalse(self.obj.is_closing)

    @mock.patch('pika.channel.Channel._rpc')
    def test_channel_open_add_callbacks_called(self, _rpc):
        with mock.patch.object(self.obj, '_add_callbacks') as _add_callbacks:
            self.obj.open()
            _add_callbacks.assert_called_once_with()

    def test_queue_bind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.queue_bind, None,
                          'foo', 'bar', 'baz')

    def test_queue_bind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.queue_bind, 'callback', 'foo',
                          'bar', 'baz')

    @mock.patch('pika.spec.Queue.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_bind_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_bind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(spec.Queue.Bind(0, 'foo', 'bar', 'baz'),
                                    mock_callback, [spec.Queue.BindOk])

    @mock.patch('pika.spec.Queue.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_bind_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_bind(mock_callback, 'foo', 'bar', 'baz', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Bind(0, 'foo', 'bar', 'baz'),
                                    mock_callback, [])

    def test_queue_declare_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.queue_declare,
                          None,
                          queue='foo')

    def test_queue_declare_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.queue_declare, 'callback', 'foo')

    @mock.patch('pika.spec.Queue.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_declare_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_declare(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Queue.Declare(0, 'foo'), mock_callback,
                                    [(spec.Queue.DeclareOk, {'queue': 'foo'})])

    @mock.patch('pika.spec.Queue.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_declare_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_declare(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Declare(0, 'foo'),
                                    mock_callback, [])

    def test_queue_delete_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.queue_delete,
                          queue='foo')

    def test_queue_delete_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.queue_delete, 'callback', 'foo')

    @mock.patch('pika.spec.Queue.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_delete_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_delete(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Queue.Delete(0, 'foo'), mock_callback,
                                    [spec.Queue.DeleteOk])

    @mock.patch('pika.spec.Queue.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_delete_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_delete(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Delete(0, 'foo'),
                                    mock_callback, [])

    def test_queue_purge_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.queue_purge,
                          queue='foo')

    def test_queue_purge_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.queue_purge, 'callback', 'foo')

    @mock.patch('pika.spec.Queue.Purge')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_purge_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_purge(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Queue.Purge(0, 'foo'), mock_callback,
                                    [spec.Queue.PurgeOk])

    @mock.patch('pika.spec.Queue.Purge')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_purge_rpc_request_nowait(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_purge(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Purge(0, 'foo'),
                                    mock_callback, [])

    def test_queue_unbind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.queue_unbind, None,
                          'foo', 'bar', 'baz')

    def test_queue_unbind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.queue_unbind, 'callback', 'foo',
                          'bar', 'baz')

    @mock.patch('pika.spec.Queue.Unbind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_unbind_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_unbind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(spec.Queue.Unbind(0, 'foo', 'bar', 'baz'),
                                    mock_callback, [spec.Queue.UnbindOk])

    def test_tx_commit_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.tx_commit, None)

    @mock.patch('pika.spec.Tx.Commit')
    @mock.patch('pika.channel.Channel._rpc')
    def test_tx_commit_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.tx_commit(mock_callback)
        rpc.assert_called_once_with(spec.Tx.Commit(),
                                    mock_callback, [spec.Tx.CommitOk])

    @mock.patch('pika.spec.Tx.Rollback')
    @mock.patch('pika.channel.Channel._rpc')
    def test_tx_rollback_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.tx_rollback(mock_callback)
        rpc.assert_called_once_with(spec.Tx.Rollback(),
                                    mock_callback, [spec.Tx.RollbackOk])

    @mock.patch('pika.spec.Tx.Select')
    @mock.patch('pika.channel.Channel._rpc')
    def test_tx_select_rpc_request(self, rpc, _unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.tx_select(mock_callback)
        rpc.assert_called_once_with(spec.Tx.Select(),
                                    mock_callback, [spec.Tx.SelectOk])

    # Test internal methods

    def test_add_callbacks_basic_cancel_empty_added(self):
        self.obj._add_callbacks()
        self.obj.callbacks.add.assert_any_call(self.obj.channel_number,
                                               spec.Basic.Cancel,
                                               self.obj._on_cancel, False)

    def test_add_callbacks_basic_get_empty_added(self):
        self.obj._add_callbacks()
        self.obj.callbacks.add.assert_any_call(self.obj.channel_number,
                                               spec.Basic.GetEmpty,
                                               self.obj._on_getempty, False)

    def test_add_callbacks_channel_close_added(self):
        self.obj._add_callbacks()
        self.obj.callbacks.add.assert_any_call(self.obj.channel_number,
                                               spec.Channel.Close,
                                               self.obj._on_close, True)

    def test_add_callbacks_channel_flow_added(self):
        self.obj._add_callbacks()
        self.obj.callbacks.add.assert_any_call(self.obj.channel_number,
                                               spec.Channel.Flow,
                                               self.obj._on_flow, False)

    def test_cleanup(self):
        self.obj._cleanup()
        self.obj.callbacks.cleanup.assert_called_once_with(
            str(self.obj.channel_number))

    def test_handle_content_frame_method_returns_none(self):
        frame_value = frame.Method(1, spec.Basic.Deliver('ctag0', 1))
        self.assertEqual(self.obj._handle_content_frame(frame_value), None)

    def test_handle_content_frame_sets_method_frame(self):
        frame_value = frame.Method(1, spec.Basic.Deliver('ctag0', 1))
        self.obj._handle_content_frame(frame_value)
        self.assertEqual(self.obj._content_assembler._method_frame, frame_value)

    def test_handle_content_frame_sets_header_frame(self):
        frame_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(frame_value)
        self.assertEqual(self.obj._content_assembler._header_frame, frame_value)

    def test_handle_content_frame_basic_deliver_called(self):
        method_value = frame.Method(1, spec.Basic.Deliver('ctag0', 1))
        self.obj._handle_content_frame(method_value)
        header_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(header_value)
        body_value = frame.Body(1, b'0123456789')
        with mock.patch.object(self.obj, '_on_deliver') as deliver:
            self.obj._handle_content_frame(body_value)
            deliver.assert_called_once_with(method_value, header_value,
                                            b'0123456789')

    def test_handle_content_frame_basic_get_called(self):
        method_value = frame.Method(1, spec.Basic.GetOk('ctag0', 1))
        self.obj._handle_content_frame(method_value)
        header_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(header_value)
        body_value = frame.Body(1, b'0123456789')
        with mock.patch.object(self.obj, '_on_getok') as getok:
            self.obj._handle_content_frame(body_value)
            getok.assert_called_once_with(method_value, header_value,
                                          b'0123456789')

    def test_handle_content_frame_basic_return_called(self):
        method_value = frame.Method(1, spec.Basic.Return(999, 'Reply Text',
                                                         'exchange_value',
                                                         'routing.key'))
        self.obj._handle_content_frame(method_value)
        header_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(header_value)
        body_value = frame.Body(1, b'0123456789')
        with mock.patch.object(self.obj, '_on_return') as basic_return:
            self.obj._handle_content_frame(body_value)
            basic_return.assert_called_once_with(method_value, header_value,
                                                 b'0123456789')

    def test_has_content_true(self):
        self.assertTrue(spec.has_content(spec.Basic.GetOk.INDEX))

    def test_has_content_false(self):
        self.assertFalse(spec.has_content(spec.Basic.Ack.INDEX))

    def test_on_cancel_not_appended_cancelled(self):
        consumer_tag = 'ctag0'
        frame_value = frame.Method(1, spec.Basic.Cancel(consumer_tag))
        self.obj._on_cancel(frame_value)
        self.assertNotIn(consumer_tag, self.obj._cancelled)

    def test_on_cancel_removed_consumer(self):
        consumer_tag = 'ctag0'
        self.obj._consumers[consumer_tag] = logging.debug
        frame_value = frame.Method(1, spec.Basic.Cancel(consumer_tag))
        self.obj._on_cancel(frame_value)
        self.assertNotIn(consumer_tag, self.obj._consumers)

    def test_on_cancelok_removed_consumer(self):
        consumer_tag = 'ctag0'
        self.obj._consumers[consumer_tag] = logging.debug
        frame_value = frame.Method(1, spec.Basic.CancelOk(consumer_tag))
        self.obj._on_cancelok(frame_value)
        self.assertNotIn(consumer_tag, self.obj._consumers)

    @mock.patch('pika.spec.Channel.CloseOk')
    def test_on_close_in_open_state(self, _unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj._send_method = mock.Mock(wraps=self.obj._send_method)
        self.obj._cleanup = mock.Mock(wraps=self.obj._cleanup)

        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.Close(400, 'error'))
        self.obj._on_close(method_frame)

        self.assertTrue(self.obj.is_closed,
                        'Channel was not closed; state=%s' % (self.obj._state,))

        self.obj._send_method.assert_called_once_with(spec.Channel.CloseOk())

        self.obj.callbacks.process.assert_any_call(
            self.obj.channel_number,
            '_on_channel_close',
            self.obj,
            self.obj,
            method_frame.method.reply_code,
            method_frame.method.reply_text)

        self.assertEqual(self.obj._cleanup.call_count, 1)

    def test_on_close_in_closing_state(self):
        self.obj._set_state(self.obj.CLOSING)
        self.obj._cleanup = mock.Mock(wraps=self.obj._cleanup)

        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.Close(400, 'error'))
        self.obj._on_close(method_frame)

        # Verify didn't alter state (will wait for CloseOk)
        self.assertTrue(self.obj.is_closing,
                        'Channel was not closed; state=%s' % (self.obj._state,))

        self.assertEqual(self.obj._closing_code_and_text, (400, 'error'))

        self.assertFalse(self.obj.callbacks.process.called,
                         self.obj.callbacks.process.call_args_list)

        self.assertFalse(self.obj._cleanup.called)

    @mock.patch('logging.Logger.warning')
    def test_on_close_warning(self, warning):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.Close(999, 'Test_Value'))
        self.obj._on_close(method_frame)
        warning.assert_called_once_with(
            'Received remote Channel.Close (%s): %r on %s',
            method_frame.method.reply_code,
            method_frame.method.reply_text,
            self.obj)

    def _verify_on_close_meta_transitions_to_closed(self, initial_state):
        self.obj._set_state(initial_state)
        self.obj._cleanup = mock.Mock(wraps=self.obj._cleanup)

        self.obj._on_close_meta(-1, 'Undefined')

        self.assertTrue(self.obj.is_closed)

        self.assertEqual(self.obj._cleanup.call_count, 1)

        self.assertEqual(self.obj.callbacks.process.call_count, 2)

        self.obj.callbacks.process.assert_any_call(
            self.obj.channel_number,
            '_on_channel_close',
            self.obj,
            self.obj,
            -1,
            'Undefined')

        self.obj.callbacks.process.assert_any_call(
            self.obj.channel_number,
            self.obj._ON_CHANNEL_CLEANUP_CB_KEY,
            self.obj,
            self.obj)

    def test_on_close_meta_in_opening_state_transitions_to_closed(self):
        self._verify_on_close_meta_transitions_to_closed(self.obj.OPENING)

    def test_on_close_meta_in_open_state_transitions_to_closed(self):
        self._verify_on_close_meta_transitions_to_closed(self.obj.OPEN)

    def test_on_close_meta_in_closing_state_transitions_to_closed(self):
        self._verify_on_close_meta_transitions_to_closed(self.obj.CLOSING)

    def test_on_close_meta_in_closed_state_is_suppressed(self):
        self.obj._cleanup = mock.Mock(wraps=self.obj._cleanup)
        self.obj._set_state(self.obj.CLOSED)

        self.obj._on_close_meta(500, 'Internal error')

        self.assertTrue(self.obj.is_closed)
        self.assertEqual(self.obj.callbacks.process.call_count, 0)
        self.assertEqual(self.obj._cleanup.call_count, 0)

    def test_on_deliver_callback_called(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        mock_callback = mock.Mock()
        self.obj._consumers[consumer_tag] = mock_callback
        method_value = frame.Method(1, spec.Basic.Deliver(consumer_tag, 1))
        header_value = frame.Header(1, 10, spec.BasicProperties())
        body_value = b'0123456789'
        self.obj._on_deliver(method_value, header_value, body_value)
        mock_callback.assert_called_with(self.obj, method_value.method,
                                         header_value.properties, body_value)

    def test_on_closeok(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._cleanup = mock.Mock(wraps=self.obj._cleanup)

        # Close from user
        self.obj.close(200, 'All is well')
        self.assertEqual(self.obj._closing_code_and_text, (0, ''))
        self.assertEqual(self.obj._state, self.obj.CLOSING)

        self.obj._on_closeok(frame.Method(self.obj.channel_number,
                                          spec.Channel.CloseOk()))

        self.assertTrue(self.obj.is_closed,
                        'Channel was not closed; state=%s' % (self.obj._state,))

        self.obj.callbacks.process.assert_any_call(
            self.obj.channel_number,
            '_on_channel_close',
            self.obj,
            self.obj,
            0, '')

        self.assertEqual(self.obj._cleanup.call_count, 1)

    def test_on_closeok_following_close_from_broker(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._cleanup = mock.Mock(wraps=self.obj._cleanup)

        # Close from user
        self.obj.close(0, 'All is well')
        self.assertEqual(self.obj._closing_code_and_text, (0, ''))
        self.assertEqual(self.obj._state, self.obj.CLOSING)

        # Close from broker before Channel.CloseOk
        self.obj._on_close(
            frame.Method(self.obj.channel_number,
                         spec.Channel.Close(400, 'broker is having a bad day')))

        self.assertEqual(self.obj._closing_code_and_text,
                         (400, 'broker is having a bad day'))
        self.assertEqual(self.obj._state, self.obj.CLOSING)


        self.obj._on_closeok(frame.Method(self.obj.channel_number,
                                          spec.Channel.CloseOk()))

        # Verify this completes closing of the channel
        self.assertTrue(self.obj.is_closed,
                        'Channel was not closed; state=%s' % (self.obj._state,))

        self.assertEqual(self.obj.callbacks.process.call_count, 2)

        self.obj.callbacks.process.assert_any_call(
            self.obj.channel_number,
            '_on_channel_close',
            self.obj,
            self.obj,
            400,
            'broker is having a bad day')

        self.assertEqual(self.obj._cleanup.call_count, 1)

    @mock.patch('logging.Logger.debug')
    def test_on_getempty(self, debug):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Basic.GetEmpty)
        self.obj._on_getempty(method_frame)
        debug.assert_called_with('Received Basic.GetEmpty: %r', method_frame)

    @mock.patch('logging.Logger.error')
    def test_on_getok_no_callback(self, error):
        method_value = frame.Method(1, spec.Basic.GetOk('ctag0', 1))
        header_value = frame.Header(1, 10, spec.BasicProperties())
        body_value = b'0123456789'
        self.obj._on_getok(method_value, header_value, body_value)
        error.assert_called_with('Basic.GetOk received with no active callback')

    def test_on_getok_callback_called(self):
        mock_callback = mock.Mock()
        self.obj._on_getok_callback = mock_callback
        method_value = frame.Method(1, spec.Basic.GetOk('ctag0', 1))
        header_value = frame.Header(1, 10, spec.BasicProperties())
        body_value = b'0123456789'
        self.obj._on_getok(method_value, header_value, body_value)
        mock_callback.assert_called_once_with(self.obj, method_value.method,
                                              header_value.properties,
                                              body_value)

    def test_on_getok_callback_reset(self):
        mock_callback = mock.Mock()
        self.obj._on_getok_callback = mock_callback
        method_value = frame.Method(1, spec.Basic.GetOk('ctag0', 1))
        header_value = frame.Header(1, 10, spec.BasicProperties())
        body_value = b'0123456789'
        self.obj._on_getok(method_value, header_value, body_value)
        self.assertIsNone(self.obj._on_getok_callback)

    @mock.patch('logging.Logger.debug')
    def test_on_confirm_selectok(self, debug):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Confirm.SelectOk())
        self.obj._on_selectok(method_frame)
        debug.assert_called_with('Confirm.SelectOk Received: %r', method_frame)

    @mock.patch('logging.Logger.debug')
    def test_on_eventok(self, debug):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Basic.GetEmpty())
        self.obj._on_eventok(method_frame)
        debug.assert_called_with('Discarding frame %r', method_frame)

    @mock.patch('logging.Logger.warning')
    def test_on_flow(self, warning):
        self.obj._has_on_flow_callback = False
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.Flow())
        self.obj._on_flow(method_frame)
        warning.assert_called_with('Channel.Flow received from server')

    @mock.patch('logging.Logger.warning')
    def test_on_flow_with_callback(self, warning):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.Flow())
        self.obj._on_flowok_callback = logging.debug
        self.obj._on_flow(method_frame)
        self.assertEqual(len(warning.call_args_list), 1)

    @mock.patch('logging.Logger.warning')
    def test_on_flowok(self, warning):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.FlowOk())
        self.obj._on_flowok(method_frame)
        warning.assert_called_with('Channel.FlowOk received with no active '
                                   'callbacks')

    def test_on_flowok_calls_callback(self):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.FlowOk())
        mock_callback = mock.Mock()
        self.obj._on_flowok_callback = mock_callback
        self.obj._on_flowok(method_frame)
        mock_callback.assert_called_once_with(method_frame.method.active)

    def test_on_flowok_callback_reset(self):
        method_frame = frame.Method(self.obj.channel_number,
                                    spec.Channel.FlowOk())
        mock_callback = mock.Mock()
        self.obj._on_flowok_callback = mock_callback
        self.obj._on_flowok(method_frame)
        self.assertIsNone(self.obj._on_flowok_callback)

    def test_on_openok_no_callback(self):
        self.obj._on_openok_callback = None
        method_value = frame.Method(1, spec.Channel.OpenOk())
        self.obj._on_openok(method_value)
        self.assertEqual(self.obj._state, self.obj.OPEN)

    def test_on_openok_callback_called(self):
        mock_callback = mock.Mock()
        self.obj._on_openok_callback = mock_callback
        method_value = frame.Method(1, spec.Channel.OpenOk())
        self.obj._on_openok(method_value)
        mock_callback.assert_called_once_with(self.obj)

    def test_onreturn(self):
        method_value = frame.Method(1, spec.Basic.Return(999, 'Reply Text',
                                                         'exchange_value',
                                                         'routing.key'))
        header_value = frame.Header(1, 10, spec.BasicProperties())
        body_value = frame.Body(1, b'0123456789')
        self.obj._on_return(method_value, header_value, body_value)
        self.obj.callbacks.process.assert_called_with(self.obj.channel_number,
                                                      '_on_return',
                                                      self.obj,
                                                      self.obj,
                                                      method_value.method,
                                                      header_value.properties,
                                                      body_value)

    @mock.patch('logging.Logger.warning')
    def test_onreturn_warning(self, warning):
        method_value = frame.Method(1, spec.Basic.Return(999, 'Reply Text',
                                                         'exchange_value',
                                                         'routing.key'))
        header_value = frame.Header(1, 10, spec.BasicProperties())
        body_value = frame.Body(1, b'0123456789')
        self.obj.callbacks.process.return_value = False
        self.obj._on_return(method_value, header_value, body_value)
        warning.assert_called_with('Basic.Return received from server (%r, %r)',
                                   method_value.method, header_value.properties)

    @mock.patch('pika.channel.Channel._rpc')
    def test_on_synchronous_complete(self, rpc):
        mock_callback = mock.Mock()
        expectation = [spec.Queue.Unbind(0, 'foo', 'bar', 'baz'), mock_callback,
                       [spec.Queue.UnbindOk]]
        self.obj._blocked = collections.deque([expectation])
        self.obj._on_synchronous_complete(frame.Method(self.obj.channel_number,
                                                       spec.Basic.Ack(1)))
        rpc.assert_called_once_with(*expectation)

    def test_repr(self):
        text = repr(self.obj)
        self.assertTrue(text.startswith('<Channel'), text)

    def test_rpc_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj._rpc,
                          spec.Basic.Cancel('tag_abc'))

    def test_rpc_while_blocking_appends_blocked_collection(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._blocking = spec.Confirm.Select()
        acceptable_replies = [
            (spec.Basic.CancelOk, {'consumer_tag': 'tag_abc'})]
        expectation = [spec.Basic.Cancel('tag_abc'), lambda *args: None,
                       acceptable_replies]
        self.obj._rpc(*expectation)
        self.assertIn(expectation, self.obj._blocked)

    def test_rpc_throws_value_error_with_unacceptable_replies(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(TypeError, self.obj._rpc,
                          spec.Basic.Cancel('tag_abc'),
                          logging.debug, 'Foo')

    def test_rpc_throws_type_error_with_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(TypeError, self.obj._rpc, spec.Channel.Open(1),
                          ['foo'], [spec.Channel.OpenOk])

    def test_rpc_enters_blocking_and_adds_on_synchronous_complete(self):
        self.obj._set_state(self.obj.OPEN)
        method_frame = spec.Channel.Open()
        self.obj._rpc(method_frame, None, [spec.Channel.OpenOk])
        self.assertEqual(self.obj._blocking, method_frame.NAME)
        self.obj.callbacks.add.assert_called_with(
            self.obj.channel_number, spec.Channel.OpenOk,
            self.obj._on_synchronous_complete,
            arguments=None)

    def test_rpc_not_blocking_and_no_on_synchronous_complete_when_no_replies(self):
        self.obj._set_state(self.obj.OPEN)
        method_frame = spec.Channel.Open()
        self.obj._rpc(method_frame, None, acceptable_replies=[])
        self.assertIsNone(self.obj._blocking)
        with self.assertRaises(AssertionError):
            self.obj.callbacks.add.assert_called_with(
                mock.ANY, mock.ANY,
                self.obj._on_synchronous_complete,
                arguments=mock.ANY)

    def test_rpc_adds_callback(self):
        self.obj._set_state(self.obj.OPEN)
        method_frame = spec.Channel.Open()
        mock_callback = mock.Mock()
        self.obj._rpc(method_frame, mock_callback, [spec.Channel.OpenOk])
        self.obj.callbacks.add.assert_called_with(self.obj.channel_number,
                                                  spec.Channel.OpenOk,
                                                  mock_callback,
                                                  arguments=None)

    def test_send_method(self):
        expectation = [2, 3]
        with mock.patch.object(self.obj.connection,
                               '_send_method') as send_method:
            self.obj._send_method(*expectation)
            send_method.assert_called_once_with(
                *[self.obj.channel_number] + expectation)

    def test_set_state(self):
        self.obj._state = channel.Channel.CLOSED
        self.obj._set_state(channel.Channel.OPENING)
        self.assertEqual(self.obj._state, channel.Channel.OPENING)

    def test_validate_channel_and_callback_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj._validate_channel_and_callback, None)

    def test_validate_channel_and_callback_raises_value_error_not_callable(self
                                                                          ):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj._validate_channel_and_callback,
                          'foo')

