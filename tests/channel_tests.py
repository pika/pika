"""
Tests for pika.channel.ContentFrameDispatcher

"""
import collections
import logging
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import channel
from pika import exceptions
from pika import frame
from pika import spec


class ChannelTest(unittest.TestCase):

    @mock.patch('pika.connection.Connection')
    def _create_connection(self, connection=None):
        return connection

    def setUp(self):
        self.connection = self._create_connection()
        self._on_open_callback = mock.Mock()
        self.obj = channel.Channel(self.connection, 1,
                                   self._on_open_callback)

    def tearDown(self):
        del self.connection
        del self._on_open_callback
        del self.obj

    def test_init_invalid_channel_number(self):
        self.assertRaises(exceptions.InvalidChannelNumber,
                          channel.Channel,
                          'Foo', self.connection)

    def test_init_channel_number(self):
        self.assertEqual(self.obj.channel_number, 1)

    def test_init_callbacks(self):
        self.assertEqual(self.obj.callbacks, self.connection.callbacks)

    def test_init_connection(self):
        self.assertEqual(self.obj.connection, self.connection)

    def test_init_frame_dispatcher(self):
        self.assertIsInstance(self.obj.frame_dispatcher,
                              channel.ContentFrameDispatcher)

    def test_init_blocked(self):
        self.assertIsInstance(self.obj._blocked, collections.deque)

    def test_init_blocking(self):
        self.assertEqual(self.obj._blocking, None)

    def test_init_flow(self):
        self.assertEqual(self.obj._flow, None)

    def test_init_has_on_flow_callback(self):
        self.assertEqual(self.obj._has_on_flow_callback, False)

    def test_init_on_open_callback(self):
        self.assertEqual(self.obj._on_open_callback, self._on_open_callback)

    def test_init_state(self):
        self.assertEqual(self.obj._state, channel.Channel.CLOSED)

    def test_init_cancelled(self):
        self.assertEqual(self.obj._cancelled, list())

    def test_init_consumers(self):
        self.assertEqual(self.obj._consumers, dict())

    def test_init_pending(self):
        self.assertEqual(self.obj._pending, dict())

    def test_init_on_get_ok_callback(self):
        self.assertEqual(self.obj._on_get_ok_callback, None)

    def test_init_on_flow_ok_callback(self):
        self.assertEqual(self.obj._on_flow_ok_callback, None)

    def test_init_reply_code(self):
        self.assertEqual(self.obj._reply_code, None)

    def test_init_reply_text(self):
        self.assertEqual(self.obj._reply_text, None)

    def test_add_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_callback(mock_callback, [spec.Basic.Qos])
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Basic.Qos,
                                                              mock_callback,
                                                              True)

    def test_add_callback_multiple_replies(self):
        mock_callback = mock.Mock()
        self.obj.add_callback(mock_callback, [spec.Basic.Qos, spec.Basic.QosOk])
        calls = [mock.call(self.obj.channel_number, spec.Basic.Qos,
                           mock_callback, True),
                 mock.call(self.obj.channel_number, spec.Basic.QosOk,
                           mock_callback, True)]
        self.connection.callbacks.add.assert_has_calls(calls)

    def test_add_on_basic_cancel_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_basic_cancel_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Basic.Cancel,
                                                              mock_callback,
                                                              False)

    def test_add_on_close_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_close_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Channel.Close,
                                                              mock_callback,
                                                              False)

    def test_add_on_flow_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_flow_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              spec.Channel.Flow,
                                                              mock_callback,
                                                              False)

    def test_add_on_return_callback(self):
        mock_callback = mock.Mock()
        self.obj.add_on_return_callback(mock_callback)
        self.connection.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                              '_on_basic_return',
                                                              mock_callback,
                                                              False)

    def test_basic_ack_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.basic_ack)

    @mock.patch('pika.channel.Channel._validate_channel_and_callback')
    def test_basic_cancel_calls_validate(self, validate):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        callback_mock = mock.Mock()
        self.obj._consumers[consumer_tag] = callback_mock
        self.obj.basic_cancel(callback_mock, consumer_tag)
        validate.assert_called_once_with(callback_mock)

    @mock.patch('pika.spec.Basic.Ack')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_ack_calls_rpc(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj.basic_ack(1, False)
        rpc.assert_called_once_with(spec.Basic.Ack(1, False))

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_cancel_no_consumer_tag(self, rpc):
        self.obj._set_state(self.obj.OPEN)
        callback_mock = mock.Mock()
        consumer_tag = 'ctag0'
        self.obj.basic_cancel(callback_mock, consumer_tag)
        self.assertFalse(rpc.called)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_cancel_channel_cancelled_appended(self, unused):
        self.obj._set_state(self.obj.OPEN)
        callback_mock = mock.Mock()
        consumer_tag = 'ctag0'
        self.obj._consumers[consumer_tag] = mock.Mock()
        self.obj.basic_cancel(callback_mock, consumer_tag)
        self.assertListEqual(self.obj._cancelled, [consumer_tag])

    def test_basic_cancel_callback_appended(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        callback_mock = mock.Mock()
        self.obj._consumers[consumer_tag] = callback_mock
        self.obj.basic_cancel(callback_mock, consumer_tag)
        expectation = [self.obj.channel_number,
                       spec.Basic.CancelOk,
                       callback_mock]
        self.obj.callbacks.add.assert_any_call(*expectation)

    def test_basic_cancel_raises_value_error(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        callback_mock = mock.Mock()
        self.obj._consumers[consumer_tag] = callback_mock
        self.assertRaises(ValueError, self.obj.basic_cancel, callback_mock,
                          consumer_tag, nowait=True)

    def test_basic_cancel_on_basic_cancel_appended(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._consumers['ctag0'] = logging.debug
        self.obj.basic_cancel(consumer_tag='ctag0')
        expectation = [self.obj.channel_number,
                       spec.Basic.CancelOk,
                       self.obj._on_basic_cancel_ok]
        self.obj.callbacks.add.assert_any_call(*expectation)

    def test_basic_consume_channel_closed(self):
        mock_callback = mock.Mock()
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.basic_consume,
                          mock_callback, 'test-queue')

    @mock.patch('pika.channel.Channel._validate_channel_and_callback')
    def test_basic_consume_calls_validate(self, validate):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        validate.assert_called_once_with(mock_callback)

    def test_basic_consume_consumer_tag(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = 'ctag1.0'
        mock_callback = mock.Mock()
        self.assertEqual(self.obj.basic_consume(mock_callback, 'test-queue'),
                         expectation)

    def test_basic_consume_consumer_tag_appended(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertIn(consumer_tag, self.obj._consumers)

    def test_basic_consume_consumer_tag_in_consumers(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertIn(consumer_tag, self.obj._consumers)

    def test_basic_consume_duplicate_consumer_tag_raises(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj._consumers[consumer_tag] = logging.debug
        self.assertRaises(exceptions.DuplicateConsumerTag,
                          self.obj.basic_consume,
                          mock_callback,
                          'test-queue',
                          False, False, consumer_tag)

    def test_basic_consume_consumers_callback_value(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertEqual(self.obj._consumers[consumer_tag], mock_callback)

    def test_basic_consume_has_pending_list(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertIn(consumer_tag, self.obj._pending)

    def test_basic_consume_consumers_pending_list_is_empty(self):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertEqual(self.obj._pending[consumer_tag], list())

    @mock.patch('pika.spec.Basic.Consume')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_consumers_rpc_called(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        expectation = spec.Basic.Consume(queue='test-queue',
                                         consumer_tag=consumer_tag,
                                         no_ack=False,
                                         exclusive=False)
        rpc.assert_called_once_with(expectation,
                                    self.obj._on_event_ok,
                                    [spec.Basic.ConsumeOk])

    @mock.patch('pika.channel.Channel._validate_channel_and_callback')
    def test_basic_get_calls_validate(self, validate):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_get(mock_callback, 'test-queue')
        validate.assert_called_once_with(mock_callback)

    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_get_callback(self, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_get(mock_callback, 'test-queue')
        self.assertEqual(self.obj._on_get_ok_callback, mock_callback)

    @mock.patch('pika.spec.Basic.Get')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_get_send_mehtod_called(self, send_method, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_get(mock_callback, 'test-queue', False)
        send_method.assert_called_once_with(spec.Basic.Get(queue='test-queue',
                                                           no_ack=False))

    def test_basic_nack_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_nack,
                          0, False, True)

    @mock.patch('pika.spec.Basic.Nack')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_nack_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj.basic_nack(1, False, True)
        rpc.assert_called_once_with(spec.Basic.Nack(1, False, True))

    def test_basic_publish_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_publish,
                          'foo', 'bar', 'baz')

    @mock.patch('pika.channel.LOGGER')
    @mock.patch('pika.spec.Basic.Publish')
    @mock.patch('pika.channel.Channel._send_method')
    def test_immediate_called_logger_warning(self, send_method, unused, logger):
        self.obj._set_state(self.obj.OPEN)
        exchange = 'basic_publish_test'
        routing_key = 'routing-key-fun'
        body = 'This is my body'
        properties = spec.BasicProperties(content_type='text/plain')
        mandatory = False
        immediate = True
        self.obj.basic_publish(exchange, routing_key, body, properties,
                               mandatory, immediate)
        logger.warning.assert_called_once_with('The immediate flag is '
                                               'deprecated in RabbitMQ')

    @mock.patch('pika.spec.Basic.Publish')
    @mock.patch('pika.channel.Channel._send_method')
    def test_basic_publish_send_method_request(self, send_method, unused):
        self.obj._set_state(self.obj.OPEN)
        exchange = 'basic_publish_test'
        routing_key = 'routing-key-fun'
        body = 'This is my body'
        properties = spec.BasicProperties(content_type='text/plain')
        mandatory = False
        immediate = False
        self.obj.basic_publish(exchange, routing_key, body, properties,
                               mandatory, immediate)
        send_method.assert_called_once_with(spec.Basic.Publish(exchange=exchange,
                                                               routing_key=routing_key,
                                                               mandatory=mandatory,
                                                               immediate=immediate),
                                            (properties, body))

    def test_basic_qos_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_qos,
                          0, False, True)

    @mock.patch('pika.spec.Basic.Qos')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_qos_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_qos(mock_callback, 10, 20, False)
        rpc.assert_called_once_with(spec.Basic.Qos(mock_callback, 10, 20,
                                                   False),
                                    mock_callback, [spec.Basic.QosOk])

    def test_basic_reject_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_reject,
                          1, False)

    @mock.patch('pika.spec.Basic.Reject')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_reject_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj.basic_reject(1, True)
        rpc.assert_called_once_with(spec.Basic.Reject(1, True))

    def test_basic_recover_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.basic_qos,
                          0, False, True)

    @mock.patch('pika.spec.Basic.Recover')
    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_recover_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.basic_recover(mock_callback, True)
        rpc.assert_called_once_with(spec.Basic.Recover(mock_callback, True),
                                    mock_callback, [spec.Basic.RecoverOk])

    def test_close_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.close)

    def test_close_state(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj.close()
        self.assertEqual(self.obj._state, channel.Channel.CLOSING)

    def test_close_reply_info(self):
        reply_code, reply_text = (999, 'Test Reply')
        self.obj._set_state(self.obj.OPEN)
        self.obj.close(reply_code, reply_text)
        self.assertEqual((self.obj._reply_code, self.obj._reply_text),
                         (reply_code, reply_text))

    def test_close_basic_cancel_called(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj._consumers['abc'] = None
        with mock.patch.object(self.obj, 'basic_cancel') as basic_cancel:
            self.obj.close()
            basic_cancel.assert_called_once_with('abc')

    def test_close_calls_shutdown(self):
        self.obj._set_state(self.obj.OPEN)
        with mock.patch.object(self.obj, '_shutdown') as shutdown:
            self.obj.close()
            shutdown.assert_called_once_with()

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

    def test_confirm_delivery_callback_without_nowait_select_ok(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = [self.obj.channel_number,
                       spec.Confirm.SelectOk,
                       self.obj._on_confirm_select_ok]
        self.obj.confirm_delivery(logging.debug)
        self.obj.callbacks.add.assert_called_with(*expectation)

    def test_confirm_delivery_callback_with_nowait(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = [self.obj.channel_number,
                       spec.Confirm.SelectOk,
                       self.obj._on_confirm_select_ok]
        self.obj.confirm_delivery(logging.debug, True)
        self.assertNotIn(mock.call(*expectation),
                         self.obj.callbacks.add.call_args_list)

    def test_confirm_delivery_callback_basic_ack(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = (self.obj.channel_number,
                       spec.Basic.Ack,
                       logging.debug,
                       False)
        self.obj.confirm_delivery(logging.debug)
        self.obj.callbacks.add.assert_any_call(*expectation)

    def test_confirm_delivery_callback_basic_nack(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = (self.obj.channel_number,
                       spec.Basic.Nack,
                       logging.debug,
                       False)
        self.obj.confirm_delivery(logging.debug)
        self.obj.callbacks.add.assert_any_call(*expectation)

    def test_confirm_delivery_no_callback_callback_call_count(self):
        self.obj._set_state(self.obj.OPEN)
        self.obj.confirm_delivery()
        expectation = [mock.call(*[self.obj.channel_number,
                                   spec.Confirm.SelectOk,
                                   self.obj._on_synchronous_complete]),
                       mock.call(*[self.obj.channel_number,
                                   spec.Confirm.SelectOk,
                                   self.obj._on_confirm_select_ok])]
        self.assertEqual(self.obj.callbacks.add.call_args_list,
                         expectation)

    def test_confirm_delivery_no_callback_no_basic_ack_callback(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = [self.obj.channel_number,
                       spec.Basic.Ack,
                       None,
                       False]
        self.obj.confirm_delivery()
        self.assertNotIn(mock.call(*expectation),
                         self.obj.callbacks.add.call_args_list)

    def test_confirm_delivery_no_callback_no_basic_nack_callback(self):
        self.obj._set_state(self.obj.OPEN)
        expectation = [self.obj.channel_number,
                       spec.Basic.Nack,
                       None,
                       False]
        self.obj.confirm_delivery()
        self.assertNotIn(mock.call(*expectation),
                         self.obj.callbacks.add.call_args_list)

    def test_consumer_tags(self):
        self.assertListEqual(self.obj.consumer_tags, self.obj._consumers.keys())

    def test_exchange_bind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.exchange_bind,
                          None, 'foo', 'bar', 'baz')

    def test_exchange_bind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.exchange_bind,
                          'callback', 'foo', 'bar', 'baz')

    @mock.patch('pika.spec.Exchange.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_bind_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_bind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(spec.Exchange.Bind(mock_callback,
                                                       'foo', 'bar', 'baz'),
                                    mock_callback, [spec.Exchange.BindOk])

    @mock.patch('pika.spec.Exchange.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_bind_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_bind(mock_callback, 'foo', 'bar', 'baz', nowait=True)
        rpc.assert_called_once_with(spec.Exchange.Bind(mock_callback,
                                                       'foo', 'bar', 'baz'),
                                    mock_callback, [])

    def test_exchange_declare_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.exchange_declare,
                          exchange='foo')

    def test_exchange_declare_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.exchange_declare,
                          'callback', 'foo')

    @mock.patch('pika.spec.Exchange.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_declare_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_declare(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Exchange.Declare(mock_callback,
                                                         'foo'),
                                    mock_callback, [spec.Exchange.DeclareOk])

    @mock.patch('pika.spec.Exchange.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_declare_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_declare(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Exchange.Declare(mock_callback,
                                                          'foo'),
                                    mock_callback, [])

    def test_exchange_delete_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.exchange_delete,
                          exchange='foo')

    def test_exchange_delete_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.exchange_delete,
                          'callback', 'foo')

    @mock.patch('pika.spec.Exchange.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_delete_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_delete(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Exchange.Delete(mock_callback,
                                                          'foo'),
                                    mock_callback, [spec.Exchange.DeleteOk])

    @mock.patch('pika.spec.Exchange.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_delete_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_delete(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Exchange.Delete(mock_callback,
                                                          'foo'),
                                    mock_callback, [])

    def test_exchange_unbind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.exchange_unbind,
                          None, 'foo', 'bar', 'baz')

    def test_exchange_unbind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.exchange_unbind,
                          'callback', 'foo', 'bar', 'baz')

    @mock.patch('pika.spec.Exchange.Unbind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_unbind_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_unbind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(spec.Exchange.Unbind(mock_callback,
                                                          'foo', 'bar', 'baz'),
                                    mock_callback, [spec.Exchange.UnbindOk])

    @mock.patch('pika.spec.Exchange.Unbind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_exchange_unbind_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.exchange_unbind(mock_callback, 'foo', 'bar', 'baz',
                                 nowait=True)
        rpc.assert_called_once_with(spec.Exchange.Unbind(mock_callback,
                                                           'foo', 'bar', 'baz'),
                                    mock_callback, [])

    def test_flow_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.flow, 'foo', True)

    def test_flow_raises_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj.flow, 'foo', True)

    @mock.patch('pika.spec.Channel.Flow')
    @mock.patch('pika.channel.Channel._rpc')
    def test_flow_on_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.flow(mock_callback, True)
        rpc.assert_called_once_with(spec.Channel.Flow(True),
                                    self.obj._on_flow_ok,
                                    [spec.Channel.FlowOk])

    @mock.patch('pika.spec.Channel.Flow')
    @mock.patch('pika.channel.Channel._rpc')
    def test_flow_off_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.flow(mock_callback, False)
        rpc.assert_called_once_with(spec.Channel.Flow(False),
                                    self.obj._on_flow_ok,
                                    [spec.Channel.FlowOk])

    @mock.patch('pika.channel.Channel._rpc')
    def test_flow_on_flow_ok_callback(self, rpc):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.flow(mock_callback, True)
        self.assertEqual(self.obj._on_flow_ok_callback, mock_callback)

    def test__handle_content_frame_method_unexpected_frame_type_raises(self):
        self.assertRaises(exceptions.UnexpectedFrameError,
                          self.obj._handle_content_frame,
                          frame.Method(1, spec.Basic.Ack()))

    def test__handle_content_frame_method_returns_none(self):
        frame_value = frame.Method(1, spec.Basic.Deliver('ctag0', 1))
        self.assertEqual(self.obj._handle_content_frame(frame_value), None)

    def test__handle_content_frame_sets_method_frame(self):
        frame_value = frame.Method(1, spec.Basic.Deliver('ctag0', 1))
        self.obj._handle_content_frame(frame_value)
        self.assertEqual(self.obj.frame_dispatcher._method_frame, frame_value)

    def test__handle_content_frame_sets_header_frame(self):
        frame_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(frame_value)
        self.assertEqual(self.obj.frame_dispatcher._header_frame, frame_value)

    def test__handle_content_frame_basic_deliver_called(self):
        method_value = frame.Method(1, spec.Basic.Deliver('ctag0', 1))
        self.obj._handle_content_frame(method_value)
        header_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(header_value)
        body_value = frame.Body(1, '0123456789')
        with mock.patch.object(self.obj, '_on_basic_deliver') as deliver:
            self.obj._handle_content_frame(body_value)
            deliver.assert_called_once_with(method_value, header_value,
                                            '0123456789')

    def test__handle_content_frame_basic_get_called(self):
        method_value = frame.Method(1, spec.Basic.GetOk('ctag0', 1))
        self.obj._handle_content_frame(method_value)
        header_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(header_value)
        body_value = frame.Body(1, '0123456789')
        with mock.patch.object(self.obj, '_on_basic_get_ok') as basic_get_ok:
            self.obj._handle_content_frame(body_value)
            basic_get_ok.assert_called_once_with(method_value, header_value,
                                                 '0123456789')

    def test__handle_content_frame_basic_return_called(self):
        method_value = frame.Method(1, spec.Basic.Return(999, 'Reply Text',
                                                         'exchange_value',
                                                         'routing.key'))
        self.obj._handle_content_frame(method_value)
        header_value = frame.Header(1, 10, spec.BasicProperties())
        self.obj._handle_content_frame(header_value)
        body_value = frame.Body(1, '0123456789')
        with mock.patch.object(self.obj, '_on_basic_return') as basic_return:
            self.obj._handle_content_frame(body_value)
            basic_return.assert_called_once_with(method_value, header_value,
                                                 '0123456789')

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
    def test_channel_open_add_callbacks_called(self, rpc):
        with mock.patch.object(self.obj, '_add_callbacks') as _add_callbacks:
            self.obj.open()
            _add_callbacks.assert_called_once_with()

    def test_queue_bind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.queue_bind,
                          None, 'foo', 'bar', 'baz')

    def test_queue_bind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.queue_bind,
                          'callback', 'foo', 'bar', 'baz')

    @mock.patch('pika.spec.Queue.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_bind_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_bind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(spec.Queue.Bind(mock_callback,
                                                       'foo', 'bar', 'baz'),
                                    mock_callback, [spec.Queue.BindOk])

    @mock.patch('pika.spec.Queue.Bind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_bind_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_bind(mock_callback, 'foo', 'bar', 'baz', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Bind(mock_callback,
                                                       'foo', 'bar', 'baz'),
                                    mock_callback, [])

    def test_queue_declare_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.queue_declare,
                          None,
                          queue='foo')

    def test_queue_declare_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.queue_declare,
                          'callback', 'foo')

    @mock.patch('pika.spec.Queue.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_declare_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_declare(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Queue.Declare(mock_callback,
                                                          'foo'),
                                    mock_callback, [spec.Queue.DeclareOk])

    @mock.patch('pika.spec.Queue.Declare')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_declare_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_declare(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Declare(mock_callback,
                                                          'foo'),
                                    mock_callback, [])

    def test_queue_delete_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.queue_delete,
                          queue='foo')

    def test_queue_delete_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.queue_delete,
                          'callback', 'foo')

    @mock.patch('pika.spec.Queue.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_delete_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_delete(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Queue.Delete(mock_callback,
                                                      'foo'),
                                    mock_callback, [spec.Queue.DeleteOk])

    @mock.patch('pika.spec.Queue.Delete')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_delete_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_delete(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Delete(mock_callback,
                                                      'foo'),
                                    mock_callback, [])

    def test_queue_purge_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.queue_purge,
                          queue='foo')

    def test_queue_purge_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.queue_purge,
                          'callback', 'foo')

    @mock.patch('pika.spec.Queue.Purge')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_purge_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_purge(mock_callback, 'foo')
        rpc.assert_called_once_with(spec.Queue.Purge(mock_callback,
                                                         'foo'),
                                    mock_callback, [spec.Queue.PurgeOk])

    @mock.patch('pika.spec.Queue.Purge')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_purge_rpc_request_nowait(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_purge(mock_callback, 'foo', nowait=True)
        rpc.assert_called_once_with(spec.Queue.Purge(mock_callback,
                                                         'foo'),
                                    mock_callback, [])

    def test_queue_unbind_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj.queue_unbind,
                          None, 'foo', 'bar', 'baz')

    def test_queue_unbind_raises_value_error_on_invalid_callback(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError,
                          self.obj.queue_unbind,
                          'callback', 'foo', 'bar', 'baz')

    @mock.patch('pika.spec.Queue.Unbind')
    @mock.patch('pika.channel.Channel._rpc')
    def test_queue_unbind_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.queue_unbind(mock_callback, 'foo', 'bar', 'baz')
        rpc.assert_called_once_with(spec.Queue.Unbind(mock_callback,
                                                         'foo', 'bar', 'baz'),
                                    mock_callback, [spec.Queue.UnbindOk])

    def test_tx_commit_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed, self.obj.tx_commit, None)

    @mock.patch('pika.spec.Tx.Commit')
    @mock.patch('pika.channel.Channel._rpc')
    def test_tx_commit_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.tx_commit(mock_callback)
        rpc.assert_called_once_with(spec.Tx.Commit(mock_callback),
                                    mock_callback, [spec.Tx.CommitOk])

    @mock.patch('pika.spec.Tx.Rollback')
    @mock.patch('pika.channel.Channel._rpc')
    def test_tx_rollback_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.tx_rollback(mock_callback)
        rpc.assert_called_once_with(spec.Tx.Rollback(mock_callback),
                                    mock_callback, [spec.Tx.RollbackOk])

    @mock.patch('pika.spec.Tx.Select')
    @mock.patch('pika.channel.Channel._rpc')
    def test_tx_select_rpc_request(self, rpc, unused):
        self.obj._set_state(self.obj.OPEN)
        mock_callback = mock.Mock()
        self.obj.tx_select(mock_callback)
        rpc.assert_called_once_with(spec.Tx.Select(mock_callback),
                                    mock_callback, [spec.Tx.SelectOk])






    def test_send_method(self):
        expectation = [2, 3]
        with mock.patch.object(self.obj.connection,
                               '_send_method') as send_method:
            self.obj._send_method(*expectation)
            send_method.assert_called_once_with(*[self.obj.channel_number] +
                                                 expectation)

    def test_set_state(self):
        self.obj._state = channel.Channel.CLOSED
        self.obj._set_state(channel.Channel.OPENING)
        self.assertEqual(self.obj._state, channel.Channel.OPENING)

    @mock.patch('pika.spec.Channel.Close')
    @mock.patch('pika.channel.Channel._send_method')
    def test_shutdown_send_method_called(self, send_method, unused):
        self.obj._set_state(self.obj.OPEN)
        self.obj._shutdown()
        expectation = spec.Channel.Close(self.obj._reply_code,
                                         self.obj._reply_text,
                                         0, 0)
        send_method.assert_called_once_with(expectation)

    def test_shutdown_state(self):
        self.obj._shutdown()
        self.assertEqual(self.obj._state, channel.Channel.CLOSING)

    def test_validate_channel_and_callback_raises_channel_closed(self):
        self.assertRaises(exceptions.ChannelClosed,
                          self.obj._validate_channel_and_callback,
                          None)

    def test_validate_channel_and_callback_raises_value_error_not_callable(self):
        self.obj._set_state(self.obj.OPEN)
        self.assertRaises(ValueError, self.obj._validate_channel_and_callback,
                          'foo')
