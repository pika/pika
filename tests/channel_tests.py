"""
Tests for pika.channel.ContentFrameDispatcher

"""
import collections
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import channel
from pika import exceptions
from pika import spec


class ChannelTest(unittest.TestCase):

    @mock.patch('pika.connection.Connection')
    def _create_connection(self, connection):
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


    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_cancel_callback_appended(self, unused):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag0'
        callback_mock = mock.Mock()
        self.obj._consumers[consumer_tag] = callback_mock
        self.obj.basic_cancel(callback_mock, consumer_tag)
        self.obj.callbacks.add.assert_called_once_with(self.obj.channel_number,
                                                       spec.Basic.CancelOk,
                                                       callback_mock)

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

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_consumer_tag(self, unused):
        self.obj._set_state(self.obj.OPEN)
        expectation = 'ctag1.0'
        mock_callback = mock.Mock()
        self.assertEqual(self.obj.basic_consume(mock_callback, 'test-queue'),
                         expectation)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_consumer_tag_appended(self, unused):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertIn(consumer_tag, self.obj._consumers)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_consumer_tag_in_consumers(self, unused):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertIn(consumer_tag, self.obj._consumers)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_consumers_callback_value(self, unused):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertEqual(self.obj._consumers[consumer_tag], mock_callback)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_has_pending_list(self, unused):
        self.obj._set_state(self.obj.OPEN)
        consumer_tag = 'ctag1.0'
        mock_callback = mock.Mock()
        self.obj.basic_consume(mock_callback, 'test-queue')
        self.assertIn(consumer_tag, self.obj._pending)

    @mock.patch('pika.channel.Channel._rpc')
    def test_basic_consume_consumers_pending_list_is_empty(self, unused):
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
        self.obj.basic_publish(exchange, routing_key, body, properties, mandatory, immediate)
        logger.warning.assert_called_once_with('The immediate flag is deprecated in RabbitMQ')

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
        self.obj.basic_publish(exchange, routing_key, body, properties, mandatory, immediate)
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
