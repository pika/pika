"""
Tests for pika.connection.Connection

"""

# Suppress pylint warnings concerning access to protected member
# pylint: disable=W0212

# Suppress pylint messages concerning missing docstrings
# pylint: disable=C0111

# Suppress pylint messages concerning invalid method name
# pylint: disable=C0103

try:
    import mock
except ImportError:
    from unittest import mock  # pylint: disable=E0611

import random
import platform
import unittest

import mock

from pika import connection, channel, credentials, exceptions, frame, spec
import pika
from pika.compat import xrange


def dummy_callback():
    """Callback method to use in tests"""
    pass


class ConstructibleConnection(connection.Connection):
    """Adds dummy overrides for `Connection`'s abstract methods so
    that we can instantiate and test it.

    """
    def _adapter_connect_stream(self):
        raise NotImplementedError

    def _adapter_disconnect_stream(self):
        raise NotImplementedError

    def _adapter_call_later(self, delay, callback):
        raise NotImplementedError

    def _adapter_remove_timeout(self, timeout_id):
        raise NotImplementedError

    def _adapter_add_callback_threadsafe(self, callback):
        raise NotImplementedError

    def _adapter_emit_data(self, data):
        raise NotImplementedError


class ConnectionTests(unittest.TestCase):  # pylint: disable=R0904
    def setUp(self):
        class ChannelTemplate(channel.Channel):
            channel_number = None

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            self.connection = ConstructibleConnection()
            self.connection._set_connection_state(
                connection.Connection.CONNECTION_OPEN)
            self.connection._opened = True

        self.channel = mock.Mock(spec=ChannelTemplate)
        self.channel.channel_number = 1
        self.channel.is_open = True
        self.channel.is_closing = False
        self.channel.is_closed = False
        self.connection._channels[self.channel.channel_number] = self.channel

    def tearDown(self):
        del self.connection
        del self.channel

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_close_calls_on_close_ready_when_no_channels(
            self, on_close_ready_mock):
        self.connection._channels = dict()
        self.connection.close()
        self.assertTrue(on_close_ready_mock.called,
                        'on_close_ready_mock should have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_close_closes_open_channels(self, on_close_ready):
        self.connection.close()
        self.channel.close.assert_called_once_with(200, 'Normal shutdown')
        self.assertFalse(on_close_ready.called)

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_close_closes_opening_channels(self, on_close_ready):
        self.channel.is_open = False
        self.channel.is_closing = False
        self.channel.is_closed = False
        self.connection.close()
        self.channel.close.assert_called_once_with(200, 'Normal shutdown')
        self.assertFalse(on_close_ready.called)

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_close_does_not_close_closing_channels(self, on_close_ready):
        self.channel.is_open = False
        self.channel.is_closing = True
        self.channel.is_closed = False
        self.connection.close()
        self.assertFalse(self.channel.close.called)
        self.assertFalse(on_close_ready.called)

    @mock.patch('pika.connection.Connection._close_channels')
    def test_close_raises_wrong_state_when_already_closed_or_closing(
            self, close_channels):
        for closed_state in (self.connection.CONNECTION_CLOSED,
                             self.connection.CONNECTION_CLOSING):
            self.connection.connection_state = closed_state
            with self.assertRaises(exceptions.ConnectionWrongStateError):
                self.connection.close()
            self.assertEqual(self.channel.close.call_count, 0)
            self.assertEqual(self.connection.connection_state, closed_state)

    @mock.patch('logging.Logger.critical')
    def test_deliver_frame_to_channel_with_frame_for_unknown_channel(
            self, critical_mock):
        unknown_channel_num = 99
        self.assertNotIn(unknown_channel_num, self.connection._channels)

        unexpected_frame = frame.Method(unknown_channel_num, mock.Mock())
        self.connection._deliver_frame_to_channel(unexpected_frame)

        critical_mock.assert_called_once_with(
            'Received %s frame for unregistered channel %i on %s',
            unexpected_frame.NAME, unknown_channel_num, self.connection)

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_cleanup_with_closing_channels(self, on_close_ready):
        """if connection is closing but closing channels remain, do not call \
        _on_close_ready

        """
        self.channel.is_open = False
        self.channel.is_closing = True
        self.channel.is_closed = False

        self.connection.close()
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_cleanup_closing_state_last_channel_calls_on_close_ready(
            self, on_close_ready_mock):
        self.connection.connection_state = self.connection.CONNECTION_CLOSING

        self.connection._on_channel_cleanup(self.channel)

        self.assertTrue(on_close_ready_mock.called,
                        '_on_close_ready should have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_cleanup_closing_state_more_channels_no_on_close_ready(
            self, on_close_ready_mock):
        self.connection.connection_state = self.connection.CONNECTION_CLOSING
        channel_mock = mock.Mock(channel_number=99, is_closing=True)
        self.connection._channels[99] = channel_mock

        self.connection._on_channel_cleanup(self.channel)

        self.assertFalse(on_close_ready_mock.called,
                         '_on_close_ready should not have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_cleanup_non_closing_state(self, on_close_ready):
        """if connection isn't closing _on_close_ready should not be called"""
        self.connection._on_channel_cleanup(mock.Mock())
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    def test_on_stream_terminated_cleans_up(self):
        """_on_stream_terminated cleans up heartbeat, adapter, and channels"""
        heartbeat = mock.Mock()
        self.connection._heartbeat_checker = heartbeat
        self.connection._adapter_disconnect_stream = mock.Mock()

        original_exc = Exception('something terrible')
        self.connection._on_stream_terminated(original_exc)

        heartbeat.stop.assert_called_once_with()

        self.channel._on_close_meta.assert_called_once_with(original_exc)

        self.assertTrue(self.connection.is_closed)

    def test_on_stream_terminated_invokes_connection_closed_callback(self):
        """_on_stream_terminated invokes `Connection.ON_CONNECTION_CLOSED` callbacks"""
        self.connection.callbacks.process = mock.Mock(
            wraps=self.connection.callbacks.process)

        self.connection._adapter_disconnect_stream = mock.Mock()

        self.connection._on_stream_terminated(Exception(1, 'error text'))

        self.connection.callbacks.process.assert_called_once_with(
            0, self.connection.ON_CONNECTION_CLOSED, self.connection,
            self.connection, mock.ANY)

        with self.assertRaises(AssertionError):
            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR, self.connection,
                self.connection, mock.ANY)

    def test_on_stream_terminated_invokes_protocol_on_connection_error_and_closed(
            self):
        """_on_stream_terminated invokes `ON_CONNECTION_ERROR` with \
        `IncompatibleProtocolError` and `ON_CONNECTION_CLOSED` callbacks"""
        with mock.patch.object(self.connection.callbacks, 'process'):

            self.connection._adapter_disconnect_stream = mock.Mock()

            self.connection._set_connection_state(
                self.connection.CONNECTION_PROTOCOL)
            self.connection._opened = False

            original_exc = exceptions.StreamLostError(1, 'error text')
            self.connection._on_stream_terminated(original_exc)

            self.assertEqual(self.connection.callbacks.process.call_count, 1)

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR, self.connection,
                self.connection, mock.ANY)

            conn_exc = self.connection.callbacks.process.call_args_list[0][0][
                4]
            self.assertIs(type(conn_exc), exceptions.IncompatibleProtocolError)
            self.assertSequenceEqual(conn_exc.args, [repr(original_exc)])

    def test_on_stream_terminated_invokes_auth_on_connection_error_and_closed(self):
        """_on_stream_terminated invokes `ON_CONNECTION_ERROR` with \
        `ProbableAuthenticationError` and `ON_CONNECTION_CLOSED` callbacks"""
        with mock.patch.object(self.connection.callbacks, 'process'):

            self.connection._adapter_disconnect_stream = mock.Mock()

            self.connection._set_connection_state(
                self.connection.CONNECTION_START)
            self.connection._opened = False

            original_exc = exceptions.StreamLostError(1, 'error text')
            self.connection._on_stream_terminated(original_exc)

            self.assertEqual(self.connection.callbacks.process.call_count, 1)

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR, self.connection,
                self.connection, mock.ANY)

            conn_exc = self.connection.callbacks.process.call_args_list[0][0][
                4]
            self.assertIs(
                type(conn_exc), exceptions.ProbableAuthenticationError)
            self.assertSequenceEqual(conn_exc.args, [repr(original_exc)])

    def test_on_stream_terminated_invokes_access_denied_on_connection_error_and_closed(
            self):
        """_on_stream_terminated invokes `ON_CONNECTION_ERROR` with \
        `ProbableAccessDeniedError` and `ON_CONNECTION_CLOSED` callbacks"""
        with mock.patch.object(self.connection.callbacks, 'process'):

            self.connection._adapter_disconnect_stream = mock.Mock()

            self.connection._set_connection_state(
                self.connection.CONNECTION_TUNE)
            self.connection._opened = False

            original_exc = exceptions.StreamLostError(1, 'error text')
            self.connection._on_stream_terminated(original_exc)

            self.assertEqual(self.connection.callbacks.process.call_count, 1)

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR, self.connection,
                self.connection, mock.ANY)

            conn_exc = self.connection.callbacks.process.call_args_list[0][0][
                4]
            self.assertIs(type(conn_exc), exceptions.ProbableAccessDeniedError)
            self.assertSequenceEqual(conn_exc.args, [repr(original_exc)])

    @mock.patch('pika.connection.Connection._adapter_connect_stream')
    def test_new_conn_should_use_first_channel(self, connect):
        """_next_channel_number in new conn should always be 1"""
        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection()

        self.assertEqual(1, conn._next_channel_number())

    def test_next_channel_number_returns_lowest_unused(self):
        """_next_channel_number must return lowest available channel number"""
        for channel_num in xrange(1, 50):
            self.connection._channels[channel_num] = True
        expectation = random.randint(5, 49)
        del self.connection._channels[expectation]
        self.assertEqual(self.connection._next_channel_number(), expectation)

    def test_add_callbacks(self):
        """make sure the callback adding works"""
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)
        for test_method, expected_key in (
            (self.connection.add_on_open_callback,
             self.connection.ON_CONNECTION_OPEN_OK),
            (self.connection.add_on_close_callback,
             self.connection.ON_CONNECTION_CLOSED)):
            self.connection.callbacks.reset_mock()
            test_method(dummy_callback)
            self.connection.callbacks.add.assert_called_once_with(
                0, expected_key, dummy_callback, False)

    def test_add_on_close_callback(self):
        """make sure the add on close callback is added"""
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)
        self.connection.add_on_open_callback(dummy_callback)
        self.connection.callbacks.add.assert_called_once_with(
            0, self.connection.ON_CONNECTION_OPEN_OK, dummy_callback, False)

    def test_add_on_open_error_callback(self):
        """make sure the add on open error callback is added"""
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)
        #Test with remove default first (also checks default is True)
        self.connection.add_on_open_error_callback(dummy_callback)
        self.connection.callbacks.remove.assert_called_once_with(
            0, self.connection.ON_CONNECTION_ERROR,
            self.connection._default_on_connection_error)
        self.connection.callbacks.add.assert_called_once_with(
            0, self.connection.ON_CONNECTION_ERROR, dummy_callback, False)

    def test_channel(self):
        """test the channel method"""
        self.connection._next_channel_number = mock.Mock(return_value=42)
        test_channel = mock.Mock(spec=channel.Channel)
        self.connection._create_channel = mock.Mock(return_value=test_channel)
        self.connection._add_channel_callbacks = mock.Mock()
        ret_channel = self.connection.channel(on_open_callback=dummy_callback)
        self.assertEqual(test_channel, ret_channel)
        self.connection._create_channel.assert_called_once_with(
            42, dummy_callback)
        self.connection._add_channel_callbacks.assert_called_once_with(42)
        test_channel.open.assert_called_once_with()

    def test_channel_on_closed_connection_raises_connection_closed(self):
        self.connection.connection_state = self.connection.CONNECTION_CLOSED
        with self.assertRaises(exceptions.ConnectionWrongStateError):
            self.connection.channel(on_open_callback=lambda *args: None)

    def test_channel_on_closing_connection_raises_connection_closed(self):
        self.connection.connection_state = self.connection.CONNECTION_CLOSING
        with self.assertRaises(exceptions.ConnectionWrongStateError):
            self.connection.channel(on_open_callback=lambda *args: None)

    def test_channel_on_init_connection_raises_connection_closed(self):
        self.connection.connection_state = self.connection.CONNECTION_INIT
        with self.assertRaises(exceptions.ConnectionWrongStateError):
            self.connection.channel(on_open_callback=lambda *args: None)

    def test_channel_on_start_connection_raises_connection_closed(self):
        self.connection.connection_state = self.connection.CONNECTION_START
        with self.assertRaises(exceptions.ConnectionWrongStateError):
            self.connection.channel(on_open_callback=lambda *args: None)

    def test_channel_on_protocol_connection_raises_connection_closed(self):
        self.connection.connection_state = self.connection.CONNECTION_PROTOCOL
        with self.assertRaises(exceptions.ConnectionWrongStateError):
            self.connection.channel(on_open_callback=lambda *args: None)

    def test_channel_on_tune_connection_raises_connection_closed(self):
        self.connection.connection_state = self.connection.CONNECTION_TUNE
        with self.assertRaises(exceptions.ConnectionWrongStateError):
            self.connection.channel(on_open_callback=lambda *args: None)

    def test_connect_no_adapter_connect_from_constructor_with_external_workflow(self):
        """check that adapter connection is not happening in constructor with external connection workflow."""
        with mock.patch.object(
                ConstructibleConnection,
                '_adapter_connect_stream') as adapter_connect_stack_mock:
            conn = ConstructibleConnection(internal_connection_workflow=False)

        self.assertFalse(adapter_connect_stack_mock.called)

        self.assertEqual(conn.connection_state, conn.CONNECTION_INIT)

    def test_client_properties(self):
        """make sure client properties has some important keys"""
        client_props = self.connection._client_properties
        self.assertTrue(isinstance(client_props, dict))
        for required_key in ('product', 'platform', 'capabilities',
                             'information', 'version'):
            self.assertTrue(required_key in client_props,
                            '%s missing' % required_key)

    def test_client_properties_default(self):
        expectation = {
            'product': connection.PRODUCT,
            'platform': 'Python %s' % platform.python_version(),
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See http://pika.rtfd.org',
            'version': pika.__version__
        }
        self.assertDictEqual(self.connection._client_properties, expectation)

    def test_client_properties_override(self):
        expectation = {
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            }
        }
        override = {
            'product': 'My Product',
            'platform': 'Your platform',
            'version': '0.1',
            'information': 'this is my app'
        }
        expectation.update(override)

        params = connection.ConnectionParameters(client_properties=override)

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection(params)

        self.assertDictEqual(conn._client_properties, expectation)

    def test_close_channels(self):
        """test closing all channels"""
        self.connection.connection_state = self.connection.CONNECTION_OPEN
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)

        opening_channel = mock.Mock(
            is_open=False, is_closed=False, is_closing=False)
        open_channel = mock.Mock(
            is_open=True, is_closed=False, is_closing=False)
        closing_channel = mock.Mock(
            is_open=False, is_closed=False, is_closing=True)
        self.connection._channels = {
            'openingc': opening_channel,
            'openc': open_channel,
            'closingc': closing_channel
        }

        self.connection._close_channels(400, 'reply text')

        opening_channel.close.assert_called_once_with(400, 'reply text')
        open_channel.close.assert_called_once_with(400, 'reply text')
        self.assertFalse(closing_channel.close.called)

        self.assertTrue('openingc' in self.connection._channels)
        self.assertTrue('openc' in self.connection._channels)
        self.assertTrue('closingc' in self.connection._channels)

        self.assertFalse(self.connection.callbacks.cleanup.called)

        # Test on closed connection
        self.connection.connection_state = self.connection.CONNECTION_CLOSED
        with self.assertRaises(AssertionError):
            self.connection._close_channels(200, 'reply text')

    @mock.patch('pika.frame.ProtocolHeader')
    def test_on_stream_connected(self, frame_protocol_header):
        """make sure the _on_stream_connected() sets the state and sends a frame"""
        self.connection.connection_state = self.connection.CONNECTION_INIT
        self.connection._adapter_connect = mock.Mock(return_value=None)
        self.connection._send_frame = mock.Mock()
        frame_protocol_header.spec = frame.ProtocolHeader
        frame_protocol_header.return_value = 'frame object'
        self.connection._on_stream_connected()
        self.assertEqual(self.connection.CONNECTION_PROTOCOL,
                         self.connection.connection_state)
        self.connection._send_frame.assert_called_once_with('frame object')

    def test_on_connection_start(self):
        """make sure starting a connection sets the correct class vars"""
        method_frame = mock.Mock()
        method_frame.method = mock.Mock()
        method_frame.method.mechanisms = str(credentials.PlainCredentials.TYPE)
        method_frame.method.version_major = 0
        method_frame.method.version_minor = 9
        #This may be incorrectly mocked, or the code is wrong
        #TODO: Code does hasattr check, should this be a has_key/in check?
        method_frame.method.server_properties = {
            'capabilities': {
                'basic.nack': True,
                'consumer_cancel_notify': False,
                'exchange_exchange_bindings': False
            }
        }
        #This will be called, but shoudl not be implmented here, just mock it
        self.connection._flush_outbound = mock.Mock()
        self.connection._adapter_emit_data = mock.Mock()
        self.connection._on_connection_start(method_frame)
        self.assertEqual(True, self.connection.basic_nack)
        self.assertEqual(False, self.connection.consumer_cancel_notify)
        self.assertEqual(False, self.connection.exchange_exchange_bindings)
        self.assertEqual(False, self.connection.publisher_confirms)

    @mock.patch('pika.heartbeat.HeartbeatChecker')
    @mock.patch('pika.frame.Method')
    @mock.patch.object(
        ConstructibleConnection,
        '_adapter_emit_data',
        spec_set=connection.Connection._adapter_emit_data)
    def test_on_connection_tune(self,
                                _adapter_emit_data,
                                method,
                                heartbeat_checker):
        """make sure _on_connection_tune tunes the connection params"""
        heartbeat_checker.return_value = 'hearbeat obj'
        self.connection._flush_outbound = mock.Mock()
        marshal = mock.Mock(return_value='ab')
        method.return_value = mock.Mock(marshal=marshal)
        #may be good to test this here, but i don't want to test too much
        self.connection._rpc = mock.Mock()

        method_frame = mock.Mock()
        method_frame.method = mock.Mock()
        method_frame.method.channel_max = 40
        method_frame.method.frame_max = 10000
        method_frame.method.heartbeat = 10

        self.connection.params.channel_max = 20
        self.connection.params.frame_max = 20000
        self.connection.params.heartbeat = 20

        #Test
        self.connection._on_connection_tune(method_frame)

        #verfy
        self.assertEqual(self.connection.CONNECTION_TUNE,
                         self.connection.connection_state)
        self.assertEqual(20, self.connection.params.channel_max)
        self.assertEqual(10000, self.connection.params.frame_max)
        self.assertEqual(20, self.connection.params.heartbeat)
        self.assertEqual(9992, self.connection._body_max_length)
        heartbeat_checker.assert_called_once_with(self.connection, 20)
        self.assertEqual(
            ['ab'],
            [call[0][0] for call in _adapter_emit_data.call_args_list])
        self.assertEqual('hearbeat obj', self.connection._heartbeat_checker)

        # Pika gives precendence to client heartbeat values if set
        # See pika/pika#965.

        # Both client and server values set. Pick client value
        method_frame.method.heartbeat = 60
        self.connection.params.heartbeat = 20
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(20, self.connection.params.heartbeat)

        # Client value is None, use the server's
        method_frame.method.heartbeat = 500
        self.connection.params.heartbeat = None
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(500, self.connection.params.heartbeat)

        # Client value is 0, use it
        method_frame.method.heartbeat = 60
        self.connection.params.heartbeat = 0
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(0, self.connection.params.heartbeat)

        # Server value is 0, client value is None
        method_frame.method.heartbeat = 0
        self.connection.params.heartbeat = None
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(0, self.connection.params.heartbeat)

        # Both client and server values are 0
        method_frame.method.heartbeat = 0
        self.connection.params.heartbeat = 0
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(0, self.connection.params.heartbeat)

        # Server value is 0, use the client's
        method_frame.method.heartbeat = 0
        self.connection.params.heartbeat = 60
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(60, self.connection.params.heartbeat)

        # Server value is 10, client passes a heartbeat function that
        # chooses max(servervalue,60). Pick 60
        def choose_max(conn, val):
            self.assertIs(conn, self.connection)
            self.assertEqual(val, 10)
            return max(val, 60)

        method_frame.method.heartbeat = 10
        self.connection.params.heartbeat = choose_max
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(60, self.connection.params.heartbeat)

    def test_on_connection_close_from_broker_passes_correct_exception(self):
        """make sure connection close from broker passes correct exception"""
        method_frame = mock.Mock()
        method_frame.method = mock.Mock(spec=spec.Connection.Close)
        method_frame.method.reply_code = 1
        method_frame.method.reply_text = 'hello'
        self.connection._terminate_stream = mock.Mock()
        self.connection._on_connection_close_from_broker(method_frame)

        #Check
        self.connection._terminate_stream.assert_called_once_with(mock.ANY)

        exc = self.connection._terminate_stream.call_args[0][0]
        self.assertIsInstance(exc, exceptions.ConnectionClosedByBroker)

        self.assertEqual(exc.reply_code, 1)
        self.assertEqual(exc.reply_text, 'hello')

    def test_on_connection_close_ok(self):
        """make sure _on_connection_close_ok terminates connection"""
        method_frame = mock.Mock()
        method_frame.method = mock.Mock(spec=spec.Connection.CloseOk)
        self.connection._terminate_stream = mock.Mock()

        self.connection._on_connection_close_ok(method_frame)

        #Check
        self.connection._terminate_stream.assert_called_once_with(None)

    @mock.patch('pika.frame.decode_frame')
    def test_on_data_available(self, decode_frame):
        """test on data available and process frame"""
        data_in = ['data']
        self.connection._frame_buffer = ['old_data']
        for frame_type in (frame.Method, spec.Basic.Deliver, frame.Heartbeat):
            frame_value = mock.Mock(spec=frame_type)
            frame_value.frame_type = 2
            frame_value.method = 2
            frame_value.channel_number = 1
            self.connection.bytes_received = 0
            self.connection._heartbeat_checker = mock.Mock()
            self.connection.frames_received = 0
            decode_frame.return_value = (2, frame_value)
            self.connection._on_data_available(data_in)
            #test value
            self.assertListEqual([], self.connection._frame_buffer)
            self.assertEqual(2, self.connection.bytes_received)
            self.assertEqual(1, self.connection.frames_received)
            if frame_type == frame.Heartbeat:
                self.assertTrue(
                    self.connection._heartbeat_checker.received.called)

    def test_add_on_connection_blocked_callback(self):
        blocked_buffer = []
        self.connection.add_on_connection_blocked_callback(
            lambda conn, frame: blocked_buffer.append((conn, frame)))

        # Simulate dispatch of blocked connection
        blocked_frame = pika.frame.Method(
            0,
            pika.spec.Connection.Blocked('reason'))
        self.connection._process_frame(blocked_frame)

        self.assertEqual(len(blocked_buffer), 1)
        conn, frame = blocked_buffer[0]
        self.assertIs(conn, self.connection)
        self.assertIs(frame, blocked_frame)

    def test_add_on_connection_unblocked_callback(self):
        unblocked_buffer = []
        self.connection.add_on_connection_unblocked_callback(
            lambda conn, frame: unblocked_buffer.append((conn, frame)))

        # Simulate dispatch of unblocked connection
        unblocked_frame = pika.frame.Method(0, pika.spec.Connection.Unblocked())
        self.connection._process_frame(unblocked_frame)

        self.assertEqual(len(unblocked_buffer), 1)
        conn, frame = unblocked_buffer[0]
        self.assertIs(conn, self.connection)
        self.assertIs(frame, unblocked_frame)

    @mock.patch.object(
        connection.Connection,
        '_adapter_connect_stream',
        spec_set=connection.Connection._adapter_connect_stream)
    @mock.patch.object(connection.Connection,
                       'add_on_connection_blocked_callback')
    @mock.patch.object(connection.Connection,
                       'add_on_connection_unblocked_callback')
    def test_create_with_blocked_connection_timeout_config(
            self, add_on_unblocked_callback_mock, add_on_blocked_callback_mock,
            connect_mock):

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection(
                parameters=connection.ConnectionParameters(
                    blocked_connection_timeout=60))

        # Check
        conn.add_on_connection_blocked_callback.assert_called_once_with(
            conn._on_connection_blocked)

        conn.add_on_connection_unblocked_callback.assert_called_once_with(
            conn._on_connection_unblocked)

    @mock.patch.object(ConstructibleConnection, '_adapter_call_later')
    @mock.patch.object(
        connection.Connection,
        '_adapter_connect_stream',
        spec_set=connection.Connection._adapter_connect_stream)
    def test_connection_blocked_sets_timer(self, connect_mock,
                                           call_later_mock):
        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection(
                parameters=connection.ConnectionParameters(
                    blocked_connection_timeout=60))

        conn._on_connection_blocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        # Check
        conn._adapter_call_later.assert_called_once_with(
            60, conn._on_blocked_connection_timeout)

        self.assertIsNotNone(conn._blocked_conn_timer)

    @mock.patch.object(ConstructibleConnection, '_adapter_call_later')
    @mock.patch.object(
        connection.Connection,
        '_adapter_connect_stream',
        spec_set=connection.Connection._adapter_connect_stream)
    def test_blocked_connection_multiple_blocked_in_a_row_sets_timer_once(
            self, connect_mock, call_later_mock):

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection(
                parameters=connection.ConnectionParameters(
                    blocked_connection_timeout=60))

        # Simulate Connection.Blocked trigger
        conn._on_connection_blocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        # Check
        conn._adapter_call_later.assert_called_once_with(
            60, conn._on_blocked_connection_timeout)

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        # Simulate Connection.Blocked trigger again
        conn._on_connection_blocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertEqual(conn._adapter_call_later.call_count, 1)
        self.assertIs(conn._blocked_conn_timer, timer)

    @mock.patch.object(connection.Connection, '_on_stream_terminated')
    @mock.patch.object(
        ConstructibleConnection,
        '_adapter_call_later',
        spec_set=connection.Connection._adapter_call_later)
    @mock.patch.object(
        connection.Connection,
        '_adapter_connect_stream',
        spec_set=connection.Connection._adapter_connect_stream)
    def test_blocked_connection_timeout_terminates_connection(
            self, connect_mock, call_later_mock, on_terminate_mock):

        with mock.patch.multiple(ConstructibleConnection,
                                 _adapter_connect_stream=mock.Mock(),
                                 _terminate_stream=mock.Mock()):
            conn = ConstructibleConnection(
                parameters=connection.ConnectionParameters(
                    blocked_connection_timeout=60))

            conn._on_connection_blocked(
                conn,
                mock.Mock(name='frame.Method(Connection.Blocked)'))

            conn._on_blocked_connection_timeout()

            # Check
            conn._terminate_stream.assert_called_once_with(mock.ANY)

            exc = conn._terminate_stream.call_args[0][0]
            self.assertIsInstance(exc, exceptions.ConnectionBlockedTimeout)
            self.assertSequenceEqual(exc.args,
                                     ['Blocked connection timeout expired.'])

            self.assertIsNone(conn._blocked_conn_timer)

    @mock.patch.object(ConstructibleConnection, '_adapter_remove_timeout')
    @mock.patch.object(
        ConstructibleConnection,
        '_adapter_call_later',
        spec_set=connection.Connection._adapter_call_later)
    @mock.patch.object(
        connection.Connection,
        '_adapter_connect_stream',
        spec_set=connection.Connection._adapter_connect_stream)
    def test_blocked_connection_unblocked_removes_timer(
            self, connect_mock, call_later_mock, remove_timeout_mock):

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection(
                parameters=connection.ConnectionParameters(
                    blocked_connection_timeout=60))

        conn._on_connection_blocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        conn._on_connection_unblocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Unblocked)'))

        # Check
        conn._adapter_remove_timeout.assert_called_once_with(timer)
        self.assertIsNone(conn._blocked_conn_timer)

    @mock.patch.object(ConstructibleConnection, '_adapter_remove_timeout')
    @mock.patch.object(
        ConstructibleConnection,
        '_adapter_call_later',
        spec_set=connection.Connection._adapter_call_later)
    @mock.patch.object(
        connection.Connection,
        '_adapter_connect_stream',
        spec_set=connection.Connection._adapter_connect_stream)
    def test_blocked_connection_multiple_unblocked_in_a_row_removes_timer_once(
            self, connect_mock, call_later_mock, remove_timeout_mock):

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection(
                parameters=connection.ConnectionParameters(
                    blocked_connection_timeout=60))

        # Simulate Connection.Blocked
        conn._on_connection_blocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        # Simulate Connection.Unblocked
        conn._on_connection_unblocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Unblocked)'))

        # Check
        conn._adapter_remove_timeout.assert_called_once_with(timer)
        self.assertIsNone(conn._blocked_conn_timer)

        # Simulate Connection.Unblocked again
        conn._on_connection_unblocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Unblocked)'))

        self.assertEqual(conn._adapter_remove_timeout.call_count, 1)
        self.assertIsNone(conn._blocked_conn_timer)

    @mock.patch.object(ConstructibleConnection, '_adapter_remove_timeout')
    @mock.patch.object(
        ConstructibleConnection,
        '_adapter_call_later',
        spec_set=connection.Connection._adapter_call_later)
    @mock.patch.object(
        connection.Connection,
        '_adapter_connect_stream',
        spec_set=connection.Connection._adapter_connect_stream)
    @mock.patch.object(
        ConstructibleConnection,
        '_adapter_disconnect_stream',
        spec_set=connection.Connection._adapter_disconnect_stream)
    def test_blocked_connection_on_stream_terminated_removes_timer(
            self, adapter_disconnect_mock, connect_mock, call_later_mock,
            remove_timeout_mock):

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            conn = ConstructibleConnection(
                parameters=connection.ConnectionParameters(
                    blocked_connection_timeout=60),
                on_open_error_callback=lambda *args: None)

        conn._on_connection_blocked(
            conn,
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        conn._on_stream_terminated(exceptions.StreamLostError())

        # Check
        conn._adapter_remove_timeout.assert_called_once_with(timer)
        self.assertIsNone(conn._blocked_conn_timer)

    @mock.patch.object(
        ConstructibleConnection,
        '_adapter_emit_data',
        spec_set=connection.Connection._adapter_emit_data)
    def test_send_message_updates_frames_sent_and_bytes_sent(
            self,
            _adapter_emit_data):
        self.connection._flush_outbound = mock.Mock()
        self.connection._body_max_length = 10000
        method = spec.Basic.Publish(
            exchange='my-exchange', routing_key='my-route')

        props = spec.BasicProperties()
        body = b'b' * 1000000

        self.connection._send_method(
            channel_number=1, method=method, content=(props, body))

        frames_sent = _adapter_emit_data.call_count
        bytes_sent = sum(
            len(call[0][0]) for call in _adapter_emit_data.call_args_list)

        self.assertEqual(self.connection.frames_sent, frames_sent)
        self.assertEqual(self.connection.bytes_sent, bytes_sent)

    def test_no_side_effects_from_message_marshal_error(self):
        # Verify that frame buffer is empty on entry
        self.assertEqual(b'', self.connection._frame_buffer)

        # Use Basic.Public with invalid body to trigger marshalling error
        method = spec.Basic.Publish()
        properties = spec.BasicProperties()
        # Verify that marshalling of method and header won't trigger error
        frame.Method(1, method).marshal()
        frame.Header(1, body_size=10, props=properties).marshal()
        # Create bogus body that should trigger an error during marshalling
        body = [1,2,3,4]
        # Verify that frame body can be created using the bogus body, but
        # that marshalling will fail
        frame.Body(1, body)
        with self.assertRaises(TypeError):
            frame.Body(1, body).marshal()

        # Now, attempt to send the method with the bogus body
        with self.assertRaises(TypeError):
            self.connection._send_method(channel_number=1,
                                    method=method,
                                    content=(properties, body))

        # Now make sure that nothing is enqueued on frame buffer
        self.assertEqual(b'', self.connection._frame_buffer)
