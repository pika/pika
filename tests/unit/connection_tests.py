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
    from unittest import mock

import random
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import connection
from pika import channel
from pika import credentials
from pika import exceptions
from pika import frame
from pika import spec
from pika.compat import xrange


def callback_method():
    """Callback method to use in tests"""
    pass


class ConnectionTests(unittest.TestCase):

    def setUp(self):
        with mock.patch('pika.connection.Connection.connect'):
            self.connection = connection.Connection()
            self.connection._set_connection_state(
                connection.Connection.CONNECTION_OPEN)

        self.channel = mock.Mock(spec=channel.Channel)
        self.channel.is_open = True
        self.connection._channels[1] = self.channel

    def tearDown(self):
        del self.connection
        del self.channel

    @mock.patch('pika.connection.Connection._send_connection_close')
    def test_close_closes_open_channels(self, send_connection_close):
        self.connection.close()
        self.channel.close.assert_called_once_with(200, 'Normal shutdown')

    @mock.patch('pika.connection.Connection._send_connection_close')
    def test_close_ignores_closed_channels(self, send_connection_close):
        for closed_state in (self.connection.CONNECTION_CLOSED,
                             self.connection.CONNECTION_CLOSING):
            self.connection.connection_state = closed_state
            self.connection.close()
            self.assertFalse(self.channel.close.called)

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_close_ready_open_channels(self, on_close_ready):
        """if open channels _on_close_ready shouldn't be called"""
        self.connection.close()
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_close_ready_no_open_channels(self, on_close_ready):
        self.connection._channels = dict()
        self.connection.close()
        self.assertTrue(on_close_ready.called,
                        '_on_close_ready should have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_cleanup_no_open_channels(self, on_close_ready):
        """Should call _on_close_ready if connection is closing and there are
        no open channels

        """
        self.connection._channels = dict()
        self.connection.close()
        self.assertTrue(on_close_ready.called,
                        '_on_close_ready should been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_cleanup_open_channels(self, on_close_ready):
        """if connection is closing but channels remain open do not call
        _on_close_ready

        """
        self.connection.close()
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    @mock.patch('pika.connection.Connection._on_close_ready')
    def test_on_channel_cleanup_non_closing_state(self, on_close_ready):
        """if connection isn't closing _on_close_ready should not be called"""
        self.connection._on_channel_cleanup(mock.Mock())
        self.assertFalse(on_close_ready.called,
                         '_on_close_ready should not have been called')

    def test_on_terminate_cleans_up(self):
        """_on_terminate cleans up heartbeat, adapter, and channels"""
        heartbeat = mock.Mock()
        self.connection.heartbeat = heartbeat
        self.connection._adapter_disconnect = mock.Mock()

        self.connection._on_terminate(0, 'Undefined')

        heartbeat.stop.assert_called_once_with()
        self.connection._adapter_disconnect.assert_called_once_with()

        self.assertTrue(self.channel._on_close.called,
                        'channel._on_close should have been called')
        method_frame = self.channel._on_close.call_args[0][0]
        self.assertEqual(method_frame.method.reply_code, 0)
        self.assertEqual(method_frame.method.reply_text, 'Undefined')

        self.assertTrue(self.connection.is_closed)

    def test_on_terminate_invokes_connection_closed_callback(self):
        """_on_terminate invokes `Connection.ON_CONNECTION_CLOSED` callbacks"""
        self.connection.callbacks.process = mock.Mock(
            wraps=self.connection.callbacks.process)

        self.connection._adapter_disconnect = mock.Mock()

        self.connection._on_terminate(1, 'error text')

        self.connection.callbacks.process.assert_called_once_with(
            0, self.connection.ON_CONNECTION_CLOSED,
            self.connection, self.connection,
            1, 'error text')

        with self.assertRaises(AssertionError):
            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR,
                self.connection, self.connection,
                mock.ANY)

    def test_on_terminate_invokes_protocol_on_connection_error_and_closed(self):
        """_on_terminate invokes `ON_CONNECTION_ERROR` with `IncompatibleProtocolError` and `ON_CONNECTION_CLOSED` callbacks"""
        with mock.patch.object(self.connection.callbacks, 'process'):

            self.connection._adapter_disconnect = mock.Mock()

            self.connection._set_connection_state(
                self.connection.CONNECTION_PROTOCOL)

            self.connection._on_terminate(1, 'error text')

            self.assertEqual(self.connection.callbacks.process.call_count, 2)

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR,
                self.connection, self.connection,
                mock.ANY)

            conn_exc = self.connection.callbacks.process.call_args_list[0][0][4]
            self.assertIs(type(conn_exc), exceptions.IncompatibleProtocolError)
            self.assertSequenceEqual(conn_exc.args, [1, 'error text'])

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_CLOSED,
                self.connection, self.connection,
                1, 'error text')

    def test_on_terminate_invokes_auth_on_connection_error_and_closed(self):
        """_on_terminate invokes `ON_CONNECTION_ERROR` with `ProbableAuthenticationError` and `ON_CONNECTION_CLOSED` callbacks"""
        with mock.patch.object(self.connection.callbacks, 'process'):

            self.connection._adapter_disconnect = mock.Mock()

            self.connection._set_connection_state(
                self.connection.CONNECTION_START)

            self.connection._on_terminate(1, 'error text')

            self.assertEqual(self.connection.callbacks.process.call_count, 2)

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR,
                self.connection, self.connection,
                mock.ANY)

            conn_exc = self.connection.callbacks.process.call_args_list[0][0][4]
            self.assertIs(type(conn_exc),
                          exceptions.ProbableAuthenticationError)
            self.assertSequenceEqual(conn_exc.args, [1, 'error text'])

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_CLOSED,
                self.connection, self.connection,
                1, 'error text')

    def test_on_terminate_invokes_access_denied_on_connection_error_and_closed(
            self):
        """_on_terminate invokes `ON_CONNECTION_ERROR` with `ProbableAccessDeniedError` and `ON_CONNECTION_CLOSED` callbacks"""
        with mock.patch.object(self.connection.callbacks, 'process'):

            self.connection._adapter_disconnect = mock.Mock()

            self.connection._set_connection_state(
                self.connection.CONNECTION_TUNE)

            self.connection._on_terminate(1, 'error text')

            self.assertEqual(self.connection.callbacks.process.call_count, 2)

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_ERROR,
                self.connection, self.connection,
                mock.ANY)

            conn_exc = self.connection.callbacks.process.call_args_list[0][0][4]
            self.assertIs(type(conn_exc), exceptions.ProbableAccessDeniedError)
            self.assertSequenceEqual(conn_exc.args, [1, 'error text'])

            self.connection.callbacks.process.assert_any_call(
                0, self.connection.ON_CONNECTION_CLOSED,
                self.connection, self.connection,
                1, 'error text')

    @mock.patch('pika.connection.Connection.connect')
    def test_new_conn_should_use_first_channel(self, connect):
        """_next_channel_number in new conn should always be 1"""
        conn = connection.Connection()
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
                (self.connection.add_backpressure_callback,
                 self.connection.ON_CONNECTION_BACKPRESSURE),
                (self.connection.add_on_open_callback,
                 self.connection.ON_CONNECTION_OPEN),
                (self.connection.add_on_close_callback,
                 self.connection.ON_CONNECTION_CLOSED)):
            self.connection.callbacks.reset_mock()
            test_method(callback_method)
            self.connection.callbacks.add.assert_called_once_with(
                0, expected_key, callback_method, False)

    def test_add_on_close_callback(self):
        """make sure the add on close callback is added"""
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)
        self.connection.add_on_open_callback(callback_method)
        self.connection.callbacks.add.assert_called_once_with(
            0, self.connection.ON_CONNECTION_OPEN, callback_method, False)

    def test_add_on_open_error_callback(self):
        """make sure the add on open error callback is added"""
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)
        #Test with remove default first (also checks default is True)
        self.connection.add_on_open_error_callback(callback_method)
        self.connection.callbacks.remove.assert_called_once_with(
            0, self.connection.ON_CONNECTION_ERROR,
            self.connection._on_connection_error)
        self.connection.callbacks.add.assert_called_once_with(
            0, self.connection.ON_CONNECTION_ERROR, callback_method, False)

    def test_channel(self):
        """test the channel method"""
        self.connection._next_channel_number = mock.Mock(return_value=42)
        test_channel = mock.Mock(spec=channel.Channel)
        self.connection._create_channel = mock.Mock(return_value=test_channel)
        self.connection._add_channel_callbacks = mock.Mock()
        ret_channel = self.connection.channel(callback_method)
        self.assertEqual(test_channel, ret_channel)
        self.connection._create_channel.assert_called_once_with(42,
                                                                callback_method)
        self.connection._add_channel_callbacks.assert_called_once_with(42)
        test_channel.open.assert_called_once_with()

    @mock.patch('pika.frame.ProtocolHeader')
    def test_connect(self, frame_protocol_header):
        """make sure the connect method sets the state and sends a frame"""
        self.connection._adapter_connect = mock.Mock(return_value=None)
        self.connection._send_frame = mock.Mock()
        frame_protocol_header.spec = frame.ProtocolHeader
        frame_protocol_header.return_value = 'frame object'
        self.connection.connect()
        self.assertEqual(self.connection.CONNECTION_PROTOCOL,
                         self.connection.connection_state)
        self.connection._send_frame.assert_called_once_with('frame object')

    def test_connect_reconnect(self):
        """try the different reconnect logic, check state & other class vars"""
        self.connection._adapter_connect = mock.Mock(return_value='error')
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)
        self.connection.remaining_connection_attempts = 2
        self.connection.params.retry_delay = 555
        self.connection.params.connection_attempts = 99
        self.connection.add_timeout = mock.Mock()
        #first failure
        self.connection.connect()
        self.connection.add_timeout.assert_called_once_with(
            555, self.connection.connect)
        self.assertEqual(1, self.connection.remaining_connection_attempts)
        self.assertFalse(self.connection.callbacks.process.called)
        self.assertEqual(self.connection.CONNECTION_INIT,
                         self.connection.connection_state)
        #fail with no attempts remaining
        self.connection.add_timeout.reset_mock()
        self.connection.connect()
        self.assertFalse(self.connection.add_timeout.called)
        self.assertEqual(99, self.connection.remaining_connection_attempts)
        self.connection.callbacks.process.assert_called_once_with(
            0, self.connection.ON_CONNECTION_ERROR, self.connection,
            self.connection, 'error')
        self.assertEqual(self.connection.CONNECTION_CLOSED,
                         self.connection.connection_state)

    def test_client_properties(self):
        """make sure client properties has some important keys"""
        client_props = self.connection._client_properties
        self.assertTrue(isinstance(client_props, dict))
        for required_key in ('product', 'platform', 'capabilities',
                             'information', 'version'):
            self.assertTrue(required_key in client_props,
                            '%s missing' % required_key)

    def test_set_backpressure_multiplier(self):
        """test setting the backpressure multiplier"""
        self.connection._backpressure = None
        self.connection.set_backpressure_multiplier(value=5)
        self.assertEqual(5, self.connection._backpressure)

    def test_close_channels(self):
        """test closing all channels"""
        self.connection.connection_state = self.connection.CONNECTION_OPEN
        self.connection.callbacks = mock.Mock(spec=self.connection.callbacks)
        open_channel = mock.Mock(is_open=True)
        closed_channel = mock.Mock(is_open=False)
        self.connection._channels = {'oc': open_channel, 'cc': closed_channel}
        self.connection._close_channels('reply code', 'reply text')
        open_channel.close.assert_called_once_with('reply code', 'reply text')
        self.assertTrue('oc' in self.connection._channels)
        self.assertTrue('cc' not in self.connection._channels)
        self.connection.callbacks.cleanup.assert_called_once_with('cc')
        #Test on closed channel
        self.connection.connection_state = self.connection.CONNECTION_CLOSED
        self.connection._close_channels('reply code', 'reply text')
        self.assertEqual({}, self.connection._channels)

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
        self.connection._on_connection_start(method_frame)
        self.assertEqual(True, self.connection.basic_nack)
        self.assertEqual(False, self.connection.consumer_cancel_notify)
        self.assertEqual(False, self.connection.exchange_exchange_bindings)
        self.assertEqual(False, self.connection.publisher_confirms)

    @mock.patch('pika.heartbeat.HeartbeatChecker')
    @mock.patch('pika.frame.Method')
    def test_on_connection_tune(self, method, heartbeat_checker):
        """make sure on connection tune turns the connection params"""
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
        self.assertEqual(['ab'], [frame_buffer.data for frame_buffer in
                                  self.connection.outbound_buffer])
        self.assertEqual('hearbeat obj', self.connection.heartbeat)

        # Repeat with smaller user heartbeat than broker
        method_frame.method.heartbeat = 60
        self.connection.params.heartbeat = 20
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(60, self.connection.params.heartbeat)

        # Repeat with user deferring to server's heartbeat timeout
        method_frame.method.heartbeat = 500
        self.connection.params.heartbeat = None
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(500, self.connection.params.heartbeat)

        # Repeat with user deferring to server's disabled heartbeat value
        method_frame.method.heartbeat = 0
        self.connection.params.heartbeat = None
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(0, self.connection.params.heartbeat)

        # Repeat with user-disabled heartbeat
        method_frame.method.heartbeat = 60
        self.connection.params.heartbeat = 0
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(0, self.connection.params.heartbeat)

        # Repeat with server-disabled heartbeat
        method_frame.method.heartbeat = 0
        self.connection.params.heartbeat = 60
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(0, self.connection.params.heartbeat)

        # Repeat with both user/server disabled heartbeats
        method_frame.method.heartbeat = 0
        self.connection.params.heartbeat = 0
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(0, self.connection.params.heartbeat)

    def test_on_connection_closed(self):
        """make sure connection close sends correct frames"""
        method_frame = mock.Mock()
        method_frame.method = mock.Mock(spec=spec.Connection.Close)
        method_frame.method.reply_code = 1
        method_frame.method.reply_text = 'hello'
        self.connection._on_terminate = mock.Mock()
        self.connection._on_connection_close(method_frame)
        #Check
        self.connection._on_terminate.assert_called_once_with(1, 'hello')

    def test_on_connection_close_ok(self):
        """make sure _on_connection_close_ok terminates connection"""
        method_frame = mock.Mock()
        method_frame.method = mock.Mock(spec=spec.Connection.CloseOk)
        self.connection.closing = (1, 'bye')
        self.connection._on_terminate = mock.Mock()

        self.connection._on_connection_close_ok(method_frame)

        #Check
        self.connection._on_terminate.assert_called_once_with(1, 'bye')

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
            self.connection.heartbeat = mock.Mock()
            self.connection.frames_received = 0
            decode_frame.return_value = (2, frame_value)
            self.connection._on_data_available(data_in)
            #test value
            self.assertListEqual([], self.connection._frame_buffer)
            self.assertEqual(2, self.connection.bytes_received)
            self.assertEqual(1, self.connection.frames_received)
            if frame_type == frame.Heartbeat:
                self.assertTrue(self.connection.heartbeat.received.called)

    @mock.patch.object(connection.Connection, 'connect',
                       spec_set=connection.Connection.connect)
    @mock.patch.object(connection.Connection,
                       'add_on_connection_blocked_callback')
    @mock.patch.object(connection.Connection,
                       'add_on_connection_unblocked_callback')
    def test_create_with_blocked_connection_timeout_config(
            self,
            add_on_unblocked_callback_mock,
            add_on_blocked_callback_mock,
            connect_mock):

        conn = connection.Connection(
            parameters=connection.ConnectionParameters(
                blocked_connection_timeout=60))

        # Check
        conn.add_on_connection_blocked_callback.assert_called_once_with(
            conn._on_connection_blocked)

        conn.add_on_connection_unblocked_callback.assert_called_once_with(
            conn._on_connection_unblocked)

    @mock.patch.object(connection.Connection, 'add_timeout')
    @mock.patch.object(connection.Connection, 'connect',
                       spec_set=connection.Connection.connect)
    def test_connection_blocked_sets_timer(
            self,
            connect_mock,
            add_timeout_mock):

        conn = connection.Connection(
            parameters=connection.ConnectionParameters(
                blocked_connection_timeout=60))

        conn._on_connection_blocked(
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        # Check
        conn.add_timeout.assert_called_once_with(
            60,
            conn._on_blocked_connection_timeout)

        self.assertIsNotNone(conn._blocked_conn_timer)

    @mock.patch.object(connection.Connection, 'add_timeout')
    @mock.patch.object(connection.Connection, 'connect',
                       spec_set=connection.Connection.connect)
    def test_multi_connection_blocked_in_a_row_sets_timer_once(
            self,
            connect_mock,
            add_timeout_mock):

        conn = connection.Connection(
            parameters=connection.ConnectionParameters(
                blocked_connection_timeout=60))

        # Simulate Connection.Blocked trigger
        conn._on_connection_blocked(
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        # Check
        conn.add_timeout.assert_called_once_with(
            60,
            conn._on_blocked_connection_timeout)

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        # Simulate Connection.Blocked trigger again
        conn._on_connection_blocked(
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertEqual(conn.add_timeout.call_count, 1)
        self.assertIs(conn._blocked_conn_timer, timer)

    @mock.patch.object(connection.Connection, '_on_terminate')
    @mock.patch.object(connection.Connection, 'add_timeout',
                       spec_set=connection.Connection.add_timeout)
    @mock.patch.object(connection.Connection, 'connect',
                       spec_set=connection.Connection.connect)
    def test_blocked_connection_timeout_teminates_connection(
            self,
            connect_mock,
            add_timeout_mock,
            on_terminate_mock):

        conn = connection.Connection(
            parameters=connection.ConnectionParameters(
                blocked_connection_timeout=60))

        conn._on_connection_blocked(
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        conn._on_blocked_connection_timeout()

        # Check
        conn._on_terminate.assert_called_once_with(
            connection.InternalCloseReasons.BLOCKED_CONNECTION_TIMEOUT,
            'Blocked connection timeout expired')

        self.assertIsNone(conn._blocked_conn_timer)

    @mock.patch.object(connection.Connection, 'remove_timeout')
    @mock.patch.object(connection.Connection, 'add_timeout',
                       spec_set=connection.Connection.add_timeout)
    @mock.patch.object(connection.Connection, 'connect',
                       spec_set=connection.Connection.connect)
    def test_connection_unblocked_removes_timer(
            self,
            connect_mock,
            add_timeout_mock,
            remove_timeout_mock):

        conn = connection.Connection(
            parameters=connection.ConnectionParameters(
                blocked_connection_timeout=60))

        conn._on_connection_blocked(
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        conn._on_connection_unblocked(
            mock.Mock(name='frame.Method(Connection.Unblocked)'))

        # Check
        conn.remove_timeout.assert_called_once_with(timer)
        self.assertIsNone(conn._blocked_conn_timer)

    @mock.patch.object(connection.Connection, 'remove_timeout')
    @mock.patch.object(connection.Connection, 'add_timeout',
                       spec_set=connection.Connection.add_timeout)
    @mock.patch.object(connection.Connection, 'connect',
                       spec_set=connection.Connection.connect)
    def test_multi_connection_unblocked_in_a_row_removes_timer_once(
            self,
            connect_mock,
            add_timeout_mock,
            remove_timeout_mock):

        conn = connection.Connection(
            parameters=connection.ConnectionParameters(
                blocked_connection_timeout=60))

        # Simulate Connection.Blocked
        conn._on_connection_blocked(
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        # Simulate Connection.Unblocked
        conn._on_connection_unblocked(
            mock.Mock(name='frame.Method(Connection.Unblocked)'))

        # Check
        conn.remove_timeout.assert_called_once_with(timer)
        self.assertIsNone(conn._blocked_conn_timer)

        # Simulate Connection.Unblocked again
        conn._on_connection_unblocked(
            mock.Mock(name='frame.Method(Connection.Unblocked)'))

        self.assertEqual(conn.remove_timeout.call_count, 1)
        self.assertIsNone(conn._blocked_conn_timer)

    @mock.patch.object(connection.Connection, 'remove_timeout')
    @mock.patch.object(connection.Connection, 'add_timeout',
                       spec_set=connection.Connection.add_timeout)
    @mock.patch.object(connection.Connection, 'connect',
                       spec_set=connection.Connection.connect)
    @mock.patch.object(connection.Connection, '_adapter_disconnect',
                       spec_set=connection.Connection._adapter_disconnect)
    def test_on_terminate_removes_timer(
            self,
            adapter_disconnect_mock,
            connect_mock,
            add_timeout_mock,
            remove_timeout_mock):

        conn = connection.Connection(
            parameters=connection.ConnectionParameters(
                blocked_connection_timeout=60))

        conn._on_connection_blocked(
            mock.Mock(name='frame.Method(Connection.Blocked)'))

        self.assertIsNotNone(conn._blocked_conn_timer)

        timer = conn._blocked_conn_timer

        conn._on_terminate(0, 'test_on_terminate_removes_timer')

        # Check
        conn.remove_timeout.assert_called_once_with(timer)
        self.assertIsNone(conn._blocked_conn_timer)
