"""
Tests for pika.connection.Connection

"""
try:
    import mock
except ImportError:
    from unittest import mock

import random
import urllib
import copy
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import connection
from pika import channel
from pika import credentials
from pika import frame
from pika import spec
from pika.compat import xrange, urlencode


def callback_method():
    """Callback method to use in tests"""
    pass


class ConnectionTests(unittest.TestCase):

    @mock.patch('pika.connection.Connection.connect')
    def setUp(self, connect):
        self.connection = connection.Connection()
        self.channel = mock.Mock(spec=channel.Channel)
        self.channel.is_open = True
        self.connection._channels[1] = self.channel
        self.connection._set_connection_state(
            connection.Connection.CONNECTION_OPEN)

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

    def test_on_disconnect(self):
        """if connection isn't closing _on_close_ready should not be called"""
        self.connection._on_disconnect(0, 'Undefined')
        self.assertTrue(self.channel._on_close.called,
                        'channel._on_close should have been called')
        method_frame = self.channel._on_close.call_args[0][0]
        self.assertEqual(method_frame.method.reply_code, 0)
        self.assertEqual(method_frame.method.reply_text, 'Undefined')

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

    def test_process_url(self):
        """test for the different query stings checked by process url"""
        url_params = {
            'backpressure_detection': None,
            'channel_max': 1,
            'connection_attempts': 2,
            'frame_max': 30000,
            'heartbeat_interval': 4,
            'locale': 'en',
            'retry_delay': 5,
            'socket_timeout': 6,
            'ssl_options': {'ssl': 'dict'}
        }
        for backpressure in ('t', 'f'):
            test_params = copy.deepcopy(url_params)
            test_params['backpressure_detection'] = backpressure
            query_string = urlencode(test_params)
            test_url = 'https://www.test.com?%s' % query_string
            params = connection.URLParameters(test_url)
            #check each value
            for t_param in ('channel_max', 'connection_attempts', 'frame_max',
                            'locale', 'retry_delay', 'socket_timeout',
                            'ssl_options'):
                self.assertEqual(test_params[t_param], getattr(params, t_param),
                                 t_param)
            self.assertEqual(params.backpressure_detection, backpressure == 't')
            self.assertEqual(test_params['heartbeat_interval'],
                             params.heartbeat)

    def test_good_connection_parameters(self):
        """make sure connection kwargs get set correctly"""
        kwargs = {
            'host': 'https://www.test.com',
            'port': 5678,
            'virtual_host': u'vvhost',
            'channel_max': 3,
            'frame_max': 40000,
            'credentials': credentials.PlainCredentials('very', 'secure'),
            'heartbeat_interval': 7,
            'backpressure_detection': False,
            'retry_delay': 3,
            'ssl': True,
            'connection_attempts': 2,
            'locale': 'en',
            'ssl_options': {'ssl': 'options'}
        }
        conn = connection.ConnectionParameters(**kwargs)
        #check values
        for t_param in ('host', 'port', 'virtual_host', 'channel_max',
                        'frame_max', 'backpressure_detection', 'ssl',
                        'credentials', 'retry_delay', 'connection_attempts',
                        'locale'):
            self.assertEqual(kwargs[t_param], getattr(conn, t_param), t_param)
        self.assertEqual(kwargs['heartbeat_interval'], conn.heartbeat)

    def test_bad_type_connection_parameters(self):
        """test connection kwargs type checks throw errors for bad input"""
        kwargs = {
            'host': 'https://www.test.com',
            'port': 5678,
            'virtual_host': 'vvhost',
            'channel_max': 3,
            'frame_max': 40000,
            'heartbeat_interval': 7,
            'backpressure_detection': False,
            'ssl': True
        }
        #Test Type Errors
        for bad_field, bad_value in (
            ('host', 15672), ('port', '5672'), ('virtual_host', True),
            ('channel_max', '4'), ('frame_max', '5'), ('credentials', 'bad'),
            ('locale', 1), ('heartbeat_interval', '6'),
            ('socket_timeout', '42'), ('retry_delay', 'two'),
            ('backpressure_detection', 'true'), ('ssl', {'ssl': 'dict'}),
            ('ssl_options', True), ('connection_attempts', 'hello')):
            bkwargs = copy.deepcopy(kwargs)
            bkwargs[bad_field] = bad_value
            self.assertRaises(TypeError, connection.ConnectionParameters,
                              **bkwargs)

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
        method_frame.method.frame_max = 10
        method_frame.method.heartbeat = 0
        self.connection.params.channel_max = 20
        self.connection.params.frame_max = 20
        self.connection.params.heartbeat = 20
        #Test
        self.connection._on_connection_tune(method_frame)
        #verfy
        self.assertEqual(self.connection.CONNECTION_TUNE,
                         self.connection.connection_state)
        self.assertEqual(20, self.connection.params.channel_max)
        self.assertEqual(10, self.connection.params.frame_max)
        self.assertEqual(20, self.connection.params.heartbeat)
        self.assertEqual(2, self.connection._body_max_length)
        heartbeat_checker.assert_called_once_with(self.connection, 20)
        self.assertEqual(['ab'], list(self.connection.outbound_buffer))
        self.assertEqual('hearbeat obj', self.connection.heartbeat)

    def test_on_connection_closed(self):
        """make sure connection close sends correct frames"""
        method_frame = mock.Mock()
        method_frame.method = mock.Mock(spec=spec.Connection.Close)
        method_frame.method.reply_code = 1
        method_frame.method.reply_text = 'hello'
        heartbeat = mock.Mock()
        self.connection.heartbeat = heartbeat
        self.connection._adapter_disconnect = mock.Mock()
        self.connection._on_connection_closed(method_frame, from_adapter=False)
        #Check
        self.assertTupleEqual((1, 'hello'), self.connection.closing)
        heartbeat.stop.assert_called_once_with()
        self.connection._adapter_disconnect.assert_called_once_with()

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
