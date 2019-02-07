"""
Test `pika.connection.Parameters`, `pika.connection.ConnectionParameters`, and
`pika.connection.URLParameters`

"""
import copy
import ssl
import unittest
import warnings

import pika
from pika.compat import urlencode, url_quote, dict_iteritems
from pika import channel, connection, credentials, spec


# disable missing-docstring
# pylint: disable=C0111

# disable invalid-name
# pylint: disable=C0103


# Unordered sequence of connection.Parameters's property getters
_ALL_PUBLIC_PARAMETERS_PROPERTIES = tuple(
    attr for attr in vars(connection.Parameters) if not attr.startswith('_')
    and issubclass(type(getattr(connection.Parameters, attr)), property))


class ChildParameters(connection.Parameters):

    def __init__(self, *args, **kwargs):
        super(ChildParameters, self).__init__(*args, **kwargs)
        self.extra = 'e'

    def __eq__(self, other):
        if isinstance(other, ChildParameters):
            return self.extra == other.extra and super(
                ChildParameters, self).__eq__(other)
        return NotImplemented


class ParametersTestsBase(unittest.TestCase):

    def setUp(self):
        warnings.resetwarnings()
        self.addCleanup(warnings.resetwarnings)

    def get_default_properties(self):
        """
        :returns: a dict of expected public property names and default values
            for `pika.connection.Parameters`

        """
        kls = connection.Parameters
        defaults = {
            'blocked_connection_timeout':
            kls.DEFAULT_BLOCKED_CONNECTION_TIMEOUT,
            'channel_max':
            kls.DEFAULT_CHANNEL_MAX,
            'client_properties':
            kls.DEFAULT_CLIENT_PROPERTIES,
            'connection_attempts':
            kls.DEFAULT_CONNECTION_ATTEMPTS,
            'credentials':
            credentials.PlainCredentials(kls.DEFAULT_USERNAME,
                                         kls.DEFAULT_PASSWORD),
            'frame_max':
            kls.DEFAULT_FRAME_MAX,
            'heartbeat':
            kls.DEFAULT_HEARTBEAT_TIMEOUT,
            'host':
            kls.DEFAULT_HOST,
            'locale':
            kls.DEFAULT_LOCALE,
            'port':
            kls.DEFAULT_PORT,
            'retry_delay':
            kls.DEFAULT_RETRY_DELAY,
            'socket_timeout':
            kls.DEFAULT_SOCKET_TIMEOUT,
            'stack_timeout':
            kls.DEFAULT_STACK_TIMEOUT,
            'ssl_options':
            kls.DEFAULT_SSL_OPTIONS,
            'virtual_host':
            kls.DEFAULT_VIRTUAL_HOST,
            'tcp_options':
            kls.DEFAULT_TCP_OPTIONS
        }

        # Make sure we didn't miss anything
        self.assertSequenceEqual(
            sorted(defaults), sorted(_ALL_PUBLIC_PARAMETERS_PROPERTIES))

        return defaults

    def assert_default_parameter_values(self, params):
        """Assert that the given parameters object has the default parameter
        values.

        :param pika.connection.Parameters params: Verify that the given params
            instance has all default property values
        """
        for name, expected_value in dict_iteritems(
                self.get_default_properties()):
            value = getattr(params, name)
            self.assertEqual(
                value,
                expected_value,
                msg='Expected %s=%r, but got %r' % (name, expected_value,
                                                    value))


class ParametersTests(ParametersTestsBase):

    def test_eq(self):
        params_1 = connection.Parameters()
        params_2 = connection.Parameters()
        params_3 = ChildParameters()

        self.assertEqual(params_1, params_2)
        self.assertEqual(params_2, params_1)

        params_1.host = 'localhost'
        params_1.port = 5672
        params_1.virtual_host = '/'
        params_1.credentials = credentials.PlainCredentials('u', 'p')
        params_2.host = 'localhost'
        params_2.port = 5672
        params_2.virtual_host = '//'
        params_2.credentials = credentials.PlainCredentials('uu', 'pp')
        self.assertEqual(params_1, params_2)
        self.assertEqual(params_2, params_1)

        params_1.host = 'localhost'
        params_1.port = 5672
        params_1.virtual_host = '/'
        params_1.credentials = credentials.PlainCredentials('u', 'p')
        params_3.host = 'localhost'
        params_3.port = 5672
        params_3.virtual_host = '//'
        params_3.credentials = credentials.PlainCredentials('uu', 'pp')
        self.assertEqual(params_1, params_3)
        self.assertEqual(params_3, params_1)

        class Foreign(object):

            def __eq__(self, other):
                return 'foobar'

        self.assertEqual(params_1 == Foreign(), 'foobar')
        self.assertEqual(Foreign() == params_1, 'foobar')

    def test_ne(self):
        params_1 = connection.Parameters()
        params_2 = connection.Parameters()
        params_3 = ChildParameters()

        params_1.host = 'localhost'
        params_1.port = 5672
        params_2.host = 'myserver.com'
        params_2.port = 5672
        self.assertNotEqual(params_1, params_2)
        self.assertNotEqual(params_2, params_1)

        params_1.host = 'localhost'
        params_1.port = 5672
        params_2.host = 'localhost'
        params_2.port = 5671
        self.assertNotEqual(params_1, params_2)
        self.assertNotEqual(params_2, params_1)

        params_1.host = 'localhost'
        params_1.port = 5672
        params_3.host = 'myserver.com'
        params_3.port = 5672
        self.assertNotEqual(params_1, params_3)
        self.assertNotEqual(params_3, params_1)

        params_1.host = 'localhost'
        params_1.port = 5672
        params_3.host = 'localhost'
        params_3.port = 5671
        self.assertNotEqual(params_1, params_3)
        self.assertNotEqual(params_3, params_1)

        self.assertNotEqual(params_1, dict(host='localhost', port=5672))
        self.assertNotEqual(dict(host='localhost', port=5672), params_1)

        class Foreign(object):

            def __ne__(self, other):
                return 'foobar'

        self.assertEqual(params_1 != Foreign(), 'foobar')
        self.assertEqual(Foreign() != params_1, 'foobar')

    def test_default_property_values(self):
        self.assert_default_parameter_values(connection.Parameters())

    def test_blocked_connection_timeout(self):
        params = connection.Parameters()

        params.blocked_connection_timeout = 60
        self.assertEqual(params.blocked_connection_timeout, 60)

        params.blocked_connection_timeout = 93.5
        self.assertEqual(params.blocked_connection_timeout, 93.5)

        params.blocked_connection_timeout = None
        self.assertIsNone(params.blocked_connection_timeout)

        with self.assertRaises(TypeError):
            params.blocked_connection_timeout = '1.5'

        with self.assertRaises(ValueError):
            params.blocked_connection_timeout = -40

    def test_channel_max(self):
        params = connection.Parameters()

        params.channel_max = 1
        self.assertEqual(params.channel_max, 1)

        params.channel_max = channel.MAX_CHANNELS
        self.assertEqual(params.channel_max, channel.MAX_CHANNELS)

        with self.assertRaises(TypeError):
            params.channel_max = 1.5

        with self.assertRaises(TypeError):
            params.channel_max = '99'

        with self.assertRaises(ValueError):
            params.channel_max = 0

        with self.assertRaises(ValueError):
            params.channel_max = -40

        with self.assertRaises(ValueError):
            params.channel_max = channel.MAX_CHANNELS + 1

    def test_connection_attempts(self):
        params = connection.Parameters()

        params.connection_attempts = 1
        self.assertEqual(params.connection_attempts, 1)

        params.connection_attempts = 10
        self.assertEqual(params.connection_attempts, 10)

        with self.assertRaises(TypeError):
            params.connection_attempts = 1.5

        with self.assertRaises(TypeError):
            params.connection_attempts = '99'

        with self.assertRaises(ValueError):
            params.connection_attempts = 0

        with self.assertRaises(ValueError):
            params.connection_attempts = -40

    def test_credentials(self):
        params = connection.Parameters()

        plain_cred = credentials.PlainCredentials('very', 'reliable')
        params.credentials = plain_cred
        self.assertEqual(params.credentials, plain_cred)

        ext_cred = credentials.ExternalCredentials()
        params.credentials = ext_cred
        self.assertEqual(params.credentials, ext_cred)

        with self.assertRaises(TypeError):
            params.credentials = connection.Parameters()

        with self.assertRaises(TypeError):
            params.credentials = repr(plain_cred)

    def test_frame_max(self):
        params = connection.Parameters()

        params.frame_max = spec.FRAME_MIN_SIZE
        self.assertEqual(params.frame_max, spec.FRAME_MIN_SIZE)

        params.frame_max = spec.FRAME_MIN_SIZE + 1
        self.assertEqual(params.frame_max, spec.FRAME_MIN_SIZE + 1)

        params.frame_max = spec.FRAME_MAX_SIZE
        self.assertEqual(params.frame_max, spec.FRAME_MAX_SIZE)

        params.frame_max = spec.FRAME_MAX_SIZE - 1
        self.assertEqual(params.frame_max, spec.FRAME_MAX_SIZE - 1)

        with self.assertRaises(TypeError):
            params.frame_max = 10000.9

        with self.assertRaises(TypeError):
            params.frame_max = '10000'

        with self.assertRaises(ValueError):
            params.frame_max = spec.FRAME_MIN_SIZE - 1

        with self.assertRaises(ValueError):
            params.frame_max = spec.FRAME_MAX_SIZE + 1

    def test_heartbeat(self):
        params = connection.Parameters()

        params.heartbeat = None
        self.assertIsNone(params.heartbeat)

        params.heartbeat = 0
        self.assertEqual(params.heartbeat, 0)

        params.heartbeat = 600
        self.assertEqual(params.heartbeat, 600)

        with self.assertRaises(TypeError):
            params.heartbeat = 1.5

        with self.assertRaises(TypeError):
            params.heartbeat = '99'

        with self.assertRaises(ValueError):
            params.heartbeat = -1

        def heartbeat_callback(_conn, _val):
            return 1
        params.heartbeat = heartbeat_callback
        self.assertTrue(callable(params.heartbeat))
        self.assertIs(params.heartbeat, heartbeat_callback)

    def test_host(self):
        params = connection.Parameters()

        params.host = '127.0.0.1'
        self.assertEqual(params.host, '127.0.0.1')

        params.host = 'myserver.com'
        self.assertEqual(params.host, 'myserver.com')

        params.host = u'myserver.com'
        self.assertEqual(params.host, u'myserver.com')

        with self.assertRaises(TypeError):
            params.host = 127

        with self.assertRaises(TypeError):
            params.host = ('127.0.0.1', 5672)

    def test_locale(self):
        params = connection.Parameters()

        params.locale = 'en_UK'
        self.assertEqual(params.locale, 'en_UK')

        params.locale = u'en_UK'
        self.assertEqual(params.locale, u'en_UK')

        with self.assertRaises(TypeError):
            params.locale = 127

    def test_port(self):
        params = connection.Parameters()

        params.port = 0
        self.assertEqual(params.port, 0)

        params.port = 5672
        self.assertEqual(params.port, 5672)

        params.port = '5672'
        self.assertEqual(params.port, 5672)

        params.port = 1.5
        self.assertEqual(params.port, 1)

        with self.assertRaises(TypeError):
            params.port = '50a'

        with self.assertRaises(TypeError):
            params.port = []

    def test_retry_delay(self):
        params = connection.Parameters()

        params.retry_delay = 0
        self.assertEqual(params.retry_delay, 0)

        params.retry_delay = 0.1
        self.assertEqual(params.retry_delay, 0.1)

        with self.assertRaises(TypeError):
            params.retry_delay = '0.1'

    def test_socket_timeout(self):
        params = connection.Parameters()

        params.socket_timeout = 1
        self.assertEqual(params.socket_timeout, 1)

        params.socket_timeout = 0.5
        self.assertEqual(params.socket_timeout, 0.5)

        params.socket_timeout = 60.5
        self.assertEqual(params.socket_timeout, 60.5)

        params.socket_timeout = None
        self.assertEqual(params.socket_timeout, None)

        with self.assertRaises(TypeError):
            params.socket_timeout = '60.5'

        with self.assertRaises(ValueError):
            params.socket_timeout = -1

        with self.assertRaises(ValueError):
            params.socket_timeout = 0

    def test_ssl_options(self):
        params = connection.Parameters()

        ssl_options = connection.SSLOptions(ssl.create_default_context())
        params.ssl_options = ssl_options
        self.assertIs(params.ssl_options, ssl_options)

        params.ssl_options = None
        self.assertIsNone(params.ssl_options)

        with self.assertRaises(TypeError):
            params.ssl_options = dict()

    def test_virtual_host(self):
        params = connection.Parameters()

        params.virtual_host = '/'
        self.assertEqual(params.virtual_host, '/')

        params.virtual_host = u'/'
        self.assertEqual(params.virtual_host, u'/')

        params.virtual_host = 'test-vhost'
        self.assertEqual(params.virtual_host, 'test-vhost')

        with self.assertRaises(TypeError):
            params.virtual_host = 99

    def test_tcp_options(self):
        params = connection.Parameters()

        opt = dict(
            TCP_KEEPIDLE=60,
            TCP_KEEPINTVL=2,
            TCP_KEEPCNT=1,
            TCP_USER_TIMEOUT=1000)
        params.tcp_options = copy.deepcopy(opt)
        self.assertEqual(params.tcp_options, opt)

        params.tcp_options = None
        self.assertIsNone(params.tcp_options)

        with self.assertRaises(TypeError):
            params.tcp_options = str(opt)


class ConnectionParametersTests(ParametersTestsBase):

    def test_default_property_values(self):
        self.assert_default_parameter_values(connection.ConnectionParameters())

    def test_explicit_ssl_with_default_port(self):
        params = connection.ConnectionParameters(
            ssl_options=connection.SSLOptions(ssl.create_default_context()))

        self.assertIsNotNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_SSL_PORT)

    def test_explicit_ssl_with_explict_port(self):
        params = connection.ConnectionParameters(
            ssl_options=connection.SSLOptions(ssl.create_default_context()),
            port=99)

        self.assertIsNotNone(params.ssl_options)
        self.assertEqual(params.port, 99)

    def test_explicit_non_ssl_with_default_port(self):
        params = connection.ConnectionParameters(ssl_options=None)

        self.assertIsNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_PORT)

    def test_explicit_non_ssl_with_explict_port(self):
        params = connection.ConnectionParameters(ssl_options=None, port=100)

        self.assertIsNone(params.ssl_options)
        self.assertEqual(params.port, 100)

    def test_exlicit_none_stack_timeout(self):
        params = connection.ConnectionParameters(stack_timeout=None)
        self.assertIsNone(params.stack_timeout)

    def test_exlicit_none_socket_timeout(self):
        params = connection.ConnectionParameters(socket_timeout=None)
        self.assertIsNone(params.socket_timeout)

    def test_good_connection_parameters(self):
        """make sure connection kwargs get set correctly"""
        kwargs = {
            'blocked_connection_timeout': 10.5,
            'channel_max': 3,
            'client_properties': {
                'good': 'day'
            },
            'connection_attempts': 2,
            'credentials': credentials.PlainCredentials('very', 'secure'),
            'frame_max': 40000,
            'heartbeat': 7,
            'host': 'https://www.test.com',
            'locale': 'en',
            'port': 5678,
            'retry_delay': 3,
            'socket_timeout': 100.5,
            'stack_timeout': 150,
            'ssl_options': None,
            'virtual_host': u'vvhost',
            'tcp_options': {
                'TCP_USER_TIMEOUT': 1000
            }
        }
        params = connection.ConnectionParameters(**kwargs)

        # Verify

        expected_values = copy.copy(kwargs)

        # Make sure we're testing all public properties
        self.assertSequenceEqual(
            sorted(expected_values), sorted(_ALL_PUBLIC_PARAMETERS_PROPERTIES))
        # Check property values
        for t_param in expected_values:
            value = getattr(params, t_param)
            self.assertEqual(
                expected_values[t_param],
                value,
                msg='Expected %s=%r, but got %r' %
                (t_param, expected_values[t_param], value))

    def test_callable_heartbeat(self):
        def heartbeat_callback(_connection, _broker_val):
            return 1
        parameters = pika.ConnectionParameters(heartbeat=heartbeat_callback)
        self.assertIs(parameters.heartbeat, heartbeat_callback)

    def test_bad_type_connection_parameters(self):
        """test connection kwargs type checks throw errors for bad input"""
        kwargs = {
            'host': 'https://www.test.com',
            'port': 5678,
            'virtual_host': 'vvhost',
            'channel_max': 3,
            'frame_max': 40000,
            'heartbeat': 7,
            'ssl': True,
            'blocked_connection_timeout': 10.5
        }
        # Test Type Errors
        for bad_field, bad_value in (
                ('host', 15672),
                ('port', '5672a'),
                ('virtual_host', True),
                ('channel_max', '4'),
                ('frame_max', '5'),
                ('credentials', 'bad'),
                ('locale', 1),
                ('heartbeat', '6'),
                ('socket_timeout', '42'),
                ('stack_timeout', '99'),
                ('retry_delay', 'two'),
                ('ssl', {'ssl': 'dict'}),
                ('ssl_options', True),
                ('connection_attempts', 'hello'),
                ('blocked_connection_timeout', set())):

            bkwargs = copy.deepcopy(kwargs)

            bkwargs[bad_field] = bad_value

            self.assertRaises(TypeError, connection.ConnectionParameters,
                              **bkwargs)

    def test_parameters_accepts_plain_string_virtualhost(self):
        parameters = pika.ConnectionParameters(virtual_host='prtfqpeo')
        self.assertEqual(parameters.virtual_host, 'prtfqpeo')

    def test_parameters_accepts_unicode_string_virtualhost(self):
        parameters = pika.ConnectionParameters(virtual_host=u'prtfqpeo')
        self.assertEqual(parameters.virtual_host, 'prtfqpeo')

    def test_parameters_accept_plain_string_locale(self):
        parameters = pika.ConnectionParameters(locale='en_US')
        self.assertEqual(parameters.locale, 'en_US')

    def test_parameters_accept_unicode_locale(self):
        parameters = pika.ConnectionParameters(locale=u'en_US')
        self.assertEqual(parameters.locale, 'en_US')


class URLParametersTests(ParametersTestsBase):

    def test_default_property_values(self):
        params = connection.URLParameters('')
        self.assert_default_parameter_values(params)

        self.assertIsNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_PORT)

    def test_no_ssl(self):
        params = connection.URLParameters('http://')
        self.assertIsNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_PORT)
        self.assert_default_parameter_values(params)

        params = connection.URLParameters('amqp://')
        self.assertIsNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_PORT)
        self.assert_default_parameter_values(params)

    def test_ssl(self):
        params = connection.URLParameters('https://')
        self.assertIsNotNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_SSL_PORT)

        params = connection.URLParameters('amqps://')
        self.assertIsNotNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_SSL_PORT)

        # Make sure the other parameters unrelated to SSL have default values
        params = connection.URLParameters('amqps://')
        params.ssl_options = None
        params.port = params.DEFAULT_PORT
        self.assert_default_parameter_values(params)

    def test_no_url_scheme_defaults_to_plaintext(self):
        params = connection.URLParameters('//')
        self.assertIsNone(params.ssl_options)
        self.assertEqual(params.port, params.DEFAULT_PORT)

    def test_good_parameters(self):
        """test for the different query stings checked by process url"""
        query_args = {
            'blocked_connection_timeout': 10.5,
            'channel_max': 3,
            'connection_attempts': 2,
            'frame_max': 40000,
            'heartbeat': 7,
            'locale': 'en_UK',
            'retry_delay': 3,
            'socket_timeout': 100.5,
            'ssl_options': None,
            'tcp_options': {
                'TCP_USER_TIMEOUT': 1000,
                'TCP_KEEPIDLE': 60
            }
        }

        test_params = dict(query_args)
        virtual_host = '/'
        query_string = urlencode(test_params)
        test_url = ('amqp://myuser:mypass@www.test.com:5678/%s?%s' % (
            url_quote(virtual_host, safe=''),
            query_string,
        ))

        params = connection.URLParameters(test_url)

        # check all value from query string

        for t_param in query_args:
            expected_value = query_args[t_param]
            actual_value = getattr(params, t_param)

            self.assertEqual(
                actual_value,
                expected_value,
                msg='Expected %s=%r, but got %r' %
                (t_param, expected_value, actual_value))

        # check all values from base URL
        self.assertEqual(params.credentials.username, 'myuser')
        self.assertEqual(params.credentials.password, 'mypass')
        self.assertEqual(params.host, 'www.test.com')
        self.assertEqual(params.port, 5678)
        self.assertEqual(params.virtual_host, virtual_host)

    def test_accepts_plain_string(self):
        parameters = pika.URLParameters('amqp://prtfqpeo:oihdglkhcp0@myserver.'
                                        'mycompany.com:5672/prtfqpeo?locale='
                                        'en_US')
        self.assertEqual(parameters.port, 5672)
        self.assertEqual(parameters.virtual_host, 'prtfqpeo')
        self.assertEqual(parameters.credentials.password, 'oihdglkhcp0')
        self.assertEqual(parameters.credentials.username, 'prtfqpeo')
        self.assertEqual(parameters.locale, 'en_US')

    def test_accepts_unicode_string(self):
        parameters = pika.URLParameters(u'amqp://prtfqpeo:oihdglkhcp0@myserver'
                                        u'.mycompany.com:5672/prtfqpeo?locale='
                                        u'en_US')
        self.assertEqual(parameters.port, 5672)
        self.assertEqual(parameters.virtual_host, 'prtfqpeo')
        self.assertEqual(parameters.credentials.password, 'oihdglkhcp0')
        self.assertEqual(parameters.credentials.username, 'prtfqpeo')
        self.assertEqual(parameters.locale, 'en_US')

    def test_uses_default_port_if_not_specified(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com')
        self.assertEqual(parameters.port, pika.URLParameters.DEFAULT_PORT)

    def test_uses_default_virtual_host_if_not_specified(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)

    def test_uses_default_virtual_host_if_only_slash_is_specified(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com/')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)

    def test_uses_default_virtual_host_via_encoded_slash_upcase(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com/%2F')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)

    def test_uses_default_virtual_host_via_encoded_slash_downcase(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com/%2f')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)

    def test_uses_default_virtual_host_via_encoded_slash_upcase_ending_with_slash(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com/%2F/')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)

    def test_uses_default_virtual_host_via_encoded_slash_downcase_ending_with_slash(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com/%2f/')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)

    def test_uses_default_virtual_host_if_only_parameters_provided(self):
        parameters = pika.URLParameters(
            'amqp://myserver.mycompany.com?frame_max=8192&locale=utf8')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)
        self.assertEqual(parameters.frame_max, 8192)
        self.assertEqual(parameters.locale, 'utf8')

    def test_uses_default_username_and_password_if_not_specified(self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com')
        self.assertEqual(parameters.credentials.username,
                         pika.URLParameters.DEFAULT_USERNAME)
        self.assertEqual(parameters.credentials.password,
                         pika.URLParameters.DEFAULT_PASSWORD)

    def test_accepts_blank_username_and_password(self):
        parameters = pika.URLParameters('amqp://:@myserver.mycompany.com')
        self.assertEqual(parameters.credentials.username, '')
        self.assertEqual(parameters.credentials.password, '')

    def test_url_decodes_username_and_password(self):
        username = '@@@@'
        password = '////'
        parameters = pika.URLParameters(
            'amqp://%40%40%40%40:%2F%2F%2F%2F@myserver.mycompany.com')
        self.assertEqual(parameters.credentials.username, username)
        self.assertEqual(parameters.credentials.password, password)
