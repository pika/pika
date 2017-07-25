"""
Test `pika.connection.Parameters`, `pika.connection.ConnectionParameters`, and
`pika.connection.URLParameters`
"""

# Disable pylint complaints about missing docstrings and invalid method names
# pylint: disable=C0111,C0103

import copy
import warnings

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika
from pika.compat import urlencode, url_quote, dict_iteritems
from pika import channel
from pika import connection
from pika import credentials
from pika import exceptions
from pika import spec


# Unordered sequence of connection.Parameters's property getters
_ALL_PUBLIC_PARAMETERS_PROPERTIES = tuple(
    attr for attr in vars(connection.Parameters)
    if not attr.startswith('_') and
    issubclass(type(getattr(connection.Parameters, attr)), property)
)


class _ParametersTestsBase(unittest.TestCase):

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
            'backpressure_detection': kls.DEFAULT_BACKPRESSURE_DETECTION,
            'blocked_connection_timeout':
                kls.DEFAULT_BLOCKED_CONNECTION_TIMEOUT,
            'channel_max': kls.DEFAULT_CHANNEL_MAX,
            'client_properties': kls.DEFAULT_CLIENT_PROPERTIES,
            'connection_attempts': kls.DEFAULT_CONNECTION_ATTEMPTS,
            'credentials': credentials.PlainCredentials(kls.DEFAULT_USERNAME,
                                                        kls.DEFAULT_PASSWORD),
            'frame_max': kls.DEFAULT_FRAME_MAX,
            'heartbeat': kls.DEFAULT_HEARTBEAT_TIMEOUT,
            'host': kls.DEFAULT_HOST,
            'locale': kls.DEFAULT_LOCALE,
            'port': kls.DEFAULT_PORT,
            'retry_delay': kls.DEFAULT_RETRY_DELAY,
            'socket_timeout': kls.DEFAULT_SOCKET_TIMEOUT,
            'ssl': kls.DEFAULT_SSL,
            'ssl_options': kls.DEFAULT_SSL_OPTIONS,
            'virtual_host': kls.DEFAULT_VIRTUAL_HOST
        }

        # Make sure we didn't miss anything
        self.assertSequenceEqual(sorted(defaults),
                                 sorted(_ALL_PUBLIC_PARAMETERS_PROPERTIES))

        return defaults

    def assert_default_parameter_values(self, params):
        """Assert that the given parameters object has the default parameter
        values.

        :param params: verify that the given params instance has all default
           property values
        :type params: one of the classes based on `pika.connection.Parameters`

        """
        for name, expected_value in dict_iteritems(
                self.get_default_properties()):
            value = getattr(params, name)
            self.assertEqual(value, expected_value,
                             msg='Expected %s=%r, but got %r' %
                             (name, expected_value, value))


class ParametersTests(_ParametersTestsBase):
    """Test `pika.connection.Parameters`"""

    def test_default_property_values(self):
        self.assert_default_parameter_values(connection.Parameters())

    def test_backpressure_detection(self):
        params = connection.Parameters()

        params.backpressure_detection = False
        self.assertEqual(params.backpressure_detection, False)

        params.backpressure_detection = True
        self.assertEqual(params.backpressure_detection, True)

        with self.assertRaises(TypeError):
            params.backpressure_detection = 1

        with self.assertRaises(TypeError):
            params.backpressure_detection = 'true'

        with self.assertRaises(TypeError):
            params.backpressure_detection = 'f'

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

    def test_host(self):
        params = connection.Parameters()

        params.host = '127.0.0.1'
        self.assertEqual(params.host, '127.0.0.1')

        params.host = 'my.server.com'
        self.assertEqual(params.host, 'my.server.com')

        params.host = u'my.server.com'
        self.assertEqual(params.host, u'my.server.com')

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

        with self.assertRaises(TypeError):
            params.port = 1.5

        with self.assertRaises(TypeError):
            params.port = '5672'

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

        with self.assertRaises(TypeError):
            params.socket_timeout = '60.5'

        with self.assertRaises(ValueError):
            params.socket_timeout = -1

        with self.assertRaises(ValueError):
            params.socket_timeout = 0

    def test_ssl(self):
        params = connection.Parameters()

        params.ssl = False
        self.assertEqual(params.ssl, False)

        params.ssl = True
        self.assertEqual(params.ssl, True)

        with self.assertRaises(TypeError):
            params.backpressure_detection = 1

        with self.assertRaises(TypeError):
            params.ssl = 'True'

        with self.assertRaises(TypeError):
            params.ssl = 'f'

    def test_ssl_options(self):
        params = connection.Parameters()

        opt = dict(key='value', key2=2, key3=dict(a=1))
        params.ssl_options = copy.deepcopy(opt)
        self.assertEqual(params.ssl_options, opt)

        params.ssl_options = None
        self.assertIsNone(params.ssl_options)

        with self.assertRaises(TypeError):
            params.ssl_options = str(opt)

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


class ConnectionParametersTests(_ParametersTestsBase):
    def test_default_property_values(self):
        self.assert_default_parameter_values(connection.ConnectionParameters())

    def test_explicit_ssl_with_default_port(self):
        params = connection.ConnectionParameters(ssl=True)

        self.assertEqual(params.ssl, True)
        self.assertEqual(params.port, params.DEFAULT_SSL_PORT)

    def test_explicit_ssl_with_explict_port(self):
        params = connection.ConnectionParameters(ssl=True, port=99)

        self.assertEqual(params.ssl, True)
        self.assertEqual(params.port, 99)

    def test_explicit_non_ssl_with_default_port(self):
        params = connection.ConnectionParameters(ssl=False)

        self.assertEqual(params.ssl, False)
        self.assertEqual(params.port, params.DEFAULT_PORT)

    def test_explicit_non_ssl_with_explict_port(self):
        params = connection.ConnectionParameters(ssl=False, port=100)

        self.assertEqual(params.ssl, False)
        self.assertEqual(params.port, 100)

    def test_good_connection_parameters(self):
        """make sure connection kwargs get set correctly"""
        kwargs = {
            'backpressure_detection': False,
            'blocked_connection_timeout': 10.5,
            'channel_max': 3,
            'client_properties': {'good': 'day'},
            'connection_attempts': 2,
            'credentials': credentials.PlainCredentials('very', 'secure'),
            'frame_max': 40000,
            'heartbeat': 7,
            'host': 'https://www.test.com',
            'locale': 'en',
            'port': 5678,
            'retry_delay': 3,
            'socket_timeout': 100.5,
            'ssl': True,
            'ssl_options': {'ssl': 'options'},
            'virtual_host': u'vvhost',
        }
        params = connection.ConnectionParameters(**kwargs)

        # Verify

        expected_values = copy.copy(kwargs)

        # Make sure we're testing all public properties
        self.assertSequenceEqual(sorted(expected_values),
                                 sorted(_ALL_PUBLIC_PARAMETERS_PROPERTIES))
        # Check property values
        for t_param in expected_values:
            value = getattr(params, t_param)
            self.assertEqual(expected_values[t_param], value,
                             msg='Expected %s=%r, but got %r' %
                             (t_param, expected_values[t_param], value))

    def test_deprecated_heartbeat_interval(self):
        with warnings.catch_warnings(record=True) as warnings_list:
            warnings.simplefilter('always')

            params = connection.ConnectionParameters(heartbeat_interval=999)
            self.assertEqual(params.heartbeat, 999)

            # Check that a warning was generated
            self.assertEqual(len(warnings_list), 1)
            self.assertIs(warnings_list[0].category,
                          DeprecationWarning)

    def test_bad_type_connection_parameters(self):
        """test connection kwargs type checks throw errors for bad input"""
        kwargs = {
            'host': 'https://www.test.com',
            'port': 5678,
            'virtual_host': 'vvhost',
            'channel_max': 3,
            'frame_max': 40000,
            'heartbeat': 7,
            'backpressure_detection': False,
            'ssl': True,
            'blocked_connection_timeout': 10.5
        }
        # Test Type Errors
        for bad_field, bad_value in (
                ('host', 15672), ('port', '5672'), ('virtual_host', True),
                ('channel_max', '4'), ('frame_max', '5'),
                ('credentials', 'bad'), ('locale', 1),
                ('heartbeat', '6'), ('socket_timeout', '42'),
                ('retry_delay', 'two'), ('backpressure_detection', 'true'),
                ('ssl', {'ssl': 'dict'}), ('ssl_options', True),
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


class URLParametersTests(_ParametersTestsBase):

    def test_default_property_values(self):
        params = connection.URLParameters('')
        self.assert_default_parameter_values(params)

        self.assertEqual(params.ssl, False)
        self.assertEqual(params.port, params.DEFAULT_PORT)

    def test_no_ssl(self):
        params = connection.URLParameters('http://')
        self.assertEqual(params.ssl, False)
        self.assertEqual(params.port, params.DEFAULT_PORT)
        self.assert_default_parameter_values(params)

        params = connection.URLParameters('amqp://')
        self.assertEqual(params.ssl, False)
        self.assertEqual(params.port, params.DEFAULT_PORT)
        self.assert_default_parameter_values(params)

    def test_ssl(self):
        params = connection.URLParameters('https://')
        self.assertEqual(params.ssl, True)
        self.assertEqual(params.port, params.DEFAULT_SSL_PORT)

        params = connection.URLParameters('amqps://')
        self.assertEqual(params.ssl, True)
        self.assertEqual(params.port, params.DEFAULT_SSL_PORT)

        # Make sure the other parameters unrelated to SSL have default values
        params = connection.URLParameters('amqps://')
        params.ssl = False
        params.port = params.DEFAULT_PORT
        self.assert_default_parameter_values(params)

    def test_no_url_scheme_defaults_to_plaintext(self):
        params = connection.URLParameters('//')
        self.assertEqual(params.ssl, False)
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
            'ssl_options': {'ssl': 'options'}
        }

        for backpressure in ('t', 'f'):
            test_params = copy.deepcopy(query_args)
            test_params['backpressure_detection'] = backpressure
            virtual_host = '/'
            query_string = urlencode(test_params)
            test_url = ('https://myuser:mypass@www.test.com:5678/%s?%s' %
                        (url_quote(virtual_host, safe=''), query_string,))

            params = connection.URLParameters(test_url)

            # check all value from query string

            for t_param in query_args:
                expected_value = query_args[t_param]
                actual_value = getattr(params, t_param)

                self.assertEqual(actual_value, expected_value,
                                 msg = 'Expected %s=%r, but got %r' %
                                 (t_param, expected_value, actual_value))

            self.assertEqual(params.backpressure_detection, backpressure == 't')

            # check all values from base URL
            self.assertEqual(params.ssl, True)
            self.assertEqual(params.credentials.username, 'myuser')
            self.assertEqual(params.credentials.password, 'mypass')
            self.assertEqual(params.host, 'www.test.com')
            self.assertEqual(params.port, 5678)
            self.assertEqual(params.virtual_host, virtual_host)

    def test_deprecated_heartbeat_interval(self):
        with warnings.catch_warnings(record=True) as warnings_list:
            warnings.simplefilter('always')

            params = pika.URLParameters(
                'amqp://prtfqpeo:oihdglkhcp0@myserver.'
                'mycompany.com:5672/prtfqpeo?heartbeat_interval=999')

            self.assertEqual(params.heartbeat, 999)

            # Check that a warning was generated
            self.assertEqual(len(warnings_list), 1)
            self.assertIs(warnings_list[0].category,
                          DeprecationWarning)

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

    def test_uses_default_virtual_host_if_only_slash_is_specified(
            self):
        parameters = pika.URLParameters('amqp://myserver.mycompany.com/')
        self.assertEqual(parameters.virtual_host,
                         pika.URLParameters.DEFAULT_VIRTUAL_HOST)

    def test_uses_default_username_and_password_if_not_specified(
            self):
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
        parameters = pika.URLParameters('amqp://%40%40%40%40:%2F%2F%2F%2F@myserver.mycompany.com')
        self.assertEqual(parameters.credentials.username, username)
        self.assertEqual(parameters.credentials.password, password)

