"""Core connection objects"""
import ast
import sys
import collections
import copy
import logging
import math
import numbers
import os
import platform
import warnings
import ssl

if sys.version_info > (3,):
    import urllib.parse as urlparse  # pylint: disable=E0611,F0401
else:
    import urlparse

from pika import __version__
from pika import callback
import pika.channel
from pika import credentials as pika_credentials
from pika import exceptions
from pika import frame
from pika import heartbeat
from pika import utils

from pika import spec

from pika.compat import (xrange, basestring, # pylint: disable=W0622
                         url_unquote, dictkeys, dict_itervalues,
                         dict_iteritems)


BACKPRESSURE_WARNING = ("Pika: Write buffer exceeded warning threshold at "
                        "%i bytes and an estimated %i frames behind")
PRODUCT = "Pika Python Client Library"

LOGGER = logging.getLogger(__name__)


class InternalCloseReasons(object):
    """Internal reason codes passed to the user's on_close_callback when the
    connection is terminated abruptly, without reply code/text from the broker.

    AMQP 0.9.1 specification cites IETF RFC 821 for reply codes. To avoid
    conflict, the `InternalCloseReasons` namespace uses negative integers. These
    are invalid for sending to the broker.
    """
    SOCKET_ERROR = -1
    BLOCKED_CONNECTION_TIMEOUT = -2


class Parameters(object):  # pylint: disable=R0902
    """Base connection parameters class definition

    :param bool backpressure_detection: `DEFAULT_BACKPRESSURE_DETECTION`
    :param float|None blocked_connection_timeout:
        `DEFAULT_BLOCKED_CONNECTION_TIMEOUT`
    :param int channel_max: `DEFAULT_CHANNEL_MAX`
    :param int connection_attempts: `DEFAULT_CONNECTION_ATTEMPTS`
    :param  credentials: `DEFAULT_CREDENTIALS`
    :param int frame_max: `DEFAULT_FRAME_MAX`
    :param int heartbeat: `DEFAULT_HEARTBEAT_TIMEOUT`
    :param str host: `DEFAULT_HOST`
    :param str locale: `DEFAULT_LOCALE`
    :param int port: `DEFAULT_PORT`
    :param float retry_delay: `DEFAULT_RETRY_DELAY`
    :param float socket_timeout: `DEFAULT_SOCKET_TIMEOUT`
    :param bool ssl: `DEFAULT_SSL`
    :param dict ssl_options: `DEFAULT_SSL_OPTIONS`
    :param str virtual_host: `DEFAULT_VIRTUAL_HOST`
    :param int tcp_options: `DEFAULT_TCP_OPTIONS`
    """

    # Declare slots to protect against accidental assignment of an invalid
    # attribute
    __slots__ = (
        '_backpressure_detection',
        '_blocked_connection_timeout',
        '_channel_max',
        '_client_properties',
        '_connection_attempts',
        '_credentials',
        '_frame_max',
        '_heartbeat',
        '_host',
        '_locale',
        '_port',
        '_retry_delay',
        '_socket_timeout',
        '_ssl',
        '_ssl_options',
        '_virtual_host',
        '_tcp_options'
    )

    DEFAULT_USERNAME = 'guest'
    DEFAULT_PASSWORD = 'guest'

    DEFAULT_BACKPRESSURE_DETECTION = False
    DEFAULT_BLOCKED_CONNECTION_TIMEOUT = None
    DEFAULT_CHANNEL_MAX = pika.channel.MAX_CHANNELS
    DEFAULT_CLIENT_PROPERTIES = None
    DEFAULT_CREDENTIALS = pika_credentials.PlainCredentials(DEFAULT_USERNAME,
                                                            DEFAULT_PASSWORD)
    DEFAULT_CONNECTION_ATTEMPTS = 1
    DEFAULT_FRAME_MAX = spec.FRAME_MAX_SIZE
    DEFAULT_HEARTBEAT_TIMEOUT = None          # None accepts server's proposal
    DEFAULT_HOST = 'localhost'
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_PORT = 5672
    DEFAULT_RETRY_DELAY = 2.0
    DEFAULT_SOCKET_TIMEOUT = 10.0
    DEFAULT_SSL = False
    DEFAULT_SSL_OPTIONS = None
    DEFAULT_SSL_PORT = 5671
    DEFAULT_VIRTUAL_HOST = '/'
    DEFAULT_TCP_OPTIONS = None

    DEFAULT_HEARTBEAT_INTERVAL = DEFAULT_HEARTBEAT_TIMEOUT # DEPRECATED

    def __init__(self):
        self._backpressure_detection = None
        self.backpressure_detection = self.DEFAULT_BACKPRESSURE_DETECTION

        # If not None, blocked_connection_timeout is the timeout, in seconds,
        # for the connection to remain blocked; if the timeout expires, the
        # connection will be torn down, triggering the connection's
        # on_close_callback
        self._blocked_connection_timeout = None
        self.blocked_connection_timeout = (
            self.DEFAULT_BLOCKED_CONNECTION_TIMEOUT)

        self._channel_max = None
        self.channel_max = self.DEFAULT_CHANNEL_MAX

        self._client_properties = None
        self.client_properties = self.DEFAULT_CLIENT_PROPERTIES

        self._connection_attempts = None
        self.connection_attempts = self.DEFAULT_CONNECTION_ATTEMPTS

        self._credentials = None
        self.credentials = self.DEFAULT_CREDENTIALS

        self._frame_max = None
        self.frame_max = self.DEFAULT_FRAME_MAX

        self._heartbeat = None
        self.heartbeat = self.DEFAULT_HEARTBEAT_TIMEOUT

        self._host = None
        self.host = self.DEFAULT_HOST

        self._locale = None
        self.locale = self.DEFAULT_LOCALE

        self._port = None
        self.port = self.DEFAULT_PORT

        self._retry_delay = None
        self.retry_delay = self.DEFAULT_RETRY_DELAY

        self._socket_timeout = None
        self.socket_timeout = self.DEFAULT_SOCKET_TIMEOUT

        self._ssl = None
        self.ssl = self.DEFAULT_SSL

        self._ssl_options = None
        self.ssl_options = self.DEFAULT_SSL_OPTIONS

        self._virtual_host = None
        self.virtual_host = self.DEFAULT_VIRTUAL_HOST

        self._tcp_options = None
        self.tcp_options = self.DEFAULT_TCP_OPTIONS

    def __repr__(self):
        """Represent the info about the instance.

        :rtype: str

        """
        return ('<%s host=%s port=%s virtual_host=%s ssl=%s>' %
                (self.__class__.__name__, self.host, self.port,
                 self.virtual_host, self.ssl))

    @property
    def backpressure_detection(self):
        """
        :returns: boolean indicating whether backpressure detection is
            enabled. Defaults to `DEFAULT_BACKPRESSURE_DETECTION`.

        """
        return self._backpressure_detection

    @backpressure_detection.setter
    def backpressure_detection(self, value):
        """
        :param bool value: boolean indicating whether to enable backpressure
            detection

        """
        if not isinstance(value, bool):
            raise TypeError('backpressure_detection must be a bool, '
                            'but got %r' % (value,))
        self._backpressure_detection = value

    @property
    def blocked_connection_timeout(self):
        """
        :returns: None or float blocked connection timeout. Defaults to
            `DEFAULT_BLOCKED_CONNECTION_TIMEOUT`.

        """
        return self._blocked_connection_timeout

    @blocked_connection_timeout.setter
    def blocked_connection_timeout(self, value):
        """
        :param value: If not None, blocked_connection_timeout is the timeout, in
            seconds, for the connection to remain blocked; if the timeout
            expires, the connection will be torn down, triggering the
            connection's on_close_callback

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('blocked_connection_timeout must be a Real '
                                'number, but got %r' % (value,))
            if value < 0:
                raise ValueError('blocked_connection_timeout must be >= 0, but '
                                 'got %r' % (value,))
        self._blocked_connection_timeout = value

    @property
    def channel_max(self):
        """
        :returns: max preferred number of channels. Defaults to
            `DEFAULT_CHANNEL_MAX`.
        :rtype: int

        """
        return self._channel_max

    @channel_max.setter
    def channel_max(self, value):
        """
        :param int value: max preferred number of channels, between 1 and
           `channel.MAX_CHANNELS`, inclusive

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError('channel_max must be an int, but got %r' % (value,))
        if value < 1 or value > pika.channel.MAX_CHANNELS:
            raise ValueError('channel_max must be <= %i and > 0, but got %r' %
                             (pika.channel.MAX_CHANNELS, value))
        self._channel_max = value

    @property
    def client_properties(self):
        """
        :returns: None or dict of client properties used to override the fields
            in the default client poperties reported  to RabbitMQ via
            `Connection.StartOk` method. Defaults to
            `DEFAULT_CLIENT_PROPERTIES`.

        """
        return self._client_properties

    @client_properties.setter
    def client_properties(self, value):
        """
        :param value: None or dict of client properties used to override the
            fields in the default client poperties reported to RabbitMQ via
            `Connection.StartOk` method.
        """
        if not isinstance(value, (dict, type(None),)):
            raise TypeError('client_properties must be dict or None, '
                            'but got %r' % (value,))
        # Copy the mutable object to avoid accidental side-effects
        self._client_properties = copy.deepcopy(value)

    @property
    def connection_attempts(self):
        """
        :returns: number of socket connection attempts. Defaults to
            `DEFAULT_CONNECTION_ATTEMPTS`.

        """
        return self._connection_attempts

    @connection_attempts.setter
    def connection_attempts(self, value):
        """
        :param int value: number of socket connection attempts of at least 1

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError('connection_attempts must be an int')
        if value < 1:
            raise ValueError('connection_attempts must be > 0, but got %r' %
                             (value,))
        self._connection_attempts = value

    @property
    def credentials(self):
        """
        :rtype: one of the classes from `pika.credentials.VALID_TYPES`. Defaults
            to `DEFAULT_CREDENTIALS`.

        """
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        """
        :param value: authentication credential object of one of the classes
            from  `pika.credentials.VALID_TYPES`

        """
        if not isinstance(value, tuple(pika_credentials.VALID_TYPES)):
            raise TypeError('Credentials must be an object of type: %r, but '
                            'got %r' % (pika_credentials.VALID_TYPES, value))
        # Copy the mutable object to avoid accidental side-effects
        self._credentials = copy.deepcopy(value)

    @property
    def frame_max(self):
        """
        :returns: desired maximum AMQP frame size to use. Defaults to
            `DEFAULT_FRAME_MAX`.

        """
        return self._frame_max

    @frame_max.setter
    def frame_max(self, value):
        """
        :param int value: desired maximum AMQP frame size to use between
            `spec.FRAME_MIN_SIZE` and `spec.FRAME_MAX_SIZE`, inclusive

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError('frame_max must be an int, but got %r' % (value,))
        if value < spec.FRAME_MIN_SIZE:
            raise ValueError('Min AMQP 0.9.1 Frame Size is %i, but got %r',
                             (spec.FRAME_MIN_SIZE, value,))
        elif value > spec.FRAME_MAX_SIZE:
            raise ValueError('Max AMQP 0.9.1 Frame Size is %i, but got %r',
                             (spec.FRAME_MAX_SIZE, value,))
        self._frame_max = value

    @property
    def heartbeat(self):
        """
        :returns: AMQP connection heartbeat timeout value for negotiation during
            connection tuning or callable which is invoked during connection tuning.
            None to accept broker's value. 0 turns heartbeat off. Defaults to
            `DEFAULT_HEARTBEAT_TIMEOUT`.
        :rtype: integer, None or callable

        """
        return self._heartbeat

    @heartbeat.setter
    def heartbeat(self, value):
        """
        :param int|None|callable value: Controls AMQP heartbeat timeout negotiation
            during connection tuning. An integer value always overrides the value
            proposed by broker. Use 0 to deactivate heartbeats and None to always
            accept the broker's proposal. If a callable is given, it will be called
            with the connection instance and the heartbeat timeout proposed by broker
            as its arguments. The callback should return a non-negative integer that
            will be used to override the broker's proposal.
        """
        if value is not None:
            if not isinstance(value, numbers.Integral) and not callable(value):
                raise TypeError('heartbeat must be an int or a callable function, but got %r' %
                                (value,))
            if not callable(value) and value < 0:
                raise ValueError('heartbeat must >= 0, but got %r' % (value,))
        self._heartbeat = value

    @property
    def host(self):
        """
        :returns: hostname or ip address of broker. Defaults to `DEFAULT_HOST`.
        :rtype: str

        """
        return self._host

    @host.setter
    def host(self, value):
        """
        :param str value: hostname or ip address of broker

        """
        if not isinstance(value, basestring):
            raise TypeError('host must be a str or unicode str, but got %r' %
                            (value,))
        self._host = value

    @property
    def locale(self):
        """
        :returns: locale value to pass to broker; e.g., 'en_US'. Defaults to
            `DEFAULT_LOCALE`.
        :rtype: str

        """
        return self._locale

    @locale.setter
    def locale(self, value):
        """
        :param str value: locale value to pass to broker; e.g., "en_US"

        """
        if not isinstance(value, basestring):
            raise TypeError('locale must be a str, but got %r' % (value,))
        self._locale = value

    @property
    def port(self):
        """
        :returns: port number of broker's listening socket. Defaults to
            `DEFAULT_PORT`.
        :rtype: int

        """
        return self._port

    @port.setter
    def port(self, value):
        """
        :param int value: port number of broker's listening socket

        """
        try:
            self._port = int(value)
        except (TypeError, ValueError):
            raise TypeError('port must be an int, but got %r' % (value,))

    @property
    def retry_delay(self):
        """
        :returns: interval between socket connection attempts; see also
            `connection_attempts`. Defaults to `DEFAULT_RETRY_DELAY`.
        :rtype: float

        """
        return self._retry_delay

    @retry_delay.setter
    def retry_delay(self, value):
        """
        :param float value: interval between socket connection attempts; see
            also `connection_attempts`.

        """
        if not isinstance(value, numbers.Real):
            raise TypeError('retry_delay must be a float or int, but got %r' %
                            (value,))
        self._retry_delay = value

    @property
    def socket_timeout(self):
        """
        :returns: socket timeout value. Defaults to `DEFAULT_SOCKET_TIMEOUT`.
        :rtype: float

        """
        return self._socket_timeout

    @socket_timeout.setter
    def socket_timeout(self, value):
        """
        :param float value: socket timeout value; NOTE: this is mostly unused
           now, owing to switchover to to non-blocking socket setting after
           initial socket connection establishment.

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('socket_timeout must be a float or int, '
                                'but got %r' % (value,))
            if not value > 0:
                raise ValueError('socket_timeout must be > 0, but got %r' %
                                 (value,))
        self._socket_timeout = value

    @property
    def ssl(self):
        """
        :returns: boolean indicating whether to connect via SSL. Defaults to
            `DEFAULT_SSL`.

        """
        return self._ssl

    @ssl.setter
    def ssl(self, value):
        """
        :param bool value: boolean indicating whether to connect via SSL

        """
        if not isinstance(value, bool):
            raise TypeError('ssl must be a bool, but got %r' % (value,))
        self._ssl = value

    @property
    def ssl_options(self):
        """
        :returns: None or a dict of options to pass to `ssl.wrap_socket`.
            Defaults to `DEFAULT_SSL_OPTIONS`.

        """
        return self._ssl_options

    @ssl_options.setter
    def ssl_options(self, value):
        """
        :param value: None, a dict of options to pass to `ssl.wrap_socket` or
            a SSLOptions object for advanced setup.

        """
        if not isinstance(value, (dict, SSLOptions, type(None))):
            raise TypeError(
                'ssl_options must be a dict, None or an SSLOptions but got %r'
                % (value, ))
        # Copy the mutable object to avoid accidental side-effects
        self._ssl_options = copy.deepcopy(value)


    @property
    def virtual_host(self):
        """
        :returns: rabbitmq virtual host name. Defaults to
            `DEFAULT_VIRTUAL_HOST`.

        """
        return self._virtual_host

    @virtual_host.setter
    def virtual_host(self, value):
        """
        :param str value: rabbitmq virtual host name

        """
        if not isinstance(value, basestring):
            raise TypeError('virtual_host must be a str, but got %r' % (value,))
        self._virtual_host = value

    @property
    def tcp_options(self):
        """
        :returns: None or a dict of options to pass to the underlying socket
        """
        return self._tcp_options

    @tcp_options.setter
    def tcp_options(self, value):
        """
        :param bool value: None or a dict of options to pass to the underlying
            socket. Currently supported are TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT
            and TCP_USER_TIMEOUT. Availability of these may depend on your platform.
        """
        if not isinstance(value, (dict, type(None))):
            raise TypeError('tcp_options must be a dict or None, but got %r' %
                            (value,))
        self._tcp_options = value


class ConnectionParameters(Parameters):
    """Connection parameters object that is passed into the connection adapter
    upon construction.

    """

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ()

    class _DEFAULT(object):
        """Designates default parameter value; internal use"""
        pass

    def __init__(self,  # pylint: disable=R0913,R0914,R0912
                 host=_DEFAULT,
                 port=_DEFAULT,
                 virtual_host=_DEFAULT,
                 credentials=_DEFAULT,
                 channel_max=_DEFAULT,
                 frame_max=_DEFAULT,
                 heartbeat=_DEFAULT,
                 ssl=_DEFAULT,
                 ssl_options=_DEFAULT,
                 connection_attempts=_DEFAULT,
                 retry_delay=_DEFAULT,
                 socket_timeout=_DEFAULT,
                 locale=_DEFAULT,
                 backpressure_detection=_DEFAULT,
                 blocked_connection_timeout=_DEFAULT,
                 client_properties=_DEFAULT,
                 tcp_options=_DEFAULT,
                 **kwargs):
        """Create a new ConnectionParameters instance. See `Parameters` for
        default values.

        :param str host: Hostname or IP Address to connect to
        :param int port: TCP port to connect to
        :param str virtual_host: RabbitMQ virtual host to use
        :param pika.credentials.Credentials credentials: auth credentials
        :param int channel_max: Maximum number of channels to allow
        :param int frame_max: The maximum byte size for an AMQP frame
        :param int|None|callable value: Controls AMQP heartbeat timeout negotiation
            during connection tuning. An integer value always overrides the value
            proposed by broker. Use 0 to deactivate heartbeats and None to always
            accept the broker's proposal. If a callable is given, it will be called
            with the connection instance and the heartbeat timeout proposed by broker
            as its arguments. The callback should return a non-negative integer that
            will be used to override the broker's proposal.
        :param bool ssl: Enable SSL
        :param dict ssl_options: None or a dict of arguments to be passed to
            ssl.wrap_socket
        :param int connection_attempts: Maximum number of retry attempts
        :param int|float retry_delay: Time to wait in seconds, before the next
        :param int|float socket_timeout: Use for high latency networks
        :param str locale: Set the locale value
        :param bool backpressure_detection: DEPRECATED in favor of
            `Connection.Blocked` and `Connection.Unblocked`. See
            `Connection.add_on_connection_blocked_callback`.
        :param blocked_connection_timeout: If not None,
            the value is a non-negative timeout, in seconds, for the
            connection to remain blocked (triggered by Connection.Blocked from
            broker); if the timeout expires before connection becomes unblocked,
            the connection will be torn down, triggering the adapter-specific
            mechanism for informing client app about the closed connection (
            e.g., on_close_callback or ConnectionClosed exception) with
            `reason_code` of `InternalCloseReasons.BLOCKED_CONNECTION_TIMEOUT`.
        :type blocked_connection_timeout: None, int, float
        :param client_properties: None or dict of client properties used to
            override the fields in the default client properties reported to
            RabbitMQ via `Connection.StartOk` method.
        :param heartbeat_interval: DEPRECATED; use `heartbeat` instead, and
            don't pass both
        :param tcp_options: None or a dict of TCP options to set for socket
        """
        super(ConnectionParameters, self).__init__()

        if backpressure_detection is not self._DEFAULT:
            self.backpressure_detection = backpressure_detection

        if blocked_connection_timeout is not self._DEFAULT:
            self.blocked_connection_timeout = blocked_connection_timeout

        if channel_max is not self._DEFAULT:
            self.channel_max = channel_max

        if client_properties is not self._DEFAULT:
            self.client_properties = client_properties

        if connection_attempts is not self._DEFAULT:
            self.connection_attempts = connection_attempts

        if credentials is not self._DEFAULT:
            self.credentials = credentials

        if frame_max is not self._DEFAULT:
            self.frame_max = frame_max

        if heartbeat is not self._DEFAULT:
            self.heartbeat = heartbeat

        try:
            heartbeat_interval = kwargs.pop('heartbeat_interval')
        except KeyError:
            # Good, this one is deprecated
            pass
        else:
            warnings.warn('heartbeat_interval is deprecated, use heartbeat',
                          DeprecationWarning, stacklevel=2)
            if heartbeat is not self._DEFAULT:
                raise TypeError('heartbeat and deprecated heartbeat_interval '
                                'are mutually-exclusive')
            self.heartbeat = heartbeat_interval

        if host is not self._DEFAULT:
            self.host = host

        if locale is not self._DEFAULT:
            self.locale = locale

        if retry_delay is not self._DEFAULT:
            self.retry_delay = retry_delay

        if socket_timeout is not self._DEFAULT:
            self.socket_timeout = socket_timeout

        if ssl is not self._DEFAULT:
            self.ssl = ssl

        if ssl_options is not self._DEFAULT:
            self.ssl_options = ssl_options

        # Set port after SSL status is known
        if port is not self._DEFAULT:
            self.port = port
        elif ssl is not self._DEFAULT:
            self.port = self.DEFAULT_SSL_PORT if self.ssl else self.DEFAULT_PORT

        if virtual_host is not self._DEFAULT:
            self.virtual_host = virtual_host

        if tcp_options is not self._DEFAULT:
            self.tcp_options = tcp_options

        if kwargs:
            raise TypeError('Unexpected kwargs: %r' % (kwargs,))


class URLParameters(Parameters):
    """Connect to RabbitMQ via an AMQP URL in the format::

         amqp://username:password@host:port/<virtual_host>[?query-string]

    Ensure that the virtual host is URI encoded when specified. For example if
    you are using the default "/" virtual host, the value should be `%2f`.

    See `Parameters` for default values.

    Valid query string values are:

        - backpressure_detection:
            DEPRECATED in favor of
            `Connection.Blocked` and `Connection.Unblocked`. See
            `Connection.add_on_connection_blocked_callback`.
        - channel_max:
            Override the default maximum channel count value
        - client_properties:
            dict of client properties used to override the fields in the default
            client properties reported to RabbitMQ via `Connection.StartOk`
            method
        - connection_attempts:
            Specify how many times pika should try and reconnect before it gives up
        - frame_max:
            Override the default maximum frame size for communication
        - heartbeat:
            Desired connection heartbeat timeout for negotiation. If not present
            the broker's value is accepted. 0 turns heartbeat off.
        - locale:
            Override the default `en_US` locale value
        - ssl:
            Toggle SSL, possible values are `t`, `f`
        - ssl_options:
            Arguments passed to :meth:`ssl.wrap_socket`
        - retry_delay:
            The number of seconds to sleep before attempting to connect on
            connection failure.
        - socket_timeout:
            Override low level socket timeout value
        - blocked_connection_timeout:
            Set the timeout, in seconds, that the connection may remain blocked
            (triggered by Connection.Blocked from broker); if the timeout
            expires before connection becomes unblocked, the connection will be
            torn down, triggering the connection's on_close_callback
        - tcp_options:
            Set the tcp options for the underlying socket.

    :param str url: The AMQP URL to connect to

    """

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ('_all_url_query_values',)


    # The name of the private function for parsing and setting a given URL query
    # arg is constructed by catenating the query arg's name to this prefix
    _SETTER_PREFIX = '_set_url_'

    def __init__(self, url):
        """Create a new URLParameters instance.

        :param str url: The URL value

        """
        super(URLParameters, self).__init__()

        self._all_url_query_values = None

        # Handle the Protocol scheme
        #
        # Fix up scheme amqp(s) to http(s) so urlparse won't barf on python
        # prior to 2.7. On Python 2.6.9,
        # `urlparse('amqp://127.0.0.1/%2f?socket_timeout=1')` produces an
        # incorrect path='/%2f?socket_timeout=1'
        if url[0:4].lower() == 'amqp':
            url = 'http' + url[4:]

        # TODO Is support for the alternative http(s) schemes intentional?

        parts = urlparse.urlparse(url)

        if parts.scheme == 'https':
            self.ssl = True
        elif parts.scheme == 'http':
            self.ssl = False
        elif parts.scheme:
            raise ValueError('Unexpected URL scheme %r; supported scheme '
                             'values: amqp, amqps' % (parts.scheme,))

        if parts.hostname is not None:
            self.host = parts.hostname

        # Take care of port after SSL status is known
        if parts.port is not None:
            self.port = parts.port
        else:
            self.port = self.DEFAULT_SSL_PORT if self.ssl else self.DEFAULT_PORT

        if parts.username is not None:
            self.credentials = pika_credentials.PlainCredentials(url_unquote(parts.username),
                                                                 url_unquote(parts.password))

        # Get the Virtual Host
        if len(parts.path) > 1:
            self.virtual_host = url_unquote(parts.path.split('/')[1])

        # Handle query string values, validating and assigning them
        self._all_url_query_values = urlparse.parse_qs(parts.query)

        for name, value in dict_iteritems(self._all_url_query_values):
            try:
                set_value = getattr(self, self._SETTER_PREFIX + name)
            except AttributeError:
                raise ValueError('Unknown URL parameter: %r' % (name,))

            try:
                (value,) = value
            except ValueError:
                raise ValueError('Expected exactly one value for URL parameter '
                                 '%s, but got %i values: %s' % (
                                     name, len(value), value))

            set_value(value)

    def _set_url_backpressure_detection(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            backpressure_detection = {'t': True, 'f': False}[value]
        except KeyError:
            raise ValueError('Invalid backpressure_detection value: %r' %
                             (value,))
        self.backpressure_detection = backpressure_detection

    def _set_url_blocked_connection_timeout(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            blocked_connection_timeout = float(value)
        except ValueError as exc:
            raise ValueError('Invalid blocked_connection_timeout value %r: %r' %
                             (value, exc,))
        self.blocked_connection_timeout = blocked_connection_timeout

    def _set_url_channel_max(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            channel_max = int(value)
        except ValueError as exc:
            raise ValueError('Invalid channel_max value %r: %r' % (value, exc,))
        self.channel_max = channel_max

    def _set_url_client_properties(self, value):
        """Deserialize and apply the corresponding query string arg"""
        self.client_properties = ast.literal_eval(value)

    def _set_url_connection_attempts(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            connection_attempts = int(value)
        except ValueError as exc:
            raise ValueError('Invalid connection_attempts value %r: %r' %
                             (value, exc,))
        self.connection_attempts = connection_attempts

    def _set_url_frame_max(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            frame_max = int(value)
        except ValueError as exc:
            raise ValueError('Invalid frame_max value %r: %r' % (value, exc,))
        self.frame_max = frame_max

    def _set_url_heartbeat(self, value):
        """Deserialize and apply the corresponding query string arg"""
        if 'heartbeat_interval' in self._all_url_query_values:
            raise ValueError('Deprecated URL parameter heartbeat_interval must '
                             'not be specified together with heartbeat')

        try:
            heartbeat_timeout = int(value)
        except ValueError as exc:
            raise ValueError('Invalid heartbeat value %r: %r' % (value, exc,))
        self.heartbeat = heartbeat_timeout

    def _set_url_heartbeat_interval(self, value):
        """Deserialize and apply the corresponding query string arg"""
        warnings.warn('heartbeat_interval is deprecated, use heartbeat',
                      DeprecationWarning, stacklevel=2)

        if 'heartbeat' in self._all_url_query_values:
            raise ValueError('Deprecated URL parameter heartbeat_interval must '
                             'not be specified together with heartbeat')

        try:
            heartbeat_timeout = int(value)
        except ValueError as exc:
            raise ValueError('Invalid heartbeat_interval value %r: %r' %
                             (value, exc,))
        self.heartbeat = heartbeat_timeout

    def _set_url_locale(self, value):
        """Deserialize and apply the corresponding query string arg"""
        self.locale = value

    def _set_url_retry_delay(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            retry_delay = float(value)
        except ValueError as exc:
            raise ValueError('Invalid retry_delay value %r: %r' % (value, exc,))
        self.retry_delay = retry_delay

    def _set_url_socket_timeout(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            socket_timeout = float(value)
        except ValueError as exc:
            raise ValueError('Invalid socket_timeout value %r: %r' %
                             (value, exc,))
        self.socket_timeout = socket_timeout

    def _set_url_ssl_options(self, value):
        """Deserialize and apply the corresponding query string arg

        """
        opts = ast.literal_eval(value)
        if opts is None:
            if self.ssl_options is not None:
                raise ValueError(
                    'Specified ssl_options=None URL arg is inconsistent with '
                    'the specified https URL scheme.')
        else:
            # Note: this is the deprecated wrap_socket signature and info:
            #
            # Internally, function creates a SSLContext with protocol
            # ssl_version and SSLContext.options set to cert_reqs.
            # If parameters keyfile, certfile, ca_certs or ciphers are set,
            # then the values are passed to SSLContext.load_cert_chain(),
            # SSLContext.load_verify_locations(), and SSLContext.set_ciphers().
            #
            # ssl.wrap_socket(sock,
            #     keyfile=None,
            #     certfile=None,
            #     server_side=False,        # Not URL-supported
            #     cert_reqs=CERT_NONE,      # Not URL-supported
            #     ssl_version=PROTOCOL_TLS, # Not URL-supported
            #     ca_certs=None,
            #     do_handshake_on_connect=True, # Not URL-supported
            #     suppress_ragged_eofs=True,    # Not URL-supported
            #     ciphers=None
            cxt = None
            if 'ca_certs' in opts:
                opt_ca_certs = opts['ca_certs']
                if os.path.isfile(opt_ca_certs):
                    cxt = ssl.create_default_context(cafile=opt_ca_certs)
                elif os.path.isdir(opt_ca_certs):
                    cxt = ssl.create_default_context(capath=opt_ca_certs)
                else:
                    LOGGER.warning('ca_certs is specified via ssl_options but '
                                   'is neither a valid file nor directory: "%s"',
                                   opt_ca_certs)

            if 'certfile' in opts:
                if os.path.isfile(opts['certfile']):
                    keyfile = opts.get('keyfile')
                    password = opts.get('password')
                    cxt.load_cert_chain(opts['certfile'], keyfile, password)
                else:
                    LOGGER.warning('certfile is specified via ssl_options but '
                                   'is not a valid file: "%s"',
                                   opts['certfile'])

            if 'ciphers' in opts:
                opt_ciphers = opts['ciphers']
                if opt_ciphers is not None:
                    cxt.set_ciphers(opt_ciphers)
                else:
                    LOGGER.warning('ciphers specified in ssl_options but '
                                   'evaluates to None')

            server_hostname = opts.get('server_hostname')
            self.ssl_options = pika.SSLOptions(context=cxt,
                                               server_hostname=server_hostname)

    def _set_url_tcp_options(self, value):
        """Deserialize and apply the corresponding query string arg"""
        self.tcp_options = ast.literal_eval(value)

class SSLOptions(object):
    """Class used to provide parameters for optional fine grained control of SSL
    socket wrapping.

    :param string keyfile: The key file to pass to SSLContext.load_cert_chain
    :param string key_password: The key password to passed to
                                                    SSLContext.load_cert_chain
    :param string certfile: The certificate file to passed to
                                                    SSLContext.load_cert_chain
    :param bool server_side: Passed to SSLContext.wrap_socket
    :param verify_mode: Passed to SSLContext.wrap_socket
    :param ssl_version: Passed to SSLContext init, defines the ssl
                                                                version to use
    :param string cafile: The CA file passed to
                                            SSLContext.load_verify_locations
    :param string capath: The CA path passed to
                                            SSLContext.load_verify_locations
    :param string cadata: The CA data passed to
                                            SSLContext.load_verify_locations
    :param do_handshake_on_connect: Passed to SSLContext.wrap_socket
    :param suppress_ragged_eofs: Passed to SSLContext.wrap_socket
    :param ciphers: Passed to SSLContext.set_ciphers
    :param server_hostname: SSLContext.wrap_socket, used to enable SNI
    """

    def __init__(self,
                 keyfile=None,
                 key_password=None,
                 certfile=None,
                 server_side=False,
                 verify_mode=ssl.CERT_NONE,
                 ssl_version=ssl.PROTOCOL_SSLv23,
                 cafile=None,
                 capath=None,
                 cadata=None,
                 do_handshake_on_connect=True,
                 suppress_ragged_eofs=True,
                 ciphers=None,
                 server_hostname=None):
        self.keyfile = keyfile
        self.key_password = key_password
        self.certfile = certfile
        self.server_side = server_side
        self.verify_mode = verify_mode
        self.ssl_version = ssl_version
        self.cafile = cafile
        self.capath = capath
        self.cadata = cadata
        self.do_handshake_on_connect = do_handshake_on_connect
        self.suppress_ragged_eofs = suppress_ragged_eofs
        self.ciphers = ciphers
        self.server_hostname = server_hostname

class Connection(object):
    """This is the core class that implements communication with RabbitMQ. This
    class should not be invoked directly but rather through the use of an
    adapter such as SelectConnection or BlockingConnection.

    :param pika.connection.Parameters parameters: Connection parameters
    :param method on_open_callback: Called when the connection is opened
    :param method on_open_error_callback: Called if the connection cant
                                   be opened
    :param method on_close_callback: Called when the connection is closed

    """

    # Disable pylint messages concerning "method could be a funciton"
    # pylint: disable=R0201

    ON_CONNECTION_BACKPRESSURE = '_on_connection_backpressure'
    ON_CONNECTION_BLOCKED = '_on_connection_blocked'
    ON_CONNECTION_CLOSED = '_on_connection_closed'
    ON_CONNECTION_ERROR = '_on_connection_error'
    ON_CONNECTION_OPEN = '_on_connection_open'
    ON_CONNECTION_UNBLOCKED = '_on_connection_unblocked'
    CONNECTION_CLOSED = 0
    CONNECTION_INIT = 1
    CONNECTION_PROTOCOL = 2
    CONNECTION_START = 3
    CONNECTION_TUNE = 4
    CONNECTION_OPEN = 5
    CONNECTION_CLOSING = 6  # client-initiated close in progress

    _STATE_NAMES = {
        CONNECTION_CLOSED: 'CLOSED',
        CONNECTION_INIT: 'INIT',
        CONNECTION_PROTOCOL: 'PROTOCOL',
        CONNECTION_START: 'START',
        CONNECTION_TUNE: 'TUNE',
        CONNECTION_OPEN: 'OPEN',
        CONNECTION_CLOSING: 'CLOSING'
    }

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None):
        """Connection initialization expects an object that has implemented the
         Parameters class and a callback function to notify when we have
         successfully connected to the AMQP Broker.

        Available Parameters classes are the ConnectionParameters class and
        URLParameters class.

        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Called when the connection is opened
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            `on_close_callback(connection, reason_code, reason_text)`, where
            `reason_code` is either an IETF RFC 821 reply code for AMQP-level
          closures or a value from `pika.connection.InternalCloseReasons` for
          internal causes, such as socket errors.

        """
        self.connection_state = self.CONNECTION_CLOSED

        # Holds timer when the initial connect or reconnect is scheduled
        self._connection_attempt_timer = None

        # Used to hold timer if configured for Connection.Blocked timeout
        self._blocked_conn_timer = None

        self.heartbeat = None

        # Set our configuration options
        self.params = (copy.deepcopy(parameters) if parameters is not None else
                       ConnectionParameters())

        # Define our callback dictionary
        self.callbacks = callback.CallbackManager()

        # Attributes that will be properly initialized by _init_connection_state
        # and/or during connection handshake.
        self.server_capabilities = None
        self.server_properties = None
        self._body_max_length = None
        self.known_hosts = None
        self.closing = None
        self._frame_buffer = None
        self._channels = None
        self._backpressure_multiplier = None
        self.remaining_connection_attempts = None

        self._init_connection_state()


        # Add the on connection error callback
        self.callbacks.add(0, self.ON_CONNECTION_ERROR,
                           on_open_error_callback or self._on_connection_error,
                           False)

        # On connection callback
        if on_open_callback:
            self.add_on_open_callback(on_open_callback)

        # On connection callback
        if on_close_callback:
            self.add_on_close_callback(on_close_callback)

        self.connect()

    def add_backpressure_callback(self, callback_method):
        """Call method "callback" when pika believes backpressure is being
        applied.

        :param method callback_method: The method to call

        """
        self.callbacks.add(0, self.ON_CONNECTION_BACKPRESSURE, callback_method,
                           False)

    def add_on_close_callback(self, callback_method):
        """Add a callback notification when the connection has closed. The
        callback will be passed the connection, the reply_code (int) and the
        reply_text (str), if sent by the remote server.

        :param method callback_method: Callback to call on close

        """
        self.callbacks.add(0, self.ON_CONNECTION_CLOSED, callback_method, False)

    def add_on_connection_blocked_callback(self, callback_method):
        """Add a callback to be notified when RabbitMQ has sent a
        ``Connection.Blocked`` frame indicating that RabbitMQ is low on
        resources. Publishers can use this to voluntarily suspend publishing,
        instead of relying on back pressure throttling. The callback
        will be passed the ``Connection.Blocked`` method frame.

        See also `ConnectionParameters.blocked_connection_timeout`.

        :param method callback_method: Callback to call on `Connection.Blocked`,
            having the signature `callback_method(pika.frame.Method)`, where the
            method frame's `method` member is of type
            `pika.spec.Connection.Blocked`

        """
        self.callbacks.add(0, spec.Connection.Blocked, callback_method, False)

    def add_on_connection_unblocked_callback(self, callback_method):
        """Add a callback to be notified when RabbitMQ has sent a
        ``Connection.Unblocked`` frame letting publishers know it's ok
        to start publishing again. The callback will be passed the
        ``Connection.Unblocked`` method frame.

        :param method callback_method: Callback to call on
            `Connection.Unblocked`, having the signature
            `callback_method(pika.frame.Method)`, where the method frame's
            `method` member is of type `pika.spec.Connection.Unblocked`

        """
        self.callbacks.add(0, spec.Connection.Unblocked, callback_method, False)

    def add_on_open_callback(self, callback_method):
        """Add a callback notification when the connection has opened.

        :param method callback_method: Callback to call when open

        """
        self.callbacks.add(0, self.ON_CONNECTION_OPEN, callback_method, False)

    def add_on_open_error_callback(self, callback_method, remove_default=True):
        """Add a callback notification when the connection can not be opened.

        The callback method should accept the connection object that could not
        connect, and an optional error message.

        :param method callback_method: Callback to call when can't connect
        :param bool remove_default: Remove default exception raising callback

        """
        if remove_default:
            self.callbacks.remove(0, self.ON_CONNECTION_ERROR,
                                  self._on_connection_error)
        self.callbacks.add(0, self.ON_CONNECTION_ERROR, callback_method, False)

    def add_timeout(self, deadline, callback_method):
        """Adapters should override to call the callback after the
        specified number of seconds have elapsed, using a timer, or a
        thread, or similar.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method

        """
        raise NotImplementedError

    def channel(self, on_open_callback, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        :param method on_open_callback: The callback when the channel is opened
        :param int channel_number: The channel number to use, defaults to the
                                   next available.
        :rtype: pika.channel.Channel

        """
        if not self.is_open:
            # TODO if state is OPENING, then ConnectionClosed might be wrong
            raise exceptions.ConnectionClosed(
                'Channel allocation requires an open connection: %s' % self)

        if not channel_number:
            channel_number = self._next_channel_number()
        self._channels[channel_number] = self._create_channel(channel_number,
                                                              on_open_callback)
        self._add_channel_callbacks(channel_number)
        self._channels[channel_number].open()
        return self._channels[channel_number]

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        if self.is_closing or self.is_closed:
            LOGGER.warning('Suppressing close request on %s', self)
            return

        # NOTE The connection is either in opening or open state

        # Initiate graceful closing of channels that are OPEN or OPENING
        if self._channels:
            self._close_channels(reply_code, reply_text)

        # Set our connection state
        self._set_connection_state(self.CONNECTION_CLOSING)
        LOGGER.info("Closing connection (%s): %s", reply_code, reply_text)
        self.closing = reply_code, reply_text

        # If there are channels that haven't finished closing yet, then
        # _on_close_ready will finally be called from _on_channel_cleanup once
        # all channels have been closed
        if not self._channels:
            # We can initiate graceful closing of the connection right away,
            # since no more channels remain
            self._on_close_ready()
        else:
            LOGGER.info('Connection.close is waiting for '
                        '%d channels to close: %s', len(self._channels), self)

    def connect(self):
        """Invoke if trying to reconnect to a RabbitMQ server. Constructing the
        Connection object should connect on its own.

        """
        assert self._connection_attempt_timer is None, (
            'connect timer was already scheduled')

        assert self.is_closed, (
            'connect expected CLOSED state, but got: {}'.format(
                self._STATE_NAMES[self.connection_state]))

        self._set_connection_state(self.CONNECTION_INIT)

        # Schedule a timer callback to start the actual connection logic from
        # event loop's context, thus avoiding error callbacks in the context of
        # the caller, which could be the constructor.
        self._connection_attempt_timer = self.add_timeout(
            0,
            self._on_connect_timer)


    def remove_timeout(self, timeout_id):
        """Adapters should override: Remove a timeout

        :param str timeout_id: The timeout id to remove

        """
        raise NotImplementedError

    def set_backpressure_multiplier(self, value=10):
        """Alter the backpressure multiplier value. We set this to 10 by default.
        This value is used to raise warnings and trigger the backpressure
        callback.

        :param int value: The multiplier value to set

        """
        self._backpressure_multiplier = value

    #
    # Connection state properties
    #

    @property
    def is_closed(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self.connection_state == self.CONNECTION_CLOSED

    @property
    def is_closing(self):
        """
        Returns True if connection is in the process of closing due to
        client-initiated `close` request, but closing is not yet complete.
        """
        return self.connection_state == self.CONNECTION_CLOSING

    @property
    def is_open(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self.connection_state == self.CONNECTION_OPEN

    #
    # Properties that reflect server capabilities for the current connection
    #

    @property
    def basic_nack(self):
        """Specifies if the server supports basic.nack on the active connection.

        :rtype: bool

        """
        return self.server_capabilities.get('basic.nack', False)

    @property
    def consumer_cancel_notify(self):
        """Specifies if the server supports consumer cancel notification on the
        active connection.

        :rtype: bool

        """
        return self.server_capabilities.get('consumer_cancel_notify', False)

    @property
    def exchange_exchange_bindings(self):
        """Specifies if the active connection supports exchange to exchange
        bindings.

        :rtype: bool

        """
        return self.server_capabilities.get('exchange_exchange_bindings', False)

    @property
    def publisher_confirms(self):
        """Specifies if the active connection can use publisher confirmations.

        :rtype: bool

        """
        return self.server_capabilities.get('publisher_confirms', False)

    #
    # Internal methods for managing the communication process
    #

    def _adapter_connect(self):
        """Subclasses should override to set up the outbound socket connection.

        :raises: NotImplementedError

        """
        raise NotImplementedError

    def _adapter_disconnect(self):
        """Subclasses should override this to cause the underlying transport
        (socket) to close.

        :raises: NotImplementedError

        """
        raise NotImplementedError

    def _add_channel_callbacks(self, channel_number):
        """Add the appropriate callbacks for the specified channel number.

        :param int channel_number: The channel number for the callbacks

        """
        # pylint: disable=W0212

        # This permits us to garbage-collect our reference to the channel
        # regardless of whether it was closed by client or broker, and do so
        # after all channel-close callbacks.
        self._channels[channel_number]._add_on_cleanup_callback(
            self._on_channel_cleanup)

    def _add_connection_start_callback(self):
        """Add a callback for when a Connection.Start frame is received from
        the broker.

        """
        self.callbacks.add(0, spec.Connection.Start, self._on_connection_start)

    def _add_connection_tune_callback(self):
        """Add a callback for when a Connection.Tune frame is received."""
        self.callbacks.add(0, spec.Connection.Tune, self._on_connection_tune)

    def _append_frame_buffer(self, value):
        """Append the bytes to the frame buffer.

        :param str value: The bytes to append to the frame buffer

        """
        self._frame_buffer += value

    @property
    def _buffer_size(self):
        """Return the suggested buffer size from the connection state/tune or
        the default if that is None.

        :rtype: int

        """
        return self.params.frame_max or spec.FRAME_MAX_SIZE

    def _check_for_protocol_mismatch(self, value):
        """Invoked when starting a connection to make sure it's a supported
        protocol.

        :param pika.frame.Method value: The frame to check
        :raises: ProtocolVersionMismatch

        """
        if (value.method.version_major,
                value.method.version_minor) != spec.PROTOCOL_VERSION[0:2]:
            # TODO This should call _on_terminate for proper callbacks and
            # cleanup
            raise exceptions.ProtocolVersionMismatch(frame.ProtocolHeader(),
                                                     value)

    @property
    def _client_properties(self):
        """Return the client properties dictionary.

        :rtype: dict

        """
        properties = {
            'product': PRODUCT,
            'platform': 'Python %s' % platform.python_version(),
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See http://pika.rtfd.org',
            'version': __version__
        }

        if self.params.client_properties:
            properties.update(self.params.client_properties)

        return properties

    def _close_channels(self, reply_code, reply_text):
        """Initiate graceful closing of channels that are in OPEN or OPENING
        states, passing reply_code and reply_text.

        :param int reply_code: The code for why the channels are being closed
        :param str reply_text: The text reason for why the channels are closing

        """
        assert self.is_open, str(self)

        for channel_number in dictkeys(self._channels):
            chan = self._channels[channel_number]
            if not (chan.is_closing or chan.is_closed):
                chan.close(reply_code, reply_text)

    def _connect(self):
        """Attempt to connect to RabbitMQ

        :rtype: bool

        """
        warnings.warn('This method is deprecated, use Connection.connect',
                      DeprecationWarning)

    def _create_channel(self, channel_number, on_open_callback):
        """Create a new channel using the specified channel number and calling
        back the method specified by on_open_callback

        :param int channel_number: The channel number to use
        :param method on_open_callback: The callback when the channel is opened

        """
        LOGGER.debug('Creating channel %s', channel_number)
        return pika.channel.Channel(self, channel_number, on_open_callback)

    def _create_heartbeat_checker(self):
        """Create a heartbeat checker instance if there is a heartbeat interval
        set.

        :rtype: pika.heartbeat.Heartbeat|None

        """
        if self.params.heartbeat is not None and self.params.heartbeat > 0:
            LOGGER.debug('Creating a HeartbeatChecker: %r',
                         self.params.heartbeat)
            return heartbeat.HeartbeatChecker(self, self.params.heartbeat)

        return None

    def _remove_heartbeat(self):
        """Stop the heartbeat checker if it exists

        """
        if self.heartbeat:
            self.heartbeat.stop()
            self.heartbeat = None

    def _deliver_frame_to_channel(self, value):
        """Deliver the frame to the channel specified in the frame.

        :param pika.frame.Method value: The frame to deliver

        """
        if not value.channel_number in self._channels:
            # This should never happen and would constitute breach of the
            # protocol
            LOGGER.critical(
                'Received %s frame for unregistered channel %i on %s',
                value.NAME, value.channel_number, self)
            return

        # pylint: disable=W0212
        self._channels[value.channel_number]._handle_content_frame(value)

    def _detect_backpressure(self):
        """Attempt to calculate if TCP backpressure is being applied due to
        our outbound buffer being larger than the average frame size over
        a window of frames.

        """
        avg_frame_size = self.bytes_sent / self.frames_sent
        buffer_size = sum([len(f) for f in self.outbound_buffer])
        if buffer_size > (avg_frame_size * self._backpressure_multiplier):
            LOGGER.warning(BACKPRESSURE_WARNING, buffer_size,
                           int(buffer_size / avg_frame_size))
            self.callbacks.process(0, self.ON_CONNECTION_BACKPRESSURE, self)

    def _ensure_closed(self):
        """If the connection is not closed, close it."""
        if self.is_open:
            self.close()

    def _flush_outbound(self):
        """Adapters should override to flush the contents of outbound_buffer
        out along the socket.

        :raises: NotImplementedError

        """
        raise NotImplementedError

    def _get_body_frame_max_length(self):
        """Calculate the maximum amount of bytes that can be in a body frame.

        :rtype: int

        """
        return (
            self.params.frame_max - spec.FRAME_HEADER_SIZE - spec.FRAME_END_SIZE
        )

    def _get_credentials(self, method_frame):
        """Get credentials for authentication.

        :param pika.frame.MethodFrame method_frame: The Connection.Start frame
        :rtype: tuple(str, str)

        """
        (auth_type,
         response) = self.params.credentials.response_for(method_frame.method)
        if not auth_type:
            # TODO this should call _on_terminate for proper callbacks and
            # cleanup instead
            raise exceptions.AuthenticationError(self.params.credentials.TYPE)
        self.params.credentials.erase_credentials()
        return auth_type, response

    def _has_pending_callbacks(self, value):
        """Return true if there are any callbacks pending for the specified
        frame.

        :param pika.frame.Method value: The frame to check
        :rtype: bool

        """
        return self.callbacks.pending(value.channel_number, value.method)

    def _init_connection_state(self):
        """Initialize or reset all of the internal state variables for a given
        connection. On disconnect or reconnect all of the state needs to
        be wiped.

        """
        # Connection state
        self._set_connection_state(self.CONNECTION_CLOSED)

        # Negotiated server properties
        self.server_properties = None

        # Outbound buffer for buffering writes until we're able to send them
        self.outbound_buffer = collections.deque([])

        # Inbound buffer for decoding frames
        self._frame_buffer = bytes()

        # Dict of open channels
        self._channels = dict()

        # Remaining connection attempts
        self.remaining_connection_attempts = self.params.connection_attempts

        # Data used for Heartbeat checking and back-pressure detection
        self.bytes_sent = 0
        self.bytes_received = 0
        self.frames_sent = 0
        self.frames_received = 0
        self.heartbeat = None

        # Default back-pressure multiplier value
        self._backpressure_multiplier = 10

        # When closing, hold reason why
        self.closing = 0, 'Not specified'

        # Our starting point once connected, first frame received
        self._add_connection_start_callback()

        # Add a callback handler for the Broker telling us to disconnect.
        # NOTE: As of RabbitMQ 3.6.0, RabbitMQ broker may send Connection.Close
        # to signal error during connection setup (and wait a longish time
        # before closing the TCP/IP stream). Earlier RabbitMQ versions
        # simply closed the TCP/IP stream.
        self.callbacks.add(0, spec.Connection.Close, self._on_connection_close)

        if self._connection_attempt_timer is not None:
            # Connection attempt timer was active when teardown was initiated
            self.remove_timeout(self._connection_attempt_timer)
            self._connection_attempt_timer = None

        if self.params.blocked_connection_timeout is not None:
            if self._blocked_conn_timer is not None:
                # Blocked connection timer was active when teardown was
                # initiated
                self.remove_timeout(self._blocked_conn_timer)
                self._blocked_conn_timer = None

            self.add_on_connection_blocked_callback(
                self._on_connection_blocked)
            self.add_on_connection_unblocked_callback(
                self._on_connection_unblocked)

    def _is_method_frame(self, value):
        """Returns true if the frame is a method frame.

        :param pika.frame.Frame value: The frame to evaluate
        :rtype: bool

        """
        return isinstance(value, frame.Method)

    def _is_protocol_header_frame(self, value):
        """Returns True if it's a protocol header frame.

        :rtype: bool

        """
        return isinstance(value, frame.ProtocolHeader)

    def _next_channel_number(self):
        """Return the next available channel number or raise an exception.

        :rtype: int

        """
        limit = self.params.channel_max or pika.channel.MAX_CHANNELS
        if len(self._channels) >= limit:
            raise exceptions.NoFreeChannels()

        for num in xrange(1, len(self._channels) + 1):
            if num not in self._channels:
                return num
        return len(self._channels) + 1

    def _on_channel_cleanup(self, channel):
        """Remove the channel from the dict of channels when Channel.CloseOk is
        sent. If connection is closing and no more channels remain, proceed to
        `_on_close_ready`.

        :param pika.channel.Channel channel: channel instance

        """
        try:
            del self._channels[channel.channel_number]
            LOGGER.debug('Removed channel %s', channel.channel_number)
        except KeyError:
            LOGGER.error('Channel %r not in channels',
                         channel.channel_number)
        if self.is_closing:
            if not self._channels:
                # Initiate graceful closing of the connection
                self._on_close_ready()
            else:
                # Once Connection enters CLOSING state, all remaining channels
                # should also be in CLOSING state. Deviation from this would
                # prevent Connection from completing its closing procedure.
                channels_not_in_closing_state = [
                    chan for chan in dict_itervalues(self._channels)
                    if not chan.is_closing]
                if channels_not_in_closing_state:
                    LOGGER.critical(
                        'Connection in CLOSING state has non-CLOSING '
                        'channels: %r', channels_not_in_closing_state)

    def _on_close_ready(self):
        """Called when the Connection is in a state that it can close after
        a close has been requested. This happens, for example, when all of the
        channels are closed that were open when the close request was made.

        """
        if self.is_closed:
            LOGGER.warning('_on_close_ready invoked when already closed')
            return

        self._send_connection_close(self.closing[0], self.closing[1])

    def _on_connected(self):
        """Invoked when the socket is connected and it's time to start speaking
        AMQP with the broker.

        """
        self._set_connection_state(self.CONNECTION_PROTOCOL)

        # Start the communication with the RabbitMQ Broker
        self._send_frame(frame.ProtocolHeader())

    def _on_blocked_connection_timeout(self):
        """ Called when the "connection blocked timeout" expires. When this
        happens, we tear down the connection

        """
        self._blocked_conn_timer = None
        self._on_terminate(InternalCloseReasons.BLOCKED_CONNECTION_TIMEOUT,
                           'Blocked connection timeout expired')

    def _on_connection_blocked(self, method_frame):
        """Handle Connection.Blocked notification from RabbitMQ broker

        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Blocked`
        """
        LOGGER.warning('Received %s from broker', method_frame)

        if self._blocked_conn_timer is not None:
            # RabbitMQ is not supposed to repeat Connection.Blocked, but it
            # doesn't hurt to be careful
            LOGGER.warning('_blocked_conn_timer %s already set when '
                           '_on_connection_blocked is called',
                           self._blocked_conn_timer)
        else:
            self._blocked_conn_timer = self.add_timeout(
                self.params.blocked_connection_timeout,
                self._on_blocked_connection_timeout)

    def _on_connection_unblocked(self, method_frame):
        """Handle Connection.Unblocked notification from RabbitMQ broker

        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Blocked`
        """
        LOGGER.info('Received %s from broker', method_frame)

        if self._blocked_conn_timer is None:
            # RabbitMQ is supposed to pair Connection.Blocked/Unblocked, but it
            # doesn't hurt to be careful
            LOGGER.warning('_blocked_conn_timer was not active when '
                           '_on_connection_unblocked called')
        else:
            self.remove_timeout(self._blocked_conn_timer)
            self._blocked_conn_timer = None

    def _on_connection_close(self, method_frame):
        """Called when the connection is closed remotely via Connection.Close
        frame from broker.

        :param pika.frame.Method method_frame: The Connection.Close frame

        """
        LOGGER.debug('_on_connection_close: frame=%s', method_frame)

        self.closing = (method_frame.method.reply_code,
                        method_frame.method.reply_text)

        self._on_terminate(self.closing[0], self.closing[1])

    def _on_connection_close_ok(self, method_frame):
        """Called when Connection.CloseOk is received from remote.

        :param pika.frame.Method method_frame: The Connection.CloseOk frame

        """
        LOGGER.debug('_on_connection_close_ok: frame=%s', method_frame)

        self._on_terminate(self.closing[0], self.closing[1])

    def _on_connection_error(self, _connection_unused, error_message=None):
        """Default behavior when the connecting connection can not connect.

        :raises: exceptions.AMQPConnectionError

        """
        raise exceptions.AMQPConnectionError(error_message or
                                             self.params.connection_attempts)

    def _on_connection_open(self, method_frame):
        """
        This is called once we have tuned the connection with the server and
        called the Connection.Open on the server and it has replied with
        Connection.Ok.
        """
        # TODO _on_connection_open - what if user started closing it already?
        # It shouldn't transition to OPEN if in closing state. Just log and skip
        # the rest.

        self.known_hosts = method_frame.method.known_hosts

        # We're now connected at the AMQP level
        self._set_connection_state(self.CONNECTION_OPEN)

        # Call our initial callback that we're open
        self.callbacks.process(0, self.ON_CONNECTION_OPEN, self, self)

    def _on_connection_start(self, method_frame):
        """This is called as a callback once we have received a Connection.Start
        from the server.

        :param pika.frame.Method method_frame: The frame received
        :raises: UnexpectedFrameError

        """
        self._set_connection_state(self.CONNECTION_START)
        if self._is_protocol_header_frame(method_frame):
            raise exceptions.UnexpectedFrameError
        self._check_for_protocol_mismatch(method_frame)
        self._set_server_information(method_frame)
        self._add_connection_tune_callback()
        self._send_connection_start_ok(*self._get_credentials(method_frame))

    def _on_connect_timer(self):
        """Callback for self._connection_attempt_timer: initiate connection
        attempt in the context of the event loop

        """
        self._connection_attempt_timer = None

        error = self._adapter_connect()
        if not error:
            self._on_connected()
            return

        self.remaining_connection_attempts -= 1
        LOGGER.warning('Could not connect, %i attempts left',
                       self.remaining_connection_attempts)
        if self.remaining_connection_attempts > 0:
            LOGGER.info('Retrying in %i seconds', self.params.retry_delay)
            self._connection_attempt_timer = self.add_timeout(
                self.params.retry_delay,
                self._on_connect_timer)
        else:
            # TODO connect must not call failure callback from constructor. The
            # current behavior is error-prone, because the user code may get a
            # callback upon socket connection failure before user's other state
            # may be sufficiently initialized. Constructors must either succeed
            # or raise an exception. To be forward-compatible with failure
            # reporting from fully non-blocking connection establishment,
            # connect() should set INIT state and schedule a 0-second timer to
            # continue the rest of the logic in a private method. The private
            # method should use itself instead of connect() as the callback for
            # scheduling retries.

            # TODO This should use _on_terminate for consistent behavior/cleanup
            self.callbacks.process(0, self.ON_CONNECTION_ERROR, self, self,
                                   error)
            self.remaining_connection_attempts = self.params.connection_attempts
            self._set_connection_state(self.CONNECTION_CLOSED)

    @staticmethod
    def _negotiate_integer_value(client_value, server_value):
        """Negotiates two values. If either of them is 0 or None,
        returns the other one. If both are positive integers, returns the
        smallest one.

        :param int client_value: The client value
        :param int server_value: The server value
        :rtype: int

        """
        if client_value == None:
            client_value = 0
        if server_value == None:
            server_value = 0

        # this is consistent with how Java client and Bunny
        # perform negotiation, see pika/pika#874
        if client_value == 0 or server_value == 0:
            val = max(client_value, server_value)
        else:
            val = min(client_value, server_value)

        return val

    @staticmethod
    def _tune_heartbeat_timeout(client_value, server_value):
        """ Determine heartbeat timeout per AMQP 0-9-1 rules

        Per https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf,

        > Both peers negotiate the limits to the lowest agreed value as follows:
        > - The server MUST tell the client what limits it proposes.
        > - The client responds and **MAY reduce those limits** for its
            connection

        If the client specifies a value, it always takes precedence.

        :param client_value: None to accept server_value; otherwise, an integral
            number in seconds; 0 (zero) to disable heartbeat.
        :param server_value: integral value of the heartbeat timeout proposed by
            broker; 0 (zero) to disable heartbeat.

        :returns: the value of the heartbeat timeout to use and return to broker
        """
        if client_value is None:
            # Accept server's limit
            timeout = server_value
        else:
            timeout = client_value

        return timeout

    def _on_connection_tune(self, method_frame):
        """Once the Broker sends back a Connection.Tune, we will set our tuning
        variables that have been returned to us and kick off the Heartbeat
        monitor if required, send our TuneOk and then the Connection. Open rpc
        call on channel 0.

        :param pika.frame.Method method_frame: The frame received

        """
        self._set_connection_state(self.CONNECTION_TUNE)

        # Get our max channels, frames and heartbeat interval
        self.params.channel_max = Connection._negotiate_integer_value(self.params.channel_max,
                                                                      method_frame.method.channel_max)
        self.params.frame_max = Connection._negotiate_integer_value(self.params.frame_max,
                                                                    method_frame.method.frame_max)

        if callable(self.params.heartbeat):
            ret_heartbeat = self.params.heartbeat(self, method_frame.method.heartbeat)
            if ret_heartbeat is None or callable(ret_heartbeat):
                # Enforce callback-specific restrictions on callback's return value
                raise TypeError('heartbeat callback must not return None '
                                'or callable, but got %r' % (ret_heartbeat,))

            # Leave it to hearbeat setter deal with the rest of the validation
            self.params.heartbeat = ret_heartbeat

        # Negotiate heatbeat timeout
        self.params.heartbeat = self._tune_heartbeat_timeout(
            client_value=self.params.heartbeat,
            server_value=method_frame.method.heartbeat)

        # Calculate the maximum pieces for body frames
        self._body_max_length = self._get_body_frame_max_length()

        # Create a new heartbeat checker if needed
        self.heartbeat = self._create_heartbeat_checker()

        # Send the TuneOk response with what we've agreed upon
        self._send_connection_tune_ok()

        # Send the Connection.Open RPC call for the vhost
        self._send_connection_open()

    def _on_data_available(self, data_in):
        """This is called by our Adapter, passing in the data from the socket.
        As long as we have buffer try and map out frame data.

        :param str data_in: The data that is available to read

        """
        self._append_frame_buffer(data_in)
        while self._frame_buffer:
            consumed_count, frame_value = self._read_frame()
            if not frame_value:
                return
            self._trim_frame_buffer(consumed_count)
            self._process_frame(frame_value)

    def _on_terminate(self, reason_code, reason_text):
        """Terminate the connection and notify registered ON_CONNECTION_ERROR
        and/or ON_CONNECTION_CLOSED callbacks

        :param integer reason_code: either IETF RFC 821 reply code for
            AMQP-level closures or a value from `InternalCloseReasons` for
            internal causes, such as socket errors
        :param str reason_text: human-readable text message describing the error
        """
        LOGGER.info(
            'Disconnected from RabbitMQ at %s:%i (%s): %s',
            self.params.host, self.params.port, reason_code,
            reason_text)

        if not isinstance(reason_code, numbers.Integral):
            raise TypeError('reason_code must be an integer, but got %r'
                            % (reason_code,))

        # Stop the heartbeat checker if it exists
        self._remove_heartbeat()

        # Remove connection management callbacks
        # TODO This call was moved here verbatim from legacy code and the
        # following doesn't seem to be right: `Connection.Open` here is
        # unexpected, we don't appear to ever register it, and the broker
        # shouldn't be sending `Connection.Open` to us, anyway.
        self._remove_callbacks(0, [spec.Connection.Close, spec.Connection.Start,
                                   spec.Connection.Open])

        if self.params.blocked_connection_timeout is not None:
            self._remove_callbacks(0, [spec.Connection.Blocked,
                                       spec.Connection.Unblocked])

        # Close the socket
        self._adapter_disconnect()

        # Determine whether this was an error during connection setup
        connection_error = None

        if self.connection_state == self.CONNECTION_PROTOCOL:
            LOGGER.error('Incompatible Protocol Versions')
            connection_error = exceptions.IncompatibleProtocolError(
                reason_code,
                reason_text)
        elif self.connection_state == self.CONNECTION_START:
            LOGGER.error('Connection closed while authenticating indicating a '
                         'probable authentication error')
            connection_error = exceptions.ProbableAuthenticationError(
                reason_code,
                reason_text)
        elif self.connection_state == self.CONNECTION_TUNE:
            LOGGER.error('Connection closed while tuning the connection '
                         'indicating a probable permission error when '
                         'accessing a virtual host')
            connection_error = exceptions.ProbableAccessDeniedError(
                reason_code,
                reason_text)
        elif self.connection_state not in [self.CONNECTION_OPEN,
                                           self.CONNECTION_CLOSED,
                                           self.CONNECTION_CLOSING]:
            LOGGER.warning('Unexpected connection state on disconnect: %i',
                           self.connection_state)

        # Transition to closed state
        self._set_connection_state(self.CONNECTION_CLOSED)

        # Inform our channel proxies
        for channel in dictkeys(self._channels):
            if channel not in self._channels:
                continue
            # pylint: disable=W0212
            self._channels[channel]._on_close_meta(reason_code, reason_text)

        # Inform interested parties
        if connection_error is not None:
            LOGGER.error('Connection setup failed due to %r', connection_error)
            self.callbacks.process(0,
                                   self.ON_CONNECTION_ERROR,
                                   self, self,
                                   connection_error)

        self.callbacks.process(0, self.ON_CONNECTION_CLOSED, self, self,
                               reason_code, reason_text)

        # Reset connection properties
        self._init_connection_state()

    def _process_callbacks(self, frame_value):
        """Process the callbacks for the frame if the frame is a method frame
        and if it has any callbacks pending.

        :param pika.frame.Method frame_value: The frame to process
        :rtype: bool

        """
        if (self._is_method_frame(frame_value) and
                self._has_pending_callbacks(frame_value)):
            self.callbacks.process(frame_value.channel_number,  # Prefix
                                   frame_value.method,  # Key
                                   self,  # Caller
                                   frame_value)  # Args
            return True
        return False

    def _process_frame(self, frame_value):
        """Process an inbound frame from the socket.

        :param frame_value: The frame to process
        :type frame_value: pika.frame.Frame | pika.frame.Method

        """
        # Will receive a frame type of -1 if protocol version mismatch
        if frame_value.frame_type < 0:
            return

        # Keep track of how many frames have been read
        self.frames_received += 1

        # Process any callbacks, if True, exit method
        if self._process_callbacks(frame_value):
            return

        # If a heartbeat is received, update the checker
        if isinstance(frame_value, frame.Heartbeat):
            if self.heartbeat:
                self.heartbeat.received()
            else:
                LOGGER.warning('Received heartbeat frame without a heartbeat '
                               'checker')

        # If the frame has a channel number beyond the base channel, deliver it
        elif frame_value.channel_number > 0:
            self._deliver_frame_to_channel(frame_value)

    def _read_frame(self):
        """Try and read from the frame buffer and decode a frame.

        :rtype tuple: (int, pika.frame.Frame)

        """
        return frame.decode_frame(self._frame_buffer)

    def _remove_callback(self, channel_number, method_class):
        """Remove the specified method_frame callback if it is set for the
        specified channel number.

        :param int channel_number: The channel number to remove the callback on
        :param pika.amqp_object.Method method_class: The method class for the
            callback

        """
        self.callbacks.remove(str(channel_number), method_class)

    def _remove_callbacks(self, channel_number, method_classes):
        """Remove the callbacks for the specified channel number and list of
        method frames.

        :param int channel_number: The channel number to remove the callback on
        :param sequence method_classes: The method classes (derived from
            `pika.amqp_object.Method`) for the callbacks

        """
        for method_frame in method_classes:
            self._remove_callback(channel_number, method_frame)

    def _rpc(self, channel_number, method,
             callback_method=None,
             acceptable_replies=None):
        """Make an RPC call for the given callback, channel number and method.
        acceptable_replies lists out what responses we'll process from the
        server with the specified callback.

        :param int channel_number: The channel number for the RPC call
        :param pika.amqp_object.Method method: The method frame to call
        :param method callback_method: The callback for the RPC response
        :param list acceptable_replies: The replies this RPC call expects

        """
        # Validate that acceptable_replies is a list or None
        if acceptable_replies and not isinstance(acceptable_replies, list):
            raise TypeError('acceptable_replies should be list or None')

        # Validate the callback is callable
        if callback_method:
            if not utils.is_callable(callback_method):
                raise TypeError('callback should be None, function or method.')

            for reply in acceptable_replies:
                self.callbacks.add(channel_number, reply, callback_method)

        # Send the rpc call to RabbitMQ
        self._send_method(channel_number, method)

    def _send_connection_close(self, reply_code, reply_text):
        """Send a Connection.Close method frame.

        :param int reply_code: The reason for the close
        :param str reply_text: The text reason for the close

        """
        self._rpc(0, spec.Connection.Close(reply_code, reply_text, 0, 0),
                  self._on_connection_close_ok, [spec.Connection.CloseOk])

    def _send_connection_open(self):
        """Send a Connection.Open frame"""
        self._rpc(0, spec.Connection.Open(self.params.virtual_host,
                                          insist=True),
                  self._on_connection_open, [spec.Connection.OpenOk])

    def _send_connection_start_ok(self, authentication_type, response):
        """Send a Connection.StartOk frame

        :param str authentication_type: The auth type value
        :param str response: The encoded value to send

        """
        self._send_method(0,
                          spec.Connection.StartOk(self._client_properties,
                                                  authentication_type, response,
                                                  self.params.locale))

    def _send_connection_tune_ok(self):
        """Send a Connection.TuneOk frame"""
        self._send_method(0, spec.Connection.TuneOk(self.params.channel_max,
                                                    self.params.frame_max,
                                                    self.params.heartbeat))

    def _send_frame(self, frame_value):
        """This appends the fully generated frame to send to the broker to the
        output buffer which will be then sent via the connection adapter.

        :param frame_value: The frame to write
        :type frame_value:  pika.frame.Frame|pika.frame.ProtocolHeader
        :raises: exceptions.ConnectionClosed

        """
        if self.is_closed:
            LOGGER.error('Attempted to send frame when closed')
            raise exceptions.ConnectionClosed

        marshaled_frame = frame_value.marshal()
        self.bytes_sent += len(marshaled_frame)
        self.frames_sent += 1
        self.outbound_buffer.append(marshaled_frame)
        self._flush_outbound()
        if self.params.backpressure_detection:
            self._detect_backpressure()

    def _send_method(self, channel_number, method, content=None):
        """Constructs a RPC method frame and then sends it to the broker.

        :param int channel_number: The channel number for the frame
        :param pika.amqp_object.Method method: The method to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        if content:
            self._send_message(channel_number, method, content)
        else:
            self._send_frame(frame.Method(channel_number, method))

    def _send_message(self, channel_number, method_frame, content):
        """Publish a message.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: A content frame, which is tuple of properties and
                              body.

        """
        length = len(content[1])
        self._send_frame(frame.Method(channel_number, method_frame))
        self._send_frame(frame.Header(channel_number, length, content[0]))

        if content[1]:
            chunks = int(math.ceil(float(length) / self._body_max_length))
            for chunk in xrange(0, chunks):
                s = chunk * self._body_max_length
                e = s + self._body_max_length
                if e > length:
                    e = length
                self._send_frame(frame.Body(channel_number, content[1][s:e]))

    def _set_connection_state(self, connection_state):
        """Set the connection state.

        :param int connection_state: The connection state to set

        """
        self.connection_state = connection_state

    def _set_server_information(self, method_frame):
        """Set the server properties and capabilities

        :param spec.connection.Start method_frame: The Connection.Start frame

        """
        self.server_properties = method_frame.method.server_properties
        self.server_capabilities = self.server_properties.get('capabilities',
                                                              dict())
        if hasattr(self.server_properties, 'capabilities'):
            del self.server_properties['capabilities']

    def _trim_frame_buffer(self, byte_count):
        """Trim the leading N bytes off the frame buffer and increment the
        counter that keeps track of how many bytes have been read/used from the
        socket.

        :param int byte_count: The number of bytes consumed

        """
        self._frame_buffer = self._frame_buffer[byte_count:]
        self.bytes_received += byte_count
