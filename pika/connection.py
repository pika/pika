"""Core connection objects"""
import ast
import sys
import collections
import logging
import math
import numbers
import platform
import threading
import warnings

if sys.version_info > (3,):
    import urllib.parse as urlparse
else:
    import urlparse

from pika import __version__
from pika import callback
from pika import channel
from pika import credentials as pika_credentials
from pika import exceptions
from pika import frame
from pika import heartbeat
from pika import utils

from pika import spec

from pika.compat import xrange, basestring, url_unquote, dictkeys


BACKPRESSURE_WARNING = ("Pika: Write buffer exceeded warning threshold at "
                        "%i bytes and an estimated %i frames behind")
PRODUCT = "Pika Python Client Library"

LOGGER = logging.getLogger(__name__)


class Parameters(object):
    """Base connection parameters class definition

    :param str DEFAULT_HOST: 'localhost'
    :param int DEFAULT_PORT: 5672
    :param str DEFAULT_VIRTUAL_HOST: '/'
    :param str DEFAULT_USERNAME: 'guest'
    :param str DEFAULT_PASSWORD: 'guest'
    :param int DEFAULT_HEARTBEAT_INTERVAL: None
    :param int DEFAULT_CHANNEL_MAX: 0
    :param int DEFAULT_FRAME_MAX: pika.spec.FRAME_MAX_SIZE
    :param str DEFAULT_LOCALE: 'en_US'
    :param int DEFAULT_CONNECTION_ATTEMPTS: 1
    :param int|float DEFAULT_RETRY_DELAY: 2.0
    :param int|float DEFAULT_SOCKET_TIMEOUT: 0.25
    :param bool DEFAULT_SSL: False
    :param dict DEFAULT_SSL_OPTIONS: {}
    :param int DEFAULT_SSL_PORT: 5671
    :param bool DEFAULT_BACKPRESSURE_DETECTION: False

    """
    DEFAULT_BACKPRESSURE_DETECTION = False
    DEFAULT_CONNECTION_ATTEMPTS = 1
    DEFAULT_CHANNEL_MAX = 0
    DEFAULT_FRAME_MAX = spec.FRAME_MAX_SIZE
    DEFAULT_HEARTBEAT_INTERVAL = None          # accept server's proposal
    DEFAULT_HOST = 'localhost'
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_PASSWORD = 'guest'
    DEFAULT_PORT = 5672
    DEFAULT_RETRY_DELAY = 2.0
    DEFAULT_SOCKET_TIMEOUT = 0.25
    DEFAULT_SSL = False
    DEFAULT_SSL_OPTIONS = {}
    DEFAULT_SSL_PORT = 5671
    DEFAULT_USERNAME = 'guest'
    DEFAULT_VIRTUAL_HOST = '/'

    def __init__(self):
        self.virtual_host = self.DEFAULT_VIRTUAL_HOST
        self.backpressure_detection = self.DEFAULT_BACKPRESSURE_DETECTION
        self.channel_max = self.DEFAULT_CHANNEL_MAX
        self.connection_attempts = self.DEFAULT_CONNECTION_ATTEMPTS
        self.credentials = self._credentials(self.DEFAULT_USERNAME,
                                             self.DEFAULT_PASSWORD)
        self.frame_max = self.DEFAULT_FRAME_MAX
        self.heartbeat = self.DEFAULT_HEARTBEAT_INTERVAL
        self.host = self.DEFAULT_HOST
        self.locale = self.DEFAULT_LOCALE
        self.port = self.DEFAULT_PORT
        self.retry_delay = self.DEFAULT_RETRY_DELAY
        self.ssl = self.DEFAULT_SSL
        self.ssl_options = self.DEFAULT_SSL_OPTIONS
        self.socket_timeout = self.DEFAULT_SOCKET_TIMEOUT

    def __repr__(self):
        """Represent the info about the instance.

        :rtype: str

        """
        return ('<%s host=%s port=%s virtual_host=%s ssl=%s>' %
                (self.__class__.__name__, self.host, self.port,
                 self.virtual_host, self.ssl))

    @staticmethod
    def _credentials(username, password):
        """Return a plain credentials object for the specified username and
        password.

        :param str username: The username to use
        :param str password: The password to use
        :rtype: pika_credentials.PlainCredentials

        """
        return pika_credentials.PlainCredentials(username, password)

    @staticmethod
    def _validate_backpressure(backpressure_detection):
        """Validate that the backpressure detection option is a bool.

        :param bool backpressure_detection: The backpressure detection value
        :rtype: bool
        :raises: TypeError

        """
        if not isinstance(backpressure_detection, bool):
            raise TypeError('backpressure detection must be a bool')
        return True

    @staticmethod
    def _validate_channel_max(channel_max):
        """Validate that the channel_max value is an int

        :param int channel_max: The value to validate
        :rtype: bool
        :raises: TypeError
        :raises: ValueError

        """
        if not isinstance(channel_max, int):
            raise TypeError('channel_max must be an int')
        if channel_max < 1 or channel_max > 65535:
            raise ValueError('channel_max must be <= 65535 and > 0')
        return True

    @staticmethod
    def _validate_connection_attempts(connection_attempts):
        """Validate that the connection_attempts value is an int

        :param int connection_attempts: The value to validate
        :rtype: bool
        :raises: TypeError
        :raises: ValueError

        """
        if not isinstance(connection_attempts, int):
            raise TypeError('connection_attempts must be an int')
        if connection_attempts < 1:
            raise ValueError('connection_attempts must be None or > 0')
        return True

    @staticmethod
    def _validate_credentials(credentials):
        """Validate the credentials passed in are using a valid object type.

        :param pika.credentials.Credentials credentials: Credentials to validate
        :rtype: bool
        :raises: TypeError

        """
        for credential_type in pika_credentials.VALID_TYPES:
            if isinstance(credentials, credential_type):
                return True
        raise TypeError('Credentials must be an object of type: %r' %
                        pika_credentials.VALID_TYPES)

    @staticmethod
    def _validate_frame_max(frame_max):
        """Validate that the frame_max value is an int and does not exceed
         the maximum frame size and is not less than the frame min size.

        :param int frame_max: The value to validate
        :rtype: bool
        :raises: TypeError
        :raises: InvalidMinimumFrameSize

        """
        if not isinstance(frame_max, int):
            raise TypeError('frame_max must be an int')
        if frame_max < spec.FRAME_MIN_SIZE:
            raise exceptions.InvalidMinimumFrameSize
        elif frame_max > spec.FRAME_MAX_SIZE:
            raise exceptions.InvalidMaximumFrameSize
        return True

    @staticmethod
    def _validate_heartbeat_interval(heartbeat_interval):
        """Validate that the heartbeat_interval value is an int

        :param int heartbeat_interval: The value to validate
        :rtype: bool
        :raises: TypeError
        :raises: ValueError

        """
        if not isinstance(heartbeat_interval, int):
            raise TypeError('heartbeat_interval must be an int')
        if heartbeat_interval < 0:
            raise ValueError('heartbeat_interval must >= 0')
        return True

    @staticmethod
    def _validate_host(host):
        """Validate that the host value is an str

        :param str|unicode host: The value to validate
        :rtype: bool
        :raises: TypeError

        """
        if not isinstance(host, basestring):
            raise TypeError('host must be a str or unicode str')
        return True

    @staticmethod
    def _validate_locale(locale):
        """Validate that the locale value is an str

        :param str locale: The value to validate
        :rtype: bool
        :raises: TypeError

        """
        if not isinstance(locale, basestring):
            raise TypeError('locale must be a str')
        return True

    @staticmethod
    def _validate_port(port):
        """Validate that the port value is an int

        :param int port: The value to validate
        :rtype: bool
        :raises: TypeError

        """
        if not isinstance(port, int):
            raise TypeError('port must be an int')
        return True

    @staticmethod
    def _validate_retry_delay(retry_delay):
        """Validate that the retry_delay value is an int or float

        :param int|float retry_delay: The value to validate
        :rtype: bool
        :raises: TypeError

        """
        if not any([isinstance(retry_delay, int),
                    isinstance(retry_delay, float)]):
            raise TypeError('retry_delay must be a float or int')
        return True

    @staticmethod
    def _validate_socket_timeout(socket_timeout):
        """Validate that the socket_timeout value is an int or float

        :param int|float socket_timeout: The value to validate
        :rtype: bool
        :raises: TypeError

        """
        if not any([isinstance(socket_timeout, int),
                    isinstance(socket_timeout, float)]):
            raise TypeError('socket_timeout must be a float or int')
        if not socket_timeout > 0:
            raise ValueError('socket_timeout must be > 0')
        return True

    @staticmethod
    def _validate_ssl(ssl):
        """Validate the SSL toggle is a bool

        :param bool ssl: The SSL enabled/disabled value
        :rtype: bool
        :raises: TypeError

        """
        if not isinstance(ssl, bool):
            raise TypeError('ssl must be a bool')
        return True

    @staticmethod
    def _validate_ssl_options(ssl_options):
        """Validate the SSL options value is a dictionary.

        :param dict|None ssl_options: SSL Options to validate
        :rtype: bool
        :raises: TypeError

        """
        if not isinstance(ssl_options, dict) and ssl_options is not None:
            raise TypeError('ssl_options must be either None or dict')
        return True

    @staticmethod
    def _validate_virtual_host(virtual_host):
        """Validate that the virtual_host value is an str

        :param str virtual_host: The value to validate
        :rtype: bool
        :raises: TypeError

        """
        if not isinstance(virtual_host, basestring):
            raise TypeError('virtual_host must be a str')
        return True


class ConnectionParameters(Parameters):
    """Connection parameters object that is passed into the connection adapter
    upon construction.

    :param str host: Hostname or IP Address to connect to
    :param int port: TCP port to connect to
    :param str virtual_host: RabbitMQ virtual host to use
    :param pika.credentials.Credentials credentials: auth credentials
    :param int channel_max: Maximum number of channels to allow
    :param int frame_max: The maximum byte size for an AMQP frame
    :param int heartbeat_interval: How often to send heartbeats
    :param bool ssl: Enable SSL
    :param dict ssl_options: Arguments passed to ssl.wrap_socket as
    :param int connection_attempts: Maximum number of retry attempts
    :param int|float retry_delay: Time to wait in seconds, before the next
    :param int|float socket_timeout: Use for high latency networks
    :param str locale: Set the locale value
    :param bool backpressure_detection: Toggle backpressure detection

    """

    def __init__(self,
                 host=None,
                 port=None,
                 virtual_host=None,
                 credentials=None,
                 channel_max=None,
                 frame_max=None,
                 heartbeat_interval=None,
                 ssl=None,
                 ssl_options=None,
                 connection_attempts=None,
                 retry_delay=None,
                 socket_timeout=None,
                 locale=None,
                 backpressure_detection=None):
        """Create a new ConnectionParameters instance.

        :param str host: Hostname or IP Address to connect to
        :param int port: TCP port to connect to
        :param str virtual_host: RabbitMQ virtual host to use
        :param pika.credentials.Credentials credentials: auth credentials
        :param int channel_max: Maximum number of channels to allow
        :param int frame_max: The maximum byte size for an AMQP frame
        :param int heartbeat_interval: How often to send heartbeats.
                                  Max between this value and server's proposal
                                  will be used. Use 0 to deactivate heartbeats
                                  and None to accept server's proposal.
        :param bool ssl: Enable SSL
        :param dict ssl_options: Arguments passed to ssl.wrap_socket
        :param int connection_attempts: Maximum number of retry attempts
        :param int|float retry_delay: Time to wait in seconds, before the next
        :param int|float socket_timeout: Use for high latency networks
        :param str locale: Set the locale value
        :param bool backpressure_detection: Toggle backpressure detection

        """
        super(ConnectionParameters, self).__init__()

        # Create the default credentials object
        if not credentials:
            credentials = self._credentials(self.DEFAULT_USERNAME,
                                            self.DEFAULT_PASSWORD)

        # Assign the values
        if host and self._validate_host(host):
            self.host = host
        if port is not None and self._validate_port(port):
            self.port = port
        if virtual_host and self._validate_virtual_host(virtual_host):
            self.virtual_host = virtual_host
        if credentials and self._validate_credentials(credentials):
            self.credentials = credentials
        if channel_max is not None and self._validate_channel_max(channel_max):
            self.channel_max = channel_max
        if frame_max is not None and self._validate_frame_max(frame_max):
            self.frame_max = frame_max
        if locale and self._validate_locale(locale):
            self.locale = locale
        if (heartbeat_interval is not None and
                self._validate_heartbeat_interval(heartbeat_interval)):
            self.heartbeat = heartbeat_interval
        if ssl is not None and self._validate_ssl(ssl):
            self.ssl = ssl
        if ssl_options and self._validate_ssl_options(ssl_options):
            self.ssl_options = ssl_options or dict()
        if (connection_attempts is not None and
                self._validate_connection_attempts(connection_attempts)):
            self.connection_attempts = connection_attempts
        if retry_delay is not None and self._validate_retry_delay(retry_delay):
            self.retry_delay = retry_delay
        if (socket_timeout is not None and
                self._validate_socket_timeout(socket_timeout)):
            self.socket_timeout = socket_timeout
        if (backpressure_detection is not None and
                self._validate_backpressure(backpressure_detection)):
            self.backpressure_detection = backpressure_detection


class URLParameters(Parameters):
    """Connect to RabbitMQ via an AMQP URL in the format::

         amqp://username:password@host:port/<virtual_host>[?query-string]

    Ensure that the virtual host is URI encoded when specified. For example if
    you are using the default "/" virtual host, the value should be `%2f`.

    Valid query string values are:

        - backpressure_detection:
            Toggle backpressure detection, possible values are `t` or `f`
        - channel_max:
            Override the default maximum channel count value
        - connection_attempts:
            Specify how many times pika should try and reconnect before it gives up
        - frame_max:
            Override the default maximum frame size for communication
        - heartbeat:
            Specify the number of seconds between heartbeat frames to ensure that
            the link between RabbitMQ and your application is up
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

    :param str url: The AMQP URL to connect to

    """

    def __init__(self, url):
        """Create a new URLParameters instance.

        :param str url: The URL value

        """
        super(URLParameters, self).__init__()
        self._process_url(url)

    def _process_url(self, url):
        """Take an AMQP URL and break it up into the various parameters.

        :param str url: The URL to parse

        """
        if url[0:4] == 'amqp':
            url = 'http' + url[4:]

        parts = urlparse.urlparse(url)

        # Handle the Protocol scheme, changing to HTTPS so urlparse doesnt barf
        if parts.scheme == 'https':
            self.ssl = True

        if self._validate_host(parts.hostname):
            self.host = parts.hostname
        if not parts.port:
            if self.ssl:
                self.port = self.DEFAULT_SSL_PORT if \
                    self.ssl else self.DEFAULT_PORT
        elif self._validate_port(parts.port):
            self.port = parts.port

        if parts.username is not None:
            self.credentials = pika_credentials.PlainCredentials(parts.username,
                                                                 parts.password)

        # Get the Virtual Host
        if len(parts.path) <= 1:
            self.virtual_host = self.DEFAULT_VIRTUAL_HOST
        else:
            path_parts = parts.path.split('/')
            virtual_host = url_unquote(path_parts[1])
            if self._validate_virtual_host(virtual_host):
                self.virtual_host = virtual_host

        # Handle query string values, validating and assigning them
        values = urlparse.parse_qs(parts.query)

        # Cast the various numeric values to the appropriate values
        for key in dictkeys(values):
            # Always reassign the first list item in query values
            values[key] = values[key].pop(0)
            if values[key].isdigit():
                values[key] = int(values[key])
            else:
                try:
                    values[key] = float(values[key])
                except ValueError:
                    pass

        if 'backpressure_detection' in values:
            if values['backpressure_detection'] == 't':
                self.backpressure_detection = True
            elif values['backpressure_detection'] == 'f':
                self.backpressure_detection = False
            else:
                raise ValueError('Invalid backpressure_detection value: %s' %
                                 values['backpressure_detection'])

        if ('channel_max' in values and
                self._validate_channel_max(values['channel_max'])):
            self.channel_max = values['channel_max']

        if ('connection_attempts' in values and
                self._validate_connection_attempts(
                    values['connection_attempts'])):
            self.connection_attempts = values['connection_attempts']

        if ('frame_max' in values and
                self._validate_frame_max(values['frame_max'])):
            self.frame_max = values['frame_max']

        if ('heartbeat' in values and
                self._validate_heartbeat_interval(values['heartbeat'])):
            self.heartbeat = values['heartbeat']

        elif ('heartbeat_interval' in values and
                self._validate_heartbeat_interval(
                    values['heartbeat_interval'])):
            warnings.warn('heartbeat_interval is deprecated, use heartbeat',
                          DeprecationWarning, stacklevel=2)
            self.heartbeat = values['heartbeat_interval']

        if 'locale' in values and self._validate_locale(values['locale']):
            self.locale = values['locale']

        if ('retry_delay' in values and
                self._validate_retry_delay(values['retry_delay'])):
            self.retry_delay = values['retry_delay']

        if ('socket_timeout' in values and
                self._validate_socket_timeout(values['socket_timeout'])):
            self.socket_timeout = values['socket_timeout']

        if 'ssl_options' in values:
            options = ast.literal_eval(values['ssl_options'])
            if self._validate_ssl_options(options):
                self.ssl_options = options


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
            on_close_callback(connection, reason_code, reason_text)

        """
        self._write_lock = threading.Lock()

        # Define our callback dictionary
        self.callbacks = callback.CallbackManager()

        # Add the on connection error callback
        self.callbacks.add(0, self.ON_CONNECTION_ERROR,
                           on_open_error_callback or self._on_connection_error,
                           False)

        self.heartbeat = None

        # On connection callback
        if on_open_callback:
            self.add_on_open_callback(on_open_callback)

        # On connection callback
        if on_close_callback:
            self.add_on_close_callback(on_close_callback)

        # Set our configuration options
        self.params = parameters or ConnectionParameters()

        # Initialize the connection state and connect
        self._init_connection_state()
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

        :param method callback_method: Callback to call on `Connection.Blocked`

        """
        self.callbacks.add(0, spec.Connection.Blocked, callback_method, False)

    def add_on_connection_unblocked_callback(self, callback_method):
        """Add a callback to be notified when RabbitMQ has sent a
        ``Connection.Unblocked`` frame letting publishers know it's ok
        to start publishing again. The callback will be passed the
        ``Connection.Unblocked`` method frame.

        :param method callback_method: Callback to call on
                                       `Connection.Unblocked`

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
            return

        if self._has_open_channels:
            self._close_channels(reply_code, reply_text)

        # Set our connection state
        self._set_connection_state(self.CONNECTION_CLOSING)
        LOGGER.info("Closing connection (%s): %s", reply_code, reply_text)
        self.closing = reply_code, reply_text

        if not self._has_open_channels:
            # if there are open channels then _on_close_ready will finally be
            # called in _on_channel_cleanup once all channels have been closed
            self._on_close_ready()

    def connect(self):
        """Invoke if trying to reconnect to a RabbitMQ server. Constructing the
        Connection object should connect on its own.

        """
        self._set_connection_state(self.CONNECTION_INIT)
        error = self._adapter_connect()
        if not error:
            return self._on_connected()
        self.remaining_connection_attempts -= 1
        LOGGER.warning('Could not connect, %i attempts left',
                       self.remaining_connection_attempts)
        if self.remaining_connection_attempts > 0:
            LOGGER.info('Retrying in %i seconds', self.params.retry_delay)
            # TODO: remove timeout if connection is closed before timer fires
            self.add_timeout(self.params.retry_delay, self.connect)
        else:
            self.callbacks.process(0, self.ON_CONNECTION_ERROR, self, self,
                                   error)
            self.remaining_connection_attempts = self.params.connection_attempts
            self._set_connection_state(self.CONNECTION_CLOSED)

    def remove_timeout(self, callback_method):
        """Adapters should override to call the callback after the
        specified number of seconds have elapsed, using a timer, or a
        thread, or similar.

        :param method callback_method: The callback to remove a timeout for

        """
        raise NotImplementedError

    def set_backpressure_multiplier(self, value=10):
        """Alter the backpressure multiplier value. We set this to 10 by default.
        This value is used to raise warnings and trigger the backpressure
        callback.

        :param int value: The multiplier value to set

        """
        self._backpressure = value

    #
    # Connections state properties
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
            raise exceptions.ProtocolVersionMismatch(frame.ProtocolHeader(),
                                                     value)

    @property
    def _client_properties(self):
        """Return the client properties dictionary.

        :rtype: dict

        """
        return {
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

    def _close_channels(self, reply_code, reply_text):
        """Close the open channels with the specified reply_code and reply_text.

        :param int reply_code: The code for why the channels are being closed
        :param str reply_text: The text reason for why the channels are closing

        """
        if self.is_open:
            for channel_number in dictkeys(self._channels):
                if self._channels[channel_number].is_open:
                    self._channels[channel_number].close(reply_code, reply_text)
                else:
                    del self._channels[channel_number]
                    # Force any lingering callbacks to be removed
                    # moved inside else block since channel's _cleanup removes
                    # callbacks
                    self.callbacks.cleanup(channel_number)
        else:
            self._channels = dict()

    def _combine(self, a, b):
        """Pass in two values, if a is 0, return b otherwise if b is 0,
        return a. If neither case matches return the smallest value.

        :param int a: The first value
        :param int b: The second value
        :rtype: int

        """
        return min(a, b) or (a or b)

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
        return channel.Channel(self, channel_number, on_open_callback)

    def _create_heartbeat_checker(self):
        """Create a heartbeat checker instance if there is a heartbeat interval
        set.

        :rtype: pika.heartbeat.Heartbeat

        """
        if self.params.heartbeat is not None and self.params.heartbeat > 0:
            LOGGER.debug('Creating a HeartbeatChecker: %r',
                         self.params.heartbeat)
            return heartbeat.HeartbeatChecker(self, self.params.heartbeat)

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
            if self._is_basic_deliver_frame(value):
                self._reject_out_of_band_delivery(value.channel_number,
                                                  value.method.delivery_tag)
            else:
                LOGGER.warning("Received %r for non-existing channel %i", value,
                               value.channel_number)
            return
        return self._channels[value.channel_number]._handle_content_frame(value)

    def _detect_backpressure(self):
        """Attempt to calculate if TCP backpressure is being applied due to
        our outbound buffer being larger than the average frame size over
        a window of frames.

        """
        avg_frame_size = self.bytes_sent / self.frames_sent
        buffer_size = sum([len(frame) for frame in self.outbound_buffer])
        if buffer_size > (avg_frame_size * self._backpressure):
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
            raise exceptions.AuthenticationError(self.params.credentials.TYPE)
        self.params.credentials.erase_credentials()
        return auth_type, response

    @property
    def _has_open_channels(self):
        """Returns true if channels are open.

        :rtype: bool

        """
        return any([self._channels[num].is_open
                    for num in dictkeys(self._channels)])

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
        self._backpressure = 10

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

    def _is_basic_deliver_frame(self, frame_value):
        """Returns true if the frame is a Basic.Deliver

        :param pika.frame.Method frame_value: The frame to check
        :rtype: bool

        """
        return isinstance(frame_value, spec.Basic.Deliver)

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
        limit = self.params.channel_max or channel.MAX_CHANNELS
        if len(self._channels) >= limit:
            raise exceptions.NoFreeChannels()

        for n in xrange(1, len(self._channels) + 1):
            if n not in self._channels:
                return n
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
        if self.is_closing and not self._has_open_channels:
            self._on_close_ready()

    def _on_close_ready(self):
        """Called when the Connection is in a state that it can close after
        a close has been requested. This happens, for example, when all of the
        channels are closed that were open when the close request was made.

        """
        if self.is_closed:
            LOGGER.warning('Invoked while already closed')
            return
        self._send_connection_close(self.closing[0], self.closing[1])

    def _on_connected(self):
        """Invoked when the socket is connected and it's time to start speaking
        AMQP with the broker.

        """
        self._set_connection_state(self.CONNECTION_PROTOCOL)

        # Start the communication with the RabbitMQ Broker
        self._send_frame(frame.ProtocolHeader())

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

    def _on_connection_error(self, connection_unused, error_message=None):
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

    def _on_connection_tune(self, method_frame):
        """Once the Broker sends back a Connection.Tune, we will set our tuning
        variables that have been returned to us and kick off the Heartbeat
        monitor if required, send our TuneOk and then the Connection. Open rpc
        call on channel 0.

        :param pika.frame.Method method_frame: The frame received

        """
        self._set_connection_state(self.CONNECTION_TUNE)

        # Get our max channels, frames and heartbeat interval
        self.params.channel_max = self._combine(self.params.channel_max,
                                                method_frame.method.channel_max)
        self.params.frame_max = self._combine(self.params.frame_max,
                                              method_frame.method.frame_max)
        if self.params.heartbeat is None:
            self.params.heartbeat = method_frame.method.heartbeat
        elif self.params.heartbeat != 0:
            self.params.heartbeat = max(self.params.heartbeat,
                                        method_frame.method.heartbeat)

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

        :param integer reason_code: HTTP error code for AMQP-reported closures
          or -1 for other errors (such as socket errors)
        :param str reason_text: human-readable text message describing the error
        """
        LOGGER.warning(
            'Disconnected from RabbitMQ at %s:%i (%s): %s',
            self.params.host, self.params.port, reason_code,
            reason_text)

        if not isinstance(reason_code, numbers.Integral):
            raise TypeError('reason_code must be an integer, but got %r'
                            % (reason_code,))

        # Stop the heartbeat checker if it exists
        self._remove_heartbeat()

        # Remove connection management callbacks
        # TODO: This call was moved here verbatim from legacy code and the
        # following doesn't seem to be right: `Connection.Open` here is
        # unexpected, we don't appear to ever register it, and the broker
        # shouldn't be sending `Connection.Open` to us, anyway.
        self._remove_callbacks(0, [spec.Connection.Close, spec.Connection.Start,
                                   spec.Connection.Open])

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
            method_frame = frame.Method(channel, spec.Channel.Close(
                reason_code,
                reason_text))
            self._channels[channel]._on_close(method_frame)

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

    def _reject_out_of_band_delivery(self, channel_number, delivery_tag):
        """Reject a delivery on the specified channel number and delivery tag
        because said channel no longer exists.

        :param int channel_number: The channel number
        :param int delivery_tag: The delivery tag

        """
        LOGGER.warning('Rejected out-of-band delivery on channel %i (%s)',
                       channel_number, delivery_tag)
        self._send_method(channel_number, spec.Basic.Reject(delivery_tag))

    def _remove_callback(self, channel_number, method_frame):
        """Remove the specified method_frame callback if it is set for the
        specified channel number.

        :param int channel_number: The channel number to remove the callback on
        :param pika.object.Method: The method frame for the callback

        """
        self.callbacks.remove(str(channel_number), method_frame)

    def _remove_callbacks(self, channel_number, method_frames):
        """Remove the callbacks for the specified channel number and list of
        method frames.

        :param int channel_number: The channel number to remove the callback on
        :param list method_frames: The method frames for the callback

        """
        for method_frame in method_frames:
            self._remove_callback(channel_number, method_frame)

    def _rpc(self, channel_number, method_frame,
             callback_method=None,
             acceptable_replies=None):
        """Make an RPC call for the given callback, channel number and method.
        acceptable_replies lists out what responses we'll process from the
        server with the specified callback.

        :param int channel_number: The channel number for the RPC call
        :param pika.object.Method method_frame: The method frame to call
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
        self._send_method(channel_number, method_frame)

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
            LOGGER.critical('Attempted to send frame when closed')
            raise exceptions.ConnectionClosed

        marshaled_frame = frame_value.marshal()
        self.bytes_sent += len(marshaled_frame)
        self.frames_sent += 1
        self.outbound_buffer.append(marshaled_frame)
        self._flush_outbound()
        if self.params.backpressure_detection:
            self._detect_backpressure()

    def _send_method(self, channel_number, method_frame, content=None):
        """Constructs a RPC method frame and then sends it to the broker.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        if not content:
            with self._write_lock:
                self._send_frame(frame.Method(channel_number, method_frame))
                return
        self._send_message(channel_number, method_frame, content)

    def _send_message(self, channel_number, method_frame, content=None):
        """Send the message directly, bypassing the single _send_frame
        invocation by directly appending to the output buffer and flushing
        within a lock.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        length = len(content[1])
        write_buffer = [frame.Method(channel_number, method_frame).marshal(),
                        frame.Header(channel_number, length,
                                     content[0]).marshal()]
        if content[1]:
            chunks = int(math.ceil(float(length) / self._body_max_length))
            for chunk in range(0, chunks):
                s = chunk * self._body_max_length
                e = s + self._body_max_length
                if e > length:
                    e = length
                write_buffer.append(frame.Body(channel_number,
                                               content[1][s:e]).marshal())

        with self._write_lock:
            self.outbound_buffer += write_buffer
            self.frames_sent += len(write_buffer)
            self._flush_outbound()
            if self.params.backpressure_detection:
                self._detect_backpressure()

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
