"""Core connection objects"""
import logging
import platform

from pika import __version__
from pika import callback
from pika import channel
from pika import credentials as pika_credentials
from pika import exceptions
from pika import frame
from pika import heartbeat
from pika import utils
from pika import simplebuffer
from pika import spec

BACKPRESSURE_WARNING = ("Pika: Write buffer exceeded warning threshold at "
                        "%i bytes and an estimated %i frames behind")
PRODUCT = "Pika Python Client Library"

LOGGER = logging.getLogger(__name__)


class ConnectionParameters(object):
    """Connection parameters object that is passed into the connection adapter
    upon construction.

    """
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_SOCKET_TIMEOUT = 0.25
    def __init__(self,
                 host='localhost',
                 port=spec.PORT,
                 virtual_host='/',
                 credentials=None,
                 channel_max=0,
                 frame_max=spec.FRAME_MAX_SIZE,
                 heartbeat_interval=0,
                 ssl=False,
                 ssl_options=None,
                 connection_attempts=1,
                 retry_delay=2.0,
                 socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                 locale=DEFAULT_LOCALE):
        """Create a new ConnectionParameters instance.

        :param str host: Hostname or IP Address to connect to.
            Defaults to localhost.
        :param int port: TCP port to connect to.
            Defaults to 5672
        :param str virtual_host: RabbitMQ virtual host to use.
            Defaults to /
        :param pika.credentials.Credentials credentials: A instance of a
            credentials class to authenticate with.
                Defaults to PlainCredentials for the guest user.
        :param int channel_max: Maximum number of channels to allow,.
            Defaults to 0 for None
        :param int frame_max: The maximum byte size for an AMQP frame.
            Defaults to 131072
        :param int heartbeat_interval: Heartbeat Interval. 0 for heartbeat off.
            Defaults to 0
        :param bool ssl: Enable SSL
            Defaults to False
        :param dict ssl_options: Arguments passed to ssl.wrap_socket as
            described at http://docs.python.org/dev/library/ssl.html
        :param int connection_attempts: Maximum number of retry attempts.
            None for infinite. Defaults to 1
        :param int|float retry_delay: Time to wait in seconds, before the next
            attempt. Defaults to 2
        :param int|float socket_timeout: Use for high latency networks
            Defaults to 0.25
        :param str locale: Set the locale value
            Defaults to en_US

        :raises: InvalidFrameSize
        :raises: TypeError

        """
        if not isinstance(host, str):
            raise TypeError("Host must be a str")
        if not isinstance(port, int):
            raise TypeError("Port must be an int")
        if not credentials:
            credentials = pika_credentials.PlainCredentials('guest', 'guest')
        else:
            self._validate_credentials(credentials)
        if not isinstance(channel_max, int):
            raise TypeError("max-channels must be an int")
        if not isinstance(frame_max, int):
            raise TypeError("frame_max must be an int")
        if frame_max < spec.FRAME_MIN_SIZE:
            raise exceptions.InvalidMinimumFrameSize
        elif frame_max > spec.FRAME_MAX_SIZE:
            raise exceptions.InvalidMaximumFrameSize
        if not isinstance(frame_max, int):
            raise TypeError("frame_max must be an int")
        if not isinstance(heartbeat_interval, int):
            raise TypeError("heartbeat must be an int")
        if not isinstance(ssl, bool):
            raise TypeError("ssl must be a bool")
        if not isinstance(ssl_options, dict) and ssl_options is not None:
            raise TypeError("ssl_options must be either None or dict")
        if (connection_attempts is not None and
            not isinstance(connection_attempts, int)):
            raise TypeError("connection_attempts must be either None or int")
        if (not isinstance(retry_delay, int) and
            not isinstance(retry_delay, float)):
            raise TypeError("retry_delay must be a float or int")
        if (not isinstance(socket_timeout, int) and
            not isinstance(socket_timeout, float)):
            raise TypeError("socket_timeout must be a float or int")

        # Assign the values
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.credentials = credentials
        self.channel_max = channel_max
        self.frame_max = frame_max
        self.locale = locale
        self.heartbeat = heartbeat_interval
        self.ssl = ssl
        self.ssl_options = ssl_options
        self.connection_attempts = connection_attempts
        self.retry_delay = retry_delay
        self.socket_timeout = socket_timeout

    def _validate_credentials(self, credentials):
        """Validate the credentials passed in are using a valid object type.

        :param pika.credentials.Credentials credentials: Credentials to validate
        :raises: TypeError

        """
        for credential_type in pika_credentials.VALID_TYPES:
            if isinstance(credentials, credential_type):
                return
        raise TypeError('Credentials must be an object of type: %r' %
                        pika_credentials.VALID_TYPES)


class Connection(object):
    """This is the core class that implements communication with RabbitMQ. This
    class should not be invoked directly but rather through the use of an
    adapter such as SelectConnection or BlockingConnection.

    """
    CONNECTION_CLOSED = 0
    CONNECTION_INIT = 1
    CONNECTION_PROTOCOL = 2
    CONNECTION_START = 3
    CONNECTION_TUNE = 4
    CONNECTION_OPEN = 5
    CONNECTION_CLOSING = 6

    def __init__(self, parameters=None,
                 on_open_callback=None):
        """Connection initialization expects a ConnectionParameters object and
        a callback function to notify when we have successfully connected
        to the AMQP Broker.

        :param parameters: Connection parameters
        :type parameters: pika.connection.ConnectionParameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method

        """
        # Define our callback dictionary
        self.callbacks = callback.CallbackManager()

        # On connection callback
        if on_open_callback:
            self.add_on_open_callback(on_open_callback)

        # Set our configuration options
        self.params = parameters or ConnectionParameters()

        # Initialize the connection state and connect
        self._init_connection_state()
        self._connect()

    def add_backpressure_callback(self, callback_method):
        """Call method "callback" when pika believes backpressure is being
        applied.

        :param method callback_method: The method to call

        """
        self.callbacks.add(0, 'backpressure', callback_method, False)

    def add_on_close_callback(self, callback_method):
        """Add a callback notification when the connection has closed.

        :param method callback_method: The callback when the channel is opened

        """
        self.callbacks.add(0, '_on_connection_closed', callback_method, False)

    def add_on_open_callback(self, callback_method):
        """Add a callback notification when the connection has opened.

        :param method callback_method: The callback when the channel is opened

        """
        self.callbacks.add(0, '_on_connection_open', callback_method, False)

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

        """
        if not channel_number:
            channel_number = self._next_channel_number()
        self._channels[channel_number] = self._create_channel(channel_number,
                                                              on_open_callback)
        self._add_channel_callbacks(channel_number)
        self._channels[channel_number].open()

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        if self.is_closing or self.is_closed:
            LOGGER.warning("Invoked while closing or closed")
            return

        # Set our connection state
        self._set_connection_state(self.CONNECTION_CLOSING)
        LOGGER.info("Closing connection (%s): %s", reply_code, reply_text)
        self.closing = reply_code, reply_text

        # If channels are open, _on_close_ready will be called when they close
        if self._has_open_channels:
            return self._close_channels(reply_code, reply_text, False)

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
        Returns a boolean reporting the current connection state.
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
        return self.server_capabilities.get('exchange_exchange_bindings',
                                            False)

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
        self.callbacks.add(channel_number,
                           spec.Channel.CloseOk,
                           self._on_channel_close)

    def _add_connection_start_callback(self):
        """Add a callback for when a Connection.Start frame is received from
        the broker.

        """
        self.callbacks.add(0, spec.Connection.Start, self._on_connection_start)

    def _add_connection_tune_callback(self):
        """Add a callback for when a Connection.Tune frame is received."""
        self.callbacks.add(0, spec.Connection.Tune, self._on_connection_tune)

    def _append_frame_buffer(self, bytes):
        """Append the bytes to the frame buffer.

        :param str bytes: The bytes to append to the frame buffer

        """
        self._frame_buffer += bytes

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
        return {'product': PRODUCT,
                'platform': 'Python %s' % platform.python_version(),
                'capabilities': {'basic.nack': True,
                                 'consumer_cancel_notify': True,
                                 'publisher_confirms': True},
                'information': 'See http://pika.github.com',
                'version': __version__}

    def _close_channel(self, channel_number, reply_code, reply_text,
                       remote=True):
        """Close the specified channel number in response to the broker sending
        a Channel.Close. If remote is True, Close.Ok will be sent

        :param int channel_number: The channel number to close
        :param int reply_code: The Channel.Close reply code from RabbitMQ
        :param str reply_text: The Channel.Close reply text from RabbitMQ
        :param bool remote: The close was due to a remote close

        """
        if self.is_open:
            LOGGER.info('Closing channel %i due to remote close (%s): %s',
                        channel_number, reply_code, reply_text)
            self._channels[channel_number].close(reply_code, reply_text, remote)
            if remote:
                self._send_channel_close_ok(channel_number)
            self._channels[channel_number].cleanup()
            del(self._channels[channel_number])

    def _close_channels(self, reply_code, reply_text, remote=False):
        """Close the open channels with the specified reply_code and reply_text.

        :param int reply_code: The code for why the channels are being closed
        :param str reply_text: The text reason for why the channels are closing

        """
        if self.is_open:
            for channel_number in self._channels.keys():
                if self._channels[channel_number].is_open:
                    self._close_channel(channel_number, reply_code,
                                        reply_text, remote)
                else:
                    del self._channels[channel_number]
        else:
            del self._channels
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
        """Call the Adapter's connect method after letting the
        ReconnectionStrategy know.

        """
        LOGGER.debug('Attempting connection')
        self._set_connection_state(self.CONNECTION_INIT)
        self._adapter_connect()
        LOGGER.debug('Connected')

    def _create_channel(self, channel_number, on_open_callback):
        """Create a new channel using the specified channel number and calling
        back the method specified by on_open_callback

        :param int channel_number: The channel number to use
        :param method on_open_callback: The callback when the channel is opened

        """
        LOGGER.debug('Creating channel')
        return channel.Channel(self, channel_number, on_open_callback)

    def _create_heartbeat_checker(self):
        """Create a heartbeat checker instance if there is a heartbeat interval
        set.

        :rtype: pika.heartbeat.Heartbeat

        """
        LOGGER.debug('Creating a HeartbeatChecker')
        if self.params.heartbeat:
            return heartbeat.HeartbeatChecker(self, self.params.heartbeat)

    def _deliver_frame_to_channel(self, value):
        """Deliver the frame to the channel specified in the frame.

        :param pika.frame.Method value: The frame to deliver

        """
        if value.channel_number in self._channels:
            return self._channels[value.channel_number].transport.deliver(value)
        if self._is_basic_deliver_frame(value):
            self._reject_out_of_band_delivery(value.channel_number,
                                              value.method.delivery_tag)
        else:
            LOGGER.warning("Received %r for non-existing channel %i",
                           value, value.channel_number)

    def _detect_backpressure(self):
        """Attempt to calculate if TCP backpressure is being applied due to
        our outbound buffer being larger than the average frame size over
        a window of frames.

        """
        avg_frame_size = self.bytes_sent / self.frames_sent
        if self.outbound_buffer.size > (avg_frame_size * self._backpressure):
            LOGGER.warning(BACKPRESSURE_WARNING,
                           self.outbound_buffer.size,
                           int(self.outbound_buffer.size / avg_frame_size))
            self.callbacks.process(0, 'backpressure', self)

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
        return (self.params.frame_max -
                spec.FRAME_HEADER_SIZE -
                spec.FRAME_END_SIZE)

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
        return bool(self._channels)

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
        # Outbound buffer for buffering writes until we're able to send them
        self.outbound_buffer = simplebuffer.SimpleBuffer()

        # Inbound buffer for decoding frames
        self._frame_buffer = ''

        # Connection state, server properties and channels all change on
        # each connection
        self.server_properties = None
        self._channels = dict()

        # Data used for Heartbeat checking and back-pressure detection
        self.bytes_sent = 0
        self.bytes_received = 0
        self.frames_sent = 0
        self.frames_received = 0
        self.heartbeat = None

        # Default back-pressure multiplier value
        self._backpressure = 10

        # Connection state
        self._set_connection_state(self.CONNECTION_CLOSED)

        # When closing, hold reason why
        self.closing = 0, ''

        # Our starting point once connected, first frame received
        self._add_connection_start_callback()

    def _is_basic_deliver_frame(self, frame_value):
        """Returns true if the frame is a Basic.Deliver

        :param pika.frame.Method frame_value: The frame to check
        :rtype: bool

        """
        return isinstance(frame_value, spec.Basic.Deliver)

    def _is_channel_close_frame(self, value):
        """Returns true if the frame is a Channel.Close frame.

        :param pika.frame.Method value: The frame to check
        :rtype: bool

        """
        return isinstance(value.method, spec.Channel.Close)

    def _is_connection_close_frame(self, value):
        """Returns true if the frame is a Connection.Close frame.

        :param pika.frame.Method value: The frame to check
        :rtype: bool

        """
        if not value:
            return False
        return isinstance(value.method, spec.Connection.Close)

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
        return  isinstance(value, frame.ProtocolHeader)

    def _next_channel_number(self):
        """Return the next available channel number or raise on exception.

        :rtype: int

        """
        limit = self.params.channel_max or channel.MAX_CHANNELS
        if len(self._channels) == limit:
            raise exceptions.NoFreeChannels()
        if not self._channels:
            return 1
        return max(self._channels.keys()) + 1

    def _on_channel_close(self, method_frame):
        """Handle a RPC request from the server to close the channel.

        :param frame pika.frame.Method method_frame: The frame received

        """
        channel_number = method_frame.channel_number
        if (self.is_open and channel_number in self._channels and
            self._channels[channel_number].is_open):
            self._channels[channel_number].on_remote_close(method_frame)
            self._close_channel(method_frame.channel_number,
                                method_frame.reply_code,
                                method_frame.reply_text)
        else:
            del self._channels[channel_number]
        if self.is_closing and not self._channels:
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
        """
        This is called by our connection Adapter to let us know that we've
        connected and we can notify our connection strategy.
        """
        self._set_connection_state(self.CONNECTION_PROTOCOL)

        # Start the communication with the RabbitMQ Broker
        self._send_frame(frame.ProtocolHeader())

    def _on_connection_closed(self, method_frame, from_adapter=False):
        """Called when the connection is closed remotely. The from_adapter value
        will be true if the connection adapter has been disconnected from
        the broker and the method was invoked directly instead of by receiving
        a Connection.Close frame.

        :param pika.frame.Method: The Connection.Close frame
        :param bool from_adapter: Called by the connection adapter

        """
        if method_frame and self._is_connection_close_frame(method_frame):
            self.closing = (method_frame.method.reply_code,
                            method_frame.method.reply_text)
        LOGGER.warning("Disconnected from RabbitMQ at %s:%i (%s): %s",
                        self.params.host, self.params.port,
                        self.closing[0], self.closing[1])
        self._set_connection_state(self.CONNECTION_CLOSED)
        self._remove_connection_callbacks()
        if not from_adapter:
            self._adapter_disconnect()
        for channel in self._channels:
            self._channels[channel].on_remote_close(method_frame)
        self._process_connection_closed_callbacks()
        self._remove_connection_callbacks()

    def _on_connection_open(self, method_frame):
        """
        This is called once we have tuned the connection with the server and
        called the Connection.Open on the server and it has replied with
        Connection.Ok.
        """
        self.known_hosts = method_frame.method.known_hosts

        # Add a callback handler for the Broker telling us to disconnect
        self.callbacks.add(0, spec.Connection.Close, self._on_connection_closed)

        # We're now connected at the AMQP level
        self._set_connection_state(self.CONNECTION_OPEN)

        # Call our initial callback that we're open
        self.callbacks.process(0, '_on_connection_open', self, self)

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
        self.params.heartbeat = self._combine(self.params.heartbeat,
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

    def _process_callbacks(self, frame_value):
        """Process the callbacks for the frame if the frame is a method frame
        and if it has any callbacks pending.

        :param pika.frame.Method frame_value: The frame to process
        :rtype: bool

        """
        if (self._is_method_frame(frame_value) and
            self._has_pending_callbacks(frame_value)):
            self.callbacks.process(frame_value.channel_number,  # Prefix
                                   frame_value.method,          # Key
                                   self,                        # Caller
                                   frame_value)                 # Args
            return True
        return False

    def _process_connection_closed_callbacks(self):
        """Process any callbacks that should be called when the connection is
        closed.

        """
        self.callbacks.process(0, '_on_connection_closed', self, self)

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

        # If we have a Method Frame and have callbacks for it
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

    def _remove_connection_callbacks(self):
        """Remove all callbacks for the connection"""
        self._remove_callbacks(0, [spec.Connection.Close,
                                   spec.Connection.Start,
                                   spec.Connection.Open])

    def _rpc(self, channel_number, method_frame,
             callback_method=None, acceptable_replies=None):
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
            raise TypeError("acceptable_replies should be list or None")

        # Validate the callback is callable
        if callback_method:
            if not utils.is_callable(callback_method):
                raise TypeError("callback should be None, function or method.")

            for reply in acceptable_replies:
                self.callbacks.add(channel_number, reply, callback_method)

        # Send the rpc call to RabbitMQ
        self._send_method(channel_number, method_frame)

    def _send_channel_close_ok(self, channel_number):
        """Send a Channel._ frame for the given channel number and remove
        the expectation of a Channel.CloseOk in the callbacks for the channel.

        :param int channel_number: The channel number to send CloseOk to

        """
        if not self.is_closed:
            self._rpc(channel_number, spec.Channel.CloseOk())
            self._remove_callback(channel_number, spec.Channel.CloseOk)

    def _send_connection_close(self, reply_code, reply_text):
        """Send a Connection.Close method frame.

        :param int reply_code: The reason for the close
        :param str reply_text: The text reason for the close

        """
        self._rpc(0, spec.Connection.Close(reply_code, reply_text, 0, 0),
                  self._on_connection_closed, [spec.Connection.CloseOk])

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
        self._send_method(0, spec.Connection.StartOk(self._client_properties,
                                                     authentication_type,
                                                     response,
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

        """
        if self.is_closed:
            raise exceptions.ConnectionClosed
        marshaled_frame = frame_value.marshal()
        self.bytes_sent += len(marshaled_frame)
        self.frames_sent += 1
        self.outbound_buffer.write(marshaled_frame)
        self._flush_outbound()
        self._detect_backpressure()

    def _send_method(self, channel_number, method_frame, content=None):
        """Constructs a RPC method frame and then sends it to the broker.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        LOGGER.debug('Sending on channel %i: %r', channel_number, method_frame)
        self._send_frame(frame.Method(channel_number, method_frame))
        if isinstance(content, tuple):
            self._send_frame(frame.Header(channel_number,
                                          len(content[1]),
                                           content[0]))
            if content[1]:
                body_buf = simplebuffer.SimpleBuffer(content[1])
                while body_buf:
                    piece_len = min(len(body_buf), self._body_max_length)
                    piece = body_buf.read_and_consume(piece_len)
                    self._send_frame(frame.Body(channel_number, piece))

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
