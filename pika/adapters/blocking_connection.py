"""Implement a blocking, procedural style connection adapter on top of the
asynchronous core.

"""
import logging
import socket
import time

from pika import callback
from pika import channel
from pika import exceptions
from pika import spec
from pika import utils
from pika.adapters import base_connection

LOGGER = logging.getLogger(__name__)


class BlockingConnection(base_connection.BaseConnection):
    """The BlockingConnection adapter is meant for simple implementations where
    you want to have blocking behavior. The behavior layered on top of the
    async library. Because of the nature of AMQP there are a few callbacks
    one needs to do, even in a blocking implementation. These include receiving
    messages from Basic.Deliver, Basic.GetOk, and Basic.Return.

    """
    WRITE_TO_READ_RATIO = 10000
    DO_HANDSHAKE = True
    SOCKET_CONNECT_TIMEOUT = .25
    SOCKET_TIMEOUT_THRESHOLD = 12
    SOCKET_TIMEOUT_MESSAGE = "Timeout exceeded, disconnected"

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        timeout_id = '%.8f' % time.time()
        self._timeouts[timeout_id] = {'deadline': deadline + time.time(),
                                      'method': callback_method}
        return timeout_id

    def channel(self, channel_number=None):
        """Create a new channel with the next available or specified channel #.

        :param int channel_number: Specify the channel number

        """
        self._channel_open = False
        if not channel_number:
            channel_number = self._next_channel_number()
        LOGGER.debug('Opening channel %i', channel_number)
        self.callbacks.add(channel_number,
                           spec.Channel.CloseOk,
                           self._on_channel_close)
        LOGGER.debug('Creating transport')
        transport = BlockingChannelTransport(self, channel_number)
        LOGGER.debug('Creating channel')
        self._channels[channel_number] = BlockingChannel(self,
                                                         channel_number,
                                                         transport)
        LOGGER.debug('Channel %i is open', channel_number)
        return self._channels[channel_number]

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        self._remove_connection_callbacks()
        super(BlockingConnection, self).close(reply_code, reply_text)
        while not self.is_closed:
            self.process_data_events()

    def disconnect(self):
        """Disconnect from the socket"""
        self.socket.close()

    def process_data_events(self):
        """Will make sure that data events are processed. Your app can
        block on this method.

        """
        try:
            if self._handle_read():
                self._socket_timeouts = 0
        except socket.timeout:
            self._handle_timeout()
        self._flush_outbound()
        self.process_timeouts()

    def process_timeouts(self):
        """Process the self._timeouts event stack"""
        for timeout_id in self._timeouts.keys():
            if self._deadline_passed(timeout_id):
                self._call_timeout_method(self._timeouts.pop(timeout_id))

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :param str timeout_id: The id of the timeout to remove

        """
        if timeout_id in self._timeouts:
            del self._timeouts[timeout_id]

    def send_method(self, channel_number, method_frame, content=None):
        """Constructs a RPC method frame and then sends it to the broker.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        self._send_method(channel_number, method_frame, content)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker"""
        super(BlockingConnection, self)._adapter_connect()
        LOGGER.debug('Setting socket connection timeout')
        self.socket.settimeout(self.SOCKET_CONNECT_TIMEOUT)
        self._frames_written_without_read = 0
        self._socket_timeouts = 0
        self._timeouts = dict()
        self._on_connected()
        while not self.is_open:
            self.process_data_events()

        LOGGER.debug('Setting socket timeout to %s', self.params.socket_timeout)
        self.socket.settimeout(self.params.socket_timeout)
        LOGGER.info('Adapter connected')

    def _adapter_disconnect(self):
        """Called if the connection is being requested to disconnect."""
        self.disconnect()
        self._check_state_on_disconnect()

    def _call_timeout_method(self, timeout_value):
        """Execute the method that was scheduled to be called.

        :param dict timeout_value: The configuration for the timeout

        """
        LOGGER.debug('Invoking scheduled call of %s', timeout_value['method'])
        timeout_value['method']()

    def _deadline_passed(self, timeout_id):
        """Returns True if the deadline has passed for the specified timeout_id.

        :param str timeout_id: The id of the timeout to check
        :rtype: bool

        """
        if timeout_id not in self._timeouts.keys():
            return False
        return self._timeouts[timeout_id]['deadline'] <= time.time()

    def _handle_disconnect(self):
        """Called internally when the socket is disconnected already"""
        LOGGER.debug('Handling disconnect')
        self.disconnect()
        self._on_connection_closed(None, True)

    def _handle_read(self):
        super(BlockingConnection, self)._handle_read()
        self._frames_written_without_read = 0

    def _handle_timeout(self):
        """Invoked whenever the socket times out"""
        self._socket_timeouts += 1
        LOGGER.warning('Handling timeout %i with a threshold of %i',
                       self._socket_timeouts, self.SOCKET_TIMEOUT_THRESHOLD)
        if (self.is_closing and
            self._socket_timeouts > self.SOCKET_TIMEOUT_THRESHOLD):
            LOGGER.critical('Closing connection due to timeout')
            self._on_connection_closed(None, True)

    def _flush_outbound(self):
        """Flush the outbound socket buffer."""
        if self.outbound_buffer.size:
            try:
                if self._handle_write():
                    self._socket_timeouts = 0
            except socket.timeout:
                return self._handle_timeout()

    def _on_connection_closed(self, method_frame, from_adapter=False):
        """Called when the connection is closed remotely. The from_adapter value
        will be true if the connection adapter has been disconnected from
        the broker and the method was invoked directly instead of by receiving
        a Connection.Close frame.

        :param pika.frame.Method: The Connection.Close frame
        :param bool from_adapter: Called by the connection adapter
        :raises: AMQPConnectionError

        """
        if self._is_connection_close_frame(method_frame):
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
        self._remove_connection_callbacks()
        if self.closing[0] != 200:
            raise exceptions.AMQPConnectionError(*self.closing)

    def _send_frame(self, frame_value):
        """This appends the fully generated frame to send to the broker to the
        output buffer which will be then sent via the connection adapter.

        :param frame_value: The frame to write
        :type frame_value:  pika.frame.Frame|pika.frame.ProtocolHeader

        """
        super(BlockingConnection, self)._send_frame(frame_value)
        self._frames_written_without_read += 1
        if self._frames_written_without_read == self.WRITE_TO_READ_RATIO:
            self._frames_written_without_read = 0
            self.process_data_events()


class BlockingChannelTransport(channel.ChannelTransport):

    no_response_frame = ['Basic.Ack', 'Basic.Reject', 'Basic.RecoverAsync']

    def __init__(self, connection, channel_number):
        super(BlockingChannelTransport, self).__init__(connection,
                                                       channel_number)
        self._replies = list()
        self._frames = dict()
        self._wait = False

    def add_reply(self, reply):
        reply = callback._name_or_value(reply)
        self._replies.append(reply)

    def on_rpc_complete(self, frame):
        key = callback._name_or_value(frame)
        self._replies.append(key)
        self._frames[key] = frame
        self._received_response = True

    def remove_reply(self, frame):
        key = callback._name_or_value(frame)
        if key in self._replies:
            self._replies.remove(key)

    def rpc(self, method_frame,
            callback_method=None, acceptable_replies=None):
        """Make an RPC call for the given callback, channel number and method.
        acceptable_replies lists out what responses we'll process from the
        server with the specified callback.

        :param pika.frame.Method method_frame: The method frame to call
        :param method callback_method: The callback for the RPC response
        :param list acceptable_replies: The replies this RPC call expects

        """
        LOGGER.debug('Sending %s RPC frame', method_frame)
        self._ensure()
        self._validate_acceptable_replies(acceptable_replies)
        self._validate_callback_method(callback_method)
        replies = list()
        for reply in acceptable_replies or list():
            LOGGER.debug('Reply: %r', reply)
            LOGGER.debug('Channel: %i', self.channel_number)
            LOGGER.debug('Callback: %r', self.on_rpc_complete)
            prefix, key = self.callbacks.add(self.channel_number,
                                             reply,
                                             self.on_rpc_complete)
            replies.append(key)
        self._received_response = False
        self.send_method(method_frame, None,
                         self._wait_on_response(method_frame))
        return self._process_replies(replies, callback_method)

    def send_method(self, method_frame, content=None, wait=True):
        """Shortcut wrapper to send a method through our connection, passing in
        our channel number.

        :param pika.frame.Method method_frame: The method frame to send
        :param str content: The content to send
        :param bool wait: Wait for a response

        """
        self.wait = wait
        self._received_response = False
        LOGGER.debug('Connection: %r', self.connection)
        self.connection.send_method(self.channel_number, method_frame, content)
        while self.connection.outbound_buffer.size > 0:
            try:
                self.connection.process_data_events()
            except exceptions.AMQPConnectionError:
                break
        while wait and not self._received_response:
            try:
                self.connection.process_data_events()
            except exceptions.AMQPConnectionError:
                break

    def _process_replies(self, replies, callback_method):
        """Process replies from RabbitMQ, looking in the stack of callback
        replies for a match. Will optionally call callback_method prior to
        returning the frame_value.

        :param list replies: The reply handles to iterate
        :param method callback_method: The method to optionally call
        :rtype: pika.frame.Frame

        """
        for reply in self._replies:
            if reply in replies:
                frame_value = self._frames[reply]
                self._received_response = True
                if callback_method:
                    callback_method(frame_value)
                del(self._frames[reply])
                return frame_value

    def _validate_acceptable_replies(self, acceptable_replies):
        """Validate the list of acceptable replies

        :param acceptable_replies:
        :raises: TypeError

        """
        if acceptable_replies and not isinstance(acceptable_replies, list):
            raise TypeError("acceptable_replies should be list or None, is %s",
                            type(acceptable_replies))

    def _validate_callback_method(self, callback_method):
        """Validate the value passed in is a method or function.

        :param method callback_method callback_method: The method to validate
        :raises: TypeError

        """
        if (callback_method is not None and
            not utils.is_callable(callback_method)):
            raise TypeError("Callback should be a function or method, is %s",
                            type(callback_method))

    def _wait_on_response(self, method_frame):
        """Returns True if the rpc call should wait on a response.

        :param pika.frame.Method method_frame: The frame to check

        """
        return method_frame.NAME not in self.no_response_frame


class BlockingChannel(channel.Channel):
    """The BlockingChannel class implements a blocking layer on top of the
    Channel class.

    """
    def __init__(self, connection, channel_number, transport=None):
        """Create a new instance of the Channel

        :param BlockingConnection connection: The connection
        :param int channel_number: The channel number for this instance
        :param BlockingChannelTransport transport: A ChannelTransport instance

        """
        # These callbacks need to be added prior to calling the parent
        self._add_transport_callbacks(connection, channel_number, transport)
        LOGGER.debug('Calling super')
        super(BlockingChannel, self).__init__(connection, channel_number, None,
                                              transport)
        self.basic_get_ = channel.Channel.basic_get
        self._consumers = {}
        LOGGER.debug('Calling open')
        self.open()

    def basic_get(self, ticket=0, queue=None, no_ack=False):
        """Get a single message from the AMQP broker. The response will include
        either a single method frame of Basic.GetEmpty or three frames:
        the method frame (Basic.GetOk), header frame and
        the body, like the reponse from Basic.Consume.  For more information
        on basic_get and its parameters, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get
        """
        self._response = None
        super(BlockingChannel, self).basic_get(self.on_basic_get, ticket,
                                               queue, no_ack)
        while not self._response:
            self.transport.connection.process_data_events()
        return self._response[0], self._response[1], self._response[2]

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False):
        """
        Publish to the channel with the given exchange, routing key and body.

        If flow control is enabled and you publish a message while another is
        sending, a ContentTransmissionForbidden exception ill be generated.
        """
        # If properties are not passed in, use the spec's default
        properties = properties or spec.BasicProperties()
        self.transport.send_method(spec.Basic.Publish(exchange=exchange,
                                                      routing_key=routing_key,
                                                      mandatory=mandatory),
                                   (properties, body), False)

    def on_basic_get(self, caller, method_frame, header_frame, body):
        self.transport._received_response = True
        self._response = method_frame, header_frame, body

    def on_basic_get_empty(self, frame):
        self.transport._received_response = True
        self._response = frame.method, None, None

    def on_openok(self, method_frame):
        """Open the channel by sending the RPC command and remove the reply
        from the transport.

        """
        super(BlockingChannel, self).on_openok(method_frame)
        self.transport.remove_reply(method_frame)

    def on_remote_close(self, method_frame):
        super(BlockingChannel, self).on_remote_close(method_frame)
        if self.transport.connection.is_open:
            raise exceptions.AMQPChannelError(method_frame.method.reply_code,
                                              method_frame.method.reply_text)

    def start_consuming(self):
        """
        Starts consuming from registered callbacks.
        """
        # Block while we have registered consumers
        while len(self._consumers):
            self.transport.connection.process_data_events()

    def stop_consuming(self, consumer_tag=None):
        """Sends off the Basic.Cancel to let RabbitMQ know to stop consuming and
        sets our internal state to exit out of the basic_consume.


        """
        LOGGER.debug('Stopping the consumption of the queues')
        if consumer_tag:
            self.basic_cancel(consumer_tag)
        else:
            for consumer_tag in self._consumers.keys():
                self.basic_cancel(consumer_tag)
        self.transport.wait = True

    def _add_transport_callbacks(self, connection, channel_number, transport):
        """Add callbacks for when the channel opens and closes.

        :param BlockingConnection connection: The connection
        :param int channel_number: The channel number for this instance
        :param BlockingChannelTransport transport: The channel transport

        """
        connection.callbacks.add(channel_number,
                                 spec.Channel.OpenOk,
                                 transport.on_rpc_complete)
        connection.callbacks.add(channel_number,
                                 spec.Channel.CloseOk,
                                 transport.on_rpc_complete)

    def _close(self):
        """Handle Channel.Close as a blocking RPC call"""
        self.callbacks.remove(str(self.channel_number), spec.Channel.CloseOk)
        self.transport.rpc(spec.Channel.Close(self.closing[0],
                                              self.closing[1],
                                              0, 0),
                           None,
                           [spec.Channel.CloseOk])
