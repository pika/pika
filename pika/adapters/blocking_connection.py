"""The blocking connection adapter module implements blocking semantics on top
of Pika's core AMQP driver. While most of the asynchronous expectations are
removed when using the blocking connection adapter, it attempts to remain true
to the asynchronous RPC nature of the AMQP protocol, supporting server sent
RPC commands.

The user facing classes in the module consist of the
:py:class:`~pika.adapters.blocking_connection.BlockingConnection`
and the :class:`~pika.adapters.blocking_connection.BlockingChannel`
classes.

"""
import logging
import select
import socket
import time
import warnings

from pika import callback
from pika import channel
from pika import exceptions
from pika import spec
from pika import utils
from pika.adapters import base_connection

LOGGER = logging.getLogger(__name__)


class ReadPoller(object):
    """A poller that will check to see if data is ready on the socket using
    very short timeouts to avoid having to read on the socket, speeding up the
    BlockingConnection._handle_read() method.

    """
    POLL_TIMEOUT = 10

    def __init__(self, fd, poll_timeout=POLL_TIMEOUT):
        """Create a new instance of the ReadPoller which wraps poll and select
        to determine if the socket has data to read on it.

        :param int fd: The file descriptor for the socket
        :param float poll_timeout: How long to wait for events (milliseconds)

        """
        self.fd = fd
        self.poll_timeout = poll_timeout
        if hasattr(select, 'poll'):
            self.poller = select.poll()
            self.poll_events = select.POLLIN | select.POLLPRI
            self.poller.register(self.fd, self.poll_events)
        else:
            self.poller = None
            self.poll_timeout = float(poll_timeout) / 1000

    def ready(self):
        """Check to see if the socket has data to read.

        :rtype: bool

        """
        if self.poller:
            events = self.poller.poll(self.poll_timeout)
            return True if events else False
        else:
            ready, unused_wri, unused_err = select.select([self.fd], [], [],
                                                          self.poll_timeout)
            return len(ready) > 0


class BlockingConnection(base_connection.BaseConnection):
    """The BlockingConnection creates a layer on top of Pika's asynchronous core
    providing methods that will block until their expected response has returned.
    Due to the asynchronous nature of the `Basic.Deliver` and `Basic.Return` calls
    from RabbitMQ to your application, you can still implement
    continuation-passing style asynchronous methods if you'd like to receive
    messages from RabbitMQ using
    :meth:`basic_consume <BlockingChannel.basic_consume>` or if you want  to be
    notified of a delivery failure when using
    :meth:`basic_publish <BlockingChannel.basic_publish>` .

    `Basic.Get` is a blocking call which will either return the Method Frame,
    Header Frame and Body of a message, or it will return a `Basic.GetEmpty`
    frame as the method frame.

    For more information about communicating with the blocking_connection
    adapter, be sure to check out the
    :class:`BlockingChannel <BlockingChannel>` class which implements the
    :class:`Channel <pika.channel.Channel>` based communication for the
    blocking_connection adapter.

    """
    WRITE_TO_READ_RATIO = 10
    DO_HANDSHAKE = True
    SLEEP_DURATION = 0.1
    SOCKET_CONNECT_TIMEOUT = 0.25
    SOCKET_TIMEOUT_THRESHOLD = 12
    SOCKET_TIMEOUT_CLOSE_THRESHOLD = 3
    SOCKET_TIMEOUT_MESSAGE = "Timeout exceeded, disconnected"

    def __init__(self, parameters=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :raises: RuntimeError

        """
        super(BlockingConnection, self).__init__(parameters, None, False)

    def add_on_close_callback(self, callback_method_unused):
        """This is not supported in BlockingConnection. When a connection is
        closed in BlockingConnection, a pika.exceptions.ConnectionClosed
        exception will raised instead.

        :param method callback_method_unused: Unused
        :raises: NotImplementedError

        """
        raise NotImplementedError('Blocking connection will raise '
                                  'ConnectionClosed exception')

    def add_on_open_callback(self, callback_method_unused):
        """This method is not supported in BlockingConnection.

        :param method callback_method_unused: Unused
        :raises: NotImplementedError

        """
        raise NotImplementedError('Connection callbacks not supported in '
                                  'BlockingConnection')

    def add_on_open_error_callback(self, callback_method_unused,
                                   remove_default=False):
        """This method is not supported in BlockingConnection.

        A pika.exceptions.AMQPConnectionError will be raised instead.

        :param method callback_method_unused: Unused
        :raises: NotImplementedError

        """
        raise NotImplementedError('Connection callbacks not supported in '
                                  'BlockingConnection')

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """

        value = {'deadline': time.time() + deadline,
                 'callback': callback_method}
        timeout_id = hash(frozenset(value.items()))
        self._timeouts[timeout_id] = value
        return timeout_id

    def channel(self, channel_number=None):
        """Create a new channel with the next available or specified channel #.

        :param int channel_number: Specify the channel number

        """
        self._channel_open = False
        if not channel_number:
            channel_number = self._next_channel_number()
        LOGGER.debug('Opening channel %i', channel_number)
        self._channels[channel_number] = BlockingChannel(self, channel_number)
        return self._channels[channel_number]

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        LOGGER.info("Closing connection (%s): %s", reply_code, reply_text)
        self._set_connection_state(self.CONNECTION_CLOSING)
        self._remove_connection_callbacks()
        if self._has_open_channels:
            self._close_channels(reply_code, reply_text)
        while self._has_open_channels:
            self.process_data_events()
        self._send_connection_close(reply_code, reply_text)
        while self.is_closing:
            self.process_data_events()
        if self.heartbeat:
            self.heartbeat.stop()
        self._remove_connection_callbacks()
        self._adapter_disconnect()

    def connect(self):
        """Invoke if trying to reconnect to a RabbitMQ server. Constructing the
        Connection object should connect on its own.

        """
        self._set_connection_state(self.CONNECTION_INIT)
        if not self._adapter_connect():
            raise exceptions.AMQPConnectionError('Could not connect')

    def process_data_events(self):
        """Will make sure that data events are processed. Your app can
        block on this method.

        """
        try:
            if self._handle_read():
                self._socket_timeouts = 0
        except AttributeError:
            raise exceptions.ConnectionClosed()
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

    def sleep(self, duration):
        """A safer way to sleep than calling time.sleep() directly which will
        keep the adapter from ignoring frames sent from RabbitMQ. The
        connection will "sleep" or block the number of seconds specified in
        duration in small intervals.

        :param int duration: The time to sleep

        """
        deadline = time.time() + duration
        while time.time() < deadline:
            time.sleep(self.SLEEP_DURATION)
            self.process_data_events()

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker

        :rtype: bool
        :raises: pika.Exceptions.AMQPConnectionError

        """
        # Remove the default behavior for connection errors
        self.callbacks.remove(0, self.ON_CONNECTION_ERROR)
        if not super(BlockingConnection, self)._adapter_connect():
            raise exceptions.AMQPConnectionError(1)
        self.socket.settimeout(self.SOCKET_CONNECT_TIMEOUT)
        self._frames_written_without_read = 0
        self._socket_timeouts = 0
        self._timeouts = dict()
        self._read_poller = ReadPoller(self.socket.fileno())
        self._on_connected()
        while not self.is_open:
            self.process_data_events()
        self.socket.settimeout(self.params.socket_timeout)
        self._set_connection_state(self.CONNECTION_OPEN)
        return True

    def _adapter_disconnect(self):
        """Called if the connection is being requested to disconnect."""
        if self.socket:
            self.socket.close()
        self.socket = None
        self._check_state_on_disconnect()
        self._init_connection_state()

    def _call_timeout_method(self, timeout_value):
        """Execute the method that was scheduled to be called.

        :param dict timeout_value: The configuration for the timeout

        """
        LOGGER.debug('Invoking scheduled call of %s', timeout_value['callback'])
        timeout_value['callback']()

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
        self.disconnect()
        self._on_connection_closed(None, True)

    def _handle_read(self):
        """If the ReadPoller says there is data to read, try adn read it in the
        _handle_read of the parent class. Once read, reset the counter that
        keeps track of how many frames have been written since the last read.

        """
        if self._read_poller.ready():
            super(BlockingConnection, self)._handle_read()
            self._frames_written_without_read = 0

    def _handle_timeout(self):
        """Invoked whenever the socket times out"""
        self._socket_timeouts += 1
        threshold = (self.SOCKET_TIMEOUT_THRESHOLD if not self.is_closing else
                     self.SOCKET_TIMEOUT_CLOSE_THRESHOLD)

        LOGGER.debug('Handling timeout %i with a threshold of %i',
                     self._socket_timeouts, threshold)
        if self.is_closing and self._socket_timeouts > threshold:
            if not self.is_closing:
                LOGGER.critical('Closing connection due to timeout')
            self._on_connection_closed(None, True)

    def _flush_outbound(self):
        """Flush the outbound socket buffer."""
        if self.outbound_buffer:
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
        :raises: pika.exceptions.ConnectionClosed

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
            self._channels[channel]._on_close(method_frame)
        self._remove_connection_callbacks()
        if self.closing[0] not in [0, 200]:
            raise exceptions.ConnectionClosed(*self.closing)

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


class BlockingChannel(channel.Channel):
    """The BlockingChannel implements blocking semantics for most things that
    one would use callback-passing-style for with the
    :py:class:`~pika.channel.Channel` class. In addition,
    the `BlockingChannel` class implements a :term:`generator` that allows you
    to :doc:`consume messages </examples/blocking_consumer_generator>` without
    using callbacks.

    Example of creating a BlockingChannel::

        import pika

        # Create our connection object
        connection = pika.BlockingConnection()

        # The returned object will be a blocking channel
        channel = connection.channel()

    :param BlockingConnection connection: The connection
    :param int channel_number: The channel number for this instance

    """
    NO_RESPONSE_FRAMES = ['Basic.Ack', 'Basic.Reject', 'Basic.RecoverAsync']

    def __init__(self, connection, channel_number):
        """Create a new instance of the Channel

        :param BlockingConnection connection: The connection
        :param int channel_number: The channel number for this instance

        """
        super(BlockingChannel, self).__init__(connection, channel_number)
        self.connection = connection
        self._confirmation = False
        self._force_data_events_override = None
        self._generator = None
        self._generator_messages = list()
        self._frames = dict()
        self._replies = list()
        self._wait = False
        self.open()

    def basic_cancel(self, consumer_tag='', nowait=False):
        """This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply. It may also be sent from the server to the client in
        the event of the consumer being unexpectedly cancelled (i.e. cancelled
        for any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of
        the loss of consumers due to events such as queue deletion.

        :param str consumer_tag: Identifier for the consumer
        :param bool nowait: Do not expect a Basic.CancelOk response

        """
        if consumer_tag not in self._consumers:
            return
        self._cancelled.append(consumer_tag)
        replies = [(spec.Basic.CancelOk,
                   {'consumer_tag': consumer_tag})] if nowait is False else []
        self._rpc(spec.Basic.Cancel(consumer_tag=consumer_tag,
                                             nowait=nowait),
                  self._on_cancelok, replies)

    def basic_get(self, queue=None, no_ack=False):
        """Get a single message from the AMQP broker. The callback method
        signature should have 3 parameters: The method frame, header frame and
        the body, like the consumer callback for Basic.Consume.

        :param queue: The queue to get a message from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a reply
        :rtype: (None, None, None)|(spec.Basic.Get,
                                    spec.Basic.Properties,
                                    str or unicode)

        """
        self._response = None
        self._send_method(spec.Basic.Get(queue=queue,
                                         no_ack=no_ack))
        while not self._response:
            self.connection.process_data_events()
        if isinstance(self._response[0], spec.Basic.GetEmpty):
            return None, None, None
        return self._response[0], self._response[1], self._response[2]

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """Publish to the channel with the given exchange, routing key and body.
        For more information on basic_publish and what the parameters do, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body
        :type body: str or unicode
        :param pika.spec.Properties properties: Basic.properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        """
        if not self.is_open:
            raise exceptions.ChannelClosed()
        if immediate:
            LOGGER.warning('The immediate flag is deprecated in RabbitMQ')
        properties = properties or spec.BasicProperties()

        if mandatory:
            self._response = None

        if isinstance(body, unicode):
            body = body.encode('utf-8')

        if self._confirmation:
            response = self._rpc(spec.Basic.Publish(exchange=exchange,
                                                    routing_key=routing_key,
                                                    mandatory=mandatory,
                                                    immediate=immediate),
                                 None,
                                 [spec.Basic.Ack,
                                  spec.Basic.Nack],
                                 (properties, body))
            if mandatory and self._response:
                response = self._response[0]
                LOGGER.warning('Message was returned (%s): %s',
                               response.reply_code,
                               response.reply_text)
                return False

            if isinstance(response.method, spec.Basic.Ack):
                return True
            elif isinstance(response.method, spec.Basic.Nack):
                return False
            else:
                raise ValueError('Unexpected frame type: %r', response)
        else:
            self._send_method(spec.Basic.Publish(exchange=exchange,
                                                 routing_key=routing_key,
                                                 mandatory=mandatory,
                                                 immediate=immediate),
                              (properties, body), False)
            if mandatory:
                if self._response:
                    response = self._response[0]
                    LOGGER.warning('Message was returned (%s): %s',
                                   response.reply_code,
                                   response.reply_text)
                    return False
                return True

    def basic_qos(self, prefetch_size=0, prefetch_count=0, all_channels=False):
        """Specify quality of service. This method requests a specific quality
        of service. The QoS can be specified for the current channel or for all
        channels on the connection. The client can request that messages be sent
        in advance so that when the client finishes processing a message, the
        following message is already held locally, rather than needing to be
        sent down the channel. Prefetching gives a performance improvement.

        :param int prefetch_size:  This field specifies the prefetch window
                                   size. The server will send a message in
                                   advance if it is equal to or smaller in size
                                   than the available prefetch size (and also
                                   falls into other prefetch limits). May be set
                                   to zero, meaning "no specific limit",
                                   although other prefetch limits may still
                                   apply. The prefetch-size is ignored if the
                                   no-ack option is set.
        :param int prefetch_count: Specifies a prefetch window in terms of whole
                                   messages. This field may be used in
                                   combination with the prefetch-size field; a
                                   message will only be sent in advance if both
                                   prefetch windows (and those at the channel
                                   and connection level) allow it. The
                                   prefetch-count is ignored if the no-ack
                                   option is set.
        :param bool all_channels: Should the QoS apply to all channels

        """
        self._rpc(spec.Basic.Qos(prefetch_size, prefetch_count, all_channels),
                  None, [spec.Basic.QosOk])

    def basic_recover(self, requeue=False):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered. This
        method replaces the asynchronous Recover.

        :param bool requeue: If False, the message will be redelivered to the
                             original recipient. If True, the server will
                             attempt to requeue the message, potentially then
                             delivering it to an alternative subscriber.

        """
        self._rpc(spec.Basic.Recover(requeue), None, [spec.Basic.RecoverOk])

    def confirm_delivery(self, nowait=False):
        """Turn on Confirm mode in the channel.

        For more information see:
            http://www.rabbitmq.com/extensions.html#confirms

        :param bool nowait: Do not send a reply frame (Confirm.SelectOk)

        """
        if (not self.connection.publisher_confirms or
            not self.connection.basic_nack):
            raise exceptions.MethodNotImplemented('Not Supported on Server')
        self._confirmation = True
        replies = [spec.Confirm.SelectOk] if nowait is False else []
        self._rpc(spec.Confirm.Select(nowait), None, replies)
        self.connection.process_data_events()

    def cancel(self):
        """Cancel the consumption of a queue, rejecting all pending messages.
        This should only work with the generator based BlockingChannel.consume
        method. If you're looking to cancel a consumer issues with
        BlockingChannel.basic_consume then you should call
        BlockingChannel.basic_cancel.

        :return int: The number of messages requeued by Basic.Nack

        """
        messages = 0
        self.basic_cancel(self._generator)
        if self._generator_messages:
            # Get the last item
            (method, properties, body) = self._generator_messages.pop()
            messages = len(self._generator_messages)
            LOGGER.info('Requeueing %i messages with delivery tag %s',
                        messages, method.delivery_tag)
            self.basic_nack(method.delivery_tag, multiple=True, requeue=True)
            self.connection.process_data_events()
        self._generator = None
        self._generator_messages = list()
        return messages

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """

        LOGGER.info('Channel.close(%s, %s)', reply_code, reply_text)
        if not self.is_open:
            raise exceptions.ChannelClosed()

        # Cancel the generator if it's running
        if self._generator:
            self.cancel()

        # If there are any consumers, cancel them as well
        if self._consumers:
            LOGGER.debug('Cancelling %i consumers', len(self._consumers))
            for consumer_tag in self._consumers.keys():
                self.basic_cancel(consumer_tag=consumer_tag)
        self._set_state(self.CLOSING)
        self._rpc(spec.Channel.Close(reply_code, reply_text, 0, 0),
                  None,
                  [spec.Channel.CloseOk])
        self._set_state(self.CLOSED)
        self._cleanup()

    def consume(self, queue):
        """Blocking consumption of a queue instead of via a callback. This
        method is a generator that returns messages a tuple of method,
        properties, and body.

        Example:

            for method, properties, body in channel.consume('queue'):
                print body
                channel.basic_ack(method.delivery_tag)

        You should call BlockingChannel.cancel() when you escape out of the
        generator loop. Also note this turns on forced data events to make
        sure that any acked messages actually get acked.

        :param queue: The queue name to consume
        :type queue: str or unicode
        :rtype: tuple(spec.Basic.Deliver, spec.BasicProperties, str or unicode)

        """
        LOGGER.debug('Forcing data events on')
        if not self._generator:
            LOGGER.debug('Issuing Basic.Consume')
            self._generator = self.basic_consume(self._generator_callback,
                                                 queue)
        while True:
            if self._generator_messages:
                yield self._generator_messages.pop(0)
            self.connection.process_data_events()

    def force_data_events(self, enable):
        """Turn on and off forcing the blocking adapter to stop and look to see
        if there are any frames from RabbitMQ in the read buffer. By default
        the BlockingChannel will check for a read after every RPC command which
        can cause performance to degrade in scenarios where you do not care if
        RabbitMQ is trying to send RPC commands to your client connection.

        Examples of RPC commands of this sort are:

        - Heartbeats
        - Connection.Close
        - Channel.Close
        - Basic.Return
        - Basic.Ack and Basic.Nack when using delivery confirmations

        Turning off forced data events can be a bad thing and prevents your
        client from properly communicating with RabbitMQ. Forced data events
        were added in 0.9.6 to enforce proper channel behavior when
        communicating with RabbitMQ.

        Note that the BlockingConnection also has the constant
        WRITE_TO_READ_RATIO which forces the connection to stop and try and
        read after writing the number of frames specified in the constant.
        This is a way to force the client to received these types of frames
        in a very publish/write IO heavy workload.

        :param bool enable: Set to False to disable

        """
        self._force_data_events_override = enable

    def exchange_bind(self, destination=None, source=None, routing_key='',
                      nowait=False, arguments=None):
        """Bind an exchange to another exchange.

        :param destination: The destination exchange to bind
        :type destination: str or unicode
        :param source: The source exchange to bind to
        :type source: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for an Exchange.BindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        replies = [spec.Exchange.BindOk] if nowait is False else []
        return self._rpc(spec.Exchange.Bind(0, destination, source,
                                            routing_key, nowait,
                                            arguments or dict()), None, replies)

    def exchange_declare(self, exchange=None,
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False, nowait=False,
                         arguments=None, type=None):
        """This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        If passive set, the server will reply with Declare-Ok if the exchange
        already exists with the same name, and raise an error if not and if the
        exchange does not already exist, the server MUST raise a channel
        exception with reply code 404 (not found).

        :param exchange: The exchange name consists of a non-empty sequence of
                          these characters: letters, digits, hyphen, underscore,
                          period, or colon.
        :type exchange: str or unicode
        :param str exchange_type: The exchange type to use
        :param bool passive: Perform a declare or just check to see if it exists
        :param bool durable: Survive a reboot of RabbitMQ
        :param bool auto_delete: Remove when no more queues are bound to it
        :param bool internal: Can only be published to by other exchanges
        :param bool nowait: Do not expect an Exchange.DeclareOk response
        :param dict arguments: Custom key/value pair arguments for the exchange
        :param str type: The deprecated exchange type parameter

        """
        if type is not None:
            warnings.warn('type is deprecated, use exchange_type instead',
                          DeprecationWarning)
            if exchange_type == 'direct' and type != exchange_type:
                exchange_type = type
        replies = [spec.Exchange.DeclareOk] if nowait is False else []
        return self._rpc(spec.Exchange.Declare(0, exchange, exchange_type,
                                               passive, durable, auto_delete,
                                               internal, nowait,
                                               arguments or dict()),
                         None, replies)

    def exchange_delete(self, exchange=None, if_unused=False, nowait=False):
        """Delete the exchange.

        :param exchange: The exchange name
        :type exchange: str or unicode
        :param bool if_unused: only delete if the exchange is unused
        :param bool nowait: Do not wait for an Exchange.DeleteOk

        """
        replies = [spec.Exchange.DeleteOk] if nowait is False else []
        return self._rpc(spec.Exchange.Delete(0, exchange, if_unused, nowait),
                         None, replies)

    def exchange_unbind(self, destination=None, source=None, routing_key='',
                        nowait=False, arguments=None):
        """Unbind an exchange from another exchange.

        :param destination: The destination exchange to unbind
        :type destination: str or unicode
        :param source: The source exchange to unbind from
        :type source: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for an Exchange.UnbindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        replies = [spec.Exchange.UnbindOk] if nowait is False else []
        return self._rpc(spec.Exchange.Unbind(0, destination, source,
                                              routing_key, nowait, arguments),
                         None, replies)

    def open(self):
        """Open the channel"""
        self._set_state(self.OPENING)
        self._add_callbacks()
        self._rpc(spec.Channel.Open(), self._on_openok, [spec.Channel.OpenOk])

    def queue_bind(self, queue, exchange, routing_key=None, nowait=False,
                   arguments=None):
        """Bind the queue to the specified exchange

        :param queue: The queue to bind to the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for a Queue.BindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        replies = [spec.Queue.BindOk] if nowait is False else []
        if routing_key is None:
            routing_key = queue
        return self._rpc(spec.Queue.Bind(0, queue, exchange, routing_key,
                                         nowait, arguments or dict()),
                         None, replies)

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        """Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Leave the queue name empty for a auto-named queue in RabbitMQ

        :param queue: The queue name
        :type queue: str or unicode
        :param bool passive: Only check to see if the queue exists
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param bool nowait: Do not wait for a Queue.DeclareOk
        :param dict arguments: Custom key/value arguments for the queue

        """
        condition = (spec.Queue.DeclareOk,
                     {'queue': queue}) if queue else spec.Queue.DeclareOk
        replies = [condition] if nowait is False else []
        return self._rpc(spec.Queue.Declare(0, queue, passive, durable,
                                            exclusive, auto_delete, nowait,
                                            arguments or dict()),
                         None, replies)

    def queue_delete(self, queue='', if_unused=False, if_empty=False,
                     nowait=False):
        """Delete a queue from the broker.

        :param queue: The queue to delete
        :type queue: str or unicode
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty
        :param bool nowait: Do not wait for a Queue.DeleteOk

        """
        replies = [spec.Queue.DeleteOk] if nowait is False else []
        return self._rpc(spec.Queue.Delete(0, queue, if_unused, if_empty,
                                           nowait), None, replies)

    def queue_purge(self, queue='', nowait=False):
        """Purge all of the messages from the specified queue

        :param queue: The queue to purge
        :type  queue: str or unicode
        :param bool nowait: Do not expect a Queue.PurgeOk response

        """
        replies = [spec.Queue.PurgeOk] if nowait is False else []
        return self._rpc(spec.Queue.Purge(0, queue, nowait), None, replies)

    def queue_unbind(self, queue='', exchange=None, routing_key=None,
                     arguments=None):
        """Unbind a queue from an exchange.

        :param queue: The queue to unbind from the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind from
        :type exchange: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        if routing_key is None:
            routing_key = queue
        return self._rpc(spec.Queue.Unbind(0, queue, exchange, routing_key,
                                           arguments or dict()), None,
                         [spec.Queue.UnbindOk])

    def start_consuming(self):
        """Starts consuming from registered callbacks."""
        while len(self._consumers):
            self.connection.process_data_events()

    def stop_consuming(self, consumer_tag=None):
        """Sends off the Basic.Cancel to let RabbitMQ know to stop consuming and
        sets our internal state to exit out of the basic_consume.

        """
        if consumer_tag:
            self.basic_cancel(consumer_tag)
        else:
            for consumer_tag in self._consumers.keys():
                self.basic_cancel(consumer_tag)
        self.wait = True

    def tx_commit(self):
        """Commit a transaction."""
        return self._rpc(spec.Tx.Commit(), None, [spec.Tx.CommitOk])

    def tx_rollback(self):
        """Rollback a transaction."""
        return self._rpc(spec.Tx.Rollback(), None, [spec.Tx.RollbackOk])

    def tx_select(self):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        """
        return self._rpc(spec.Tx.Select(), None, [spec.Tx.SelectOk])

    # Internal methods

    def _add_reply(self, reply):
        reply = callback._name_or_value(reply)
        self._replies.append(reply)

    def _add_callbacks(self):
        """Add callbacks for when the channel opens and closes."""
        self.connection.callbacks.add(self.channel_number,
                                      spec.Channel.Close,
                                      self._on_close)
        self.callbacks.add(self.channel_number,
                           spec.Basic.GetEmpty,
                           self._on_getempty,
                           False)
        self.callbacks.add(self.channel_number,
                           spec.Basic.Cancel,
                           self._on_cancel,
                           False)
        self.connection.callbacks.add(self.channel_number,
                                      spec.Channel.CloseOk,
                                      self._on_rpc_complete)

    def _generator_callback(self, unused, method, properties, body):
        """Called when a message is received from RabbitMQ and appended to the
        list of messages to be returned when a message is received by RabbitMQ.

        :param pika.spec.Basic.Deliver: The method frame received
        :param pika.spec.BasicProperties: The  message properties
        :param body: The body received
        :type body: str or unicode

        """
        self._generator_messages.append((method, properties, body))
        LOGGER.debug('%i pending messages', len(self._generator_messages))

    def _on_cancel(self, method_frame):
        """Raises a ConsumerCanceled exception after processing the frame


        :param pika.frame.Method method_frame: The method frame received

        """
        super(BlockingChannel, self)._on_cancel(method_frame)
        raise exceptions.ConsumerCancelled(method_frame.method)

    def _on_getok(self, method_frame, header_frame, body):
        """Called in reply to a Basic.Get when there is a message.

        :param pika.frame.Method method_frame: The method frame received
        :param pika.frame.Header header_frame: The header frame received
        :param body: The body received
        :type body: str or unicode

        """
        self._received_response = True
        self._response = method_frame.method, header_frame.properties, body

    def _on_getempty(self, frame):
        self._received_response = True
        self._response = frame.method, None, None

    def _on_close(self, method_frame):
        LOGGER.warning('Received Channel.Close, closing: %r', method_frame)
        if not self.connection.is_closed:
            self._send_method(spec.Channel.CloseOk(), None, False)
        self._set_state(self.CLOSED)
        self._cleanup()
        self._generator_messages = list()
        self._generator = None
        if method_frame is None:
            raise exceptions.ChannelClosed(0, 'Not specified')
        else:
            raise exceptions.ChannelClosed(method_frame.method.reply_code,
                                           method_frame.method.reply_text)

    def _on_openok(self, method_frame):
        """Open the channel by sending the RPC command and remove the reply
        from the stack of replies.

        """
        super(BlockingChannel, self)._on_openok(method_frame)
        self._remove_reply(method_frame)

    def _on_return(self, method_frame, header_frame, body):
        """Called when a Basic.Return is received from publishing

        :param pika.frame.Method method_frame: The method frame received
        :param pika.frame.Header header_frame: The header frame received
        :param body: The body received
        :type body: str or unicode

        """
        self._received_response = True
        self._response = method_frame.method, header_frame.properties, body

    def _on_rpc_complete(self, frame):
        key = callback._name_or_value(frame)
        self._replies.append(key)
        self._frames[key] = frame
        self._received_response = True

    def _process_replies(self, replies, callback):
        """Process replies from RabbitMQ, looking in the stack of callback
        replies for a match. Will optionally call callback prior to
        returning the frame_value.

        :param list replies: The reply handles to iterate
        :param method callback: The method to optionally call
        :rtype: pika.frame.Frame

        """
        for reply in self._replies:
            if reply in replies:
                frame_value = self._frames[reply]
                self._received_response = True
                if callback:
                    callback(frame_value)
                del(self._frames[reply])
                return frame_value

    def _remove_reply(self, frame):
        key = callback._name_or_value(frame)
        if key in self._replies:
            self._replies.remove(key)

    def _rpc(self, method_frame, callback=None, acceptable_replies=None,
             content=None, force_data_events=True):
        """Make an RPC call for the given callback, channel number and method.
        acceptable_replies lists out what responses we'll process from the
        server with the specified callback.

        :param pika.amqp_object.Method method_frame: The method frame to call
        :param method callback: The callback for the RPC response
        :param list acceptable_replies: The replies this RPC call expects
        :param tuple content: Properties and Body for content frames
        :param bool force_data_events: Call process data events before reply
        :rtype: pika.frame.Method

        """
        if self.is_closed:
            raise exceptions.ChannelClosed
        self._validate_acceptable_replies(acceptable_replies)
        self._validate_callback(callback)
        replies = list()
        for reply in acceptable_replies or list():
            if isinstance(reply, tuple):
                reply, arguments = reply
            else:
                arguments = None
            prefix, key = self.callbacks.add(self.channel_number,
                                             reply,
                                             self._on_rpc_complete,
                                             arguments=arguments)
            replies.append(key)
        self._received_response = False
        self._send_method(method_frame, content,
                          self._wait_on_response(method_frame))
        if force_data_events and self._force_data_events_override is not False:
            self.connection.process_data_events()
        return self._process_replies(replies, callback)

    def _send_method(self, method_frame, content=None, wait=False):
        """Shortcut wrapper to send a method through our connection, passing in
        our channel number.

        :param pika.amqp_object.Method method_frame: The method frame to send
        :param content: The content to send
        :type content: tuple
        :param bool wait: Wait for a response

        """
        self.wait = wait
        self._received_response = False
        self.connection.send_method(self.channel_number, method_frame, content)
        while wait and not self._received_response:
            try:
                self.connection.process_data_events()
            except exceptions.AMQPConnectionError:
                break

    def _validate_acceptable_replies(self, acceptable_replies):
        """Validate the list of acceptable replies

        :param acceptable_replies:
        :raises: TypeError

        """
        if acceptable_replies and not isinstance(acceptable_replies, list):
            raise TypeError("acceptable_replies should be list or None, is %s",
                            type(acceptable_replies))

    def _validate_callback(self, callback):
        """Validate the value passed in is a method or function.

        :param method callback callback: The method to validate
        :raises: TypeError

        """
        if (callback is not None and
            not utils.is_callable(callback)):
            raise TypeError("Callback should be a function or method, is %s",
                            type(callback))

    def _wait_on_response(self, method_frame):
        """Returns True if the rpc call should wait on a response.

        :param pika.frame.Method method_frame: The frame to check

        """
        return method_frame.NAME not in self.NO_RESPONSE_FRAMES
