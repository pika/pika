"""BlockingConnection adapter implemented as a wrapper around SelectConnection.
All public API calls complete synchronously without callbacks.
"""

from collections import namedtuple, deque
import logging
import time

import pika.channel
from pika import exceptions
import pika.spec
# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters.select_connection import SelectConnection

LOGGER = logging.getLogger(__name__)


class _CallbackResult(object):
    """ CallbackResult is a non-thread-safe implementation for receiving
    callback results; INTERNAL USE ONLY!
    """
    def __init__(self, value_class=None):
        """
        :param callable value_class: only needed if the CallbackResult
                                     instance will be used with
                                     `set_value_once` and `append_element`.
                                     *args and **kwargs of the value setter
                                     methods will be passed to this class.

        """
        self._value_class = value_class
        self.reset()

    def reset(self):
        self._ready = False
        self._values = None

    def __bool__(self):
        """ Called by python runtime to implement truth value testing and the
        built-in operation bool(); NOTE: python 3.x
        """
        return self.is_ready()

    # python 2.x version of __bool__
    __nonzero__ = __bool__

    def __enter__(self):
        """ Entry into context manager that automatically resets the object
        on exit; this usage pattern helps garbage-collection by eliminating
        potential circular references.
        """
        return self

    def __exit__(self, *args, **kwargs):
        self.reset()

    def is_ready(self):
        return self._ready

    @property
    def ready(self):
        return self._ready

    def signal_once(self, *_args, **_kwargs):
        """ Set as ready

        :raises AssertionError: if result was already signalled
        """
        assert not self._ready, '_CallbackResult was already set'
        self._ready = True

    def set_value_once(self, *args, **kwargs):
        """ Set as ready with value; the value may be retrived via the `value`
        property getter

        :raises AssertionError: if result was already set
        """
        self.signal_once()
        try:
            self._values = (self._value_class(*args, **kwargs),)
        except Exception as e:
            LOGGER.error(
                "set_value_once failed: value_class=%r; args=%r; kwargs=%r",
                self._value_class, args, kwargs)
            raise

    def append_element(self, *args, **kwargs):
        """
        """
        assert not self._ready or isinstance(self._values, list), (
            '_CallbackResult state is incompatible with append_element: '
            'ready=%r; values=%r' % (self._ready, self._values))

        try:
            value = self._value_class(*args, **kwargs)
        except Exception as e:
            LOGGER.error(
                "append_element failed: value_class=%r; args=%r; kwargs=%r",
                self._value_class, args, kwargs)
            raise

        if self._values is None:
            self._values = [value]
        else:
            self._values.append(value)

        self._ready = True


    @property
    def value(self):
        """
        :returns: a reference to the value that was set via `set_value_once`
        :raises AssertionError: if result was not set or value is incompatible
                                with `set_value_once`
        """
        assert self._ready, '_CallbackResult was not set'
        assert isinstance(self._values, tuple) and len(self._values) == 1, (
            '_CallbackResult value is incompatible with set_value_once: %r'
            % (self._values,))

        return self._values[0]


    @property
    def elements(self):
        """
        :returns: a reference to the list containing one or more elements that
            were added via `append_element`
        :raises AssertionError: if result was not set or value is incompatible
                                with `append_element`
        """
        assert self._ready, '_CallbackResult was not set'
        assert isinstance(self._values, list) and len(self._values) > 0, (
            '_CallbackResult value is incompatible with append_element: %r'
            % (self._values,))

        return self._values


class BlockingConnection(object):
    """
    TODO flesh out docstring

    """
    # Connection-opened callback args
    _OnOpenedArgs = namedtuple('BlockingConnection__OnOpenedArgs',
                               'connection')

    # Connection-establishment error callback args
    _OnOpenErrorArgs = namedtuple('BlockingConnection__OnOpenErrorArgs',
                                  'connection error_text')

    # Connection-closing callback args
    _OnClosedArgs = namedtuple('BlockingConnection__OnClosedArgs',
                               'connection reason_code reason_text')

    # Channel-opened callback args
    _OnChannelOpenedArgs = namedtuple(
        'BlockingConnection__OnChannelOpenedArgs',
        'channel')

    def __init__(self, parameters=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :raises: RuntimeError

        """
        # Receives on_open_callback args from Connection
        self._opened_result = _CallbackResult(self._OnOpenedArgs)

        # Receives on_open_error_callback args from Connection
        self._open_error_result= _CallbackResult(self._OnOpenErrorArgs)

        # Receives on_close_callback args from Connection
        self._closed_result = _CallbackResult(self._OnClosedArgs)

        # Set to True when when user calls close() on the connection
        # NOTE: this is a workaround to detect socket error because
        # on_close_callback passes reason_code=0 when called due to socket error
        self._user_initiated_close = False

        # TODO verify suitability of stop_ioloop_on_close value
        self._impl = SelectConnection(
            parameters=parameters,
            on_open_callback=self._opened_result.set_value_once,
            on_open_error_callback=self._open_error_result.set_value_once,
            on_close_callback=self._closed_result.set_value_once,
            stop_ioloop_on_close=False)

        self._process_io_for_connection_setup()

    def _clean_up(self):
        """ Perform clean-up that is necessary for re-connecting

        """
        self._opened_result.reset()
        self._closed_result.reset()
        self._open_error_result.reset()
        self._user_initiated_close = False

    def _process_io_for_connection_setup(self):
        """ Perform follow-up processing for connection setup request: flush
        connection output and process input while waiting for connection-open
        or connection-error.

        :raises AMQPConnectionError: on connection open error
        """
        self._flush_output(self._opened_result.is_ready,
                           self._open_error_result.is_ready)

        if self._open_error_result.ready:
            raise exceptions.AMQPConnectionError(
                self._open_error_result.value.error_text)

        assert self._opened_result.ready
        assert self._opened_result.value.connection is self._impl

    def _flush_output(self, *waiters):
        """ Flush output and process input while waiting for any of the given
        callbacks to return true. The wait is aborted upon connection-close.
        Otherwise, processing continues until the output is flushed AND at least
        one of the callbacks returns true. If there are no callbacks, then
        processing ends when all output is flushed.

        :param waiters: sequence of zero or more callables taking no args and
                        returning true when it's time to stop processing.
                        Their results are OR'ed together.
        """
        if self._impl.is_closed:
            raise exceptions.ConnectionClosed()

        # Conditions for terminating the processing loop:
        #   connection closed
        #         OR
        #   empty outbound buffer and no waiters
        #         OR
        #   empty outbound buffer and any waiter is ready
        is_done = (lambda:
            self._closed_result.ready or
            (not self._impl.outbound_buffer and
             (not waiters or any(ready() for ready in  waiters))))

        # Process I/O until our completion condition is satisified
        while not is_done():
            self._impl.ioloop.poll()
            self._impl.ioloop.process_timeouts()

        if self._closed_result.ready:
            result = self._closed_result.value
            if result.reason_code not in [0, 200]:
                LOGGER.critical('Connection close detected; result=%r', result)
                raise exceptions.ConnectionClosed(*result)
            elif not self._user_initiated_close:
                # NOTE: unfortunately, upon socket error, on_close_callback
                # presently passes reason_code=0, so we don't detect that as an
                # error
                LOGGER.critical('Connection close detected')
                raise exceptions.ConnectionClosed()
            else:
                LOGGER.info('Connection closed; result=%r', result)

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        LOGGER.info('Closing connection (%s): %s', reply_code, reply_text)
        self._user_initiated_close = True
        self._impl.close(reply_code, reply_text)

        self._flush_output(self._closed_result.is_ready)

        assert self._closed_result.ready

    def channel(self, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        :rtype: pika.synchronous_connection.BlockingChannel
        """
        with _CallbackResult(self._OnChannelOpenedArgs) as openedArgs:
            channel = self._impl.channel(
                on_open_callback=openedArgs.set_value_once,
                channel_number=channel_number)

            channel = BlockingChannel(channel, self)
            channel._flush_output(openedArgs.is_ready)

        return channel

    def connect(self):
        """Invoke if trying to reconnect to a RabbitMQ server. Constructing the
        Connection object should connect on its own.

        """
        assert not self._impl.is_open, (
            'Connection was not closed; connection_state=%r'
            % (self._impl.connection_state,))

        self._clean_up()

        self._impl.connect()

        self._process_io_for_connection_setup()

    def sleep(self, duration):
        """A safer way to sleep than calling time.sleep() directly which will
        keep the adapter from ignoring frames sent from RabbitMQ. The
        connection will "sleep" or block the number of seconds specified in
        duration in small intervals.

        :param float duration: The time to sleep in seconds

        """
        assert duration >= 0, duration

        deadline = time.time() + duration
        self._flush_output(lambda: time.time() >= deadline)

    #
    # Connections state properties
    #

    @property
    def is_closed(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_closed

    @property
    def is_closing(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_closing

    @property
    def is_open(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_open

    #
    # Properties that reflect server capabilities for the current connection
    #

    @property
    def basic_nack_supported(self):
        """Specifies if the server supports basic.nack on the active connection.

        :rtype: bool

        """
        return self._impl.basic_nack

    @property
    def consumer_cancel_notify_supported(self):
        """Specifies if the server supports consumer cancel notification on the
        active connection.

        :rtype: bool

        """
        return self._impl.consumer_cancel_notify

    @property
    def exchange_exchange_bindings_supported(self):
        """Specifies if the active connection supports exchange to exchange
        bindings.

        :rtype: bool

        """
        return self._impl.exchange_exchange_bindings

    @property
    def publisher_confirms_supported(self):
        """Specifies if the active connection can use publisher confirmations.

        :rtype: bool

        """
        return self._impl.publisher_confirms


class ConsumerDeliveryEvt(object):
    """Consumer delivery event returned via `BlockingChannel.get_event`;
    contains method, properties, and body of the delivered message.
    """

    __slots__ = ('method', 'properties', 'body')

    def __init__(self, method, properties, body):
        """
        :param spec.Basic.Deliver method: NOTE: consumer_tag and delivery_tag
          are valid only within the current channel
        :param spec.BasicProperties properties: message properties
        :param body: message body; None if no body
        :type body: str or unicode or None
        """
        self.method = method
        self.properties = properties
        self.body = body


class ConsumerCancellationEvt(object):
    """Server-initiated consumer cancellation event returned by
    `BlockingChannel.get_event`. After receiving this event, there will be no
    further deliveries for the consumer identified by `consumer_tag`
    """

    __slots__ = ('consumer_tag')

    def __init__(self, consumer_tag):
        """
        :param str consumer_tag: Identifier for the cancelled consumer, valid
          within the current channel.
        """
        self.consumer_tag = consumer_tag

    def __repr__(self):
        return "%s(consumer_tag=%r)" % (self.__class__.__name__,
                                        self.consumer_tag)


class ReturnedMessage(object):
    """Represents a message returned via Basic.Return"""

    __slots__ = ('method', 'properties', 'body')

    def __init__(self, method, properties, body):
        """
        :param spec.Basic.Return method:
        :param spec.BasicProperties properties: message properties
        :param body: message body; None if no body
        :type body: str or unicode or None
        """
        self.method = method
        self.properties = properties
        self.body = body


class UnroutableError(exceptions.AMQPError):
    """Exception containing one or more unroutable returned messages"""

    def __init__(self, messages):
        """
        :param messages: sequence of returned unroutable messages
        :type messages: sequence of ReturnedMessage objects
        """
        super(UnroutableError, self).__init__(
            "%s unroutable message(s) returned" % (len(messages)))

        self.messages = messages


class NackError(exceptions.AMQPError):
    """This exception is raised when a message published in
    publisher-acknowledgements mode is Nack'ed by the broker
    """

    def __init__(self, messages):
        """
        :param messages: sequence of returned unroutable messages
        :type messages: sequence of ReturnedMessage objects
        """
        super(NackError, self).__init__(
            "%s message(s) NACKed" % (len(messages)))

        self.messages = messages


class BlockingChannel(object):
    """The BlockingChannel implements blocking semantics for most things that
    one would use callback-passing-style for with the
    :py:class:`~pika.channel.Channel` class. In addition,
    the `BlockingChannel` class implements a :term:`generator` that allows
    you to :doc:`consume messages </examples/blocking_consumer_generator>`
    without using callbacks.

    Example of creating a BlockingChannel::

        import pika

        # Create our connection object
        connection = pika.BlockingConnection()

        # The returned object will be a synchronous channel
        channel = connection.channel()

    :param channel_impl: Channel implementation object as returned from
                         SelectConnection.channel()
    :param BlockingConnection connection: The connection object

    TODO fill in missing channel methods see BlockingChannel methods in
    http://pika.readthedocs.org/en/latest/modules/adapters/blocking.html

    """

    # Used for Basic.Deliver, Basic.Cancel, Basic.Return and Basic.GetOk from
    # broker
    #_OnMessageDeliveredArgs = namedtuple(
    _RxMessageArgs = namedtuple(
        'BlockingChannel__RxMessageArgs',
        [
            # implementation Channel instance
            'channel',
            # Basic.Deliver, Basic.Cancel, Basic.Return or Basic.GetOk
            'method',
            # pika.spec.BasicProperties; ignore if Basic.Cancel
            'properties',
            # returned message body; ignore if Basic.Cancel
            'body'
        ])


    # For use by any _CallbackResult that expects method_frame as the only
    # arg
    _MethodFrameCallbackResultArgs = namedtuple(
        'BlockingChannel__MethodFrameCallbackResultArgs',
        'method_frame')

    # Broker's basic-ack/basic-nack when delivery confirmation is enabled;
    # may concern a single or multiple messages
    _OnMessageConfirmationReportArgs = namedtuple(
        'BlockingChannel__OnMessageConfirmationReportArgs',
        'method_frame')

    # Basic.GetEmpty response args
    _OnGetEmptyResponseArgs = namedtuple(
        'BlockingChannel__OnGetEmptyResponseArgs',
        'method_frame')

    # Parameters for broker-inititated Channel.Close request: reply_code
    # holds the broker's non-zero error code and reply_text holds the
    # corresponding error message text.
    _OnChannelClosedByBrokerArgs = namedtuple(
        'BlockingChannel__OnChannelClosedByBrokerArgs',
        'method_frame')


    def __init__(self, channel_impl, connection):
        """Create a new instance of the Channel

        :param channel_impl: Channel implementation object as returned from
                             SelectConnection.channel()
        :param BlockingConnection connection: The connection object

        """
        self._impl = channel_impl
        self._connection = connection

        # Whether RabbitMQ delivery confirmation has been enabled
        self._delivery_confirmation = False

        # Receives message delivery confirmation report (Basic.ack or
        # Basic.nack) from broker when delivery confirmations are enabled
        self._message_confirmation_result = _CallbackResult(
            self._OnMessageConfirmationReportArgs)

        # deque of pending events: ConsumerDeliveryEvt and
        # ConsumerCancellationEvt objects that will be returned by
        # `BlockingChannel.get_event()`
        self._pending_events = deque()

        # Holds ReturnedMessage objects received via Basic.Return
        self._returned_messages = []

        # Receives Basic.ConsumeOk reply from server
        self._basic_consume_ok_result = _CallbackResult()

        # Receives the broker-inititated Channel.Close parameters
        self._channel_closed_by_broker_result = _CallbackResult(
            self._OnChannelClosedByBrokerArgs)

        # Receives args from Basic.GetEmpty response
        #  http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get
        self._basic_getempty_result = _CallbackResult(
            self._OnGetEmptyResponseArgs)

        self._impl.add_on_return_callback(self._on_message_returned)

        self._impl.add_on_cancel_callback(
            lambda method_frame:
                self._pending_events.append(
                    ConsumerCancellationEvt(method_frame.method.consumer_tag))
        )

        self._impl.add_callback(
            self._basic_consume_ok_result.signal_once,
            replies=[pika.spec.Basic.ConsumeOk],
            one_shot=False)

        self._impl.add_callback(
            self._channel_closed_by_broker_result.set_value_once,
            replies=[pika.spec.Channel.Close],
            one_shot=True)

        self._impl.add_callback(
            self._basic_getempty_result.set_value_once,
            replies=[pika.spec.Basic.GetEmpty],
            one_shot=False)

        LOGGER.info("Created channel=%s", self._impl.channel_number)

    @property
    def connection(self):
        return self._connection

    @property
    def is_closed(self):
        """Returns True if the channel is closed.

        :rtype: bool

        """
        return self._impl.is_closed

    @property
    def is_closing(self):
        """Returns True if the channel is closing.

        :rtype: bool

        """
        return self._impl.is_closing

    @property
    def is_open(self):
        """Returns True if the channel is open.

        :rtype: bool

        """
        return self._impl.is_open

    _ALWAYS_READY_WAITERS = ((lambda: True), )

    def _flush_output(self, *waiters):
        """ Flush output and process input while waiting for any of the given
        callbacks to return true. The wait is aborted upon channel-close or
        connection-close.
        Otherwise, processing continues until the output is flushed AND at least
        one of the callbacks returns true. If there are no callbacks, then
        processing ends when all output is flushed.

        :param waiters: sequence of zero or more callables taking no args and
                        returning true when it's time to stop processing.
                        Their results are OR'ed together.
        """
        if self._impl.is_closed:
            raise exceptions.ChannelClosed()

        if not waiters:
            waiters = self._ALWAYS_READY_WAITERS

        self._connection._flush_output(
            self._channel_closed_by_broker_result.is_ready,
            *waiters)

        if self._channel_closed_by_broker_result:
            # Channel was force-closed by broker
            raise exceptions.ChannelClosed(
                self._channel_closed_by_broker_result.value)

    def _on_message_returned(self, channel, method, properties, body):
        """ Called as the result of Basic.Return from broker. Appends the info
        as ReturnedMessage to self._returned_messages.

        :param pika.Channel channel: our self._impl channel
        :param pika.spec.Basic.Return method:
        :param pika.spec.BasicProperties properties: message properties
        :param body: returned message body; may be None if no body
        :type body: str, unicode or None

        """
        assert channel is self._impl, (
            channel.channel_number, self._impl.channel_number)

        assert isinstance(method, pika.spec.Basic.Return), method
        assert isinstance(properties, pika.spec.BasicProperties), (
            properties)

        LOGGER.warn(
            "Published message was returned: _delivery_confirmation=%s; "
            "channel=%s; method=%r; properties=%r; body_size=%d; "
            "body_prefix=%.255r", self._delivery_confirmation,
            channel.channel_number, method, properties,
            len(body) if body is not None else None, body)

        self._returned_messages.append(
            ReturnedMessage(method, properties, body))

    def _raiseAndClearIfReturnedMessagesPending(self):
        """If there are returned messages, raise UnroutableError exception and
        empty the returned messages list

        :raises UnroutableError: if returned messages are present
        """
        if self._returned_messages:
            messages = self._returned_messages
            self._returned_messages = []

            raise UnroutableError(messages)

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """
        LOGGER.info('Channel.close(%s, %s)', reply_code, reply_text)
        try:
            with _CallbackResult() as close_ok_result:
                self._impl.add_callback(callback=close_ok_result.signal_once,
                                        replies=[pika.spec.Channel.CloseOk],
                                        one_shot=True)

                self._impl.close(reply_code=reply_code, reply_text=reply_text)
                self._flush_output(close_ok_result.is_ready)
        finally:
            # Clean up members that might inhibit garbage collection
            self._message_confirmation_result.reset()

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Acknowledge one or more messages. When sent by the client, this
        method acknowledges one or more messages delivered via the Deliver or
        Get-Ok methods. When sent by server, this method acknowledges one or
        more messages published with the Publish method on a channel in
        confirm mode. The acknowledgement can be for a single message or a
        set of messages up to and including a specific message.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        """
        self._impl.basic_ack(delivery_tag=delivery_tag, multiple=multiple)
        self._flush_output()

    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        """This method allows a client to reject one or more incoming messages.
        It can be used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        self._impl.basic_nack(delivery_tag=delivery_tag, multiple=multiple,
                              requeue=requeue)
        self._flush_output()

    def create_consumer(self, queue='', no_ack=False, exclusive=False,
                        arguments=None):
        """Sends the AMQP command Basic.Consume to the broker.

        For more information on basic_consume, see:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param queue: The queue to consume from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a response
        :param bool exclusive: Don't allow other consumers on the queue
        :param dict arguments: Custom key/value pair arguments for the consume
        :returns: consumer tag
        :rtype: str

        """
        with self._basic_consume_ok_result as ok_result:
            ok_result.reset()
            consumer_tag = self._impl.basic_consume(
                consumer_callback=lambda channel, method, properties, body:
                    self._pending_events.append(
                        ConsumerDeliveryEvt(method, properties, body)),
                queue=queue,
                no_ack=no_ack,
                exclusive=exclusive,
                arguments=arguments)

            self._flush_output(ok_result.is_ready)

        return consumer_tag

    def cancel_consumer(self, consumer_tag):
        """ Cancel consumer with the given consumer_tag

        :param str consumer_tag: consumer tag
        :returns: a (possibly empty) sequence of BlockingChannel.Message
            objects corresponding to messages delivered for the given consumer
            before the cancel operation completed that were not yet yielded by
            `BlockingChannel.consume_messages()` generator.
        """
        was_active = (consumer_tag in self._impl._consumers)
        if was_active:
            with _CallbackResult() as cancel_ok_result:
                self._impl.basic_cancel(
                    callback=cancel_ok_result.signal_once,
                    consumer_tag=consumer_tag,
                    nowait=False)

                # Flush output and wait for Basic.CancelOk or broker-initiated
                # Basic.Cancel
                self._flush_output(
                    cancel_ok_result.is_ready,
                    lambda: consumer_tag not in self._impl._consumers)
        else:
            LOGGER.warn("User is attempting to cancel an unknown consumer=%s; "
                        "already cancelled by user or broker?", consumer_tag)

        # NOTE: new events may have arrived for this consumer while we were
        # waiting for Basic.CancelOk

        # Remove pending events destined for this consumer
        remaining_events = deque()
        unprocessed_messages = []
        while self._pending_events:
            evt = self._pending_events.popleft()
            if evt.consumer_tag == consumer_tag:
                if type(evt) is ConsumerDeliveryEvt:
                    unprocessed_messages.append(evt)
                else:
                    # A broker-initiated Basic.Cancel must have arrived before
                    # our cancel request completed
                    assert type(evt) is ConsumerCancellationEvt, type(evt)
                    LOGGER.warn("cancel_consumer: discarding evt=%s", evt)
            else:
                remaining_events.append(evt)

        self._pending_events = remaining_events

        return unprocessed_messages

    def has_event(self):
        """Check if at least one event is available to be returned by
        `BlockingChannel.get_event` without blocking.

        :returns: True if at least one event is available and may be retrieved
          by calling `BlockingChannel.get_event()` without blocking; False if no
          events are available.
        :rtype: bool
        """
        return bool(self._pending_events)

    def get_event(self, inactivity_timeout=None):
        """Returns the next event; if an event is not available immediately,
        blocks and waits for an incoming event.

        :param float inactivity_timeout: if a number is given (in seconds), will
            cause the method to return None after the given period of
            inactivity; this permits for pseudo-regular maintenance activities
            to be carried out by the user while waiting for messages to arrive.
            If None is given (default), then the method blocks until the next
            event arrives. NOTE that timing granularity is limited by the timer
            resolution of the underlying implementation.

        :returns: May return one of the following
            `ConsumerDeliveryEvt` - message delivered for one of the active
                consumers
            `ConsumerCancellationEvt` - broker-initiated consumer cancellation.
            None - upon expiration of inactivity timeout, if one was specified.
            NOTE: other events types may be added in the future (e.g.,
            flow-control)


        Blocking example with inactivity timeout ::

            cons1_id = channel.create_consumer(queue="apples")
            cons2_id = channel.create_consumer(queue="oranges")

            num_consumers = 2

            while True:
                evt = channel.get_event(inactivity_timeout=10)
                if type(evt) is ConsumerDeliveryEvt:
                    channel.basic_ack(evt.method.delivery_tag)
                    print "Acked:", evt
                elif type(evt) is ConsumerCancellationEvt:
                    print "ERROR: Consumer cancelled by broker:", evt
                    num_consumers -= 1
                    if num_consumers <= 0:
                        break
                elif evt is None:
                    print "INACTIVITY TIMEOUT"
                else:
                    print "WARNING: unexpected evt=", evt

        """
        if self.has_event():
            return self._pending_events.popleft()

        # Wait for an event
        with _CallbackResult() as timeoutResult:
            if inactivity_timeout is None:
                waiters = (self.has_event,)
            else:
                waiters = (self.has_event, timeoutResult.is_ready)
                # Start inactivity timer
                timeout_id = self._connection.add_timeout(
                    inactivity_timeout,
                    timeoutResult.signal_once)

            # Wait for message delivery or inactivity timeout, whichever
            # occurs first
            try:
                self._flush_output(*waiters)
            finally:
                if inactivity_timeout is not None:
                    # Reset timer
                    if not timeoutResult:
                        self._connection.remove_timeout(timeout_id)
                    else:
                        timeoutResult.reset()


        if self.has_event():
            # Got an event
            return self._pending_events.popleft()
        else:
            # Inactivity timeout
            return None


    def read_events(self, inactivity_timeout=None):
        """A blocking generator iterator that wraps `BlockingChanel.get_event`
        and yields its results.

        See `BlockingChanel.get_event` for more information about events.

        :param float inactivity_timeout: if a number is given (in seconds), will
            cause the generator to yield None after the given period of
            inactivity; this permits for pseudo-regular maintenance activities
            to be carried out by the user while waiting for messages to arrive.
            NOTE that the underlying implementation doesn't permit a high level
            of timing granularity.

        Example with inactivity timeout ::

            cons1_id = create_consumer(queue="apples")
            cons2_id = create_consumer(queue="oranges")

            num_consumers = 2

            for evt in read_events(inactivity_timeout=10):
                if type(evt) is ConsumerDeliveryEvt:
                    channel.basic_ack(evt.delivery_tag)
                    print "Acked:", evt
                elif type(evt) is ConsumerCancellationEvt:
                    print "ERROR: Consumer cancelled by broker:", evt
                    num_consumers -= 1
                    if num_consumers <= 0:
                        break
                elif evt is None:
                    print "INACTIVITY TIMEOUT"
                else:
                    print "WARNING: unexpected evt=", evt

        """
        while True:
            yield self.get_event(inactivity_timeout)

    def basic_get(self, queue=None, no_ack=False):
        """Get a single message from the AMQP broker. Returns a sequence with
        the method frame, message properties, and body.

        :param queue: Name of queue to get a message from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a reply
        :returns: a three-tuple; (None, None, None) if the queue was empty;
            otherwise (method, properties, body); NOTE: body may be None
        :rtype: (None, None, None)|(spec.Basic.GetOk,
                                    spec.BasicProperties,
                                    str or unicode or None)
        """
        get_ok_result = _CallbackResult(self._RxMessageArgs)
        assert not self._basic_getempty_result
        with get_ok_result, self._basic_getempty_result:
            self._impl.basic_get(callback=get_ok_result.set_value_once,
                                 queue=queue,
                                 no_ack=no_ack)
            self._flush_output(get_ok_result.is_ready,
                               self._basic_getempty_result.is_ready)
            if get_ok_result:
                evt = get_ok_result.value
                return (evt.method, evt.properties, evt.body)
            else:
                assert self._basic_getempty_result, (
                    "wait completed without GetOk and GetEmpty")
                return None, None, None

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """Publish to the channel with the given exchange, routing key and body.
        Returns a boolean value indicating the success of the operation.

        This is the legacy BlockingChannel method for publishing. See also
        `BasicChannel.publish` that provides more information about failures.

        For more information on basic_publish and what the parameters do, see:

            http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        NOTE: mandatory and immediate may be enabled even without delivery
          confirmation, but in the absence of delivery confirmation the
          synchronous implementation has no way to know how long to wait for
          the return.

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body; None if no body
        :type body: str or unicode or None
        :param pika.spec.BasicProperties properties: message properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        :returns: None if delivery confirmation is not enabled; otherwise
                  returns False if the message could not be deliveved (
                  Basic.nack or msg return) and True if the message was
                  delivered (Basic.ack and no msg return)
        """
        if self._delivery_confirmation:
            # In publisher-acknowledgments mode
            try:
                self.publish(exchange, routing_key, body, properties,
                             mandatory, immediate)
            except (NackError, UnroutableError):
                return False
            else:
                return True
        else:
            # In non-publisher-acknowledgments mode
            try:
                self.publish(exchange, routing_key, body, properties,
                             mandatory, immediate)
            except UnroutableError:
                # Suppress the error as it applies to previously-published
                # messages (it's beein logged already), and repeat the publish
                # call
                self.publish(exchange, routing_key, body, properties,
                             mandatory, immediate)
            return None

    def publish(self, exchange, routing_key, body,
                properties=None, mandatory=False, immediate=False):
        """Publish to the channel with the given exchange, routing key, and
        body. Unlike the legacy `BlockingChannel.basic_publish`, this method
        provides more information about failures via exceptions.

        For more information on basic_publish and what the parameters do, see:

            http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        NOTE: mandatory and immediate may be enabled even without delivery
          confirmation, but in the absence of delivery confirmation the
          synchronous implementation has no way to know how long to wait for
          the Basic.Return.

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body; None if no body
        :type body: str or unicode or None
        :param pika.spec.BasicProperties properties: message properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        :raises UnroutableError: In publisher-acknowledgments mode (see
            `BlockingChannel.confirm_delivery`), raised if the given message
            message is returned via Basic.Return. In
            non-publisher-acknowledgements mode, raised before attempting to
            publish the given message if there are pending unroutable messages.

        :raises NackError: raised when a message published in
            publisher-acknowledgements mode is Nack'ed by the broker. See
            `BlockingChannel.confirm_delivery`.

        """
        if self._delivery_confirmation:
            # In publisher-acknowledgments mode
            with self._message_confirmation_result:
                self._impl.basic_publish(exchange=exchange,
                                         routing_key=routing_key,
                                         body=body,
                                         properties=properties,
                                         mandatory=mandatory,
                                         immediate=immediate)

                self._flush_output(self._message_confirmation_result.is_ready)
                conf_method = (self._message_confirmation_result.value
                               .method_frame
                               .method)
                if isinstance(conf_method, pika.spec.Basic.Nack):
                    # Broker was unable to process the message due to internal
                    # error
                    LOGGER.warn(
                        "Message was Nack'ed by broker: nack=%r; channel=%s; "
                        "exchange=%s; routing_key=%s; mandatory=%r; "
                        "immediate=%r", conf_method, self._impl.channel_number,
                        exchange, routing_key, mandatory, immediate)
                    returned_messages = self._returned_messages
                    self._returned_messages = []
                    raise NackError(returned_messages)
                elif isinstance(conf_method, pika.spec.Basic.Ack):
                    self._raiseAndClearIfReturnedMessagesPending()
                else:
                    # Should never happen
                    raise TypeError('Unexpected method type: %r', conf_method)
        else:
            # In non-publisher-acknowledgments mode

            # Raise UnroutableError before publishing if returned messages are
            # pending
            self._raiseAndClearIfReturnedMessagesPending()

            # Publish
            self._impl.basic_publish(exchange=exchange,
                                     routing_key=routing_key,
                                     body=body,
                                     properties=properties,
                                     mandatory=mandatory,
                                     immediate=immediate)

            self._flush_output()

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
        with _CallbackResult() as qos_ok_result:
            self._impl.basic_qos(callback=qos_ok_result.signal_once,
                                 prefetch_size=prefetch_size,
                                 prefetch_count=prefetch_count,
                                 all_channels=all_channels)
            self._flush_output(qos_ok_result.is_ready)

    def basic_recover(self, requeue=False):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered. This
        method replaces the asynchronous Recover.

        :param bool requeue: If False, the message will be redelivered to the
                             original recipient. If True, the server will
                             attempt to requeue the message, potentially then
                             delivering it to an alternative subscriber.

        """
        with _CallbackResult() as recover_ok_result:
            self._impl.basic_recover(callback=recover_ok_result.signal_once,
                                     requeue=requeue)
            self._flush_output(recover_ok_result.is_ready)

    def basic_reject(self, delivery_tag=None, requeue=True):
        """Reject an incoming message. This method allows a client to reject a
        message. It can be used to interrupt and cancel large incoming messages,
        or return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        self._impl.basic_reject(delivery_tag=delivery_tag, requeue=requeue)
        self._flush_output()

    def confirm_delivery(self):
        """Turn on RabbitMQ-proprietary Confirm mode in the channel.

        For more information see:
            http://www.rabbitmq.com/extensions.html#confirms

        :raises UnroutableError: when unroutable messages that were sent prior
            to this call are returned before we receive Confirm.Select-ok
        """
        if self._delivery_confirmation:
            LOGGER.error('confirm_delivery: confirmation was already enabled '
                         'on channel=%s', self._impl.channel_number)
            return

        with _CallbackResult() as select_ok_result:
            self._impl.add_callback(callback=select_ok_result.signal_once,
                                    replies=[pika.spec.Confirm.SelectOk],
                                    one_shot=True)

            self._impl.confirm_delivery(
                callback=self._message_confirmation_result.set_value_once,
                nowait=False)

            self._flush_output(select_ok_result.is_ready)

        self._delivery_confirmation = True

        # Unroutable messages returned after this point will be in the context
        # of publisher acknowledgments
        self._raiseAndClearIfReturnedMessagesPending()

    def force_data_events(self, enable):
        """Turn on and off forcing the blocking adapter to stop and look to see
        if there are any frames from RabbitMQ in the read buffer. By default
        the BlockingChannel will check for a read after every RPC command which
        can cause performance to degrade in scenarios where you do not care if
        RabbitMQ is trying to send RPC commands to your client connection.

        NOTE: This is a NO-OP here, since we're fixing the performance issue;
        provided for API compatibility with BlockingChannel

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
        # This is a NO-OP here, since we're fixing the performance issue
        LOGGER.warn("%s.force_data_events() is a NO-OP",
                    self.__class__.__name__)
        pass

    def exchange_bind(self, destination=None, source=None, routing_key='',
                      arguments=None):
        """Bind an exchange to another exchange.

        :param destination: The destination exchange to bind
        :type destination: str or unicode
        :param source: The source exchange to bind to
        :type source: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        with _CallbackResult() as bind_ok_result:
            self._impl.exchange_bind(
                callback=bind_ok_result.signal_once,
                destination=destination,
                source=source,
                routing_key=routing_key,
                nowait=False,
                arguments=arguments)

            self._flush_output(bind_ok_result.is_ready)

    def exchange_declare(self, exchange=None,
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False,
                         arguments=None, **kwargs):
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
        :param dict arguments: Custom key/value pair arguments for the exchange
        :param str type: via kwargs: the deprecated exchange type parameter

        """
        assert len(kwargs) <= 1, kwargs

        with _CallbackResult() as declare_ok_result:
            self._impl.exchange_declare(
                callback=declare_ok_result.signal_once,
                exchange=exchange,
                exchange_type=exchange_type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                nowait=False,
                arguments=arguments,
                type=kwargs["type"] if kwargs else None)

            self._flush_output(declare_ok_result.is_ready)

    def exchange_delete(self, exchange=None, if_unused=False):
        """Delete the exchange.

        :param exchange: The exchange name
        :type exchange: str or unicode
        :param bool if_unused: only delete if the exchange is unused

        """
        with _CallbackResult() as delete_ok_result:
            self._impl.exchange_delete(
                callback=delete_ok_result.signal_once,
                exchange=exchange,
                if_unused=if_unused,
                nowait=False)

            self._flush_output(delete_ok_result.is_ready)

    def exchange_unbind(self, destination=None, source=None, routing_key='',
                        arguments=None):
        """Unbind an exchange from another exchange.

        :param destination: The destination exchange to unbind
        :type destination: str or unicode
        :param source: The source exchange to unbind from
        :type source: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        with _CallbackResult() as unbind_ok_result:
            self._impl.exchange_unbind(
                callback=unbind_ok_result.signal_once,
                destination=destination,
                source=source,
                routing_key=routing_key,
                nowait=False,
                arguments=arguments)

            self._flush_output(unbind_ok_result.is_ready)

    def queue_bind(self, queue, exchange, routing_key=None,
                   arguments=None):
        """Bind the queue to the specified exchange

        :param queue: The queue to bind to the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        with _CallbackResult() as bind_ok_result:
            self._impl.queue_bind(callback=bind_ok_result.signal_once,
                                  queue=queue,
                                  exchange=exchange,
                                  routing_key=routing_key,
                                  nowait=False,
                                  arguments=arguments)

            self._flush_output(bind_ok_result.is_ready)

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False,
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
        :param dict arguments: Custom key/value arguments for the queue

        """
        with _CallbackResult() as declare_ok_result:
            self._impl.queue_declare(
                callback=declare_ok_result.signal_once,
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                nowait=False,
                arguments=arguments)

            self._flush_output(declare_ok_result.is_ready)

    def queue_delete(self, queue='', if_unused=False, if_empty=False):
        """Delete a queue from the broker.

        :param queue: The queue to delete
        :type queue: str or unicode
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty

        """
        with _CallbackResult() as delete_ok_result:
            self._impl.queue_delete(callback=delete_ok_result.signal_once,
                                    queue=queue,
                                    if_unused=if_unused,
                                    if_empty=if_empty,
                                    nowait=False)

            self._flush_output(delete_ok_result.is_ready)

    def queue_purge(self, queue=''):
        """Purge all of the messages from the specified queue

        :param queue: The queue to purge
        :type  queue: str or unicode

        """
        with _CallbackResult() as purge_ok_result:
            self._impl.queue_purge(callback=purge_ok_result.signal_once,
                                   queue=queue,
                                   nowait=False)

            self._flush_output(purge_ok_result.is_ready)

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
        with _CallbackResult() as unbind_ok_result:
            self._impl.queue_unbind(callback=unbind_ok_result.signal_once,
                                    queue=queue,
                                    exchange=exchange,
                                    routing_key=routing_key,
                                    arguments=arguments)
            self._flush_output(unbind_ok_result.is_ready)

    def tx_select(self):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        """
        with _CallbackResult() as select_ok_result:
            self._impl.tx_select(select_ok_result.signal_once)
            self._flush_output(select_ok_result.is_ready)

    def tx_commit(self):
        """Commit a transaction."""
        with _CallbackResult() as commit_ok_result:
            self._impl.tx_commit(commit_ok_result.signal_once)
            self._flush_output(commit_ok_result.is_ready)

    def tx_rollback(self):
        """Rollback a transaction."""
        with _CallbackResult() as rollback_ok_result:
            self._impl.tx_rollback(rollback_ok_result.signal_once)
            self._flush_output(rollback_ok_result.is_ready)
