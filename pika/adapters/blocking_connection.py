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
# Disable "access to protected member warnings: this wrapper implementation is
# a friend of those instances
# pylint: disable=W0212

from collections import namedtuple, deque
import contextlib
import functools
import logging
import time

import pika.channel

from pika import compat
from pika import exceptions

import pika.spec

# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters.select_connection import SelectConnection


LOGGER = logging.getLogger(__name__)


class _CallbackResult(object):
    """ CallbackResult is a non-thread-safe implementation for receiving
    callback results; INTERNAL USE ONLY!
    """
    __slots__ = ('_value_class', '_ready', '_values')
    def __init__(self, value_class=None):
        """
        :param callable value_class: only needed if the CallbackResult
                                     instance will be used with
                                     `set_value_once` and `append_element`.
                                     *args and **kwargs of the value setter
                                     methods will be passed to this class.

        """
        self._value_class = value_class
        self._ready = None
        self._values = None
        self.reset()

    def reset(self):
        """Reset value, but not _value_class"""
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
        """Reset value"""
        self.reset()

    def is_ready(self):
        """
        :returns: True if the object is in a signaled state
        """
        return self._ready

    @property
    def ready(self):
        """True if the object is in a signaled state"""
        return self._ready

    def signal_once(self, *_args, **_kwargs):
        """ Set as ready

        :raises AssertionError: if result was already signalled
        """
        assert not self._ready, '_CallbackResult was already set'
        self._ready = True

    def set_value_once(self, *args, **kwargs):
        """ Set as ready with value; the value may be retrieved via the `value`
        property getter

        :raises AssertionError: if result was already set
        """
        self.signal_once()
        try:
            self._values = (self._value_class(*args, **kwargs),)
        except Exception:
            LOGGER.error(
                "set_value_once failed: value_class=%r; args=%r; kwargs=%r",
                self._value_class, args, kwargs)
            raise

    def append_element(self, *args, **kwargs):
        """Append an element to values"""
        assert not self._ready or isinstance(self._values, list), (
            '_CallbackResult state is incompatible with append_element: '
            'ready=%r; values=%r' % (self._ready, self._values))

        try:
            value = self._value_class(*args, **kwargs)
        except Exception:
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


class _IoloopTimerContext(object):
    """Context manager for registering and safely unregistering a
    SelectConnection ioloop-based timer
    """

    def __init__(self, duration, connection):
        """
        :param float duration: non-negative timer duration in seconds
        :param SelectConnection connection:
        """
        assert hasattr(connection, 'add_timeout'), connection
        self._duration = duration
        self._connection = connection
        self._callback_result = _CallbackResult()
        self._timer_id = None

    def __enter__(self):
        """Register a timer"""
        self._timer_id = self._connection.add_timeout(
            self._duration,
            self._callback_result.signal_once)
        return self

    def __exit__(self, *_args, **_kwargs):
        """Unregister timer if it hasn't fired yet"""
        if not self._callback_result:
            self._connection.remove_timeout(self._timer_id)

    def is_ready(self):
        """
        :returns: True if timer has fired, False otherwise
        """
        return self._callback_result.is_ready()


class _TimerEvt(object):
    """Represents a timer created via `BlockingConnection.add_timeout`"""
    __slots__ = ('timer_id', '_callback')

    def __init__(self, callback):
        """
        :param callback: see callback in `BlockingConnection.add_timeout`
        """
        self._callback = callback

        # Will be set to timer id returned from the underlying implementation's
        # `add_timeout` method
        self.timer_id = None

    def __repr__(self):
        return '<%s timer_id=%s callback=%s>' % (self.__class__.__name__,
                                                 self.timer_id, self._callback)

    def dispatch(self):
        """Dispatch the user's callback method"""
        self._callback()


class _ConnectionBlockedUnblockedEvtBase(object):
    """Base class for `_ConnectionBlockedEvt` and `_ConnectionUnblockedEvt`"""
    __slots__ = ('_callback', '_method_frame')

    def __init__(self, callback, method_frame):
        """
        :param callback: see callback parameter in
          `BlockingConnection.add_on_connection_blocked_callback` and
          `BlockingConnection.add_on_connection_unblocked_callback`
        :param pika.frame.Method method_frame: with method_frame.method of type
          `pika.spec.Connection.Blocked` or `pika.spec.Connection.Unblocked`
        """
        self._callback = callback
        self._method_frame = method_frame

    def __repr__(self):
        return '<%s callback=%s, frame=%s>' % (self.__class__.__name__,
                                               self._callback,
                                               self._method_frame)

    def dispatch(self):
        """Dispatch the user's callback method"""
        self._callback(self._method_frame)


class _ConnectionBlockedEvt(_ConnectionBlockedUnblockedEvtBase):
    """Represents a Connection.Blocked notification from RabbitMQ broker`"""
    pass


class _ConnectionUnblockedEvt(_ConnectionBlockedUnblockedEvtBase):
    """Represents a Connection.Unblocked notification from RabbitMQ broker`"""
    pass


class BlockingConnection(object):
    """The BlockingConnection creates a layer on top of Pika's asynchronous core
    providing methods that will block until their expected response has
    returned. Due to the asynchronous nature of the `Basic.Deliver` and
    `Basic.Return` calls from RabbitMQ to your application, you can still
    implement continuation-passing style asynchronous methods if you'd like to
    receive messages from RabbitMQ using
    :meth:`basic_consume <BlockingChannel.basic_consume>` or if you want to be
    notified of a delivery failure when using
    :meth:`basic_publish <BlockingChannel.basic_publish>`.

    For more information about communicating with the blocking_connection
    adapter, be sure to check out the
    :class:`BlockingChannel <BlockingChannel>` class which implements the
    :class:`Channel <pika.channel.Channel>` based communication for the
    blocking_connection adapter.

    To prevent recursion/reentrancy, the blocking connection and channel
    implementations queue asynchronously-delivered events received
    in nested context (e.g., while waiting for `BlockingConnection.channel` or
    `BlockingChannel.queue_declare` to complete), dispatching them synchronously
    once nesting returns to the desired context. This concerns all callbacks,
    such as those registered via `BlockingConnection.add_timeout`,
    `BlockingConnection.add_on_connection_blocked_callback`,
    `BlockingConnection.add_on_connection_unblocked_callback`,
    `BlockingChannel.basic_consume`, etc.

    Blocked Connection deadlock avoidance: when RabbitMQ becomes low on
    resources, it emits Connection.Blocked (AMQP extension) to the client
    connection when client makes a resource-consuming request on that connection
    or its channel (e.g., `Basic.Publish`); subsequently, RabbitMQ suspsends
    processing requests from that connection until the affected resources are
    restored. See http://www.rabbitmq.com/connection-blocked.html. This
    may impact `BlockingConnection` and `BlockingChannel` operations in a
    way that users might not be expecting. For example, if the user dispatches
    `BlockingChannel.basic_publish` in non-publisher-confirmation mode while
    RabbitMQ is in this low-resource state followed by a synchronous request
    (e.g., `BlockingConnection.channel`, `BlockingChannel.consume`,
    `BlockingChannel.basic_consume`, etc.), the synchronous request will block
    indefinitely (until Connection.Unblocked) waiting for RabbitMQ to reply. If
    the blocked state persists for a long time, the blocking operation will
    appear to hang. In this state, `BlockingConnection` instance and its
    channels will not dispatch user callbacks. SOLUTION: To break this potential
    deadlock, applications may configure the `blocked_connection_timeout`
    connection parameter when instantiating `BlockingConnection`. Upon blocked
    connection timeout, this adapter will raise ConnectionClosed exception with
    first exception arg of
    `pika.connection.InternalCloseReasons.BLOCKED_CONNECTION_TIMEOUT`. See
    `pika.connection.ConnectionParameters` documentation to learn more about
    `blocked_connection_timeout` configuration.

    """
    # Connection-opened callback args
    _OnOpenedArgs = namedtuple('BlockingConnection__OnOpenedArgs',
                               'connection')

    # Connection-establishment error callback args
    _OnOpenErrorArgs = namedtuple('BlockingConnection__OnOpenErrorArgs',
                                  'connection error')

    # Connection-closing callback args
    _OnClosedArgs = namedtuple('BlockingConnection__OnClosedArgs',
                               'connection reason_code reason_text')

    # Channel-opened callback args
    _OnChannelOpenedArgs = namedtuple(
        'BlockingConnection__OnChannelOpenedArgs',
        'channel')

    def __init__(self, parameters=None, _impl_class=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param _impl_class: for tests/debugging only; implementation class;
            None=default

        :raises RuntimeError:

        """
        # Used by the _acquire_event_dispatch decorator; when already greater
        # than 0, event dispatch is already acquired higher up the call stack
        self._event_dispatch_suspend_depth = 0

        # Connection-specific events that are ready for dispatch: _TimerEvt,
        # _ConnectionBlockedEvt, _ConnectionUnblockedEvt
        self._ready_events = deque()

        # Channel numbers of channels that are requesting a call to their
        # BlockingChannel._dispatch_events method; See
        # `_request_channel_dispatch`
        self._channels_pending_dispatch = set()

        # Receives on_open_callback args from Connection
        self._opened_result = _CallbackResult(self._OnOpenedArgs)

        # Receives on_open_error_callback args from Connection
        self._open_error_result = _CallbackResult(self._OnOpenErrorArgs)

        # Receives on_close_callback args from Connection
        self._closed_result = _CallbackResult(self._OnClosedArgs)

        # Set to True when when user calls close() on the connection
        # NOTE: this is a workaround to detect socket error because
        # on_close_callback passes reason_code=0 when called due to socket error
        self._user_initiated_close = False

        impl_class = _impl_class or SelectConnection
        self._impl = impl_class(
            parameters=parameters,
            on_open_callback=self._opened_result.set_value_once,
            on_open_error_callback=self._open_error_result.set_value_once,
            on_close_callback=self._closed_result.set_value_once,
            stop_ioloop_on_close=False)

        self._impl.ioloop.activate_poller()

        self._process_io_for_connection_setup()

    def __repr__(self):
        return '<%s impl=%r>' % (self.__class__.__name__, self._impl)

    def _cleanup(self):
        """Clean up members that might inhibit garbage collection"""
        self._impl.ioloop.deactivate_poller()
        self._ready_events.clear()
        self._opened_result.reset()
        self._open_error_result.reset()
        self._closed_result.reset()

    @contextlib.contextmanager
    def _acquire_event_dispatch(self):
        """ Context manager that controls access to event dispatcher for
        preventing reentrancy.

        The "as" value is True if the managed code block owns the event
        dispatcher and False if caller higher up in the call stack already owns
        it. Only managed code that gets ownership (got True) is permitted to
        dispatch
        """
        try:
            # __enter__ part
            self._event_dispatch_suspend_depth += 1
            yield self._event_dispatch_suspend_depth == 1
        finally:
            # __exit__ part
            self._event_dispatch_suspend_depth -= 1

    def _process_io_for_connection_setup(self):
        """ Perform follow-up processing for connection setup request: flush
        connection output and process input while waiting for connection-open
        or connection-error.

        :raises AMQPConnectionError: on connection open error
        """
        if not self._open_error_result.ready:
            self._flush_output(self._opened_result.is_ready,
                               self._open_error_result.is_ready)

        if self._open_error_result.ready:
            try:
                exception_or_message = self._open_error_result.value.error
                if isinstance(exception_or_message, Exception):
                    raise exception_or_message
                raise exceptions.AMQPConnectionError(exception_or_message)
            finally:
                self._cleanup()

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
        if self.is_closed:
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
                    (not waiters or any(ready() for ready in waiters))))

        # Process I/O until our completion condition is satisified
        while not is_done():
            self._impl.ioloop.poll()
            self._impl.ioloop.process_timeouts()

        if self._open_error_result.ready or self._closed_result.ready:
            try:
                if not self._user_initiated_close:
                    if self._open_error_result.ready:
                        maybe_exception = self._open_error_result.value.error
                        LOGGER.error('Connection open failed - %r',
                                     maybe_exception)
                        if isinstance(maybe_exception, Exception):
                            raise maybe_exception
                        else:
                            raise exceptions.ConnectionClosed(maybe_exception)
                    else:
                        result = self._closed_result.value
                        LOGGER.error('Connection close detected; result=%r',
                                     result)
                        raise exceptions.ConnectionClosed(result.reason_code,
                                                          result.reason_text)
                else:
                    LOGGER.info('Connection closed; result=%r',
                                self._closed_result.value)
            finally:
                self._cleanup()

    def _request_channel_dispatch(self, channel_number):
        """Called by BlockingChannel instances to request a call to their
        _dispatch_events method or to terminate `process_data_events`;
        BlockingConnection will honor these requests from a safe context.

        :param int channel_number: positive channel number to request a call
            to the channel's `_dispatch_events`; a negative channel number to
            request termination of `process_data_events`
        """
        self._channels_pending_dispatch.add(channel_number)

    def _dispatch_channel_events(self):
        """Invoke the `_dispatch_events` method on open channels that requested
        it
        """
        if not self._channels_pending_dispatch:
            return

        with self._acquire_event_dispatch() as dispatch_acquired:
            if not dispatch_acquired:
                # Nested dispatch or dispatch blocked higher in call stack
                return

            candidates = list(self._channels_pending_dispatch)
            self._channels_pending_dispatch.clear()

            for channel_number in candidates:
                if channel_number < 0:
                    # This was meant to terminate process_data_events
                    continue

                try:
                    impl_channel = self._impl._channels[channel_number]
                except KeyError:
                    continue

                if impl_channel.is_open:
                    impl_channel._get_cookie()._dispatch_events()

    def _on_timer_ready(self, evt):
        """Handle expiry of a timer that was registered via `add_timeout`

        :param _TimerEvt evt:

        """
        self._ready_events.append(evt)

    def _on_connection_blocked(self, user_callback, method_frame):
        """Handle Connection.Blocked notification from RabbitMQ broker

        :param callable user_callback: callback passed to
           `add_on_connection_blocked_callback`
        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Blocked`
        """
        self._ready_events.append(
            _ConnectionBlockedEvt(user_callback, method_frame))

    def _on_connection_unblocked(self, user_callback, method_frame):
        """Handle Connection.Unblocked notification from RabbitMQ broker

        :param callable user_callback: callback passed to
           `add_on_connection_unblocked_callback`
        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Blocked`
        """
        self._ready_events.append(
            _ConnectionUnblockedEvt(user_callback, method_frame))

    def _dispatch_connection_events(self):
        """Dispatch ready connection events"""
        if not self._ready_events:
            return

        with self._acquire_event_dispatch() as dispatch_acquired:
            if not dispatch_acquired:
                # Nested dispatch or dispatch blocked higher in call stack
                return

            # Limit dispatch to the number of currently ready events to avoid
            # getting stuck in this loop
            for _ in compat.xrange(len(self._ready_events)):
                try:
                    evt = self._ready_events.popleft()
                except IndexError:
                    # Some events (e.g., timers) must have been cancelled
                    break

                evt.dispatch()

    def add_on_connection_blocked_callback(self, callback):
        """Add a callback to be notified when RabbitMQ has sent a
        `Connection.Blocked` frame indicating that RabbitMQ is low on
        resources. Publishers can use this to voluntarily suspend publishing,
        instead of relying on back pressure throttling. The callback
        will be passed the `Connection.Blocked` method frame.

        See also `ConnectionParameters.blocked_connection_timeout`.

        :param method callback: Callback to call on `Connection.Blocked`,
            having the signature `callback(pika.frame.Method)`, where the
            method frame's `method` member is of type
            `pika.spec.Connection.Blocked`

        """
        self._impl.add_on_connection_blocked_callback(
            functools.partial(self._on_connection_blocked, callback))

    def add_on_connection_unblocked_callback(self, callback):
        """Add a callback to be notified when RabbitMQ has sent a
        `Connection.Unblocked` frame letting publishers know it's ok
        to start publishing again. The callback will be passed the
        `Connection.Unblocked` method frame.

        :param method callback: Callback to call on
            `Connection.Unblocked`, having the signature
            `callback(pika.frame.Method)`, where the method frame's
            `method` member is of type `pika.spec.Connection.Unblocked`

        """
        self._impl.add_on_connection_unblocked_callback(
            functools.partial(self._on_connection_unblocked, callback))

    def add_timeout(self, deadline, callback):
        """Create a single-shot timer to fire after deadline seconds. Do not
        confuse with Tornado's timeout where you pass in the time you want to
        have your callback called. Only pass in the seconds until it's to be
        called.

        NOTE: the timer callbacks are dispatched only in the scope of
        specially-designated methods: see
        `BlockingConnection.process_data_events` and
        `BlockingChannel.start_consuming`.

        :param float deadline: The number of seconds to wait to call callback
        :param callable callback: The callback method with the signature
            callback()

        :returns: opaque timer id

        """
        if not callable(callback):
            raise ValueError(
                'callback parameter must be callable, but got %r'
                % (callback,))

        evt = _TimerEvt(callback=callback)
        timer_id = self._impl.add_timeout(
            deadline,
            functools.partial(self._on_timer_ready, evt))
        evt.timer_id = timer_id

        return timer_id

    def remove_timeout(self, timeout_id):
        """Remove a timer if it's still in the timeout stack

        :param timeout_id: The opaque timer id to remove

        """
        # Remove from the impl's timeout stack
        self._impl.remove_timeout(timeout_id)

        # Remove from ready events, if the timer fired already
        for i, evt in enumerate(self._ready_events):
            if isinstance(evt, _TimerEvt) and evt.timer_id == timeout_id:
                index_to_remove = i
                break
        else:
            # Not found
            return

        del self._ready_events[index_to_remove]

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        if self.is_closed:
            LOGGER.debug('Close called on closed connection (%s): %s',
                         reply_code, reply_text)
            return

        LOGGER.info('Closing connection (%s): %s', reply_code, reply_text)

        self._user_initiated_close = True

        # Close channels that remain opened
        for impl_channel in pika.compat.dictvalues(self._impl._channels):
            channel = impl_channel._get_cookie()
            if channel.is_open:
                try:
                    channel.close(reply_code, reply_text)
                except exceptions.ChannelClosed as exc:
                    # Log and suppress broker-closed channel
                    LOGGER.warning('Got ChannelClosed while closing channel '
                                   'from connection.close: %r', exc)

        # Close the connection
        self._impl.close(reply_code, reply_text)

        self._flush_output(self._closed_result.is_ready)

    def process_data_events(self, time_limit=0):
        """Will make sure that data events are processed. Dispatches timer and
        channel callbacks if not called from the scope of BlockingConnection or
        BlockingChannel callback. Your app can block on this method.

        :param float time_limit: suggested upper bound on processing time in
            seconds. The actual blocking time depends on the granularity of the
            underlying ioloop. Zero means return as soon as possible. None means
            there is no limit on processing time and the function will block
            until I/O produces actionable events. Defaults to 0 for backward
            compatibility. This parameter is NEW in pika 0.10.0.
        """
        with self._acquire_event_dispatch() as dispatch_acquired:
            # Check if we can actually process pending events
            common_terminator = lambda: bool(dispatch_acquired and
                                             (self._channels_pending_dispatch or
                                              self._ready_events))
            if time_limit is None:
                self._flush_output(common_terminator)
            else:
                with _IoloopTimerContext(time_limit, self._impl) as timer:
                    self._flush_output(timer.is_ready, common_terminator)

        if self._ready_events:
            self._dispatch_connection_events()

        if self._channels_pending_dispatch:
            self._dispatch_channel_events()

    def sleep(self, duration):
        """A safer way to sleep than calling time.sleep() directly that would
        keep the adapter from ignoring frames sent from the broker. The
        connection will "sleep" or block the number of seconds specified in
        duration in small intervals.

        :param float duration: The time to sleep in seconds

        """
        assert duration >= 0, duration

        deadline = time.time() + duration
        time_limit = duration
        # Process events at least once
        while True:
            self.process_data_events(time_limit)
            time_limit = deadline - time.time()
            if time_limit <= 0:
                break

    def channel(self, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        :rtype: pika.adapters.blocking_connection.BlockingChannel
        """
        with _CallbackResult(self._OnChannelOpenedArgs) as opened_args:
            impl_channel = self._impl.channel(
                channel_number=channel_number,
                on_open_callback=opened_args.set_value_once)

            # Create our proxy channel
            channel = BlockingChannel(impl_channel, self)

            # Link implementation channel with our proxy channel
            impl_channel._set_cookie(channel)

            # Drive I/O until Channel.Open-ok
            channel._flush_output(opened_args.is_ready)

        return channel

    def __enter__(self):
        # Prepare `with` context
        return self

    def __exit__(self, exc_type, value, traceback):
        # Close connection after `with` context
        self.close()

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
        Returns True if connection is in the process of closing due to
        client-initiated `close` request, but closing is not yet complete.
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

    # Legacy property names for backward compatibility
    basic_nack = basic_nack_supported
    consumer_cancel_notify = consumer_cancel_notify_supported
    exchange_exchange_bindings = exchange_exchange_bindings_supported
    publisher_confirms = publisher_confirms_supported


class _ChannelPendingEvt(object):
    """Base class for BlockingChannel pending events"""
    pass


class _ConsumerDeliveryEvt(_ChannelPendingEvt):
    """This event represents consumer message delivery `Basic.Deliver`; it
    contains method, properties, and body of the delivered message.
    """

    __slots__ = ('method', 'properties', 'body')

    def __init__(self, method, properties, body):
        """
        :param spec.Basic.Deliver method: NOTE: consumer_tag and delivery_tag
          are valid only within source channel
        :param spec.BasicProperties properties: message properties
        :param body: message body; empty string if no body
        :type body: str or unicode
        """
        self.method = method
        self.properties = properties
        self.body = body


class _ConsumerCancellationEvt(_ChannelPendingEvt):
    """This event represents server-initiated consumer cancellation delivered to
    client via Basic.Cancel. After receiving Basic.Cancel, there will be no
    further deliveries for the consumer identified by `consumer_tag` in
    `Basic.Cancel`
    """

    __slots__ = 'method_frame'

    def __init__(self, method_frame):
        """
        :param pika.frame.Method method_frame: method frame with method of type
            `spec.Basic.Cancel`
        """
        self.method_frame = method_frame

    def __repr__(self):
        return '<%s method_frame=%r>' % (self.__class__.__name__,
                                         self.method_frame)

    @property
    def method(self):
        """method of type spec.Basic.Cancel"""
        return self.method_frame.method


class _ReturnedMessageEvt(_ChannelPendingEvt):
    """This event represents a message returned by broker via `Basic.Return`"""

    __slots__ = ('callback', 'channel', 'method', 'properties', 'body')

    def __init__(self, callback, channel, method, properties, body):
        """
        :param callable callback: user's callback, having the signature
            callback(channel, method, properties, body), where
                                channel: pika.Channel
                                method: pika.spec.Basic.Return
                                properties: pika.spec.BasicProperties
                                body: str, unicode, or bytes (python 3.x)

        :param pika.Channel channel:
        :param pika.spec.Basic.Return method:
        :param pika.spec.BasicProperties properties:
        :param body: str, unicode, or bytes (python 3.x)
        """
        self.callback = callback
        self.channel = channel
        self.method = method
        self.properties = properties
        self.body = body

    def __repr__(self):
        return ('<%s callback=%r channel=%r method=%r properties=%r '
                'body=%.300r>') % (self.__class__.__name__, self.callback,
                                   self.channel, self.method, self.properties,
                                   self.body)

    def dispatch(self):
        """Dispatch user's callback"""
        self.callback(self.channel, self.method, self.properties, self.body)


class ReturnedMessage(object):
    """Represents a message returned via Basic.Return in publish-acknowledgments
    mode
    """

    __slots__ = ('method', 'properties', 'body')

    def __init__(self, method, properties, body):
        """
        :param spec.Basic.Return method:
        :param spec.BasicProperties properties: message properties
        :param body: message body; empty string if no body
        :type body: str or unicode
        """
        self.method = method
        self.properties = properties
        self.body = body


class _ConsumerInfo(object):
    """Information about an active consumer"""

    __slots__ = ('consumer_tag', 'no_ack', 'on_message_callback',
                 'alternate_event_sink', 'state')

    # Consumer states
    SETTING_UP = 1
    ACTIVE = 2
    TEARING_DOWN = 3
    CANCELLED_BY_BROKER = 4

    def __init__(self, consumer_tag, no_ack, on_message_callback=None,
                 alternate_event_sink=None):
        """
        NOTE: exactly one of callback/alternate_event_sink musts be non-None.

        :param str consumer_tag:
        :param bool no_ack: the no-ack value for the consumer
        :param callable on_message_callback: The function for dispatching messages to
            user, having the signature:
            on_message_callback(channel, method, properties, body)
                channel: BlockingChannel
                method: spec.Basic.Deliver
                properties: spec.BasicProperties
                body: str or unicode
        :param callable alternate_event_sink: if specified, _ConsumerDeliveryEvt
            and _ConsumerCancellationEvt objects will be diverted to this
            callback instead of being deposited in the channel's
            `_pending_events` container. Signature:
            alternate_event_sink(evt)
        """
        assert (on_message_callback is None) != (alternate_event_sink is None), (
            'exactly one of on_message_callback/alternate_event_sink must be non-None',
            on_message_callback, alternate_event_sink)
        self.consumer_tag = consumer_tag
        self.no_ack = no_ack
        self.on_message_callback = on_message_callback
        self.alternate_event_sink = alternate_event_sink
        self.state = self.SETTING_UP

    @property
    def setting_up(self):
        """True if in SETTING_UP state"""
        return self.state == self.SETTING_UP

    @property
    def active(self):
        """True if in ACTIVE state"""
        return self.state == self.ACTIVE

    @property
    def tearing_down(self):
        """True if in TEARING_DOWN state"""
        return self.state == self.TEARING_DOWN

    @property
    def cancelled_by_broker(self):
        """True if in CANCELLED_BY_BROKER state"""
        return self.state == self.CANCELLED_BY_BROKER


class _QueueConsumerGeneratorInfo(object):
    """Container for information about the active queue consumer generator """
    __slots__ = ('params', 'consumer_tag', 'pending_events')

    def __init__(self, params, consumer_tag):
        """
        :params tuple params: a three-tuple (queue, no_ack, exclusive) that were
           used to create the queue consumer
        :param str consumer_tag: consumer tag
        """
        self.params = params
        self.consumer_tag = consumer_tag
        #self.messages = deque()

        # Holds pending events of types _ConsumerDeliveryEvt and
        # _ConsumerCancellationEvt
        self.pending_events = deque()

    def __repr__(self):
        return '<%s params=%r consumer_tag=%r>' % (
            self.__class__.__name__, self.params, self.consumer_tag)


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

    """

    # Used as value_class with _CallbackResult for receiving Basic.GetOk args
    _RxMessageArgs = namedtuple(
        'BlockingChannel__RxMessageArgs',
        [
            'channel',  # implementation pika.Channel instance
            'method',   # Basic.GetOk
            'properties',  # pika.spec.BasicProperties
            'body'      # str, unicode, or bytes (python 3.x)
        ])


    # For use as value_class with any _CallbackResult that expects method_frame
    # as the only arg
    _MethodFrameCallbackResultArgs = namedtuple(
        'BlockingChannel__MethodFrameCallbackResultArgs',
        'method_frame')

    # Broker's basic-ack/basic-nack args when delivery confirmation is enabled;
    # may concern a single or multiple messages
    _OnMessageConfirmationReportArgs = namedtuple(
        'BlockingChannel__OnMessageConfirmationReportArgs',
        'method_frame')

    # Parameters for broker-initiated Channel.Close request: reply_code
    # holds the broker's non-zero error code and reply_text holds the
    # corresponding error message text.
    _OnChannelClosedByBrokerArgs = namedtuple(
        'BlockingChannel__OnChannelClosedByBrokerArgs',
        'method_frame')

    # For use as value_class with _CallbackResult expecting Channel.Flow
    # confirmation.
    _FlowOkCallbackResultArgs = namedtuple(
        'BlockingChannel__FlowOkCallbackResultArgs',
        'active'  # True if broker will start or continue sending; False if not
    )

    _CONSUMER_CANCELLED_CB_KEY = 'blocking_channel_consumer_cancelled'

    def __init__(self, channel_impl, connection):
        """Create a new instance of the Channel

        :param channel_impl: Channel implementation object as returned from
                             SelectConnection.channel()
        :param BlockingConnection connection: The connection object

        """
        self._impl = channel_impl
        self._connection = connection

        # A mapping of consumer tags to _ConsumerInfo for active consumers
        self._consumer_infos = dict()

        # Queue consumer generator generator info of type
        # _QueueConsumerGeneratorInfo created by BlockingChannel.consume
        self._queue_consumer_generator = None

        # Whether RabbitMQ delivery confirmation has been enabled
        self._delivery_confirmation = False

        # Receives message delivery confirmation report (Basic.ack or
        # Basic.nack) from broker when delivery confirmations are enabled
        self._message_confirmation_result = _CallbackResult(
            self._OnMessageConfirmationReportArgs)

        # deque of pending events: _ConsumerDeliveryEvt and
        # _ConsumerCancellationEvt objects that will be returned by
        # `BlockingChannel.get_event()`
        self._pending_events = deque()

        # Holds a ReturnedMessage object representing a message received via
        # Basic.Return in publisher-acknowledgments mode.
        self._puback_return = None

        # Receives Basic.ConsumeOk reply from server
        self._basic_consume_ok_result = _CallbackResult()

        # Receives the broker-inititated Channel.Close parameters
        self._channel_closed_by_broker_result = _CallbackResult(
            self._OnChannelClosedByBrokerArgs)

        # Receives args from Basic.GetEmpty response
        #  http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get
        self._basic_getempty_result = _CallbackResult(
            self._MethodFrameCallbackResultArgs)

        self._impl.add_on_cancel_callback(self._on_consumer_cancelled_by_broker)

        self._impl.add_callback(
            self._basic_consume_ok_result.signal_once,
            replies=[pika.spec.Basic.ConsumeOk],
            one_shot=False)

        self._impl.add_callback(
            self._on_channel_closed,
            replies=[pika.spec.Channel.Close],
            one_shot=True)

        self._impl.add_callback(
            self._basic_getempty_result.set_value_once,
            replies=[pika.spec.Basic.GetEmpty],
            one_shot=False)

        LOGGER.info("Created channel=%s", self.channel_number)

    def __int__(self):
        """Return the channel object as its channel number

        NOTE: inherited from legacy BlockingConnection; might be error-prone;
        use `channel_number` property instead.

        :rtype: int

        """
        return self.channel_number

    def __repr__(self):
        return '<%s impl=%r>' % (self.__class__.__name__, self._impl)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        try:
            self.close()
        except exceptions.ChannelClosed:
            pass

    def _cleanup(self):
        """Clean up members that might inhibit garbage collection"""
        self._message_confirmation_result.reset()
        self._pending_events = deque()
        self._consumer_infos = dict()

    @property
    def channel_number(self):
        """Channel number"""
        return self._impl.channel_number

    @property
    def connection(self):
        """The channel's BlockingConnection instance"""
        return self._connection

    @property
    def is_closed(self):
        """Returns True if the channel is closed.

        :rtype: bool

        """
        return self._impl.is_closed

    @property
    def is_closing(self):
        """Returns True if client-initiated closing of the channel is in
        progress.

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
        if self.is_closed:
            raise exceptions.ChannelClosed()

        if not waiters:
            waiters = self._ALWAYS_READY_WAITERS

        self._connection._flush_output(
            self._channel_closed_by_broker_result.is_ready,
            *waiters)

        if self._channel_closed_by_broker_result:
            # Channel was force-closed by broker
            self._cleanup()
            method = (
                self._channel_closed_by_broker_result.value.method_frame.method)
            raise exceptions.ChannelClosed(method.reply_code, method.reply_text)

    def _on_puback_message_returned(self, channel, method, properties, body):
        """Called as the result of Basic.Return from broker in
        publisher-acknowledgements mode. Saves the info as a ReturnedMessage
        instance in self._puback_return.

        :param pika.Channel channel: our self._impl channel
        :param pika.spec.Basic.Return method:
        :param pika.spec.BasicProperties properties: message properties
        :param body: returned message body; empty string if no body
        :type body: str, unicode

        """
        assert channel is self._impl, (
            channel.channel_number, self.channel_number)

        assert isinstance(method, pika.spec.Basic.Return), method
        assert isinstance(properties, pika.spec.BasicProperties), (
            properties)

        LOGGER.warning(
            "Published message was returned: _delivery_confirmation=%s; "
            "channel=%s; method=%r; properties=%r; body_size=%d; "
            "body_prefix=%.255r", self._delivery_confirmation,
            channel.channel_number, method, properties,
            len(body) if body is not None else None, body)

        self._puback_return = ReturnedMessage(method, properties, body)


    def _add_pending_event(self, evt):
        """Append an event to the channel's list of events that are ready for
        dispatch to user and signal our connection that this channel is ready
        for event dispatch

        :param _ChannelPendingEvt evt: an event derived from _ChannelPendingEvt
        """
        self._pending_events.append(evt)
        self.connection._request_channel_dispatch(self.channel_number)


    def _on_channel_closed(self, method_frame):
        """Called by impl when a channel is closed by the broker
        via Channel.Close

        :param pika.Channel channel: channel closed by the
            `spec.Channel.Close` method
        :param int reply_code: The reply code sent via Channel.Close
        :param str reply_text: The reply text sent via Channel.Close

        """
        LOGGER.debug('_on_channel_closed_by_broker %s', method_frame)
        self._channel_closed_by_broker_result.set_value_once(method_frame)
        channel_number = method_frame.channel_number
        self.connection._request_channel_dispatch(-channel_number)
        self._cleanup()
        method = method_frame.method
        raise exceptions.ChannelClosed(method.reply_code,
                                       method.reply_text)

    def _on_consumer_cancelled_by_broker(self, method_frame):
        """Called by impl when broker cancels consumer via Basic.Cancel.

        This is a RabbitMQ-specific feature. The circumstances include deletion
        of queue being consumed as well as failure of a HA node responsible for
        the queue being consumed.

        :param pika.frame.Method method_frame: method frame with the
            `spec.Basic.Cancel` method

        """
        evt = _ConsumerCancellationEvt(method_frame)

        consumer = self._consumer_infos[method_frame.method.consumer_tag]

        # Don't interfere with client-initiated cancellation flow
        if not consumer.tearing_down:
            consumer.state = _ConsumerInfo.CANCELLED_BY_BROKER

        if consumer.alternate_event_sink is not None:
            consumer.alternate_event_sink(evt)
        else:
            self._add_pending_event(evt)

    def _on_consumer_message_delivery(self, _channel, method, properties, body):
        """Called by impl when a message is delivered for a consumer

        :param Channel channel: The implementation channel object
        :param spec.Basic.Deliver method:
        :param pika.spec.BasicProperties properties: message properties
        :param body: delivered message body; empty string if no body
        :type body: str, unicode, or bytes (python 3.x)

        """
        evt = _ConsumerDeliveryEvt(method, properties, body)

        consumer = self._consumer_infos[method.consumer_tag]

        if consumer.alternate_event_sink is not None:
            consumer.alternate_event_sink(evt)
        else:
            self._add_pending_event(evt)

    def _on_consumer_generator_event(self, evt):
        """Sink for the queue consumer generator's consumer events; append the
        event to queue consumer generator's pending events buffer.

        :param evt: an object of type _ConsumerDeliveryEvt or
          _ConsumerCancellationEvt
        """
        self._queue_consumer_generator.pending_events.append(evt)
        # Schedule termination of connection.process_data_events using a
        # negative channel number
        self.connection._request_channel_dispatch(-self.channel_number)

    def _cancel_all_consumers(self):
        """Cancel all consumers.

        NOTE: pending non-ackable messages will be lost; pending ackable
        messages will be rejected.

        """
        if self._consumer_infos:
            LOGGER.debug('Cancelling %i consumers', len(self._consumer_infos))

            if self._queue_consumer_generator is not None:
                # Cancel queue consumer generator
                self.cancel()

            # Cancel consumers created via basic_consume
            for consumer_tag in pika.compat.dictkeys(self._consumer_infos):
                self.basic_cancel(consumer_tag)

    def _dispatch_events(self):
        """Called by BlockingConnection to dispatch pending events.

        `BlockingChannel` schedules this callback via
        `BlockingConnection._request_channel_dispatch`
        """
        while self._pending_events:
            evt = self._pending_events.popleft()

            if type(evt) is _ConsumerDeliveryEvt:
                consumer_info = self._consumer_infos[evt.method.consumer_tag]
                consumer_info.on_message_callback(self, evt.method,
                                                  evt.properties, evt.body)

            elif type(evt) is _ConsumerCancellationEvt:
                del self._consumer_infos[evt.method_frame.method.consumer_tag]

                self._impl.callbacks.process(self.channel_number,
                                             self._CONSUMER_CANCELLED_CB_KEY,
                                             self,
                                             evt.method_frame)
            else:
                evt.dispatch()


    def close(self, reply_code=0, reply_text="Normal shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """
        LOGGER.debug('Channel.close(%s, %s)', reply_code, reply_text)

        # Cancel remaining consumers
        self._cancel_all_consumers()

        # Close the channel
        try:
            with _CallbackResult() as close_ok_result:
                self._impl.add_callback(callback=close_ok_result.signal_once,
                                        replies=[pika.spec.Channel.CloseOk],
                                        one_shot=True)

                self._impl.close(reply_code=reply_code, reply_text=reply_text)
                self._flush_output(close_ok_result.is_ready)
        finally:
            self._cleanup()

    def flow(self, active):
        """Turn Channel flow control off and on.

        NOTE: RabbitMQ doesn't support active=False; per
        https://www.rabbitmq.com/specification.html: "active=false is not
        supported by the server. Limiting prefetch with basic.qos provides much
        better control"

        For more information, please reference:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#channel.flow

        :param bool active: Turn flow on (True) or off (False)

        :returns: True if broker will start or continue sending; False if not
        :rtype: bool

        """
        with _CallbackResult(self._FlowOkCallbackResultArgs) as flow_ok_result:
            self._impl.flow(active=active,
                            callback=flow_ok_result.set_value_once)
            self._flush_output(flow_ok_result.is_ready)
            return flow_ok_result.value.active

    def add_on_cancel_callback(self, callback):
        """Pass a callback function that will be called when Basic.Cancel
        is sent by the broker. The callback function should receive a method
        frame parameter.

        :param callable callback: a callable for handling broker's Basic.Cancel
            notification with the call signature: callback(method_frame)
            where method_frame is of type `pika.frame.Method` with method of
            type `spec.Basic.Cancel`

        """
        self._impl.callbacks.add(self.channel_number,
                                 self._CONSUMER_CANCELLED_CB_KEY,
                                 callback,
                                 one_shot=False)

    def add_on_return_callback(self, callback):
        """Pass a callback function that will be called when a published
        message is rejected and returned by the server via `Basic.Return`.

        :param callable callback: The method to call on callback with the
            signature callback(channel, method, properties, body), where
            channel: pika.Channel
            method: pika.spec.Basic.Return
            properties: pika.spec.BasicProperties
            body: str, unicode, or bytes (python 3.x)

        """
        self._impl.add_on_return_callback(
            lambda _channel, method, properties, body: (
                self._add_pending_event(
                    _ReturnedMessageEvt(
                        callback, self, method, properties, body))))

    def basic_consume(self,
                      queue,
                      on_message_callback,
                      no_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None):
        """Sends the AMQP command Basic.Consume to the broker and binds messages
        for the consumer_tag to the consumer callback. If you do not pass in
        a consumer_tag, one will be automatically generated for you. Returns
        the consumer tag.

        NOTE: the consumer callbacks are dispatched only in the scope of
        specially-designated methods: see
        `BlockingConnection.process_data_events` and
        `BlockingChannel.start_consuming`.

        For more information about Basic.Consume, see:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param queue: The queue from which to consume
        :type queue: str or unicode
        :param callable on_message_callback: Required function for dispatching messages
            to user, having the signature:
            on_message_callback(channel, method, properties, body)
                channel: BlockingChannel
                method: spec.Basic.Deliver
                properties: spec.BasicProperties
                body: str or unicode
        :param bool no_ack: Tell the broker to not expect a response (i.e.,
          no ack/nack)
        :param bool exclusive: Don't allow other consumers on the queue
        :param consumer_tag: You may specify your own consumer tag; if left
          empty, a consumer tag will be generated automatically
        :type consumer_tag: str or unicode
        :param dict arguments: Custom key/value pair arguments for the consumer
        :returns: consumer tag
        :rtype: str

        :raises pika.exceptions.DuplicateConsumerTag: if consumer with given
            consumer_tag is already present.

        """
        if not callable(on_message_callback):
            raise ValueError('callback on_message_callback must be callable; got %r'
                             % on_message_callback)

        return self._basic_consume_impl(
            queue=queue,
            on_message_callback=on_message_callback,
            no_ack=no_ack,
            exclusive=exclusive,
            consumer_tag=consumer_tag,
            arguments=arguments)

    def _basic_consume_impl(self,
                            queue,
                            no_ack,
                            exclusive,
                            consumer_tag,
                            arguments=None,
                            on_message_callback=None,
                            alternate_event_sink=None):
        """The low-level implementation used by `basic_consume` and `consume`.
        See `basic_consume` docstring for more info.

        NOTE: exactly one of on_message_callback/alternate_event_sink musts be
        non-None.

        This method has one additional parameter alternate_event_sink over the
        args described in `basic_consume`.

        :param callable alternate_event_sink: if specified, _ConsumerDeliveryEvt
            and _ConsumerCancellationEvt objects will be diverted to this
            callback instead of being deposited in the channel's
            `_pending_events` container. Signature:
            alternate_event_sink(evt)

        :raises pika.exceptions.DuplicateConsumerTag: if consumer with given
            consumer_tag is already present.

        """
        if (on_message_callback is None) == (alternate_event_sink is None):
            raise ValueError(
                ('exactly one of on_message_callback/alternate_event_sink must '
                 'be non-None', on_message_callback, alternate_event_sink))

        if not consumer_tag:
            # Need a consumer tag to register consumer info before sending
            # request to broker, because I/O might dispatch incoming messages
            # immediately following Basic.Consume-ok before _flush_output
            # returns
            consumer_tag = self._impl._generate_consumer_tag()

        if consumer_tag in self._consumer_infos:
            raise exceptions.DuplicateConsumerTag(consumer_tag)

        # Create new consumer
        self._consumer_infos[consumer_tag] = _ConsumerInfo(
            consumer_tag,
            no_ack=no_ack,
            on_message_callback=on_message_callback,
            alternate_event_sink=alternate_event_sink)

        try:
            with self._basic_consume_ok_result as ok_result:
                tag = self._impl.basic_consume(
                    on_message_callback=self._on_consumer_message_delivery,
                    queue=queue,
                    no_ack=no_ack,
                    exclusive=exclusive,
                    consumer_tag=consumer_tag,
                    arguments=arguments)

                assert tag == consumer_tag, (tag, consumer_tag)

                self._flush_output(ok_result.is_ready)
        except Exception:
            # If channel was closed, self._consumer_infos will be empty
            if consumer_tag in self._consumer_infos:
                del self._consumer_infos[consumer_tag]
                # Schedule termination of connection.process_data_events using a
                # negative channel number
                self.connection._request_channel_dispatch(-self.channel_number)
            raise

        # NOTE: Consumer could get cancelled by broker immediately after opening
        # (e.g., queue getting deleted externally)
        if self._consumer_infos[consumer_tag].setting_up:
            self._consumer_infos[consumer_tag].state = _ConsumerInfo.ACTIVE

        return consumer_tag

    def basic_cancel(self, consumer_tag):
        """This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply.

        NOTE: When cancelling a no_ack=False consumer, this implementation
        automatically Nacks and suppresses any incoming messages that have not
        yet been dispatched to the consumer's callback. However, when cancelling
        a no_ack=True consumer, this method will return any pending messages
        that arrived before broker confirmed the cancellation.

        :param str consumer_tag: Identifier for the consumer; the result of
            passing a consumer_tag that was created on another channel is
            undefined (bad things will happen)

        :returns: (NEW IN pika 0.10.0) empty sequence for a no_ack=False
            consumer; for a no_ack=True consumer, returns a (possibly empty)
            sequence of pending messages that arrived before broker confirmed
            the cancellation (this is done instead of via consumer's callback in
            order to prevent reentrancy/recursion. Each message is four-tuple:
            (channel, method, properties, body)
                channel: BlockingChannel
                method: spec.Basic.Deliver
                properties: spec.BasicProperties
                body: str or unicode
        """
        try:
            consumer_info = self._consumer_infos[consumer_tag]
        except KeyError:
            LOGGER.warning(
                "User is attempting to cancel an unknown consumer=%s; "
                "already cancelled by user or broker?", consumer_tag)
            return []

        try:
            # Assertion failure here is most likely due to reentrance
            assert consumer_info.active or consumer_info.cancelled_by_broker, (
                consumer_info.state)

            # Assertion failure here signals disconnect between consumer state
            # in BlockingChannel and Channel
            assert (consumer_info.cancelled_by_broker or
                    consumer_tag in self._impl._consumers), consumer_tag

            no_ack = consumer_info.no_ack

            consumer_info.state = _ConsumerInfo.TEARING_DOWN

            with _CallbackResult() as cancel_ok_result:
                # Nack pending messages for no_ack=False consumer
                if not no_ack:
                    pending_messages = self._remove_pending_deliveries(
                        consumer_tag)
                    if pending_messages:
                        # NOTE: we use impl's basic_reject to avoid the
                        # possibility of redelivery before basic_cancel takes
                        # control of nacking.
                        # NOTE: we can't use basic_nack with the multiple option
                        # to avoid nacking messages already held by our client.
                        for message in pending_messages:
                            self._impl.basic_reject(message.method.delivery_tag,
                                                    requeue=True)

                # Cancel the consumer; impl takes care of rejecting any
                # additional deliveries that arrive for a no_ack=False
                # consumer
                self._impl.basic_cancel(
                    consumer_tag=consumer_tag,
                    callback=cancel_ok_result.signal_once)

                # Flush output and wait for Basic.Cancel-ok or
                # broker-initiated Basic.Cancel
                self._flush_output(
                    cancel_ok_result.is_ready,
                    lambda: consumer_tag not in self._impl._consumers)

            if no_ack:
                # Return pending messages for no_ack=True consumer
                return [
                    (evt.method, evt.properties, evt.body)
                    for evt in self._remove_pending_deliveries(consumer_tag)]
            else:
                # impl takes care of rejecting any incoming deliveries during
                # cancellation
                messages = self._remove_pending_deliveries(consumer_tag)
                assert not messages, messages

                return []
        finally:
            # NOTE: The entry could be purged if channel or connection closes
            if consumer_tag in self._consumer_infos:
                del self._consumer_infos[consumer_tag]
                # Schedule termination of connection.process_data_events using a
                # negative channel number
                self.connection._request_channel_dispatch(-self.channel_number)

    def _remove_pending_deliveries(self, consumer_tag):
        """Extract _ConsumerDeliveryEvt objects destined for the given consumer
        from pending events, discarding the _ConsumerCancellationEvt, if any

        :param str consumer_tag:

        :returns: a (possibly empty) sequence of _ConsumerDeliveryEvt destined
            for the given consumer tag
        """
        remaining_events = deque()
        unprocessed_messages = []
        while self._pending_events:
            evt = self._pending_events.popleft()
            if type(evt) is _ConsumerDeliveryEvt:
                if evt.method.consumer_tag == consumer_tag:
                    unprocessed_messages.append(evt)
                    continue
            if type(evt) is _ConsumerCancellationEvt:
                if evt.method_frame.method.consumer_tag == consumer_tag:
                    # A broker-initiated Basic.Cancel must have arrived
                    # before our cancel request completed
                    continue

            remaining_events.append(evt)

        self._pending_events = remaining_events

        return unprocessed_messages

    def start_consuming(self):
        """Processes I/O events and dispatches timers and `basic_consume`
        callbacks until all consumers are cancelled.

        NOTE: this blocking function may not be called from the scope of a
        pika callback, because dispatching `basic_consume` callbacks from this
        context would constitute recursion.

        :raises pika.exceptions.RecursionError: if called from the scope of a
            `BlockingConnection` or `BlockingChannel` callback

        """
        # Check if called from the scope of an event dispatch callback
        with self.connection._acquire_event_dispatch() as dispatch_allowed:
            if not dispatch_allowed:
                raise exceptions.RecursionError(
                    'start_consuming may not be called from the scope of '
                    'another BlockingConnection or BlockingChannel callback')

        # Process events as long as consumers exist on this channel
        while self._consumer_infos:
            self.connection.process_data_events(time_limit=None)

    def stop_consuming(self, consumer_tag=None):
        """ Cancels all consumers, signalling the `start_consuming` loop to
        exit.

        NOTE: pending non-ackable messages will be lost; pending ackable
        messages will be rejected.

        """
        if consumer_tag:
            self.basic_cancel(consumer_tag)
        else:
            self._cancel_all_consumers()

    def consume(self, queue, no_ack=False,
                exclusive=False, arguments=None,
                inactivity_timeout=None):
        """Blocking consumption of a queue instead of via a callback. This
        method is a generator that yields each message as a tuple of method,
        properties, and body. The active generator iterator terminates when the
        consumer is cancelled by client or broker.

        Example:

            for method, properties, body in channel.consume('queue'):
                print body
                channel.basic_ack(method.delivery_tag)

        You should call `BlockingChannel.cancel()` when you escape out of the
        generator loop.

        If you don't cancel this consumer, then next call on the same channel
        to `consume()` with the exact same (queue, no_ack, exclusive) parameters
        will resume the existing consumer generator; however, calling with
        different parameters will result in an exception.

        :param queue: The queue name to consume
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a ack/nack response
        :param bool exclusive: Don't allow other consumers on the queue
        :param dict arguments: Custom key/value pair arguments for the consumer
        :param float inactivity_timeout: if a number is given (in
            seconds), will cause the method to yield (None, None, None) after the
            given period of inactivity; this permits for pseudo-regular maintenance
            activities to be carried out by the user while waiting for messages
            to arrive. If None is given (default), then the method blocks until
            the next event arrives. NOTE that timing granularity is limited by
            the timer resolution of the underlying implementation.
            NEW in pika 0.10.0.

        :yields: tuple(spec.Basic.Deliver, spec.BasicProperties, str or unicode)

        :raises ValueError: if consumer-creation parameters don't match those
            of the existing queue consumer generator, if any.
            NEW in pika 0.10.0
        """
        params = (queue, no_ack, exclusive)

        if self._queue_consumer_generator is not None:
            if params != self._queue_consumer_generator.params:
                raise ValueError(
                    'Consume with different params not allowed on existing '
                    'queue consumer generator; previous params: %r; '
                    'new params: %r'
                    % (self._queue_consumer_generator.params,
                       (queue, no_ack, exclusive)))
        else:
            LOGGER.debug('Creating new queue consumer generator; params: %r',
                         params)
            # Need a consumer tag to register consumer info before sending
            # request to broker, because I/O might pick up incoming messages
            # in addition to Basic.Consume-ok
            consumer_tag = self._impl._generate_consumer_tag()

            self._queue_consumer_generator = _QueueConsumerGeneratorInfo(
                params,
                consumer_tag)

            try:
                self._basic_consume_impl(
                    queue=queue,
                    no_ack=no_ack,
                    exclusive=exclusive,
                    consumer_tag=consumer_tag,
                    arguments=arguments,
                    alternate_event_sink=self._on_consumer_generator_event)
            except Exception:
                self._queue_consumer_generator = None
                raise

            LOGGER.info('Created new queue consumer generator %r',
                        self._queue_consumer_generator)

        while self._queue_consumer_generator is not None:
            if self._queue_consumer_generator.pending_events:
                evt = self._queue_consumer_generator.pending_events.popleft()
                if type(evt) is _ConsumerCancellationEvt:
                    # Consumer was cancelled by broker
                    self._queue_consumer_generator = None
                    break
                else:
                    yield (evt.method, evt.properties, evt.body)
                    continue

            # Wait for a message to arrive
            if inactivity_timeout is None:
                self.connection.process_data_events(time_limit=None)
                continue

            # Wait with inactivity timeout
            wait_start_time = time.time()
            wait_deadline = wait_start_time + inactivity_timeout
            delta = inactivity_timeout

            while (self._queue_consumer_generator is not None and
                   not self._queue_consumer_generator.pending_events):
                self.connection.process_data_events(time_limit=delta)

                if not self._queue_consumer_generator:
                    # Consumer was cancelled by client
                    break

                if self._queue_consumer_generator.pending_events:
                    # Got message(s)
                    break

                delta = wait_deadline - time.time()
                if delta <= 0.0:
                    # Signal inactivity timeout
                    yield (None, None, None)
                    break

    def get_waiting_message_count(self):
        """Returns the number of messages that may be retrieved from the current
        queue consumer generator via `BlockingChannel.consume` without blocking.
        NEW in pika 0.10.0

        :rtype: int
        """
        if self._queue_consumer_generator is not None:
            pending_events = self._queue_consumer_generator.pending_events
            count = len(pending_events)
            if count and type(pending_events[-1]) is _ConsumerCancellationEvt:
                count -= 1
        else:
            count = 0

        return count

    def cancel(self):
        """Cancel the queue consumer created by `BlockingChannel.consume`,
        rejecting all pending ackable messages.

        NOTE: If you're looking to cancel a consumer issued with
        BlockingChannel.basic_consume then you should call
        BlockingChannel.basic_cancel.

        :return int: The number of messages requeued by Basic.Nack.
            NEW in 0.10.0: returns 0

        """
        if self._queue_consumer_generator is None:
            LOGGER.warning('cancel: queue consumer generator is inactive '
                           '(already cancelled by client or broker?)')
            return 0

        try:
            _, no_ack, _ = self._queue_consumer_generator.params
            if not no_ack:
                # Reject messages held by queue consumer generator; NOTE: we
                # can't use basic_nack with the multiple option to avoid nacking
                # messages already held by our client.
                pending_events = self._queue_consumer_generator.pending_events
                for _ in compat.xrange(self.get_waiting_message_count()):
                    evt = pending_events.popleft()
                    self._impl.basic_reject(evt.method.delivery_tag,
                                            requeue=True)

            self.basic_cancel(self._queue_consumer_generator.consumer_tag)
        finally:
            self._queue_consumer_generator = None

        # Return 0 for compatibility with legacy implementation; the number of
        # nacked messages is not meaningful since only messages consumed with
        # no_ack=False may be nacked, and those arriving after calling
        # basic_cancel will be rejected automatically by impl channel, so we'll
        # never know how many of those were nacked.
        return 0

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

    def basic_get(self, queue, no_ack=False):
        """Get a single message from the AMQP broker. Returns a sequence with
        the method frame, message properties, and body.

        :param queue: Name of queue from which to get a message
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a reply
        :returns: a three-tuple; (None, None, None) if the queue was empty;
            otherwise (method, properties, body); NOTE: body may be None
        :rtype: (None, None, None)|(spec.Basic.GetOk,
                                    spec.BasicProperties,
                                    str or unicode or None)
        """
        assert not self._basic_getempty_result
        # NOTE: nested with for python 2.6 compatibility
        with _CallbackResult(self._RxMessageArgs) as get_ok_result:
            with self._basic_getempty_result:
                self._impl.basic_get(queue=queue,
                                     no_ack=no_ack,
                                     callback=get_ok_result.set_value_once)
                self._flush_output(get_ok_result.is_ready,
                                   self._basic_getempty_result.is_ready)
                if get_ok_result:
                    evt = get_ok_result.value
                    return evt.method, evt.properties, evt.body
                else:
                    assert self._basic_getempty_result, (
                        "wait completed without GetOk and GetEmpty")
                    return None, None, None

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """Publish to the channel with the given exchange, routing key and body.
        Returns a boolean value indicating the success of the operation.

        This is the legacy BlockingChannel method for publishing. See also
        `BlockingChannel.publish` that provides more information about failures.

        For more information on basic_publish and what the parameters do, see:

            http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        NOTE: mandatory and immediate may be enabled even without delivery
          confirmation, but in the absence of delivery confirmation the
          synchronous implementation has no way to know how long to wait for
          the Basic.Return or lack thereof.

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body; empty string if no body
        :type body: str or unicode
        :param pika.spec.BasicProperties properties: message properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        :returns: True if delivery confirmation is not enabled (NEW in pika
            0.10.0); otherwise returns False if the message could not be
            delivered (Basic.nack and/or Basic.Return) and True if the message
            was delivered (Basic.ack and no Basic.Return)
        """
        try:
            self.publish(exchange, routing_key, body, properties,
                         mandatory, immediate)
        except (exceptions.NackError, exceptions.UnroutableError):
            return False
        else:
            return True

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
        :param body: The message body; empty string if no body
        :type body: str or unicode
        :param pika.spec.BasicProperties properties: message properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        :raises UnroutableError: raised when a message published in
            publisher-acknowledgments mode (see
            `BlockingChannel.confirm_delivery`) is returned via `Basic.Return`
            followed by `Basic.Ack`.
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
                    LOGGER.warning(
                        "Message was Nack'ed by broker: nack=%r; channel=%s; "
                        "exchange=%s; routing_key=%s; mandatory=%r; "
                        "immediate=%r", conf_method, self.channel_number,
                        exchange, routing_key, mandatory, immediate)
                    if self._puback_return is not None:
                        returned_messages = [self._puback_return]
                        self._puback_return = None
                    else:
                        returned_messages = []
                    raise exceptions.NackError(returned_messages)

                else:
                    assert isinstance(conf_method, pika.spec.Basic.Ack), (
                        conf_method)

                    if self._puback_return is not None:
                        # Unroutable message was returned
                        messages = [self._puback_return]
                        self._puback_return = None
                        raise exceptions.UnroutableError(messages)
        else:
            # In non-publisher-acknowledgments mode
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
                                   no-ack option is set in the consumer.
        :param int prefetch_count: Specifies a prefetch window in terms of whole
                                   messages. This field may be used in
                                   combination with the prefetch-size field; a
                                   message will only be sent in advance if both
                                   prefetch windows (and those at the channel
                                   and connection level) allow it. The
                                   prefetch-count is ignored if the no-ack
                                   option is set in the consumer.
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
            self._impl.basic_recover(requeue=requeue,
                                     callback=recover_ok_result.signal_once)
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
        """
        if self._delivery_confirmation:
            LOGGER.error('confirm_delivery: confirmation was already enabled '
                         'on channel=%s', self.channel_number)
            return

        with _CallbackResult() as select_ok_result:
            self._impl.confirm_delivery(
                ack_nack_callback=self._message_confirmation_result.set_value_once,
                callback=select_ok_result.signal_once)

            self._flush_output(select_ok_result.is_ready)

        self._delivery_confirmation = True

        # Unroutable messages returned after this point will be in the context
        # of publisher acknowledgments
        self._impl.add_on_return_callback(self._on_puback_message_returned)

    def exchange_declare(self, exchange=None,
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False,
                         arguments=None):
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

        :returns: Method frame from the Exchange.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.DeclareOk`

        """
        with _CallbackResult(
            self._MethodFrameCallbackResultArgs) as declare_ok_result:
            self._impl.exchange_declare(
                exchange=exchange,
                exchange_type=exchange_type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                arguments=arguments,
                callback=declare_ok_result.set_value_once)

            self._flush_output(declare_ok_result.is_ready)
            return declare_ok_result.value.method_frame

    def exchange_delete(self, exchange=None, if_unused=False):
        """Delete the exchange.

        :param exchange: The exchange name
        :type exchange: str or unicode
        :param bool if_unused: only delete if the exchange is unused

        :returns: Method frame from the Exchange.Delete-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.DeleteOk`

        """
        with _CallbackResult(
            self._MethodFrameCallbackResultArgs) as delete_ok_result:
            self._impl.exchange_delete(
                exchange=exchange,
                if_unused=if_unused,
                callback=delete_ok_result.set_value_once)

            self._flush_output(delete_ok_result.is_ready)
            return delete_ok_result.value.method_frame

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

        :returns: Method frame from the Exchange.Bind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.BindOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                bind_ok_result:
            self._impl.exchange_bind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments,
                callback=bind_ok_result.set_value_once)

            self._flush_output(bind_ok_result.is_ready)
            return bind_ok_result.value.method_frame

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

        :returns: Method frame from the Exchange.Unbind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.UnbindOk`

        """
        with _CallbackResult(
            self._MethodFrameCallbackResultArgs) as unbind_ok_result:
            self._impl.exchange_unbind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments,
                callback=unbind_ok_result.set_value_once)

            self._flush_output(unbind_ok_result.is_ready)
            return unbind_ok_result.value.method_frame

    def queue_declare(self, queue, passive=False, durable=False,
                      exclusive=False, auto_delete=False,
                      arguments=None):
        """Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Use an empty string as the queue name for the broker to auto-generate
        one. Retrieve this auto-generated queue name from the returned
        `spec.Queue.DeclareOk` method frame.

        :param queue: The queue name
        :type queue: str or unicode; if empty string, the broker will create a
          unique queue name;
        :param bool passive: Only check to see if the queue exists
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param dict arguments: Custom key/value arguments for the queue

        :returns: Method frame from the Queue.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.DeclareOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                declare_ok_result:
            self._impl.queue_declare(
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments,
                callback=declare_ok_result.set_value_once)

            self._flush_output(declare_ok_result.is_ready)
            return declare_ok_result.value.method_frame

    def queue_delete(self, queue, if_unused=False, if_empty=False):
        """Delete a queue from the broker.

        :param queue: The queue to delete
        :type queue: str or unicode
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty

        :returns: Method frame from the Queue.Delete-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.DeleteOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                delete_ok_result:
            self._impl.queue_delete(queue=queue,
                                    if_unused=if_unused,
                                    if_empty=if_empty,
                                    callback=delete_ok_result.set_value_once)

            self._flush_output(delete_ok_result.is_ready)
            return delete_ok_result.value.method_frame

    def queue_purge(self, queue):
        """Purge all of the messages from the specified queue

        :param queue: The queue to purge
        :type  queue: str or unicode

        :returns: Method frame from the Queue.Purge-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.PurgeOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                purge_ok_result:
            self._impl.queue_purge(queue=queue,
                                   callback=purge_ok_result.set_value_once)
            self._flush_output(purge_ok_result.is_ready)
            return purge_ok_result.value.method_frame

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

        :returns: Method frame from the Queue.Bind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.BindOk`

        """
        with _CallbackResult(
            self._MethodFrameCallbackResultArgs) as bind_ok_result:
            self._impl.queue_bind(queue=queue,
                                  exchange=exchange,
                                  routing_key=routing_key,
                                  arguments=arguments,
                                  callback=bind_ok_result.set_value_once)
            self._flush_output(bind_ok_result.is_ready)
            return bind_ok_result.value.method_frame

    def queue_unbind(self, queue, exchange=None, routing_key=None,
                     arguments=None):
        """Unbind a queue from an exchange.

        :param queue: The queue to unbind from the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind from
        :type exchange: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Queue.Unbind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.UnbindOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                unbind_ok_result:
            self._impl.queue_unbind(queue=queue,
                                    exchange=exchange,
                                    routing_key=routing_key,
                                    arguments=arguments,
                                    callback=unbind_ok_result.set_value_once)
            self._flush_output(unbind_ok_result.is_ready)
            return unbind_ok_result.value.method_frame

    def tx_select(self):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        :returns: Method frame from the Tx.Select-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Tx.SelectOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                select_ok_result:
            self._impl.tx_select(select_ok_result.set_value_once)

            self._flush_output(select_ok_result.is_ready)
            return select_ok_result.value.method_frame

    def tx_commit(self):
        """Commit a transaction.

        :returns: Method frame from the Tx.Commit-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Tx.CommitOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                commit_ok_result:
            self._impl.tx_commit(commit_ok_result.set_value_once)

            self._flush_output(commit_ok_result.is_ready)
            return commit_ok_result.value.method_frame

    def tx_rollback(self):
        """Rollback a transaction.

        :returns: Method frame from the Tx.Commit-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Tx.CommitOk`

        """
        with _CallbackResult(self._MethodFrameCallbackResultArgs) as \
                rollback_ok_result:
            self._impl.tx_rollback(rollback_ok_result.set_value_once)

            self._flush_output(rollback_ok_result.is_ready)
            return rollback_ok_result.value.method_frame
