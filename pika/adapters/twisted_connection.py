"""Using Pika with a Twisted reactor.

The interfaces in this module are Deferred-based when possible. This means that
the connection.channel() method and most of the channel methods return
Deferreds instead of taking a callback argument and that basic_consume()
returns a Twisted DeferredQueue where messages from the server will be
stored. Refer to the docstrings for TwistedProtocolConnection.channel() and the
TwistedChannel class for details.

"""

import functools
import logging

from twisted.internet import (
    defer, error as twisted_error, reactor, protocol)

import pika.connection
from pika import exceptions
from pika.adapters.utils import nbio_interface
from pika.adapters.utils.io_services_utils import check_callback_arg

# Twistisms
# pylint: disable=C0111,C0103

LOGGER = logging.getLogger(__name__)


class ClosableDeferredQueue(defer.DeferredQueue):
    """
    Like the normal Twisted DeferredQueue, but after close() is called with an
    exception instance all pending Deferreds are errbacked and further attempts
    to call get() or put() return a Failure wrapping that exception.
    """

    def __init__(self, size=None, backlog=None):
        self.closed = None
        super(ClosableDeferredQueue, self).__init__(size, backlog)

    def put(self, obj):
        """
        Like the original :meth:`DeferredQueue.put` method, but returns an
        errback if the queue is closed.

        """
        if self.closed:
            return defer.fail(self.closed)
        return defer.DeferredQueue.put(self, obj)

    def get(self):
        """
        Returns a Deferred that will fire with the next item in the queue, when
        it's available.

        The Deferred will errback if the queue is closed.

        :return: Deferred that fires with the next item.
        :rtype: Deferred

        """
        if self.closed:
            return defer.fail(self.closed)
        return defer.DeferredQueue.get(self)

    def close(self, reason):
        """Closes the queue.

        Errback the pending calls to :meth:`get()`.

        """
        self.closed = reason
        while self.waiting:
            self.waiting.pop().errback(reason)
        self.pending = []


class TwistedChannel(object):
    """A wrapper around Pika's Channel.

    Channel methods that normally take a callback argument are wrapped to
    return a Deferred that fires with whatever would be passed to the callback.
    If the channel gets closed, all pending Deferreds are errbacked with a
    ChannelClosed exception. The returned Deferreds fire with whatever
    arguments the callback to the original method would receive.

    The basic_consume method is wrapped in a special way, see its docstring for
    details.
    """

    WRAPPED_METHODS = ('exchange_declare', 'exchange_delete', 'queue_declare',
                       'queue_bind', 'queue_purge', 'queue_unbind', 'basic_qos',
                       'basic_get', 'basic_recover', 'tx_select', 'tx_commit',
                       'tx_rollback', 'flow', 'basic_cancel')

    def __init__(self, channel):
        self._channel = channel
        self._closed = None
        self._calls = set()
        self._consumers = {}

        channel.add_on_close_callback(self.channel_closed)

    def channel_closed(self, _channel, reason):
        # enter the closed state
        self._closed = reason
        # errback all pending calls
        for d in self._calls:
            d.errback(self._closed)
        # close all open queues
        for consumers in self._consumers.values():
            for c in consumers:
                c.close(self._closed)
        # release references to stored objects
        self._calls = set()
        self._consumers = {}

    def basic_consume(self,
                      queue,
                      auto_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None):
        """Consume from a server queue.

        Sends the AMQP 0-9-1 command Basic.Consume to the broker and binds
        messages for the consumer_tag to a
        :class:`ClosableDeferredQueue`. If you do not pass in a
        consumer_tag, one will be automatically generated for you.

        For more information on basic_consume, see:
        Tutorial 2 at http://www.rabbitmq.com/getstarted.html
        http://www.rabbitmq.com/confirms.html
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param queue: The queue to consume from. Use the empty string to
            specify the most recent server-named queue for this channel.
        :type queue: str or unicode
        :param bool auto_ack: if set to True, automatic acknowledgement mode
            will be used (see http://www.rabbitmq.com/confirms.html). This
            corresponds with the 'no_ack' parameter in the basic.consume AMQP
            0.9.1 method
        :param bool exclusive: Don't allow other consumers on the queue
        :param consumer_tag: Specify your own consumer tag
        :type consumer_tag: str or unicode
        :param dict arguments: Custom key/value pair arguments for the consumer
        :return: Deferred that fires with a tuple
            ``(queue_object, consumer_tag)``.  The queue object is an instance
            of :class:`ClosableDeferredQueue`, where data received from
            the queue will be stored. Clients should use its
            :meth:`get() <ClosableDeferredQueue.get>` method to fetch an
            individual message, which will return a Deferred firing with the
            tuple ``(channel, method, properties, body)``, where:
             - channel: pika.Channel
             - method: pika.spec.Basic.Deliver
             - properties: pika.spec.BasicProperties
             - body: str, unicode, or bytes (python 3.x)
        :rtype: Deferred

        """
        if self._closed:
            return defer.fail(self._closed)

        queue_obj = ClosableDeferredQueue()
        self._consumers.setdefault(queue, set()).add(queue_obj)
        d = defer.Deferred()

        try:
            self._channel.basic_consume(
                queue=queue,
                on_message_callback=lambda *args: queue_obj.put(args),
                auto_ack=auto_ack,
                exclusive=exclusive,
                consumer_tag=consumer_tag,
                arguments=arguments,
                callback=lambda frame: d.callback(
                    (queue_obj, frame.method.consumer_tag)
                ),
            )
        except Exception:  # pylint: disable-msg=W0703
            return defer.fail()

        return d

    def queue_delete(self, *args, **kwargs):
        """Wraps the method the same way all the others are wrapped, but removes
        the reference to the queue object after it gets deleted on the server.

        See :meth:`Channel.queue_delete <pika.channel.Channel.queue_delete>`.

        """
        wrapped = self._wrap_channel_method('queue_delete')
        queue_name = kwargs['queue']

        d = wrapped(*args, **kwargs)
        return d.addCallback(self._clear_consumer, queue_name)

    def basic_publish(self, *args, **kwargs):
        """Make sure the channel is not closed and then publish. Return a
        Deferred that fires with the result of the channel's basic_publish.

        See :meth:`Channel.basic_publish <pika.channel.Channel.basic_publish>`.

        """
        if self._closed:
            return defer.fail(self._closed)
        return defer.succeed(self._channel.basic_publish(*args, **kwargs))

    def _wrap_channel_method(self, name):
        """Wrap Pika's Channel method to make it return a Deferred that fires
        when the method completes and errbacks if the channel gets closed. If
        the original method's callback would receive more than one argument, the
        Deferred fires with a tuple of argument values.

        """
        method = getattr(self._channel, name)

        @functools.wraps(method)
        def wrapped(*args, **kwargs):
            if self._closed:
                return defer.fail(self._closed)

            d = defer.Deferred()
            self._calls.add(d)
            d.addCallback(self._clear_call, d)

            def single_argument(*args):
                """
                Make sure that the deferred is called with a single argument.
                In case the original callback fires with more than one, convert
                to a tuple.
                """
                if len(args) > 1:
                    d.callback(tuple(args))
                else:
                    d.callback(*args)

            kwargs['callback'] = single_argument

            try:
                method(*args, **kwargs)
            except Exception:  # pylint: disable-msg=W0703
                return defer.fail()
            return d

        return wrapped

    def _clear_consumer(self, ret, queue_name):
        self._consumers.pop(queue_name, None)
        return ret

    def _clear_call(self, ret, d):
        self._calls.discard(d)
        return ret

    def __getattr__(self, name):
        # Wrap methods defined in WRAPPED_METHODS, forward the rest of accesses
        # to the channel.
        if name in self.WRAPPED_METHODS:
            return self._wrap_channel_method(name)
        return getattr(self._channel, name)


class _TwistedConnectionAdapter(pika.connection.Connection):
    """A Twisted-specific implementation of a Pika Connection.

    NOTE: since `base_connection.BaseConnection`'s primary responsibility is
    management of the transport, we use `pika.connection.Connection` directly as
    our base class because this adapter uses a different transport management
    strategy.

    """
    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 custom_reactor=None):
        super(_TwistedConnectionAdapter, self).__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback,
            internal_connection_workflow=False)

        self._reactor = custom_reactor or reactor
        self._transport = None  # to be provided by `connection_made()`

    def _adapter_add_timeout(self, deadline, callback):
        """Implement
        :py:meth:`pika.connection.Connection._adapter_add_timeout()`.

        """
        check_callback_arg(callback, 'callback')
        return _TimerHandle(self._reactor.callLater(deadline, callback))

    def _adapter_remove_timeout(self, timeout_id):
        """Implement
        :py:meth:`pika.connection.Connection._adapter_remove_timeout()`.

        """
        timeout_id.cancel()

    def _adapter_add_callback_threadsafe(self, callback):
        """Implement
        :py:meth:`pika.connection.Connection._adapter_add_callback_threadsafe()`.

        """
        check_callback_arg(callback, 'callback')
        self._reactor.callFromThread(callback)

    def _adapter_connect_stream(self):
        """Implement pure virtual
        :py:ref:meth:`pika.connection.Connection._adapter_connect_stream()`
         method.

        NOTE: This should not be called due to our initialization of Connection
        via `internal_connection_workflow=False`
        """
        raise NotImplementedError

    def _adapter_disconnect_stream(self):
        """Implement pure virtual
        :py:ref:meth:`pika.connection.Connection._adapter_disconnect_stream()`
         method.

        """
        self._transport.abort()

    def _adapter_emit_data(self, data):
        """Implement pure virtual
        :py:ref:meth:`pika.connection.Connection._adapter_emit_data()` method.

        """
        self._transport.write(data)

    def _adapter_get_write_buffer_size(self):
        """Implement pure virtual
        :py:ref:meth:`pika.connection.Connection._adapter_emit_data()` method.

        This method only belongs in SelectConnection, no others need it
        and twisted transport doesn't expose it.
        """
        raise NotImplementedError

    def connection_made(self, transport):
        """Introduces transport to protocol after transport is connected.

        :param twisted.internet.interfaces.ITransport transport:
        :raises Exception: Exception-based exception on error

        """
        self._transport = transport
        # Let connection know that stream is available
        self._on_stream_connected()

    def connection_lost(self, error):
        """Called upon loss or closing of TCP connection.

        NOTE: `connection_made()` and `connection_lost()` are each called just
        once and in that order. All other callbacks are called between them.

        :param BaseException | None error: An exception (check for
            `BaseException`) indicates connection failure. None indicates that
            connection was closed on this side, such as when it's aborted.
        :raises Exception: Exception-based exception on error

        """
        self._transport = None
        LOGGER.log(logging.DEBUG if error is None else logging.ERROR,
                   'connection_lost: %r',
                   error)
        self._on_stream_terminated(error)

    def data_received(self, data):
        """Called to deliver incoming data from the server to the protocol.

        :param data: Non-empty data bytes.
        :raises Exception: Exception-based exception on error

        """
        self._on_data_available(data)


class TwistedProtocolConnection(protocol.Protocol):
    """A Pika-specific implementation of a Twisted Protocol. Allows using
    Twisted's non-blocking connectTCP/connectSSL methods for connecting to the
    server.

    It has one caveat: TwistedProtocolConnection objects have a ready
    instance variable that's a Deferred which fires when the connection is
    ready to be used (the initial AMQP handshaking has been done). You *have*
    to wait for this Deferred to fire before requesting a channel.

    Since it's Twisted handling connection establishing it does not accept
    connect callbacks, you have to implement that within Twisted. Also remember
    that the host, port and ssl values of the connection parameters are ignored
    because, yet again, it's Twisted who manages the connection.

    """
    def __init__(self,
                 parameters=None,
                 on_close_callback=None):

        self.ready = defer.Deferred()
        self._impl = _TwistedConnectionAdapter(
            parameters=parameters,
            on_open_callback=self.connectionReady,
            on_open_error_callback=self.connectionFailed,
            on_close_callback=on_close_callback,
        )

    def channel(self, channel_number=None):  # pylint: disable-msg=W0221
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        :param int channel_number: The channel number to use, defaults to the
                                   next available.
        :returns: a Deferred that fires with an instance of a wrapper around the
            Pika Channel class.
        :rtype: Deferred

        """
        d = defer.Deferred()
        self._impl.channel(channel_number, d.callback)
        return d.addCallback(TwistedChannel)

    # IProtocol methods

    def dataReceived(self, data):
        # Pass the bytes to Pika for parsing
        self._impl.data_received(data)

    def connectionLost(self, reason=protocol.connectionDone):
        self._impl.connection_lost(reason)
        # Let the caller know there's been an error
        d, self.ready = self.ready, None
        if d:
            d.errback(reason)

    def makeConnection(self, transport):
        self._impl.connection_made(transport)
        protocol.Protocol.makeConnection(self, transport)

    # Our own methods

    def connectionReady(self, _res):
        d, self.ready = self.ready, None
        if d:
            d.callback(self)

    def connectionFailed(self, _connection, _error_message=None):
        d, self.ready = self.ready, None
        if d:
            attempts = self._impl.params.connection_attempts
            exc = exceptions.AMQPConnectionError(attempts)
            d.errback(exc)


class _TimerHandle(nbio_interface.AbstractTimerReference):
    """This module's adaptation of `nbio_interface.AbstractTimerReference`.

    """

    def __init__(self, handle):
        """

        :param twisted.internet.base.DelayedCall handle:
        """
        self._handle = handle

    def cancel(self):
        if self._handle is not None:
            try:
                self._handle.cancel()
            except (twisted_error.AlreadyCalled,
                    twisted_error.AlreadyCancelled):
                pass

            self._handle = None
