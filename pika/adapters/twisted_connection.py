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

from twisted.internet import defer, error as twisted_error, reactor

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
        if self.closed:
            return defer.fail(self.closed)
        return defer.DeferredQueue.put(self, obj)

    def get(self):
        if self.closed:
            return defer.fail(self.closed)
        return defer.DeferredQueue.get(self)

    def close(self, reason):
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
        self.__channel = channel
        self.__closed = None
        self.__calls = set()
        self.__consumers = {}

        channel.add_on_close_callback(self.channel_closed)

    def channel_closed(self, _channel, reason):
        # enter the closed state
        self.__closed = reason
        # errback all pending calls
        for d in self.__calls:
            d.errback(self.__closed)
        # close all open queues
        for consumers in self.__consumers.values():
            for c in consumers:
                c.close(self.__closed)
        # release references to stored objects
        self.__calls = set()
        self.__consumers = {}

    def basic_consume(self, *args, **kwargs):
        """Consume from a server queue. Returns a Deferred that fires with a
        tuple: (queue_object, consumer_tag). The queue object is an instance of
        ClosableDeferredQueue, where data received from the queue will be
        stored. Clients should use its get() method to fetch individual
        message.
        """
        if self.__closed:
            return defer.fail(self.__closed)

        queue = ClosableDeferredQueue()
        queue_name = kwargs['queue']
        kwargs['on_message_callback'] = lambda *args: queue.put(args)
        self.__consumers.setdefault(queue_name, set()).add(queue)
        d = defer.Deferred()
        kwargs["callback"] = lambda frame: d.callback(
            (queue, frame.consumer_tag)
        )

        try:
            self.__channel.basic_consume(*args, **kwargs)
        except Exception:  # pylint: disable-msg=W0703
            return defer.fail()

        return d

    def queue_delete(self, *args, **kwargs):
        """Wraps the method the same way all the others are wrapped, but removes
        the reference to the queue object after it gets deleted on the server.

        """
        wrapped = self.__wrap_channel_method('queue_delete')
        queue_name = kwargs['queue']

        d = wrapped(*args, **kwargs)
        return d.addCallback(self.__clear_consumer, queue_name)

    def basic_publish(self, *args, **kwargs):
        """Make sure the channel is not closed and then publish. Return a
        Deferred that fires with the result of the channel's basic_publish.

        """
        if self.__closed:
            return defer.fail(self.__closed)
        return defer.succeed(self.__channel.basic_publish(*args, **kwargs))

    def __wrap_channel_method(self, name):
        """Wrap Pika's Channel method to make it return a Deferred that fires
        when the method completes and errbacks if the channel gets closed. If
        the original method's callback would receive more than one argument, the
        Deferred fires with a tuple of argument values.

        """
        method = getattr(self.__channel, name)

        @functools.wraps(method)
        def wrapped(*args, **kwargs):
            if self.__closed:
                return defer.fail(self.__closed)

            d = defer.Deferred()
            self.__calls.add(d)
            d.addCallback(self.__clear_call, d)

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

    def __clear_consumer(self, ret, queue_name):
        self.__consumers.pop(queue_name, None)
        return ret

    def __clear_call(self, ret, d):
        self.__calls.discard(d)
        return ret

    def __getattr__(self, name):
        # Wrap methods defined in WRAPPED_METHODS, forward the rest of accesses
        # to the channel.
        if name in self.WRAPPED_METHODS:
            return self.__wrap_channel_method(name)
        return getattr(self.__channel, name)


class TwistedProtocolConnection(pika.connection.Connection):
    """A hybrid between a Pika Connection and a Twisted Protocol. Allows using
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

    NOTE: since `base_connection.BaseConnection`'s primary responsibility is
    management of the transport, we use `pika.connection.Connection` directly as
    our base class because this adapter uses a different transport management
    strategy.

    """
    def __init__(self,
                 parameters=None,
                 on_close_callback=None,
                 custom_reactor=None):

        super(TwistedProtocolConnection, self).__init__(
            parameters=parameters,
            on_open_callback=self.connectionReady,
            on_open_error_callback=self.connectionFailed,
            on_close_callback=on_close_callback,
            internal_connection_workflow=False)

        self.ready = defer.Deferred()
        self._reactor = custom_reactor or reactor
        self._transport = None  # to be provided by `makeConnection()`

    def channel(self, channel_number=None):  # pylint: disable-msg=W0221
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        Return a Deferred that fires with an instance of a wrapper around the
        Pika Channel class.

        :param int channel_number: The channel number to use, defaults to the
                                   next available.

        """
        d = defer.Deferred()
        super(TwistedProtocolConnection, self).channel(channel_number,
                                                       d.callback)
        return d.addCallback(TwistedChannel)

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

    # IProtocol methods

    def dataReceived(self, data):
        # Pass the bytes to Pika for parsing
        self._on_data_available(data)

    def connectionLost(self, reason):
        self._transport = None

        # Let the caller know there's been an error
        d, self.ready = self.ready, None
        if d:
            d.errback(reason)

        self._on_stream_terminated(reason)

    def makeConnection(self, transport):
        self._transport = transport
        self._on_stream_connected()
        self.connectionMade()

    def connectionMade(self):
        # Tell everyone we're connected
        pass

    # Our own methods

    def connectionReady(self, res):
        d, self.ready = self.ready, None
        if d:
            d.callback(res)

    def connectionFailed(self, _connection, _error_message=None):
        d, self.ready = self.ready, None
        if d:
            attempts = self.params.connection_attempts
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
