# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Using Pika with a Twisted reactor.

Supports two methods of estabilishing the connection, using TwistedConnection
or TwistedProtocolConnection. For details about each method, see the docstrings
of the corresponding classes.

The interfaces in this module are Deferred-based when possible. This means that
the connection.channel() method and most of the channel methods return
Deferreds instead of taking a callback argument and that basic_consume()
returns a Twisted DeferredQueue where messages from the server will be
stored. Refer to the docstrings for TwistedConnection.channel() and the
TwistedChannel class for details.
"""
import functools
import time
from twisted.internet import defer, error, reactor
from twisted.python import log

from pika import exceptions
from pika.adapters.base_connection import BaseConnection, READ, WRITE


class ClosableDeferredQueue(defer.DeferredQueue):
    """
    Like the normal Twisted DeferredQueue, but after close() is called with an
    Exception instance all pending Deferreds are errbacked and further attempts
    to call get() or put() return a Failure wrapping that exception.
    """
    def __init__(self, size=None, backlog=None):
        self.closed = None
        defer.DeferredQueue.__init__(self, size, backlog)

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
    """
    A wrapper wround Pika's Channel.

    Channel methods that are normally take a callback argument are wrapped to
    return a Deferred that fires with whatever would be passed to the callback.
    If the channel gets closed, all pending Deferreds are errbacked with a
    ChannelClosed exception. The returned Deferreds fire with whatever
    arguments the callback to the original method would receive.

    The basic_consume method is wrapped in a special way, see its docstring for
    details.
    """

    WRAPPED_METHODS = ('exchange_declare', 'exchange_delete',
                       'queue_declare', 'queue_bind', 'queue_purge',
                       'queue_unbind', 'basic_qos', 'basic_get',
                       'basic_recover', 'tx_select', 'tx_commit',
                       'tx_rollback', 'flow', 'basic_cancel')

    def __init__(self, channel):
        self.__channel = channel
        self.__closed = None
        self.__calls = set()
        self.__consumers = {}

        channel.add_on_close_callback(self.channel_closed)

    def channel_closed(self, code, text):
        # enter the closed state
        self.__closed = exceptions.ChannelClosed(code, text)
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
        """
        Consume from a server queue. Returns a Deferred that fires with a
        tuple: (queue_object, consumer_tag). The queue object is an instance of
        ClosableDeferredQueue, where data received from the queue will be
        stored. Clients should use its get() method to fetch individual
        message.
        """
        if self.__closed:
            return defer.fail(self.__closed)

        queue = ClosableDeferredQueue()
        queue_name = kwargs['queue']
        kwargs['consumer_callback'] = lambda *args: queue.put(args)
        self.__consumers.setdefault(queue_name, set()).add(queue)

        try:
            consumer_tag = self.__channel.basic_consume(*args, **kwargs)
        except:
            return defer.fail()

        return defer.succeed((queue, consumer_tag))

    def queue_delete(self, *args, **kwargs):
        """
        Wraps the method the same way all the others are wrapped, but removes
        the reference to the queue object after it gets deleted on the server.
        """
        wrapped = self.__wrap_channel_method('queue_delete')
        queue_name = kwargs['queue']

        d = wrapped(*args, **kwargs)
        return d.addCallback(self.__clear_consumer, queue_name)

    def __wrap_channel_method(self, name):
        """
        Wrap Pika's Channel method to make it return a Deferred that fires when
        the method completes and errbacks if the channel gets closed. If the
        original method's callback would receive more than one argument, the
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
            kwargs['callback'] = d.callback

            try:
                method(*args, **kwargs)
            except:
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


class IOLoopReactorAdapter(object):
    """
    An adapter providing Pika's IOLoop interface using a Twisted reactor.

    Accepts a TwistedConnection object and a Twisted reactor object.
    """
    def __init__(self, connection, reactor):
        self.connection = connection
        self.reactor = reactor
        self.started = False

    def add_timeout(self, deadline, callback):
        secs = deadline - time.time()
        return self.reactor.callLater(secs, callback)

    def remove_timeout(self, call):
        call.cancel()

    def stop(self):
        # Guard against stopping the reactor multiple times
        if not self.started:
            return
        self.started = False
        self.reactor.stop()

    def start(self):
        # Guard against starting the reactor multiple times
        if self.started:
            return
        self.started = True
        self.reactor.run()

    def remove_handler(self, _):
        # The fileno is irrelevant, as it's the connection's job to provide it
        # to the reactor when asked to do so. Removing the handler from the
        # ioloop is removing it from the reactor in Twisted's parlance.
        self.reactor.removeReader(self.connection)
        self.reactor.removeWriter(self.connection)

    def update_handler(self, _, event_state):
        # Same as in remove_handler, the fileno is irrelevant. First remove the
        # connection entirely from the reactor, then add it back depending on
        # the event state.
        self.reactor.removeReader(self.connection)
        self.reactor.removeWriter(self.connection)

        if event_state & READ:
            self.reactor.addReader(self.connection)

        if event_state & WRITE:
            self.reactor.addWriter(self.connection)


class TwistedConnection(BaseConnection):
    """
    A standard Pika connection adapter. You instantiate the class passing the
    connection parameters and the connected callback and when it gets called
    you can start using it.

    The problem is that connection estabilishing is done using the blocking
    socket module. For instance, if the host you are connecting to is behind a
    misconfigured firewall that just drops packets, the whole process will
    freeze until the connection timeout passes. To work around that problem,
    use TwistedProtocolConnection, but read its docstring first.

    Objects of this class get put in the Twisted reactor which will notify them
    when the socket connection becomes readable or writable, so apart from
    implementing the BaseConnection interface, they also provide Twisted's
    IReadWriteDescriptor interface.
    """

    # BaseConnection methods

    def _adapter_connect(self):
        # Connect (blockignly!) to the server
        BaseConnection._adapter_connect(self)
        # Pnce that's done, create an I/O loop by adapting the Twisted reactor
        self.ioloop = IOLoopReactorAdapter(self, reactor)
        # Set the I/O events we're waiting for (see IOLoopReactorAdapter
        # docstrings for why it's OK to pass None as the file descriptor)
        self.ioloop.update_handler(None, self.event_state)

        # Let everyone know we're connected
        self._on_connected()

    def _adapter_disconnect(self):
        # Remove from the IOLoop
        self.ioloop.remove_handler(None)

        # Close our socket since the Connection class told us to do so
        self.socket.close()

    def _on_connected(self):
        # Call superclass and then update the event state to flush the outgoing
        # frame out. Commit 50d842526d9f12d32ad9f3c4910ef60b8c301f59 removed a
        # self._flush_outbound call that was in _send_frame which previously
        # made this step unnecessary.
        BaseConnection._on_connected(self)
        self._manage_event_state()

    def _handle_disconnect(self):
        # Do not stop the reactor, this would cause the entire process to exit,
        # just fire the disconnect callbacks
        self._on_connection_closed(None, True)

    def channel(self, channel_number=None):
        """
        Return a Deferred that fires with an instance of a wrapper aroud the
        Pika Channel class.
        """
        d = defer.Deferred()
        BaseConnection.channel(self, d.callback, channel_number)
        return d.addCallback(TwistedChannel)

    # IReadWriteDescriptor methods

    def fileno(self):
        return self.socket.fileno()

    def logPrefix(self):
        return "twisted-pika"

    def connectionLost(self, reason):
        # If the connection was not closed cleanly, log the error
        if not reason.check(error.ConnectionDone):
            log.err(reason)

        self._handle_disconnect()

    def doRead(self):
        self._handle_read()

    def doWrite(self):
        self._handle_write()


class TwistedProtocolConnection(BaseConnection):
    """
    Ahybrid between a Pika Connection and a Twisted Protocol. Allows using
    Twisted's non-blocking connectTCP/connectSSL methods for connecting to the
    server.

    It has one caveat: TwistedProtocolConnection objects have a connected
    instance variable that's a Deferred which fires when the connection is
    ready to be used (the initial AMQP handshaking has been done). You *have*
    to wait for this Deferred to fire before requesting a channel.

    Since it's Twisted handling connection estabilishing, it does not accept
    connect callbacks or reconnection strategy objects, you have to implement
    that within Twisted. Also remember that the host, port and ssl values of
    the connection parameters are ignored because, yet again, it's Twisted who
    manages the connection.
    """

    # BaseConnection methods

    def __init__(self, parameters):
        self.connected = defer.Deferred()
        BaseConnection.__init__(self, parameters, self.connected.callback)

    def _adapter_connect(self):
        # We get connected by Twisted, as is normal for protocols
        pass

    def _send_frame(self, frame):
        marshalled_frame = frame.marshal()
        self.bytes_sent += len(marshalled_frame)
        self.frames_sent += 1

        # XXX: no backpressure support yet

        # Send data the Twisted way, by writing to the transport. No need for
        # buffering, Twisted handles that by itself.
        self.transport.write(marshalled_frame)

    def channel(self, channel_number=None):
        """
        Return a Deferred that fires with an instance of a wrapper aroud the
        Pika Channel class.
        """
        d = defer.Deferred()
        BaseConnection.channel(self, d.callback, channel_number)
        return d.addCallback(TwistedChannel)

    # IProtocol methods

    def dataReceived(self, data):
        # Pass the bytes to Pika for parsing
        self._on_data_available(data)

    def connectionLost(self, reason):
        # If the connection was not closed cleanly, log the error
        if not reason.check(error.ConnectionDone):
            log.err(reason)

    def makeConnection(self, transport):
        self.transport = transport
        self.connectionMade()

    def connectionMade(self):
        # Tell everyone we're connected
        self._on_connected()
