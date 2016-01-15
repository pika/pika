"""Using Pika with a asyncio IOLoop.

Supports two methods of establishing the connection, using AsyncioConnection
or AsyncioProtocolConnection. For details about each method, see the docstrings
of the corresponding classes.

The interfaces in this module are Deferred-based when possible. This means that
the connection.channel() method and most of the channel methods return
Deferreds instead of taking a callback argument and that basic_consume()
returns a Twisted DeferredQueue where messages from the server will be
stored. Refer to the docstrings for TwistedConnection.channel() and the
TwistedChannel class for details.

"""
import functools
import asyncio
import time
from logging import getLogger

from pika import exceptions
from pika.adapters import base_connection


class defer:

    @staticmethod
    def fail(failure):
        f = asyncio.Future()
        f.set_exception(failure)
        return f

    @staticmethod
    def succeed(res):
        f = asyncio.Future()
        f.set_result(res)
        return f


class ClosableQueue(asyncio.Queue):
    """
    Like the normal Twisted DeferredQueue, but after close() is called with an
    Exception instance all pending Deferreds are errbacked and further attempts
    to call get() or put() return a Failure wrapping that exception.
    """

    def __init__(self, maxsize=0, *, loop=None):
        self.closed = None
        super().__init__(maxsize, loop=loop)

    def put(self, obj):
        if self.closed:
            return defer.fail(self.closed)
        return super().put(obj)

    def get(self):
        if self.closed:
            return defer.fail(self.closed)
        return super().get()

    def close(self, reason):
        self.closed = reason
        while self.waiting:
            self.waiting.pop().set_exception(reason)
        self.pending = []


class AsyncioChannel:
    """A wrapper wround Pika's Channel.

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
        print('Init AsycnioChannel', channel)
        if isinstance(channel, asyncio.Future):
            channel = channel.result()
        self.__channel = channel
        self.__closed = None
        self.__calls = set()
        self.__consumers = {}

        channel.add_on_close_callback(self.channel_closed)

    def channel_closed(self, channel, reply_code, reply_text):
        # enter the closed state
        self.__closed = exceptions.ChannelClosed(reply_code, reply_text)
        # errback all pending calls
        for d in self.__calls:
            d.set_exception(self.__closed)
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

        queue = ClosableQueue()
        queue_name = kwargs['queue']

        def consumer_callback(*args):
            print('in callback', time.time())
            queue.put_nowait(args)

        kwargs['consumer_callback'] = consumer_callback
        self.__consumers.setdefault(queue_name, set()).add(queue)

        try:
            consumer_tag = self.__channel.basic_consume(*args, **kwargs)
        except:
            return defer.fail()

        return defer.succeed((queue, consumer_tag))

    def queue_delete(self, *args, **kwargs):
        """Wraps the method the same way all the others are wrapped, but removes
        the reference to the queue object after it gets deleted on the server.

        """
        wrapped = self.__wrap_channel_method('queue_delete')
        queue_name = kwargs['queue']

        d = wrapped(*args, **kwargs)
        d.add_done_callback(functools.partial(self.__clear_consumer, queue_name))
        return d

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

            d = asyncio.Future()
            self.__calls.add(d)
            d.add_done_callback(functools.partial(self.__clear_call, d))

            def single_argument(*args):
                """
                Make sure that the deferred is called with a single argument.
                In case the original callback fires with more than one, convert
                to a tuple.
                """
                if len(args) > 1:
                    d.set_result(tuple(args))
                else:
                    d.set_result(*args)

            kwargs['callback'] = single_argument
            # print('Call wrapped', method, args, kwargs) 
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


class IOLoopAdapter:
    """An adapter providing Pika's IOLoop interface using a asyncio IOLoop.

    Accepts a AsyncioConnection object and a asyncio IOLoop.

    """

    def __init__(self, connection, loop):
        self.connection = connection
        self.loop = loop
        self.started = False

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: twisted.internet.interfaces.IDelayedCall

        """
        return self.loop.call_later(deadline, callback_method)

    def remove_timeout(self, call):
        """Remove a call

        :param twisted.internet.interfaces.IDelayedCall call: The call to cancel

        """
        call.cancel()

    def stop(self):
        # Guard against stopping the loop multiple times
        if not self.started:
            return
        self.started = False
        self.loop.stop()

    def start(self):
        # Guard against starting the loop multiple times
        if self.started:
            return
        self.started = True
        self.loop.run_forever()

    def remove_handler(self, _):
        # The fileno is irrelevant, as it's the connection's job to provide it
        # to the loop when asked to do so. Removing the handler from the
        # ioloop is removing it from the loop in Twisted's parlance.
        self.loop.remove_reader(self.connection.fileno())
        self.loop.remove_writer(self.connection.fileno())

    def update_handler(self, _, event_state):
        # Same as in remove_handler, the fileno is irrelevant. First remove the
        # connection entirely from the reactor, then add it back depending on
        # the event state.
        self.reactor.remove_reader(self.connection.fileno())
        self.reactor.remove_writer(self.connection.fileno())

        if event_state & self.connection.READ:
            self.reactor.add_reader(self.connection.fileno())

        if event_state & self.connection.WRITE:
            self.reactor.add_writer(self.connection.fileno())


class AsyncioConnection(base_connection.BaseConnection):
    """A standard Pika connection adapter. You instantiate the class passing the
    connection parameters and the connected callback and when it gets called
    you can start using it.

    The problem is that connection establishing is done using the blocking
    socket module. For instance, if the host you are connecting to is behind a
    misconfigured firewall that just drops packets, the whole process will
    freeze until the connection timeout passes. To work around that problem,
    use TwistedProtocolConnection, but read its docstring first.

    Objects of this class get put in the Twisted reactor which will notify them
    when the socket connection becomes readable or writable, so apart from
    implementing the BaseConnection interface, they also provide Twisted's
    IReadWriteDescriptor interface.

    """

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=False,
                 loop=None):
        loop = loop or asyncio.get_event_loop()
        super().__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback,
            ioloop=IOLoopAdapter(self, loop),
            stop_ioloop_on_close=stop_ioloop_on_close)

    def _adapter_connect(self):
        """Connect to the RabbitMQ broker"""
        # Connect (blockignly!) to the server
        error = super()._adapter_connect()
        if not error:
            # Set the I/O events we're waiting for (see IOLoopReactorAdapter
            # docstrings for why it's OK to pass None as the file descriptor)
            self.ioloop.update_handler(None, self.event_state)
        return error

    def _adapter_disconnect(self):
        """Called when the adapter should disconnect"""
        self.ioloop.remove_handler(None)
        self._cleanup_socket()

    def _handle_disconnect(self):
        """Do not stop the reactor, this would cause the entire process to exit,
        just fire the disconnect callbacks

        """
        self._on_connection_closed(None, True)

    def _on_connected(self):
        """Call superclass and then update the event state to flush the outgoing
        frame out. Commit 50d842526d9f12d32ad9f3c4910ef60b8c301f59 removed a
        self._flush_outbound call that was in _send_frame which previously
        made this step unnecessary.

        """
        super()._on_connected()
        self._manage_event_state()

    def channel(self, channel_number=None):
        """Return a Deferred that fires with an instance of a wrapper around the
        Pika Channel class.

        """
        d = asyncio.Future()
        base_connection.BaseConnection.channel(self, d.set_result, channel_number)
        d.add_done_callback(AsyncioChannel)
        return d

    # IReadWriteDescriptor methods

    def fileno(self):
        return self.socket.fileno()

    def logPrefix(self):
        return "asyncio-pika"

    def connectionLost(self, reason):
        # If the connection was not closed cleanly, log the error
        if not reason.check(error.ConnectionDone):
            log.err(reason)

        self._handle_disconnect()

    def doRead(self):
        self._handle_read()

    def doWrite(self):
        self._handle_write()
        self._manage_event_state()


class AsyncioProtocolConnection(base_connection.BaseConnection):
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

    """

    def __init__(self, parameters, loop=None):
        self.ready = asyncio.Future()
        loop = loop or asyncio.get_event_loop()
        print('APC init')
        super().__init__(
            parameters=parameters,
            on_open_callback=self.connection_ready,
            on_open_error_callback=self.connection_failed,
            on_close_callback=None,
            ioloop=IOLoopAdapter(self, loop),
            stop_ioloop_on_close=False)

    def connect(self):
        print('APC connect')
        # The connection is open asynchronously by Twisted, so skip the whole
        # connect() part, except for setting the connection state
        self._set_connection_state(self.CONNECTION_INIT)

    def _adapter_connect(self):
        # Should never be called, as we override connect() and leave the
        # building of a TCP connection to Twisted, but implement anyway to keep
        # the interface
        return False

    def _adapter_disconnect(self):
        # Disconnect from the server
        self.transport.close()

    def _flush_outbound(self):
        """Override BaseConnection._flush_outbound to send all bufferred data
        the Twisted way, by writing to the transport. No need for buffering,
        Twisted handles that for us.
        """
        while self.outbound_buffer:
            self.transport.write(self.outbound_buffer.popleft())

    def channel(self, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        Return a Deferred that fires with an instance of a wrapper around the
        Pika Channel class.

        :param int channel_number: The channel number to use, defaults to the
                                   next available.

        """
        d = asyncio.Future()
        base_connection.BaseConnection.channel(
            self, lambda ch: d.set_result(AsyncioChannel(ch)), channel_number)
        return d

    # IProtocol methods

    def data_received(self, data):
        # Pass the bytes to Pika for parsing
        self._on_data_available(data)

    def connection_lost(self, reason):
        # Let the caller know there's been an error
        d, self.ready = self.ready, None
        if d:
            d.set_exception(reason)

    # def makeConnection(self, transport):
    #     self.connection_made()

    def connection_made(self, transport):
        # Tell everyone we're connected
        self.transport = transport
        print('APC connection_made', transport)
        self._on_connected()
        print('APC connection_made 2', transport)

    # Our own methods

    def connection_ready(self, res):
        print('APC connection_ready')
        d, self.ready = self.ready, None
        if d:
            d.set_result(res)

    def connection_failed(self, connection_unused, error_message=None):
        d, self.ready = self.ready, None
        if d:
            attempts = self.params.connection_attempts
            exc = exceptions.AMQPConnectionError(attempts)
            d.set_exception(exc)
