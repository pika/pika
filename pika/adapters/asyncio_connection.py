"""Using Pika with a asyncio IOLoop.

This is port of Twisted pika adapter from :pika.adapters.twisted_connection


The interfaces in this module are Future-based when possible. This means that
the connection.channel() method and most of the channel methods return
Futures instead of taking a callback argument and that basic_consume()
returns a asyncion.Queue subclass instance where messages from the server will be
stored. Refer to the docstrings for AsyncioProtocolConnection.channel() and the
AsyncioChannel class for details.

Usage example:

################
#RPC client:
################

import asyncio

import pika
from pika.adapters import asyncio_connection

@asyncio.coroutine
def make_connection(loop, host="localhost", port=5672):

    def connection_factory():
        params = pika.ConnectionParameters()
        return asyncio_connection.AsyncioProtocolConnection(params, loop=loop)

    transport, connection = yield from loop.create_connection(connection_factory, host, port)

    yield from connection.ready # important!
    return connection


@asyncio.coroutine
def client(loop):
    global c
    conn = yield from make_connection(loop)
    chan = yield from conn.channel()

    result = yield from chan.queue_declare(exclusive=True)
    cb_queue = result.method.queue
    queue, ctag = yield from chan.basic_consume(queue=cb_queue, no_ack=True)

    yield from chan.basic_publish(
        exchange='',
        routing_key='rpc_queue',
        properties=pika.BasicProperties(
             reply_to=cb_queue,
             ),
        body='Hello World!')

    ch, method, props, body = yield from queue.get()
    print(body)

loop = asyncio.get_event_loop()

try:
    task = asyncio.ensure_future(client(loop))
    loop.run_until_complete(task)
except KeyboardInterrupt:
    print('Done')


################
# RPC server
################

import asyncio
import pika

from pika.adapters import asyncio_connection


@asyncio.coroutine
def make_connection(loop, host="localhost", port=5672):

    def connection_factory():
        params = pika.ConnectionParameters()
        return asyncio_connection.AsyncioProtocolConnection(params, loop=loop)

    transport, connection = yield from loop.create_connection(connection_factory, host, port)
    yield from connection.ready # important!
    return connection


@asyncio.coroutine
def server(loop):
    conn = yield from make_connection(loop)
    chan = yield from conn.channel()

    yield from chan.queue_declare(queue='rpc_queue')
    queue, ctag = yield from chan.basic_consume(queue='rpc_queue', no_ack=True)

    while True:
        ch, method, props, body = yield from queue.get()
        yield from chan.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            body=body[::-1])


loop = asyncio.get_event_loop()

try:
    task = asyncio.ensure_future(server(loop))
    loop.run_until_complete(task)
except KeyboardInterrupt:
    print('Done')

"""
import functools
import asyncio
import socket
from logging import getLogger

from pika import exceptions
from pika.adapters import base_connection



def _fail(failure=None):
    f = asyncio.Future()
    f.set_exception(failure)
    return f


def _succeed(res):
    f = asyncio.Future()
    f.set_result(res)
    return f


class ClosableQueue(asyncio.Queue):
    """
    Like the normal asycio Queue, but after close() is called with an
    Exception instance all pending Futures are errbacked and further attempts
    to call get() or put() return a Failure wrapping that exception.
    """

    def __init__(self, maxsize=0, *, loop=None):
        self.closed = None
        super().__init__(maxsize, loop=loop)

    def put(self, obj):
        if self.closed:
            return _fail(self.closed)
        return super().put(obj)

    def get(self):
        if self.closed:
            return _fail(self.closed)
        return super().get()

    def close(self, reason):
        self.closed = reason
        while self._getters:
            waiter = self._getters.popleft()
            if not waiter.done():
                waiter.set_exception(reason)
        self._queue.clear()


class AsyncioChannel:
    """A wrapper wround Pika's Channel.

    Channel methods that normally take a callback argument are wrapped to
    return a Future that fires with whatever would be passed to the callback.
    If the channel gets closed, all pending Future are set_exception with a
    ChannelClosed exception. The returned Future fire with whatever
    arguments the callback to the original method would receive.

    The basic_consume method is wrapped in a special way, see its docstring for
    details.
    """

    WRAPPED_METHODS = ('exchange_declare', 'exchange_delete', 'queue_declare',
                       'queue_bind', 'queue_purge', 'queue_unbind', 'basic_qos',
                       'basic_get', 'basic_recover', 'tx_select', 'tx_commit',
                       'tx_rollback', 'flow', 'basic_cancel')

    def __init__(self, channel):
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
        """Consume from a server queue. Returns a Future that fires with a
        tuple: (queue_object, consumer_tag). The queue object is an instance of
        ClosableQueue, where data received from the queue will be
        stored. Clients should use its get() method to fetch individual
        message.
        """
        if self.__closed:
            return _fail(self.__closed)

        queue = ClosableQueue()
        queue_name = kwargs['queue']

        def consumer_callback(*args):
            queue.put_nowait((self, *args[1:]))

        kwargs['consumer_callback'] = consumer_callback
        self.__consumers.setdefault(queue_name, set()).add(queue)

        try:
            consumer_tag = self.__channel.basic_consume(*args, **kwargs)
        except:
            return _fail()

        return _succeed((queue, consumer_tag))

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
        Future that fires with the result of the channel's basic_publish.

        """
        if self.__closed:
            return _fail(self.__closed)
        return _succeed(self.__channel.basic_publish(*args, **kwargs))

    def __wrap_channel_method(self, name):
        """Wrap Pika's Channel method to make it return a Future that fires
        when the method completes and errbacks if the channel gets closed. If
        the original method's callback would receive more than one argument, the
        Future fires with a tuple of argument values.

        """
        method = getattr(self.__channel, name)

        @functools.wraps(method)
        def wrapped(*args, **kwargs):
            if self.__closed:
                return _fail(self.__closed)

            d = asyncio.Future()
            self.__calls.add(d)
            d.add_done_callback(functools.partial(self.__clear_call, d))

            def single_argument(*args):
                """
                Make sure that the Future is called with a single argument.
                In case the original callback fires with more than one, convert
                to a tuple.
                """
                if len(args) > 1:
                    d.set_result(tuple(args))
                else:
                    d.set_result(*args)

            kwargs['callback'] = single_argument
            try:
                method(*args, **kwargs)
            except Exception as e:
                return _fail(e)
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
        :rtype: asyncio.Handle

        """
        return self.loop.call_later(deadline, callback_method)

    def remove_timeout(self, call):
        """Remove a call

        :param asyncio.Handle call: The call to cancel

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
        # ioloop is removing it from the loop in acyncio's parlance.
        self.loop.remove_reader(self.connection.fileno())
        self.loop.remove_writer(self.connection.fileno())

    def update_handler(self, _, event_state):
        # Same as in remove_handler, the fileno is irrelevant. First remove the
        # connection entirely from the reactor, then add it back depending on
        # the event state.
        self.loop.remove_reader(self.connection.fileno())
        self.loop.remove_writer(self.connection.fileno())

        if event_state & self.connection.READ:
            self.loop.add_reader(self.connection.fileno(), self._handle_read)

        if event_state & self.connection.WRITE:
            self.loop.add_writer(self.connection.fileno(), self._handle_write)


class AsyncioProtocolConnection(base_connection.BaseConnection, asyncio.Protocol):
    """A hybrid between a Pika Connection and a acyncio Protocol. Allows using
    asyncio's non-blocking loop.create_connection method for connecting to the
    server.

    It has one caveat: AcyncioProtocolConnection objects have a ready
    instance variable that's a Future which fires when the connection is
    ready to be used (the initial AMQP handshaking has been done). You *have*
    to wait for this Future to fire before requesting a channel.

    Since it's asyncio handling connection establishing it does not accept
    connect callbacks, you have to implement that within asyncio. Also remember
    that the host, port and ssl values of the connection parameters are ignored
    because, yet again, it's asyncio who manages the connection.

    """

    def __init__(self, parameters, loop=None, connection_lost_callback=None):
        self.ready = asyncio.Future()
        loop = loop or asyncio.get_event_loop()
        self.connection_lost_callback = connection_lost_callback
        super().__init__(
            parameters=parameters,
            on_open_callback=self.connection_ready,
            on_open_error_callback=self.connection_failed,
            on_close_callback=None,
            ioloop=IOLoopAdapter(self, loop),
            stop_ioloop_on_close=False)

    def connect(self):
        # The connection is open asynchronously by asyncio, so skip the whole
        # connect() part, except for setting the connection state
        self._set_connection_state(self.CONNECTION_INIT)

    def _adapter_connect(self):
        # Should never be called, as we override connect() and leave the
        # building of a TCP connection to acyncio, but implement anyway to keep
        # the interface
        return False

    def _adapter_disconnect(self):
        # Disconnect from the server
        self.transport.close()

    def _flush_outbound(self):
        """Override BaseConnection._flush_outbound to send all bufferred data
        the acyncio way, by writing to the transport. No need for buffering,
        acyncio handles that for us.
        """
        while self.outbound_buffer:
            self.transport.write(self.outbound_buffer.popleft())

    def channel(self, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        Return a Future that fires with an instance of a wrapper around the
        Pika Channel class.

        :param int channel_number: The channel number to use, defaults to the
                                   next available.

        """
        d = asyncio.Future()
        base_connection.BaseConnection.channel(
            self, lambda ch: d.set_result(AsyncioChannel(ch)), channel_number)
        return d

    # Protocol methods

    def data_received(self, data):
        # Pass the bytes to Pika for parsing
        self._on_data_available(data)

    def connection_lost(self, reason):
        # Let the caller know there's been an error
        d, self.ready = self.ready, None
        if d:
            d.set_exception(reason)
        else:
            # Let client to handle disconnect
            if self.connection_lost_callback:
                self.connection_lost_callback(reason)

    def connection_made(self, transport):
        # Tell everyone we're connected
        transport._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.transport = transport
        self._on_connected()

    # Our own methods

    def connection_ready(self, res):
        d, self.ready = self.ready, None
        if d:
            d.set_result(res)

    def connection_failed(self, connection_unused, error_message=None):
        d, self.ready = self.ready, None
        if d:
            attempts = self.params.connection_attempts
            exc = exceptions.AMQPConnectionError(attempts)
            d.set_exception(exc)
