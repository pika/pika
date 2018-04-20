"""Using Pika with a Twisted reactor.

Supports two methods of establishing the connection, using TwistedConnection
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
import logging
import socket

from zope.interface import implementer
from twisted.internet.interfaces import (IReadWriteDescriptor,
                                         IHalfCloseableDescriptor)
from twisted.internet import (defer, error as twisted_error, reactor,
                              threads as twisted_threads)
import twisted.python.failure

from pika import exceptions
from pika.adapters import base_connection
from pika.adapters.utils import nbio_interface, io_services_utils
from pika.adapters.utils.io_services_utils import (check_callback_arg,
                                                   check_fd_arg)

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

    def channel_closed(self, _channel, reply_code, reply_text):
        # enter the closed state
        self.__closed = exceptions.ChannelClosed(reply_code, reply_text)
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
        kwargs['callback'] = lambda *args: queue.put(args)
        self.__consumers.setdefault(queue_name, set()).add(queue)

        try:
            consumer_tag = self.__channel.basic_consume(*args, **kwargs)
        # TODO this except without types would suppress system-exiting
        # exceptions, such as SystemExit and KeyboardInterrupt. It should be at
        # least `except Exception` and preferably more specific.
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
            # TODO this except without types would suppress system-exiting
            # exceptions, such as SystemExit and KeyboardInterrupt. It should be
            # at least `except Exception` and preferably more specific.
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


class TwistedConnection(base_connection.BaseConnection):
    """A standard Pika connection adapter. You instantiate the class passing the
    connection parameters and the connected callback and when it gets called
    you can start using it.

    The problem is that connection establishing is done using the blocking
    socket module. For instance, if the host you are connecting to is behind a
    misconfigured firewall that just drops packets, the whole process will
    freeze until the connection timeout passes. To work around that problem,
    use TwistedProtocolConnection, but read its docstring first.

    """

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 custom_ioloop=None,
                 internal_connection_workflow=True):
        """
        :param parameters:
        :param on_open_callback:
        :param on_open_error_callback:
        :param on_close_callback:
        :param custom_ioloop:
        :param internal_connection_workflow:

        """
        if isinstance(custom_ioloop, nbio_interface.AbstractIOServices):
            nbio = custom_ioloop
        else:
            nbio = _TwistedIOServicesAdapter(custom_ioloop)

        super(TwistedConnection, self).__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback,
            nbio=nbio,
            internal_connection_workflow=internal_connection_workflow)

    @classmethod
    def create_connection(cls,
                          connection_configs,
                          on_done,
                          custom_ioloop=None,
                          workflow=None):
        """Implement
        :py:classmethod:`pika.adapters.BaseConnection.create_connection()`.

        """
        nbio = _TwistedIOServicesAdapter(custom_ioloop)

        def connection_factory(params):
            """Connection factory."""
            if params is None:
                raise ValueError('Expected pika.connection.Parameters '
                                 'instance, but got None in params arg.')
            return cls(
                parameters=params,
                custom_ioloop=nbio,
                internal_connection_workflow=False)

        return cls._start_connection_workflow(
            connection_configs=connection_configs,
            connection_factory=connection_factory,
            nbio=nbio,
            workflow=workflow,
            on_done=on_done)

    def channel(self, channel_number=None):
        """Return a Deferred that fires with an instance of a wrapper around the
        Pika Channel class.

        """
        d = defer.Deferred()
        super(TwistedConnection, self).channel(channel_number, d.callback)
        return d.addCallback(TwistedChannel)


class TwistedProtocolConnection(base_connection.BaseConnection):
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

    class _PikaTransportAdapter(nbio_interface.AbstractStreamTransport):
        """Maps AbstractStreamTransport methods to twisted ITCPTransport"""

        def __init__(self, transport):
            self._transport = transport

        def abort(self):
            """Implement `AbstractStreamTransport.abort()`"""
            self._transport.abortConnection()

        def get_protocol(self):
            """Implement `AbstractStreamTransport.get_protocol()`"""
            raise NotImplementedError

        def write(self, data):
            """Implement `AbstractStreamTransport.write()`"""
            self._transport.write(data)

        def get_write_buffer_size(self):
            """Implement `AbstractStreamTransport.get_write_buffer_size ()`"""
            raise NotImplementedError


    def __init__(self,
                 parameters=None,
                 on_close_callback=None,
                 custom_ioloop=None):

        self.ready = defer.Deferred()

        super(TwistedProtocolConnection, self).__init__(
            parameters=parameters,
            on_open_callback=self.connectionReady,
            on_open_error_callback=self.connectionFailed,
            on_close_callback=on_close_callback,
            nbio=_TwistedIOServicesAdapter(custom_ioloop),
            internal_connection_workflow=False)

    @classmethod
    def create_connection(cls, *args, **kwargs):
        """Implement `BaseConnection.create_connection()`"""
        raise NotImplementedError('create_connection()')

    def _adapter_disconnect(self):
        """Override `BaseConnection._adapter_disconnect()"""
        self._transport.abort()

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
        d = defer.Deferred()
        base_connection.BaseConnection.channel(self, channel_number, d.callback)
        return d.addCallback(TwistedChannel)

    # IProtocol methods

    def dataReceived(self, data):
        # Pass the bytes to Pika for parsing
        self._proto_data_received(data)

    def connectionLost(self, reason):
        # Let the caller know there's been an error
        d, self.ready = self.ready, None
        if d:
            d.errback(reason)

        self._proto_connection_lost(reason)

    def makeConnection(self, transport):
        self._proto_connection_made(self._PikaTransportAdapter(transport))
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


class _TwistedIOServicesAdapter(
        io_services_utils.SocketConnectionMixin,
        io_services_utils.StreamingConnectionMixin,
        nbio_interface.AbstractIOServices,
        nbio_interface.AbstractFileDescriptorServices):
    """Implements
    :py:class:`.utils.nbio_interface.AbstractIOServices` interface
    on top of :py:class:`twisted.internet.reactor`.

    NOTE:
    :py:class:`.utils.nbio_interface.AbstractFileDescriptorServices`
    interface is only required by the mixins.

    """

    @implementer(IHalfCloseableDescriptor, IReadWriteDescriptor)
    class _SocketReadWriteDescriptor(object):
        """File descriptor wrapper for `add/remove-Writer/Reader`. A given
        instance must represent both reader and writer, otherwise reactor
        may invoke e.g., doRead on a descriptor registered for on_writable
        callbacks, thus causing that event to be lost.

        """

        def __init__(self, fd, on_readable=None, on_writable=None):

            assert on_readable is not None or on_writable is not None, (
                'At least one of on_readable/on_writable must be non-None.')
            assert callable(on_readable) or callable(on_writable), (
                'One or both of on_readable/on_writable must be callable.')

            self._fd = fd

            self._on_readable = on_readable
            self._on_writable = on_writable

        def __repr__(self):
            return '{}: fd={}, on_readable={!r}, on_writable={!r}'.format(
                self.__class__.__name__,
                self._fd,
                self._on_readable,
                self.on_writable)

        @property
        def on_readable(self):
            return self._on_readable

        @on_readable.setter
        def on_readable(self, value):
            assert callable(value) or value is None, (
                'on_readable value must be callable or None.')
            self._on_readable = value

        @property
        def on_writable(self):
            return self._on_writable

        @on_writable.setter
        def on_writable(self, value):
            assert callable(value) or value is None, (
                'on_writable value must be callable or None.')
            self._on_writable = value

        def logPrefix(self):
            return self.__class__.__name__

        def fileno(self):
            """
            :raise: If the descriptor no longer has a valid file descriptor
                number associated with it.

            :return: The platform-specified representation of a file descriptor
                number.  Or C{-1} if the descriptor no longer has a valid file
                descriptor number associated with it.  As long as the descriptor
                is valid, calls to this method on a particular instance must
                return the same value.
            """
            return self._fd

        def doRead(self):
            """
            Some data is available for reading on your descriptor.

            @return: If an error is encountered which causes the descriptor to
                no longer be valid, a L{Failure} should be returned.  Otherwise,
                L{None}.
            """
            if self._on_readable is not None:
                try:
                    self._on_readable()
                except Exception:  # pylint: disable=W0703
                    LOGGER.exception('Exception from user\'s on_readable() '
                                     'callback; fd=%s', self.fileno())
                    return twisted.python.failure.Failure()
            else:
                LOGGER.warning('Reactor called %s.doRead() but on_readable is '
                               'None; fd=%s.', self.logPrefix(), self.fileno())

            return None

        def doWrite(self):
            """
            Some data can be written to your descriptor.

            @return: If an error is encountered which causes the descriptor to
                no longer be valid, a L{Failure} should be returned.  Otherwise,
                L{None}.
            """
            if self._on_writable is not None:
                try:
                    self._on_writable()
                except Exception:  # pylint: disable=W0703
                    LOGGER.exception('Exception from user\'s on_writable() '
                                     'callback; fd=%s', self.fileno())
                    return twisted.python.failure.Failure()
            else:
                LOGGER.warning('Reactor called %s.doWrite() but on_writable is '
                               'None; fd=%s.', self.logPrefix(), self.fileno())

            return None

        def connectionLost(self, reason):
            """
            Called when the connection is shut down.

            NOTE: even though we implement `IHalfCloseableDescriptor`, reactor
            still calls  `connectionLost()` instead of `readConnectionLost()`
            and `writeConnectionLost()`.

            Clear any circular references here, and any external references
            to this Protocol.  The connection has been closed. The C{reason}
            Failure wraps a L{twisted.internet.error.ConnectionDone} or
            L{twisted.internet.error.ConnectionLost} instance (or a subclass
            of one of those).

            @type reason: L{twisted.python.failure.Failure}
            """
            LOGGER.error('Reactor called %s.connectionLost(%r); fd=%s.',
                         self.logPrefix(), reason, self.fileno())

            if self._on_writable is not None:
                # Convert this to a writable event for compatibility with pika's
                # other I/O loops
                LOGGER.debug('%s: Converting connectionLost() to doWrite() for '
                             'compatibility with our other I/O loops; fd=%s',
                             self.logPrefix(), self.fileno())
                self.doWrite()

        def readConnectionLost(self, reason):
            """
            Indicates read connection was lost.
            """
            LOGGER.error('Reactor called %s.readConnectionLost(%r); fd=%s.',
                         self.logPrefix(), reason, self.fileno())
            return

            # NOTE: This appears to be unnecessary according to our async
            # services tests.
            #
            # # For compatibility with our own select/poll implementation's
            # # handling of closed input, we treat it as a readable event
            # if self._on_readable is not None:
            #     self._on_readable()
            # else:
            #     LOGGER.debug('Suppressing reactor\'s call to '
            #                  '%s.readConnectionLost(%r) on writable '
            #                  'descriptor; fd=%s.',
            #                  self.logPrefix(), reason, self.fileno())

        def writeConnectionLost(self, reason):
            """
            Indicates write connection was lost.

            :param reason: A failure instance indicating the reason why the
                           connection was lost.  L{error.ConnectionLost} and
                           L{error.ConnectionDone} are of special note, but the
                           failure may be of other classes as well.

            """
            LOGGER.error('Reactor called %s.writeConnectionLost(%r); fd=%s.',
                         self.logPrefix(), reason, self.fileno())
            return

            # NOTE: This appears to be unnecessary according to our async
            # services tests.
            #
            # # For compatibility with our own select/poll implementation's
            # # handling POLLERR and similar, we treat it as a writable event
            # if self._on_writable is not None:
            #     self._on_writable()
            # else:
            #     LOGGER.debug('Suppressing reactor\'s call to '
            #                  '%s.writeConnectionLost(%r) on readable '
            #                  'descriptor; fd=%s.',
            #                  self.logPrefix(), reason, self.fileno())


    def __init__(self, in_reactor):
        """
        :param None | twisted.internet.interfaces.IReactorFDSet reactor:

        """
        self._reactor = in_reactor or reactor

        # Mapping of fd to _SocketReadWriteDescriptor
        self._fd_watchers = dict()

    def get_native_ioloop(self):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractIOServices.get_native_ioloop()`.

        """
        return self._reactor

    def close(self):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractIOServices.close()`.

        """
        # NOTE Twisted reactor doesn't seem to have an equivalent of `close()`
        # that other I/O loops have.
        pass

    def run(self):
        """Implement :py:meth:`.utils.nbio_interface.AbstractIOServices.run()`.

        """
        # NOTE: pika doesn't need signal handlers and installing them causes
        # exceptions in our tests that run the loop from a thread.
        self._reactor.run(installSignalHandlers=False)

    def stop(self):
        """Implement :py:meth:`.utils.nbio_interface.AbstractIOServices.stop()`.

        """
        self._reactor.stop()

    def add_callback_threadsafe(self, callback):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractIOServices.add_callback_threadsafe()`.

        """
        check_callback_arg(callback, 'callback')
        self._reactor.callFromThread(callback)

    def call_later(self, delay, callback):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractIOServices.call_later()`.

        """
        check_callback_arg(callback, 'callback')
        return _TimerHandle(self._reactor.callLater(delay, callback))

    def getaddrinfo(self, host, port, on_done, family=0, socktype=0, proto=0,
                    flags=0):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractIOServices.getaddrinfo()`.

        """
        # Use thread pool to run getaddrinfo asynchronously
        return _TwistedDeferredIOReference(
            twisted_threads.deferToThreadPool(
                self._reactor, self._reactor.getThreadPool(),
                socket.getaddrinfo,
                # NOTE: python 2.x getaddrinfo only takes positional args
                host,
                port,
                family,
                socktype,
                proto,
                flags),
            on_done)

    def set_reader(self, fd, on_readable):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractFileDescriptorServices.set_reader()`.

        """
        LOGGER.debug('%s.set_reader(%s, %s)',
                     self.__class__.__name__, fd, on_readable)
        check_fd_arg(fd)
        check_callback_arg(on_readable, 'or_readable')
        try:
            descriptor = self._fd_watchers[fd]
        except KeyError:
            descriptor = self._SocketReadWriteDescriptor(
                fd,
                on_readable=on_readable)
            self._fd_watchers[fd] = descriptor
        else:
            descriptor.on_readable = on_readable

        self._reactor.addReader(descriptor)

    def remove_reader(self, fd):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractFileDescriptorServices.remove_reader()`.

        """
        LOGGER.debug('%s.remove_reader(%s)', self.__class__.__name__, fd)
        check_fd_arg(fd)
        try:
            descriptor = self._fd_watchers[fd]
        except KeyError:
            return False

        if descriptor.on_readable is None:
            assert descriptor.on_writable is not None, (
                '_SocketReadWriteDescriptor was neither readable nor writable.')
            return False

        descriptor.on_readable = None

        self._reactor.removeReader(descriptor)

        if descriptor.on_writable is None:
            self._fd_watchers.pop(fd)

        return True

    def set_writer(self, fd, on_writable):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractFileDescriptorServices.set_writer()`.

        """
        LOGGER.debug('%s.set_writer(%s, %s)',
                     self.__class__.__name__, fd, on_writable)
        check_fd_arg(fd)
        check_callback_arg(on_writable, 'on_writable')
        try:
            descriptor = self._fd_watchers[fd]
        except KeyError:
            descriptor = self._SocketReadWriteDescriptor(
                fd,
                on_writable=on_writable)
            self._fd_watchers[fd] = descriptor
        else:
            descriptor.on_writable = on_writable

        self._reactor.addWriter(descriptor)

    def remove_writer(self, fd):
        """Implement
        :py:meth:`.utils.nbio_interface.AbstractFileDescriptorServices.remove_writer()`.

        """
        LOGGER.debug('%s.remove_writer(%s)', self.__class__.__name__, fd)
        check_fd_arg(fd)
        try:
            descriptor = self._fd_watchers[fd]
        except KeyError:
            return False

        if descriptor.on_writable is None:
            assert descriptor.on_readable is not None, (
                '_SocketReadWriteDescriptor was neither writable nor readable.')
            return False

        descriptor.on_writable = None

        self._reactor.removeWriter(descriptor)

        if descriptor.on_readable is None:
            self._fd_watchers.pop(fd)
        return True


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


class _TwistedDeferredIOReference(nbio_interface.AbstractIOReference):
    """This module's adaptation of `nbio_interface.AbstractIOReference`
    for twisted defer.Deferred.

    On failure, extract the original exception from the Twisted Failure
    exception to pass to user's callback.

    """

    def __init__(self, deferred, on_done):
        """
        :param defer.Deferred deferred:
        :param callable on_done: user callback that takes the completion result
            or exception (check for `BaseException`) as its only arg. It will
            not be called if the operation was cancelled.

        """
        check_callback_arg(on_done, 'on_done')

        self._deferred = deferred
        self._cancelling = False

        def on_done_adapter(result):
            """Handle completion callback from the deferred instance. On
            Failure, extract the original exception from the Twisted Failure
            exception to pass to the user's callback.

            """

            # NOTE: Twisted makes callback for cancelled deferred, but pika
            # doesn't want that
            if not self._cancelling:
                if isinstance(result, twisted.python.failure.Failure):
                    LOGGER.debug(
                        'Deferred operation completed with Failure: %r',
                        result)
                    # Extract the original exception
                    result = result.value
                on_done(result)

        deferred.addBoth(on_done_adapter)

    def cancel(self):
        """Cancel pending operation

        :returns: False if was already done or cancelled; True otherwise

        """
        already_processed = (
            self._deferred.called and
            not isinstance(self._deferred.result, defer.Deferred))

        # So that our callback wrapper will know to suppress the mandatory
        # errorback from Deferred.cancel()
        self._cancelling = True

        # Always call through to cancel() in case our Deferred was waiting for
        # another one to complete, so the other one would get cancelled, too
        self._deferred.cancel()

        return not already_processed
