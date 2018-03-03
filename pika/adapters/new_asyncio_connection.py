"""Use pika with the Asyncio EventLoop"""

import asyncio
import logging
import socket
import ssl

from pika.adapters import base_connection, ioloop_interface


LOGGER = logging.getLogger(__name__)


class AsyncioConnection(base_connection.BaseConnection):
    """ The AsyncioConnection runs on the Asyncio EventLoop.

    """
    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 custom_ioloop=None):
        """ Create a new instance of the AsyncioConnection class, connecting
        to RabbitMQ automatically

        :param pika.connection.Parameters parameters: Connection parameters
        :param callable on_open_callback: The method to call when the connection
            is open
        :param callable on_open_error_callback: Method to call if the connection
            can't be opened.
        :param asyncio.AbstractEventLoop custom_ioloop: Defaults to
            asyncio.get_event_loop().

        """
        ioloop = AsyncioAsyncServicesAdapter(custom_ioloop)

        super().__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            ioloop
        )


class AsyncioAsyncServicesAdapter(ioloop_interface.AbstractAsyncServices):
    """Implement ioloop_interface.AbstractAsyncServices on top of asyncio"""

    def __init__(self, loop=None):
        """

        :param asyncio.AbstractEventLoop|None loop: If None, gets default event
            loop from asyncio.

        """
        self._loop = loop or asyncio.get_event_loop()

    def close(self):
        """Release IOLoop's resources.

        the `close()` method is intended to be called by the application or test
        code only after `start()` returns. After calling `close()`, no other
        interaction with the closed instance of `IOLoop` should be performed.

        """
        self._loop.close()

    def get_native_ioloop(self):
        """Returns the native I/O loop instance, such as Twisted reactor,
        asyncio's or tornado's event loop

        """
        return self._loop

    def start(self):
        """Run the I/O loop. It will loop until requested to exit. See `stop()`.

        """
        self._loop.run_forever()

    def stop(self):
        """Request exit from the ioloop. The loop is NOT guaranteed to
        stop before this method returns.

        To invoke `stop()` safely from a thread other than this IOLoop's thread,
        call it via `add_callback_threadsafe`; e.g.,

            `ioloop.add_callback_threadsafe(ioloop.stop)`

        """
        self._loop.stop()

    def add_callback_threadsafe(self, callback):
        """Requests a call to the given function as soon as possible. It will be
        called from this IOLoop's thread.

        NOTE: This is the only thread-safe method offered by the IOLoop adapter.
              All other manipulations of the IOLoop adapter and objects governed
              by it must be performed from the IOLoop's thread.

        :param method callback: The callback method; must be callable.
        """
        self._loop.call_soon_threadsafe(callback)

    def call_later(self, delay, callback):
        """Add the callback to the IOLoop timer to be called after delay seconds
        from the time of call on best-effort basis. Returns a handle to the
        timeout.

        :param int delay: The number of seconds to wait to call callback
        :param method callback: The callback method
        :rtype: handle to the created timeout that may be passed to
            `remove_timeout()`

        """
        self._loop.call_later(delay, callback)

    def remove_timeout(self, timeout_handle):
        """Remove a timeout

        :param timeout_handle: Handle of timeout to remove

        """
        timeout_handle.cancel()

    def set_reader(self, fd, on_readable):
        """Call the given callback when the file descriptor is readable.
        Replace prior reader, if any, for the given file descriptor.

        :param fd: file descriptor
        :param callable on_readable: a callback taking no args to be notified
            when fd becomes readable.

        """
        self._loop.add_reader(fd, on_readable)

    def remove_reader(self, fd):
        """Stop watching the given file descriptor for readability

        :param fd: file descriptor.
        :returns: True if reader was removed; False if none was registered.

        """
        return self._loop.remove_reader(fd)

    def set_writer(self, fd, on_writable):
        """Call the given callback whenever the file descriptor is writable.
        Replace prior writer callback, if any, for the given file descriptor.

        :param fd: file descriptor
        :param callable on_writable: a callback taking no args to be notified
            when fd becomes writable.

        """
        self._loop.add_writer(fd, on_writable)

    def remove_writer(self, fd):
        """Stop watching the given file descriptor for writability

        :param fd: file descriptor
        :returns: True if writer was removed; False if none was registered.

        """
        return self._loop.remove_writer(fd)

    def getaddrinfo(self, host, port, on_done, family=0, socktype=0, proto=0,
                    flags=0):
        """Perform the equivalent of `socket.getaddrinfo()` asynchronously.

        See `socket.getaddrinfo()` for the standard args.

        :param callable on_done: user callback that takes the return value of
            `socket.getaddrinfo()` upon successful completion or exception upon
            failure as its only arg. It will not be called if the operation
            was successfully cancelled.
        :rtype: AbstractAsyncReference

        """
        return self._schedule_and_wrap_in_async_ref(
            self._loop.getaddrinfo(host,
                                   port,
                                   family=family,
                                   type=socktype,
                                   proto=proto,
                                   flags=flags),
            on_done)

    def connect_socket(self, sock, resolved_addr, on_done):
        """Perform the equivalent of `socket.connect()` on a previously-resolved
        address asynchronously.

        IMPLEMENTATION NOTE: Pika's connection logic resolves the addresses
            prior to making socket connections, so we don't need to burden the
            implementations of this method with the extra logic of asynchronous
            DNS resolution. Implementations can use `socket.inet_pton()` to
            verify the address.

        :param socket.socket sock: non-blocking socket that needs to be
            connected as per `socket.socket.connect()`
        :param tuple resolved_addr: resolved destination address/port two-tuple
            as per `socket.socket.connect()`, except that the first element must
            be an actual IP address that's consistent with the given socket's
            address family.
        :param callable on_done: user callback that takes None upon successful
            completion or Exception-based exception upon error as its only arg.
            It will not be called if the operation was successfully cancelled.

        :rtype: AbstractAsyncReference
        :raises ValueError: if host portion of `resolved_addr` is not an IP
            address or is inconsistent with the socket's address family as
            validated via `socket.inet_pton()`
        """
        try:
            socket.inet_pton(sock.family, resolved_addr[0])
        except Exception as exc:
            msg = ('connect_socket() called with invalid or unresolved IP '
                   'address {!r} for socket {}: {!r}').format(resolved_addr,
                                                              sock, exc)
            LOGGER.error(msg)
            raise ValueError(msg)

        return self._schedule_and_wrap_in_async_ref(
            self._loop.sock_connect(sock, resolved_addr), on_done)

    def create_streaming_connection(self,
                                    protocol_factory,
                                    sock,
                                    on_done,
                                    ssl_context=None,
                                    server_hostname=None):
        """Perform SSL session establishment, if requested, on the already-
        connected socket and link the streaming transport/protocol pair.

        NOTE: This method takes ownership of the socket.

        :param callable protocol_factory: returns an instance with the
            `AbstractStreamProtocol` interface. The protocol's
            `connection_made(transport)` method will be called to link it to
            the transport after remaining connection activity (e.g., SSL session
            establishment), if any, is completed successfully.
        :param socket.socket sock: Already-connected, non-blocking
            `socket.SOCK_STREAM` socket to be used by the transport. We take
            ownership of this socket.
        :param callable on_done: User callback
            `on_done(Exception | (transport, protocol))` to be notified when the
            asynchronous operation completes. Exception-based arg indicates
            failure; otherwise the two-tuple will contain the linked
            transport/protocol pair, with the AbstractStreamTransport and
            AbstractStreamProtocol respectively.
        :param None | ssl.SSLContext ssl_context: if None, this will proceed as
            a plaintext connection; otherwise, if not None, SSL session
            establishment will be performed prior to linking the transport and
            protocol.
        :param str | None server_hostname: For use during SSL session
            establishment to match against the target server's certificate. The
            value `None` disables this check (which is a huge security risk)
        :rtype: AbstractAsyncReference
        """
        if not isinstance(ssl_context, (type(None), ssl.SSLContext)):
            raise ValueError('Expected ssl_context=None | ssl.SSLContext, but '
                             'got {!r}'.format(ssl_context))

        if server_hostname is not None and ssl_context is None:
            raise ValueError('Non-None server_hostname must not be passed '
                             'without ssl context')

        return self._schedule_and_wrap_in_async_ref(
            self._loop.create_connection(protocol_factory,
                                         sock=sock,
                                         ssl=ssl_context,
                                         server_hostname=server_hostname),
            on_done)

    def _schedule_and_wrap_in_async_ref(self, coro, on_done):
        """Schedule the coroutine to run and return AsyncReferenceAdapter

        :param coroutine-obj coro:
        :param callable on_done: user callback that takes the completion result
            or exception as its only arg. It will not be called if the operation
            was successfully cancelled.
        :rtype: AsyncReferenceAdapter which is derived from
            ioloop_interface.AbstractAsyncReference

        """
        if not callable(on_done):
            raise TypeError(
                'on_done arg must be callable, but got {!r}'.format(on_done))

        return AsyncReferenceAdapter(
            asyncio.ensure_future(coro, loop=self._loop),
            on_done)


class AsyncReferenceAdapter(ioloop_interface.AbstractAsyncReference):
    """This module's adaptation of `ioloop_interface.AbstractAsyncReference`"""

    def __init__(self, future, on_done):
        """
        :param asyncio.Future future:
        :param callable on_done: user callback that takes the completion result
            or exception as its only arg. It will not be called if the operation
            was successfully cancelled.

        """
        if not callable(on_done):
            raise TypeError(
                'on_done arg must be callable, but got {!r}'.format(on_done))

        self._future = future

        def on_done_adapter(future):
            """Handle completion callback from the future instance"""

            # NOTE: Asyncio schedules callback for cancelled futures, but pika
            # doesn't want that
            if not future.cancelled():
                on_done(future.exception() or future.result())

        future.add_done_callback(on_done_adapter)

    def cancel(self):
        """Cancel pending operation

        :returns: False if was already done or cancelled; True otherwise

        """
        return self._future.cancel()
