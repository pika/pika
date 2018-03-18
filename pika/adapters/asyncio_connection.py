"""Use pika with the Asyncio EventLoop"""

import asyncio
import logging

from pika.adapters import async_service_utils, base_connection, async_interface


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
        super().__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            _AsyncioAsyncServicesAdapter(custom_ioloop))


class _AsyncioAsyncServicesAdapter(
        async_service_utils.AsyncSocketConnectionMixin,
        async_service_utils.AsyncStreamingConnectionMixin,
        async_interface.AbstractAsyncServices):
    """Implement async_interface.AbstractAsyncServices on top of asyncio

    """

    def __init__(self, loop=None):
        """
        :param asyncio.AbstractEventLoop | None loop: If None, gets default
            event loop from asyncio.

        """
        self._loop = loop or asyncio.get_event_loop()

    def get_native_ioloop(self):
        """Returns the native I/O loop instance, such as Twisted reactor,
        asyncio's or tornado's event loop

        """
        return self._loop

    def close(self):
        """Release IOLoop's resources.

        the `close()` method is intended to be called by Pika's own test
        code only after `start()` returns. After calling `close()`, no other
        interaction with the closed instance of `IOLoop` should be performed.

        NOTE: This method is provided for Pika's own test scripts that need to
        be able to run I/O loops generically to test multiple Connection Adapter
        implementations. Pika users should use the native I/O loop's API
        instead.

        """
        self._loop.close()

    def run(self):
        """Run the I/O loop. It will loop until requested to exit. See `stop()`.

        NOTE: This method is provided for Pika's own test scripts that need to
        be able to run I/O loops generically to test multiple Connection Adapter
        implementations (not all of the supported I/O Loop frameworks have
        methods named start/stop). Pika users should use the native I/O loop's
        API instead.

        """
        self._loop.run_forever()

    def stop(self):
        """Request exit from the ioloop. The loop is NOT guaranteed to
        stop before this method returns.

        NOTE: This method is provided for Pika's own test scripts that need to
        be able to run I/O loops generically to test multiple Connection Adapter
        implementations (not all of the supported I/O Loop frameworks have
        methods named start/stop). Pika users should use the native I/O loop's
        API instead.

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
        :rtype: _TimerHandle

        """
        return _TimerHandle(self._loop.call_later(delay, callback))

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
            `socket.getaddrinfo()` upon successful completion or exception
            (check for `BaseException`) upon failure as its only arg. It will
            not be called if the operation was cancelled.
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

    def _schedule_and_wrap_in_async_ref(self, coro, on_done):
        """Schedule the coroutine to run and return _AsyncioAsyncReference

        :param coroutine-obj coro:
        :param callable on_done: user callback that takes the completion result
            or exception as its only arg. It will not be called if the operation
            was cancelled.
        :rtype: _AsyncioAsyncReference which is derived from
            async_interface.AbstractAsyncReference

        """
        if not callable(on_done):
            raise TypeError(
                'on_done arg must be callable, but got {!r}'.format(on_done))

        return _AsyncioAsyncReference(
            asyncio.ensure_future(coro, loop=self._loop),
            on_done)


class _TimerHandle(async_interface.AbstractTimerReference):
    """This module's adaptation of `async_interface.AbstractTimerReference`.

    """

    def __init__(self, handle):
        """

        :param asyncio.Handle handle:
        """
        self._handle = handle

    def cancel(self):
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None


class _AsyncioAsyncReference(async_interface.AbstractAsyncReference):
    """This module's adaptation of `async_interface.AbstractAsyncReference`.

    """

    def __init__(self, future, on_done):
        """
        :param asyncio.Future future:
        :param callable on_done: user callback that takes the completion result
            or exception as its only arg. It will not be called if the operation
            was cancelled.

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
        :rtype: bool

        """
        return self._future.cancel()
