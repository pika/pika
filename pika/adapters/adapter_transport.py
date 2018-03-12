"""Plaintext and SSL/TLS transport abstractions for `BaseConnection`

"""
import abc
import collections
import errno
import functools
import logging
import numbers
import os
import socket
import ssl

import pika.compat
from pika.adapters.select_connection import PollEvents


LOGGER = logging.getLogger(__name__)


def _retry_on_sigint(func):
    """Function decorator for retrying on SIGINT.

    """

    @functools.wraps(func)
    def retry_sigint_wrap(*args, **kwargs):
        """Wrapper for decorated function"""
        while True:
            try:
                return func(*args, **kwargs)
            except pika.compat.SOCKET_ERROR as error:
                if error.errno == errno.EINTR:
                    continue
                else:
                    raise

    return retry_sigint_wrap


class AbstractTransport(pika.compat.AbstractBase):
    """Abstract Base Class for plain and SSL socket transports"""

    TRY_AGAIN_SOCK_ERROR_CODES = (errno.EAGAIN, errno.EWOULDBLOCK,)

    # NOTE: We need `PollEvents.ERROR` because Windows `select()` signals failed
    # connection establishment only via `exceptfds` (doesn't signal failure to
    # connect via `writefds`). So, we add `PollEvents.ERROR` to all filter
    # bitmasks to support sockets in any state of connectivity.

    _CONNECTING_EVENTS_FILTER = PollEvents.WRITE | PollEvents.ERROR

    _READ_ONLY_EVENTS_FILTER = PollEvents.READ | PollEvents.ERROR

    _WRITE_ONLY_EVENTS_FILTER = PollEvents.WRITE | PollEvents.ERROR

    _READ_WRITE_EVENTS_FILTER = (_READ_ONLY_EVENTS_FILTER |
                                 _WRITE_ONLY_EVENTS_FILTER)

    class EndOfInput(EnvironmentError):
        """Exception to raise when `socket.recv()` returns empty data

        """
        def __init__(self):
            super(AbstractTransport.EndOfInput, self).__init__(
                -1, 'End of input stream')


    def __init__(self, sock, on_connected=None):
        """

        :param sock: Non-blocking socket on which connection establishment has
                     been initiated.
        :param callable|None on_connected: if callable, will be called upon
            detection of successful connection establishment with the transport
            instance as its only arg and the transport will idle until
            kick-started via `begin_transporting()`. If None, the transport will
            idle until kick-started via `begin_transporting()`

        """
        if not (callable(on_connected) or on_connected is None):
            raise TypeError('on_connected must be callable or None.')

        self.sock = sock
        self.on_connected_callback = on_connected
        self.tx_buffers = None
        self.sink = None
        self.max_rx_bytes = None

    @abc.abstractmethod
    def poll_which_events(self):
        """Return socket events of interest for the next I/O poll.

        :return: a bitmask of PollEvents

        """
        pass

    def handle_events(self, events):
        """Handle the indicated I/O events on the socket by invoking the
        corresponding `_on_writable()`, `_on_readable()`, `_on_error()`.

        :param int events: Indicated bitmask of socket events from PollEvents
        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions
        :raises AbstractTransport.EndOfInput: upon shutdown of input stream
        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions

        """
        LOGGER.debug('handle_events: fd=%s; events=%s',
                     self.sock.fileno(),
                     events)

        if events & PollEvents.WRITE:
            self._on_writable()

        if events & PollEvents.READ:
            self._on_readable()

        if (events & PollEvents.ERROR and
                not events & (PollEvents.WRITE | PollEvents.READ)):
            # Error indicacation without corresponding READ and/or WRITE. This
            # happens with select on Windows: when non-blocking connection
            # attempt fails, Windows reports the failure through `exceptfds`,
            # but not via `writefds`. This could also happen with EPOLL if only
            # watching for the error event.
            self._on_error()

    def begin_transporting(self, tx_buffers, rx_sink, max_rx_bytes):
        """Begin transporting data; if called before connection establishment
        completes, will facilitate connection establishment in due course

        :param deque tx_buffers: container for outgoing frame buffers
        :param callable rx_sink: a method that takes incoming data bytes as its
                                 only arg.
        :param int max_rx_bytes: byte limit for `sock.recv()` calls

        """
        if not isinstance(tx_buffers, collections.deque):
            raise TypeError(
                'Expected tx_buffers of deque class, but got {!r}'.format(
                    tx_buffers))
        if not callable(rx_sink):
            raise TypeError(
                'Expected callable rx_sink, but got {!r}'.format(rx_sink))

        if not isinstance(max_rx_bytes, numbers.Integral):
            raise TypeError(
                'Expected integer max_rx_bytes, but got {!r}'.format(
                    max_rx_bytes))

        if max_rx_bytes <= 0:
            raise ValueError(
                'Expected max_rx_bytes > 0, but got {!r}'.format(max_rx_bytes))

        self.on_connected_callback = None
        self.tx_buffers = tx_buffers
        self.sink = rx_sink
        self.max_rx_bytes = max_rx_bytes


    def _on_error(self):
        """Called by `handle_events()` when ERROR is indicated without READ or
         WRITE.

        This happens with select on Windows: when non-blocking connection
        attempt fails, Windows reports the failure through `exceptfds`,
        but not via `writefds`. This could also happen with EPOLL if only
        watching for the error event.

        :raises pika.compat.SOCKET_ERROR: if error is set on the socket

        """
        error_code = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if error_code:
            self.on_connected_callback = None
            error_msg = os.strerror(error_code)
            LOGGER.warning(
                '%s connection failed: fd=%s errno=%s (%s)',
                self.__class__.__name__, self.sock.fileno(), error_code,
                error_msg)
            raise pika.compat.SOCKET_ERROR(error_code, error_msg)

        else:
            LOGGER.error(
                '%s._on_error() called, but there is no error on socket; fd=%s',
                self.__class__.__name__, self.sock.fileno())

    @abc.abstractmethod
    def _on_readable(self):
        """Called by `handle_events()` when readable socket is indicated.

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions
        :raises AbstractTransport.EndOfInput: upon shutdown of input stream

        """
        pass

    @abc.abstractmethod
    def _on_writable(self):
        """Called by `handle_events()` when writable socket is indicated.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the "try again" exceptions

        """
        pass

    def _ingest(self):
        """Utility method for use by subclasses to ingest data from socket and
        dispatch it to sink.

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR
        :raises AbstractTransport.EndOfInput: upon shutdown of input stream

        """
        data = self._sigint_safe_recv(self.sock, self.max_rx_bytes)

        # Empty data, should disconnect
        if not data:
            LOGGER.error('Socket EOF')
            raise self.EndOfInput()

        # Pass the data into our top level frame dispatching method
        self.sink(data)

    def _emit(self):
        """Utility method for use by subclasses to emit data from tx_buffers.
        This method sends frames from `tx_buffers` until all frames are
        exhausted or sending is interrupted by an exception. Maintains integrity
        of `self.tx_buffers`.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR

        """
        while self.tx_buffers:
            num_bytes_sent = self._sigint_safe_send(self.sock,
                                                    self.tx_buffers[0])

            frame = self.tx_buffers.popleft()
            if num_bytes_sent < len(frame):
                LOGGER.debug('Partial send, requeing remaining data.')
                self.tx_buffers.appendleft(frame[num_bytes_sent:])

    @staticmethod
    @_retry_on_sigint
    def _sigint_safe_recv(sock, max_bytes):
        """Receive data from socket, retrying on SIGINT.

        :param sock: stream or SSL socket
        :param max_bytes: maximum number of bytes to receive
        :return: received data or empty bytes on end of file
        :raises: whatever the corresponding `sock.recv()` raises except socket
                 error with errno.EINTR

        """
        return sock.recv(max_bytes)

    @staticmethod
    @_retry_on_sigint
    def _sigint_safe_send(sock, data):
        """Send data to socket, retrying on SIGINT.

        :param sock: stream or SSL socket
        :param data: data bytes to send
        :returns: number of bytes actually sent
        :raises: whatever the corresponding `sock.send()` raises except socket
                 error with errno.EINTR

        """
        return sock.send(data)


class PlainTransport(AbstractTransport):
    """Implementation of plaintext transport"""

    def poll_which_events(self):
        """Return socket events of interest for the next I/O poll.

        :return: a bitmask of PollEvents

        """
        if self.on_connected_callback is not None:
            events = self._CONNECTING_EVENTS_FILTER
        elif self.tx_buffers is not None:
            if self.tx_buffers:
                events = self._READ_WRITE_EVENTS_FILTER
            else:
                events = self._READ_ONLY_EVENTS_FILTER
        else:
            events = 0

        return events

    def _on_readable(self):
        """Ingest data from socket and dispatch it to sink.

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the "try again" exception
        :raises AbstractTransport.EndOfInput: upon shutdown of input stream

        """
        assert self.on_connected_callback is None

        try:
            self._ingest()
        except pika.compat.SOCKET_ERROR as error:
            if error.errno in self.TRY_AGAIN_SOCK_ERROR_CODES:
                LOGGER.debug('Recv would block on fd=%s', self.sock.fileno())
                return
            else:
                raise

    def _on_writable(self):
        """Emit data from tx_buffers.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the "try again" exception
        :raises pika.compat.SOCKET_ERROR: if connection establishment failed

        """
        if self.on_connected_callback is not None:
            try:
                self._conclude_connection_establishment()
            finally:
                self.on_connected_callback = None
        else:
            try:
                self._emit()
            except pika.compat.SOCKET_ERROR as error:
                if error.errno in self.TRY_AGAIN_SOCK_ERROR_CODES:
                    LOGGER.debug('Send would block on fd=%s.',
                                 self.sock.fileno())
                else:
                    raise

    def _conclude_connection_establishment(self):
        """Called once when `on_connected_callback` is available and socket
        becomes writable. If socket is not in error state, invokes
        `on_connected_callback()`, otherwise raises `pika.compat.SOCKET_ERROR`.

        :raises pika.compat.SOCKET_ERROR: if connection establishment failed
        """
        error_code = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if not error_code:
            LOGGER.info(
                '%s socket connection established; fd=%s',
                self.__class__.__name__, self.sock.fileno())
            self.on_connected_callback(self)
        else:
            error_msg = os.strerror(error_code)
            LOGGER.warning(
                '%s connection failed: fd=%s errno=%s (%s)',
                self.__class__.__name__, self.sock.fileno(), error_code,
                error_msg)
            raise pika.compat.SOCKET_ERROR(error_code, error_msg)


class SSLTransport(AbstractTransport):
    """Implementation of SSL transport."""

    def __init__(self, sock, on_connected=None):
        """
        :param sock: Non-blocking SSL socket on which plaintext connection
                     establishment has been initiated or already established.
        :param callable|None on_connected: if callable, will be called upon
            successful completion of SSL handshake with the transport instance
            as its only arg after which the transport will idle until
            kick-started via `begin_transporting()`. If None, the transport will
            idle until kick-started via `begin_transporting()`

        """
        super(SSLTransport, self).__init__(sock, on_connected)

        # Bootstrap SSL handshake; completion of TCP connection handshake is
        # indicated by socket becoming writable
        self._readable_action = None
        self._writable_action = (None if on_connected is None
                                 else self._do_handshake)

        self._do_handshake_initiated = False

    def begin_transporting(self, tx_buffers, rx_sink, max_rx_bytes):
        """Begin transporting data; if called before connection establishment
        completes, will facilitate connection establishment in due course

        :param deque tx_buffers: container for ougoing frame buffers
        :param callable rx_sink: a method that takes incoming data bytes as its
                                 only arg.
        :param int max_rx_bytes: byte limit for `sock.recv()` calls

        """
        super(SSLTransport, self).begin_transporting(tx_buffers,
                                                     rx_sink,
                                                     max_rx_bytes)
        if not self._do_handshake_initiated:
            self._readable_action = None
            self._writable_action = self._do_handshake
        else:
            # Bootstrap ingester in case some incoming data ended up in SSL
            # rx buffer; it will also configure our event action state
            self._ingest()

    def poll_which_events(self):
        """Return socket events of interest for the next I/O poll.

        :return: a bitmask of PollEvents

        """
        events = 0
        if self._readable_action is not None:
            events |= self._READ_ONLY_EVENTS_FILTER
        if self._writable_action is not None:
            events |= self._WRITE_ONLY_EVENTS_FILTER

        # The transport would stall without events
        assert events or (self.on_connected_callback is None and
                          self.tx_buffers is None)

        return events

    def _on_readable(self):
        """Handles readability indication.

        :raises: whatever the corresponding `sock.recv()` raises except
                 the socket error with errno.EINTR and the ssl.SSLError
                 exception with SSL_ERROR_WANT_READ/WRITE
        :raises AbstractTransport.EndOfInput: upon shutdown of input stream

        """
        if self._readable_action is not None:
            self._readable_action()
        else:
            LOGGER.debug('SSL readable handler is suppressed.')

    def _on_writable(self):
        """Handles writability indication.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the ssl.SSLError exception
                 with SSL_ERROR_WANT_READ/WRITE

        """
        if self._writable_action is not None:
            self._writable_action()
        else:
            LOGGER.debug('SSL writable handler is suppressed.')

    def _do_handshake(self):
        """Perform SSL handshake

        :raises ssl.SSLError: whatever the corresponding `sock.do_handshake()`
                              raises except the ssl.SSLError exception with
                              SSL_ERROR_WANT_READ/WRITE.

        """
        self._do_handshake_initiated = True
        try:
            self.sock.do_handshake()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                LOGGER.debug('SSL handshake wants read; fd=%s.',
                             self.sock.fileno())
                self._readable_action = self._do_handshake
                self._writable_action = None
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                LOGGER.debug('SSL handshake wants write. fd=%s',
                             self.sock.fileno())
                self._readable_action = None
                self._writable_action = self._do_handshake
            else:
                LOGGER.error('SSL handshake failed. fd=%s',
                             self.sock.fileno())
                raise
        else:
            LOGGER.info('SSL handshake completed successfully. fd=%s',
                        self.sock.fileno())
            if self.on_connected_callback is None:
                # Bootstrap ingester in case some incoming data ended up in SSL
                # rx buffer; it will also configure our event action state
                self._ingest()
            else:
                try:
                    self.on_connected_callback(self)
                finally:
                    self.on_connected_callback = None
                    self._readable_action = None
                    self._writable_action = None

    def _ingest(self):
        """Ingest data from socket and dispatch it to sink until an exception
        occurs (typically ssl.SSLError with SSL_ERROR_WANT_READ/WRITE).

        :raises: whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR and the ssl.SSLError exception
                 with SSL_ERROR_WANT_READ/WRITE
        :raises AbstractTransport.EndOfInput: upon shutdown of input stream

        """
        try:
            # Empty out SSL rx buffer
            while True:
                super(SSLTransport, self)._ingest()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                LOGGER.debug('SSL ingester wants read.')
                self._readable_action = self._ingest
                self._writable_action = self._emit if self.tx_buffers else None
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                LOGGER.debug('SSL ingester wants write.')
                self._readable_action = None
                self._writable_action = self._ingest
            else:
                raise

    def _emit(self):
        """Emit data from tx_buffers.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR and the ssl.SSLError exception
                 with SSL_ERROR_WANT_READ/WRITE

        """
        try:
            super(SSLTransport, self)._emit()
        except ssl.SSLError as error:
            if error.errno == ssl.SSL_ERROR_WANT_READ:
                LOGGER.debug('SSL emitter wants read.')
                self._readable_action = self._emit
                self._writable_action = None
            elif error.errno == ssl.SSL_ERROR_WANT_WRITE:
                LOGGER.debug('SSL emitter wants write.')
                # TODO Do we need to re-boostrap ingester as in _do_handshake?
                self._readable_action = self._ingest
                self._writable_action = self._emit
            else:
                raise
        else:
            # TODO Do we need to re-boostrap ingester as in _do_handshake?
            self._readable_action = self._ingest
            self._writable_action = self._emit if self.tx_buffers else None
