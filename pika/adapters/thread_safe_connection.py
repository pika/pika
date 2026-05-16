"""Thread-safe connection wrapper for pika.

Runs SelectConnection's IOLoop in a dedicated background thread so that
all socket I/O — including writes to _tx_buffers — stays confined to one
thread.  External threads publish by scheduling callbacks via
add_callback_threadsafe, which uses an atomic deque append + a wakeup
signal to hand work to the IOLoop thread without a lock.

This eliminates the ``IndexError: pop from an empty deque`` race seen when
multiple threads call basic_publish() on a shared connection directly
(issues #1144 and #511).
"""
import functools
import itertools
import logging
import threading

from pika.adapters.select_connection import SelectConnection

LOGGER = logging.getLogger(__name__)


class ThreadSafeChannel:
    """Thread-safe wrapper around :class:`pika.channel.Channel`.

    Every write operation is routed through the parent connection's
    ``add_callback_threadsafe`` so that ``_tx_buffers`` is only ever
    touched from the IOLoop thread.
    """

    def __init__(self, channel, connection):
        self._channel = channel
        self._connection = connection

    def basic_publish(self,
                      exchange,
                      routing_key,
                      body,
                      properties=None,
                      mandatory=False):
        """Schedule a publish in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.
        """
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_publish,
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=mandatory,
            ))

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Schedule an acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.
        """
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_ack,
                delivery_tag=delivery_tag,
                multiple=multiple,
            ))

    def basic_nack(self, delivery_tag=0, multiple=False, requeue=True):
        """Schedule a negative acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.
        """
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_nack,
                delivery_tag=delivery_tag,
                multiple=multiple,
                requeue=requeue,
            ))

    def basic_reject(self, delivery_tag=0, requeue=True):
        """Schedule a rejection in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.
        """
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_reject,
                delivery_tag=delivery_tag,
                requeue=requeue,
            ))

    def queue_declare(self,
                      queue,
                      passive=False,
                      durable=False,
                      exclusive=False,
                      auto_delete=False,
                      arguments=None):
        """Declare a queue and block the calling thread until Queue.DeclareOk arrives.

        Safe to call from any thread.

        :returns: The Queue.DeclareOk method frame.
        :rtype: pika.frame.Method
        """
        ready = threading.Event()
        result = [None]

        def _declare():

            def _on_declare(method_frame):
                result[0] = method_frame
                ready.set()

            self._channel.queue_declare(
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments,
                callback=_on_declare,
            )

        self._connection.add_callback_threadsafe(_declare)
        ready.wait()
        return result[0]

    @property
    def channel_number(self):
        return self._channel.channel_number

    @property
    def is_open(self):
        return self._channel.is_open

    @property
    def is_closed(self):
        return self._channel.is_closed


class ThreadSafeConnection:
    """Pika connection that is safe to use from multiple threads.

    .. note:: Each instance starts a background thread named
        ``pika-ioloop-N`` (where *N* is a per-process sequence number)
        so that multiple connections are distinguishable in stack traces
        and thread listings.

    Internally wraps :class:`~pika.adapters.SelectConnection` and runs its
    IOLoop in a single dedicated background thread (the *IOLoop thread*).
    All channel operations submitted from external threads are routed
    through :meth:`add_callback_threadsafe` so that ``_tx_buffers`` has
    exactly one owner — the IOLoop thread.

    Usage::

        conn = ThreadSafeConnection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()

        # safe to call from any number of threads simultaneously
        threading.Thread(target=ch.basic_publish,
                         kwargs=dict(exchange='', routing_key='q',
                                     body='hello')).start()

        conn.close()

    :param parameters: Connection parameters.
    :type parameters: pika.connection.Parameters
    :param callable | None on_open_error_callback:
        Called in the IOLoop thread if the connection cannot be established.
        Signature: ``on_open_error_callback(connection, exception)``
    :param callable | None on_close_callback:
        Called in the IOLoop thread when the connection is closed.
        Signature: ``on_close_callback(connection, reason)``
    :raises Exception: if the connection cannot be established.
    """

    _instance_counter = itertools.count(1)

    def __init__(self,
                 parameters,
                 on_open_error_callback=None,
                 on_close_callback=None):
        self._user_on_open_error_callback = on_open_error_callback
        self._user_on_close_callback = on_close_callback

        self._connect_error = None
        self._connected_event = threading.Event()

        self._channel_waiters_lock = threading.Lock()
        self._closed_reason = None
        self._pending_channel_waiters = []

        self._connection = SelectConnection(
            parameters=parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

        self._ioloop_thread = threading.Thread(
            target=self._connection.ioloop.start,
            name=f'pika-ioloop-{next(self._instance_counter)}',
            daemon=True,
        )
        self._ioloop_thread.start()

        # Block the calling thread until the connection is open or fails.
        self._connected_event.wait()
        if self._connect_error is not None:
            raise self._connect_error

    # ------------------------------------------------------------------
    # IOLoop-thread callbacks
    # ------------------------------------------------------------------

    def _on_connection_open(self, _connection):
        self._connected_event.set()

    def _on_connection_open_error(self, _connection, error):
        self._connect_error = error
        # Stop the IOLoop so the background thread can exit cleanly.
        self._connection.ioloop.stop()
        self._connected_event.set()
        if self._user_on_open_error_callback:
            self._user_on_open_error_callback(_connection, error)

    def _on_connection_closed(self, _connection, reason):
        # Connection is gone — stop the IOLoop so the thread exits.
        self._connection.ioloop.stop()
        with self._channel_waiters_lock:
            self._closed_reason = reason
            for evt, err in self._pending_channel_waiters:
                err[0] = reason
                evt.set()
            self._pending_channel_waiters.clear()
        if self._user_on_close_callback:
            self._user_on_close_callback(_connection, reason)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def channel(self):
        """Open a new channel and return a :class:`ThreadSafeChannel`.

        Blocks the calling thread until the channel is open.  The returned
        channel's methods are safe to call from any thread.

        :rtype: ThreadSafeChannel
        :raises Exception: if the connection is closed before the channel opens.
        """
        ready = threading.Event()
        result = [None]
        error = [None]

        with self._channel_waiters_lock:
            if self._closed_reason is not None:
                raise self._closed_reason
            self._pending_channel_waiters.append((ready, error))

        def _open():

            def _on_open(ch):
                result[0] = ch
                ready.set()

            self._connection.channel(on_open_callback=_on_open)

        self._connection.add_callback_threadsafe(_open)
        ready.wait()

        with self._channel_waiters_lock:
            try:
                self._pending_channel_waiters.remove((ready, error))
            except ValueError:
                pass

        if error[0] is not None:
            raise error[0]

        return ThreadSafeChannel(result[0], self._connection)

    def close(self, timeout=10):
        """Close the connection and block until the IOLoop thread exits.

        Schedules a clean Connection.Close handshake and waits up to
        *timeout* seconds for the IOLoop thread to finish.  If the thread
        is still alive after the timeout (e.g. the broker never sends
        Connection.CloseOk), the IOLoop is force-stopped via
        ``add_callback_threadsafe`` and joined once more.

        :param float | None timeout: Seconds to wait for a clean close before
            force-stopping the IOLoop. Defaults to 10 seconds, which is
            sufficient for a healthy broker on any reasonable network.
            Pass ``None`` to wait indefinitely.
        """
        self._connection.add_callback_threadsafe(self._connection.close)
        self._ioloop_thread.join(timeout=timeout)
        if self._ioloop_thread.is_alive():
            self._connection.add_callback_threadsafe(
                self._connection.ioloop.stop)
            self._ioloop_thread.join()

    def add_callback_threadsafe(self, callback):
        """Schedule *callback* to run in the IOLoop thread.

        The only thread-safe method on the underlying connection — exposed
        here so callers can schedule arbitrary work (e.g. queue_declare)
        without going through the wrapper.

        :param callable callback: Zero-argument callable.
        """
        self._connection.add_callback_threadsafe(callback)

    @property
    def is_open(self):
        return self._connection.is_open

    @property
    def is_closed(self):
        return self._connection.is_closed
