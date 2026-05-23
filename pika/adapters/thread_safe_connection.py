"""Thread-safe connection wrapper for pika.

Runs SelectConnection's IOLoop in a dedicated background thread so that
all socket I/O - including writes to _tx_buffers - stays confined to one
thread.  External threads publish by scheduling callbacks via
add_callback_threadsafe, which uses an atomic deque append + a wakeup
signal to hand work to the IOLoop thread without a lock.

This eliminates the ``IndexError: pop from an empty deque`` race seen when
multiple threads call basic_publish() on a shared connection directly
(issues #1144 and #511).
"""
import concurrent.futures
import itertools
import logging
import threading

from pika.adapters.select_connection import SelectConnection

LOGGER = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 10


class ThreadSafeChannel:
    """Thread-safe wrapper around :class:`pika.channel.Channel`.

    Every write operation is routed through the parent connection's
    ``add_callback_threadsafe`` so that ``_tx_buffers`` is only ever
    touched from the IOLoop thread.

    .. rubric:: Consumer callback threading model

    The *on_message_callback* registered with :meth:`basic_consume` is
    dispatched on a per-channel worker thread, **not** the IOLoop
    thread.  This means:

    - Blocking operations (database writes, HTTP calls, even
      :meth:`queue_declare` / :meth:`basic_qos` / :meth:`basic_cancel`)
      are safe inside delivery callbacks.
    - The IOLoop thread is never starved by slow consumer processing,
      so heartbeats are always sent on time.
    - Messages are delivered to the callback **in order** (a single
      worker thread per channel).
    - All :class:`ThreadSafeChannel` methods (:meth:`basic_ack`,
      :meth:`basic_nack`, :meth:`basic_reject`, :meth:`basic_publish`,
      :meth:`queue_declare`, etc.) are safe to call from within the
      callback.

    .. rubric:: IOLoop-thread callbacks

    Callables passed directly to
    :meth:`~ThreadSafeConnection.add_callback_threadsafe` still run
    on the IOLoop thread.  These must return quickly and must not call
    blocking channel methods (which would deadlock).
    """

    def __init__(self, channel, wrapper):
        self._channel = channel
        self._wrapper = wrapper
        self._consumer_work_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix='pika-consumer',
        )
        self._pool_shutdown = False

    def _check_not_closed(self):
        """Raise if the connection is known to be closed.

        Called from fire-and-forget methods to prevent silently dropping
        work when the connection is already gone.
        """
        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason

    def _dispatch_consumer_callback(self, callback, method, properties, body):
        """Execute a consumer callback on the pool worker thread.

        Catches and logs exceptions so that one failing delivery does not
        prevent subsequent deliveries from being processed.
        """
        try:
            callback(self, method, properties, body)
        except Exception:
            LOGGER.exception('Unhandled exception in consumer callback')

    def _shutdown_pool(self):
        """Shut down the consumer work pool, allowing in-flight work to finish."""
        if not self._pool_shutdown:
            self._pool_shutdown = True
            self._consumer_work_pool.shutdown(wait=True)

    def basic_publish(self,
                      exchange,
                      routing_key,
                      body,
                      properties=None,
                      mandatory=False):
        """Schedule a publish in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _publish():
            try:
                self._channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=properties,
                    mandatory=mandatory,
                )
            except Exception:
                LOGGER.warning('basic_publish failed (channel may have closed)',
                               exc_info=True)

        self._wrapper.add_callback_threadsafe(_publish)

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Schedule an acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _ack():
            try:
                self._channel.basic_ack(delivery_tag=delivery_tag,
                                        multiple=multiple)
            except Exception:
                LOGGER.warning('basic_ack failed (channel may have closed)',
                               exc_info=True)

        self._wrapper.add_callback_threadsafe(_ack)

    def basic_nack(self, delivery_tag=0, multiple=False, requeue=True):
        """Schedule a negative acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _nack():
            try:
                self._channel.basic_nack(delivery_tag=delivery_tag,
                                         multiple=multiple,
                                         requeue=requeue)
            except Exception:
                LOGGER.warning('basic_nack failed (channel may have closed)',
                               exc_info=True)

        self._wrapper.add_callback_threadsafe(_nack)

    def basic_reject(self, delivery_tag=0, requeue=True):
        """Schedule a rejection in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _reject():
            try:
                self._channel.basic_reject(delivery_tag=delivery_tag,
                                           requeue=requeue)
            except Exception:
                LOGGER.warning('basic_reject failed (channel may have closed)',
                               exc_info=True)

        self._wrapper.add_callback_threadsafe(_reject)

    def basic_qos(self,
                  prefetch_size=0,
                  prefetch_count=0,
                  global_qos=False,
                  timeout=DEFAULT_RPC_TIMEOUT):
        """Set channel QoS and block until Basic.QosOk arrives.

        Safe to call from any thread.

        :param int prefetch_size: Prefetch window in octets (0 = no limit).
        :param int prefetch_count: Prefetch window in whole messages (0 = no limit).
        :param bool global_qos: Apply QoS to all consumers on the channel.
        :param float | None timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Basic.QosOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        ready = threading.Event()
        result = [None]
        error: list[BaseException | None] = [None]

        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason
            self._wrapper._blocking_waiters.append((ready, error))

        def _qos():

            def _on_qos_ok(method_frame):
                result[0] = method_frame
                ready.set()

            def _on_chan_close(ch, reason):
                if not ready.is_set():
                    if error[0] is None:
                        error[0] = reason
                    ready.set()

            try:
                self._channel.add_on_close_callback(_on_chan_close)
                self._channel.basic_qos(
                    prefetch_size=prefetch_size,
                    prefetch_count=prefetch_count,
                    global_qos=global_qos,
                    callback=_on_qos_ok,
                )
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper.add_callback_threadsafe(_qos)

        if not ready.wait(timeout=timeout):
            raise TimeoutError(f'basic_qos timed out after {timeout} seconds')

        with self._wrapper._channel_waiters_lock:
            try:
                self._wrapper._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

        if error[0] is not None:
            raise error[0]
        return result[0]

    def basic_consume(self,
                      queue,
                      on_message_callback,
                      auto_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None,
                      timeout=DEFAULT_RPC_TIMEOUT):
        """Register a consumer and block until Basic.ConsumeOk arrives.

        The *on_message_callback* is dispatched on the channel's worker
        thread, not the IOLoop thread.  All :class:`ThreadSafeChannel`
        methods are safe to call from within the callback.

        Because the channel uses a single worker thread, deliveries are
        processed serially.  A callback that blocks (e.g. on a database
        write or a call to :meth:`queue_declare`) delays subsequent
        deliveries on the same channel until it returns.

        Safe to call from any thread.

        :param str queue: Queue to consume from.
        :param callable on_message_callback:
            ``callback(channel, method, properties, body)``
        :param bool auto_ack: Disable manual acknowledgement.
        :param bool exclusive: Request exclusive consumer access.
        :param str | None consumer_tag: Client-provided tag; generated if omitted.
        :param dict | None arguments: Additional AMQP arguments.
        :param float | None timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The consumer tag assigned by the broker.
        :rtype: str
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        ready = threading.Event()
        result = [None]
        error: list[BaseException | None] = [None]

        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason
            self._wrapper._blocking_waiters.append((ready, error))

        def _consume():

            def _wrapped_callback(ch, method, properties, body):
                try:
                    self._consumer_work_pool.submit(
                        self._dispatch_consumer_callback, on_message_callback,
                        method, properties, body)
                except RuntimeError:
                    LOGGER.debug(
                        'Consumer delivery dropped: work pool shut down')

            def _on_consume_ok(method_frame):
                result[0] = method_frame.method.consumer_tag
                ready.set()

            def _on_chan_close(ch, reason):
                if not ready.is_set():
                    if error[0] is None:
                        error[0] = reason
                    ready.set()

            try:
                self._channel.add_on_close_callback(_on_chan_close)
                self._channel.basic_consume(
                    queue=queue,
                    on_message_callback=_wrapped_callback,
                    auto_ack=auto_ack,
                    exclusive=exclusive,
                    consumer_tag=consumer_tag,
                    arguments=arguments,
                    callback=_on_consume_ok,
                )
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper.add_callback_threadsafe(_consume)

        if not ready.wait(timeout=timeout):
            raise TimeoutError(
                f'basic_consume timed out after {timeout} seconds')

        with self._wrapper._channel_waiters_lock:
            try:
                self._wrapper._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

        if error[0] is not None:
            raise error[0]
        return result[0]

    def basic_cancel(self, consumer_tag, timeout=DEFAULT_RPC_TIMEOUT):
        """Cancel a consumer and block until Basic.CancelOk arrives.

        Safe to call from any thread.

        :param str consumer_tag: Tag returned by :meth:`basic_consume`.
        :param float | None timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Basic.CancelOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        ready = threading.Event()
        result = [None]
        error: list[BaseException | None] = [None]

        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason
            self._wrapper._blocking_waiters.append((ready, error))

        def _cancel():

            def _on_cancel_ok(method_frame):
                result[0] = method_frame
                ready.set()

            def _on_chan_close(ch, reason):
                if not ready.is_set():
                    if error[0] is None:
                        error[0] = reason
                    ready.set()

            try:
                self._channel.add_on_close_callback(_on_chan_close)
                self._channel.basic_cancel(
                    consumer_tag=consumer_tag,
                    callback=_on_cancel_ok,
                )
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper.add_callback_threadsafe(_cancel)

        if not ready.wait(timeout=timeout):
            raise TimeoutError(
                f'basic_cancel timed out after {timeout} seconds')

        with self._wrapper._channel_waiters_lock:
            try:
                self._wrapper._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

        if error[0] is not None:
            raise error[0]
        return result[0]

    def queue_declare(self,
                      queue,
                      passive=False,
                      durable=False,
                      exclusive=False,
                      auto_delete=False,
                      arguments=None,
                      timeout=DEFAULT_RPC_TIMEOUT):
        """Declare a queue and block the calling thread until Queue.DeclareOk arrives.

        Safe to call from any thread.

        :param float | None timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Queue.DeclareOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        ready = threading.Event()
        result = [None]
        error: list[BaseException | None] = [None]

        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason
            self._wrapper._blocking_waiters.append((ready, error))

        def _declare():

            def _on_declare(method_frame):
                result[0] = method_frame
                ready.set()

            def _on_chan_close(ch, reason):
                if not ready.is_set():
                    if error[0] is None:
                        error[0] = reason
                    ready.set()

            try:
                self._channel.add_on_close_callback(_on_chan_close)
                self._channel.queue_declare(
                    queue=queue,
                    passive=passive,
                    durable=durable,
                    exclusive=exclusive,
                    auto_delete=auto_delete,
                    arguments=arguments,
                    callback=_on_declare,
                )
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper.add_callback_threadsafe(_declare)

        if not ready.wait(timeout=timeout):
            raise TimeoutError(
                f'queue_declare timed out after {timeout} seconds')

        with self._wrapper._channel_waiters_lock:
            try:
                self._wrapper._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

        if error[0] is not None:
            raise error[0]
        return result[0]

    def close(self, reply_code=0, reply_text='Normal shutdown', timeout=10):
        """Close the channel and block until the Channel.CloseOk arrives.

        Shuts down the consumer work pool after the channel is closed,
        allowing any in-flight delivery callbacks to complete.

        If the channel is already closed or closing, returns immediately.
        Safe to call from any thread.

        :param int reply_code: Close reason code to send to the broker.
        :param str reply_text: Close reason text to send to the broker.
        :param float | None timeout: Seconds to wait for Channel.CloseOk
            before treating the channel as closed regardless.  Defaults to
            10 seconds.  Pass ``None`` to wait indefinitely.
        :raises Exception: if the connection is closed or the channel is closed
            by the broker rather than by this client.
        """
        ready = threading.Event()
        error: list[BaseException | None] = [None]

        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason
            self._wrapper._blocking_waiters.append((ready, error))

        def _close():
            if self._channel.is_closed or self._channel.is_closing:
                ready.set()
                return

            def _on_channel_close(channel, reason):
                from pika.exceptions import ChannelClosedByClient
                if not isinstance(reason, ChannelClosedByClient):
                    error[0] = reason
                ready.set()

            try:
                self._channel.add_on_close_callback(_on_channel_close)
                self._channel.close(reply_code=reply_code,
                                    reply_text=reply_text)
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper.add_callback_threadsafe(_close)

        if not ready.wait(timeout=timeout):
            LOGGER.warning('Channel %s close timed out after %s seconds',
                           self._channel.channel_number, timeout)

        self._shutdown_pool()

        with self._wrapper._channel_waiters_lock:
            try:
                self._wrapper._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

        if error[0] is not None:
            raise error[0]

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

    Can also be used as a context manager::

        with ThreadSafeConnection(pika.ConnectionParameters('localhost')) as conn:
            ch = conn.channel()
            ch.basic_publish(exchange='', routing_key='q', body='hello')

    :param parameters: Connection parameters.
    :type parameters: pika.connection.Parameters
    :param callable | None on_open_error_callback:
        Called in the IOLoop thread if the connection cannot be established.
        Signature: ``on_open_error_callback(connection, exception)``
    :param callable | None on_close_callback:
        Called in the IOLoop thread when the connection is closed.
        Signature: ``on_close_callback(connection, reason)``
    :param float | None timeout: Seconds to wait for the AMQP connection
        to be established.  Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
        Pass ``None`` to wait indefinitely (relying on the socket timeout
        in *parameters*).  On timeout the IOLoop is stopped and
        :class:`TimeoutError` is raised.
    :raises Exception: if the connection cannot be established.
    :raises TimeoutError: if *timeout* expires before the connection opens.
    """

    _instance_counter = itertools.count(1)

    def __init__(self,
                 parameters,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 timeout=DEFAULT_RPC_TIMEOUT):
        self._user_on_open_error_callback = on_open_error_callback
        self._user_on_close_callback = on_close_callback

        self._connect_error = None
        self._connected_event = threading.Event()

        self._channel_waiters_lock = threading.Lock()
        self._closed_reason = None
        self._blocking_waiters: list[tuple[threading.Event,
                                           list[BaseException | None]]] = []
        self._channels: list[ThreadSafeChannel] = []

        self._connection = SelectConnection(
            parameters=parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

        def _run_ioloop():
            try:
                self._connection.ioloop.start()
            except Exception as exc:
                # An unhandled exception in a callback killed the IOLoop.
                # Wake every blocked caller so they do not hang forever.
                LOGGER.exception('IOLoop thread crashed: %r', exc)
                with self._channel_waiters_lock:
                    if self._closed_reason is None:
                        self._closed_reason = exc
                    for evt, err in self._blocking_waiters:
                        if err[0] is None:
                            err[0] = self._closed_reason
                        evt.set()
                    self._blocking_waiters.clear()
                # Wake __init__ if it crashed before the connection opened.
                if not self._connected_event.is_set():
                    self._connect_error = exc
                    self._connected_event.set()
            # IOLoop has exited - safe to block waiting for pool workers.
            self._shutdown_all_consumer_pools()

        self._ioloop_thread = threading.Thread(
            target=_run_ioloop,
            name=f'pika-ioloop-{next(self._instance_counter)}',
            daemon=True,
        )
        self._ioloop_thread.start()

        # Block the calling thread until the connection is open or fails.
        if not self._connected_event.wait(timeout=timeout):
            self._connect_error = TimeoutError(
                f'connection attempt timed out after {timeout} seconds')
            self._connection.ioloop.add_callback_threadsafe(
                self._connection.ioloop.stop)
            raise self._connect_error
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
        # Connection is gone - stop the IOLoop so the thread exits.
        # Pool shutdown happens after ioloop.start() returns in _run_ioloop
        # (calling shutdown(wait=True) here would deadlock the IOLoop thread
        # if a pool worker is mid-callback).
        self._connection.ioloop.stop()
        with self._channel_waiters_lock:
            self._closed_reason = reason
            for evt, err in self._blocking_waiters:
                err[0] = reason
                evt.set()
            self._blocking_waiters.clear()
        if self._user_on_close_callback:
            self._user_on_close_callback(_connection, reason)

    def _shutdown_all_consumer_pools(self):
        """Shut down all tracked channel consumer pools."""
        with self._channel_waiters_lock:
            channels = list(self._channels)
        for ch in channels:
            ch._shutdown_pool()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def channel(self, timeout=DEFAULT_RPC_TIMEOUT):
        """Open a new channel and return a :class:`ThreadSafeChannel`.

        Blocks the calling thread until the channel is open.  The returned
        channel's methods are safe to call from any thread.

        :param float | None timeout: Seconds to wait for Channel.OpenOk.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :rtype: ThreadSafeChannel
        :raises Exception: if the connection is closed before the channel opens.
        :raises TimeoutError: if *timeout* expires before the channel opens.
        """
        ready = threading.Event()
        result = [None]
        error: list[BaseException | None] = [None]

        with self._channel_waiters_lock:
            if self._closed_reason is not None:
                raise self._closed_reason
            self._blocking_waiters.append((ready, error))

        def _open():

            def _on_open(ch):
                result[0] = ch
                ready.set()

            try:
                self._connection.channel(on_open_callback=_on_open)
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._connection.ioloop.add_callback_threadsafe(_open)

        if not ready.wait(timeout=timeout):
            raise TimeoutError(
                f'channel open timed out after {timeout} seconds')

        with self._channel_waiters_lock:
            try:
                self._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

        if error[0] is not None:
            raise error[0]

        ch = ThreadSafeChannel(result[0], self)
        with self._channel_waiters_lock:
            self._channels.append(ch)
        return ch

    def close(self, timeout=10):
        """Close the connection and block until the IOLoop thread exits.

        Schedules a clean Connection.Close handshake and waits up to
        *timeout* seconds for the IOLoop thread to finish.  If the thread
        is still alive after the timeout (e.g. the broker never sends
        Connection.CloseOk), the IOLoop is force-stopped via
        ``add_callback_threadsafe`` and joined once more.

        Safe to call from any thread including the IOLoop thread itself
        (e.g. from within a channel callback).  When called from the IOLoop
        thread the close is initiated synchronously and the method returns
        immediately without joining.

        Calling ``close()`` on an already-closed connection is a no-op.

        :param float | None timeout: Seconds to wait for a clean close before
            force-stopping the IOLoop. Defaults to 10 seconds, which is
            sufficient for a healthy broker on any reasonable network.
            Pass ``None`` to wait indefinitely.
        """
        if threading.current_thread() is self._ioloop_thread:
            try:
                self._connection.close()
            except Exception:
                LOGGER.debug('connection.close() raised from IOLoop thread',
                             exc_info=True)
            return
        with self._channel_waiters_lock:
            if self._closed_reason is not None:
                return

        def _safe_close():
            try:
                self._connection.close()
            except Exception:
                # Already closing or closed — _on_connection_closed will wake waiters.
                LOGGER.debug(
                    'connection.close() raised (already closing or closed)',
                    exc_info=True)

        self._connection.ioloop.add_callback_threadsafe(_safe_close)
        self._ioloop_thread.join(timeout=timeout)
        if self._ioloop_thread.is_alive():
            self._connection.ioloop.add_callback_threadsafe(
                self._connection.ioloop.stop)
            self._ioloop_thread.join()
            # _on_connection_closed may not have fired if the IOLoop was
            # force-stopped before the broker sent Connection.CloseOk.
            # Wake any threads still blocked so they do not hang forever.
            forced = Exception(
                'connection force-closed: broker did not respond to close')
            with self._channel_waiters_lock:
                if self._closed_reason is None:
                    self._closed_reason = forced
                    for evt, err in self._blocking_waiters:
                        if err[0] is None:
                            err[0] = self._closed_reason
                        evt.set()
                    self._blocking_waiters.clear()
            self._shutdown_all_consumer_pools()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def add_callback_threadsafe(self, callback):
        """Schedule *callback* to run in the IOLoop thread.

        Safe to call from any thread.  Exposed so callers can schedule
        arbitrary work without going through the wrapper methods.

        :param callable callback: Zero-argument callable.
        """
        self._connection.ioloop.add_callback_threadsafe(callback)

    @property
    def is_open(self):
        return self._connection.is_open

    @property
    def is_closed(self):
        return self._connection.is_closed
