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
from __future__ import annotations

import concurrent.futures
import itertools
import logging
import threading
from threading import Event
from typing import Any

from typing_extensions import Literal, Self

from pika import spec
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

    def __init__(self, channel, wrapper) -> None:
        self._channel = channel
        self._wrapper = wrapper
        self._consumer_work_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix='pika-consumer',
        )
        self._pool_shutdown = False
        self._next_publish_seq_no: int | None = None
        self._confirm_select_ok = None

    def _check_not_closed(self) -> None:
        """Raise if the connection is known to be closed.

        Called from fire-and-forget methods to prevent silently dropping
        work when the connection is already gone.
        """
        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason

    @staticmethod
    def _safe_dispatch(label, callback, *args) -> None:
        """Execute *callback* on the pool worker, logging any exception.

        Used to wrap user callbacks before submitting them to a
        :class:`concurrent.futures.ThreadPoolExecutor` so that exceptions
        are not silently lost in the unobserved ``Future`` and so that one
        failing dispatch does not prevent subsequent dispatches from being
        processed.

        :param str label: Human-readable name of the callback for log lines.
        :param callable callback: The user callback.
        :param args: Positional arguments forwarded to *callback*.
        """
        try:
            callback(*args)
        except Exception:
            LOGGER.exception('Unhandled exception in %s', label)

    def _shutdown_pool(self) -> None:
        """Shut down the consumer work pool, allowing in-flight work to finish."""
        if not self._pool_shutdown:
            self._pool_shutdown = True
            self._consumer_work_pool.shutdown(wait=True)

    def _register_waiter(self) -> tuple[Event, list[BaseException | None]]:
        """Create and register a blocking waiter.

        :returns: (ready, error) tuple for use with ``ready.wait()``
        :raises Exception: if the connection is already closed.
        """
        ready = threading.Event()
        error: list[BaseException | None] = [None]
        with self._wrapper._channel_waiters_lock:
            if self._wrapper._closed_reason is not None:
                raise self._wrapper._closed_reason
            self._wrapper._blocking_waiters.append((ready, error))
        return ready, error

    def _unregister_waiter(self, ready: Event,
                           error: list[BaseException | None]) -> None:
        """Remove a waiter from the blocking list.
        :param Event ready: Threading event signalling that the RPC response has arrived
        :param error: list containing an exception if the RPC response was an error, or None if it was successful
        """
        with self._wrapper._channel_waiters_lock:
            try:
                self._wrapper._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

    def _blocking_rpc(self, method_name: str, channel_method, timeout: int,
                      *args, **kwargs) -> Any:
        """Execute a channel RPC and block until the broker responds.

        Handles the waiter lifecycle: registers the calling thread's event
        in ``_blocking_waiters``, schedules the RPC on the IOLoop thread,
        waits for the response (or timeout/error), unregisters, and returns
        the result or raises.

        *channel_method* is called on the IOLoop thread with (*args,
        **kwargs, callback=<success_cb>).  The success callback receives
        the broker's response frame and must be accepted as a keyword
        argument named ``callback``.

        :param str method_name: Human-readable name for timeout messages.
        :param callable channel_method: Bound method on the raw channel.
        :param timeout: Seconds to wait.
        :returns: The broker response frame.
        :raises TimeoutError: if *timeout* expires.
        :raises Exception: if the connection or channel closes first.
        """
        ready, error = self._register_waiter()
        result = [None]

        def _invoke() -> None:

            def _on_ok(method_frame) -> None:
                result[0] = method_frame
                ready.set()

            def _on_chan_close(ch, reason) -> None:
                if not ready.is_set():
                    if error[0] is None:
                        error[0] = reason
                    ready.set()

            try:
                self._channel.add_on_close_callback(_on_chan_close)
                channel_method(*args, **kwargs, callback=_on_ok)
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper.add_callback_threadsafe(_invoke)

        try:
            if not ready.wait(timeout=timeout):
                raise TimeoutError(
                    f'{method_name} timed out after {timeout} seconds')
        finally:
            self._unregister_waiter(ready, error)

        if error[0] is not None:
            raise error[0]
        return result[0]

    def basic_publish(self,
                      exchange,
                      routing_key,
                      body,
                      properties=None,
                      mandatory: bool = False,
                      on_publish=None) -> None:
        """Schedule a publish in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :param callable on_publish: Optional callback invoked on the
            **IOLoop thread** immediately after the publish frame is
            written successfully, with the delivery tag (int) as its
            sole argument.  Only meaningful when publisher confirms are
            enabled via :meth:`confirm_delivery`; ignored otherwise.
            Must return quickly (same contract as any
            :meth:`~ThreadSafeConnection.add_callback_threadsafe`
            callback).
        :raises Exception: if the connection is already closed.
        :param bool mandatory: If True, return unroutable messages to the publisher
        """
        self._check_not_closed()

        def _publish() -> None:
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
                return
            if self._next_publish_seq_no is not None:
                self._next_publish_seq_no += 1
                if on_publish is not None:
                    on_publish(self._next_publish_seq_no)

        self._wrapper.add_callback_threadsafe(_publish)

    def basic_ack(self, delivery_tag: int = 0, multiple: bool = False) -> None:
        """Schedule an acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :raises Exception: if the connection is already closed.
        :param int delivery_tag: Server-assigned delivery tag
        :param bool multiple: If True, apply to all messages up to and including this delivery tag
        """
        self._check_not_closed()

        def _ack() -> None:
            try:
                self._channel.basic_ack(delivery_tag=delivery_tag,
                                        multiple=multiple)
            except Exception:
                LOGGER.warning('basic_ack failed (channel may have closed)',
                               exc_info=True)

        self._wrapper.add_callback_threadsafe(_ack)

    def basic_nack(self,
                   delivery_tag: int = 0,
                   multiple: bool = False,
                   requeue: bool = True) -> None:
        """Schedule a negative acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :raises Exception: if the connection is already closed.
        :param int delivery_tag: Server-assigned delivery tag
        :param bool multiple: If True, apply to all messages up to and including this delivery tag
        :param bool requeue: If True, requeue the message on the broker
        """
        self._check_not_closed()

        def _nack() -> None:
            try:
                self._channel.basic_nack(delivery_tag=delivery_tag,
                                         multiple=multiple,
                                         requeue=requeue)
            except Exception:
                LOGGER.warning('basic_nack failed (channel may have closed)',
                               exc_info=True)

        self._wrapper.add_callback_threadsafe(_nack)

    def basic_reject(self, delivery_tag: int = 0, requeue: bool = True) -> None:
        """Schedule a rejection in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :raises Exception: if the connection is already closed.
        :param int delivery_tag: Server-assigned delivery tag
        :param bool requeue: If True, requeue the message on the broker
        """
        self._check_not_closed()

        def _reject() -> None:
            try:
                self._channel.basic_reject(delivery_tag=delivery_tag,
                                           requeue=requeue)
            except Exception:
                LOGGER.warning('basic_reject failed (channel may have closed)',
                               exc_info=True)

        self._wrapper.add_callback_threadsafe(_reject)

    def basic_qos(self,
                  prefetch_size: int = 0,
                  prefetch_count: int = 0,
                  global_qos: bool = False,
                  timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Set channel QoS and block until Basic.QosOk arrives.

        Safe to call from any thread.

        :param int prefetch_size: Prefetch window in octets (0 = no limit).
        :param int prefetch_count: Prefetch window in whole messages (0 = no limit).
        :param bool global_qos: Apply QoS to all consumers on the channel.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Basic.QosOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'basic_qos',
            self._channel.basic_qos,
            timeout,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
            global_qos=global_qos,
        )

    def basic_get(self,
                  queue,
                  auto_ack: bool = False,
                  timeout: int = DEFAULT_RPC_TIMEOUT) -> tuple[None, ...]:
        """Get a single message from the broker and block until it arrives.

        Returns a ``(method, properties, body)`` tuple if a message is
        available, or ``(None, None, None)`` if the queue is empty.

        Safe to call from any thread.

        :param str queue: The queue to get a message from.
        :param bool auto_ack: Do not require acknowledgement.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: ``(method, properties, body)`` or ``(None, None, None)``.
        :rtype: tuple
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        ready, error = self._register_waiter()
        result = [None, None, None]

        def _get() -> None:

            def _on_get_ok(ch, method, properties, body) -> None:
                result[0] = method
                result[1] = properties
                result[2] = body
                ready.set()

            def _on_get_empty(method_frame) -> None:
                ready.set()

            def _on_chan_close(ch, reason) -> None:
                if not ready.is_set():
                    if error[0] is None:
                        error[0] = reason
                    ready.set()

            try:
                self._channel.add_on_close_callback(_on_chan_close)
                self._channel.add_callback(
                    _on_get_empty,
                    replies=[spec.Basic.GetEmpty],
                    one_shot=True,
                )
                self._channel.basic_get(
                    queue=queue,
                    callback=_on_get_ok,
                    auto_ack=auto_ack,
                )
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper.add_callback_threadsafe(_get)

        try:
            if not ready.wait(timeout=timeout):
                raise TimeoutError(
                    f'basic_get timed out after {timeout} seconds')
        finally:
            self._unregister_waiter(ready, error)

        if error[0] is not None:
            raise error[0]
        return tuple(result)

    def add_on_cancel_callback(self, callback) -> None:
        """Register a callback for server-initiated consumer cancellation.

        The broker sends ``Basic.Cancel`` when a consumer is cancelled by
        the server (for example, when the queue the consumer is bound to
        is deleted).  Without registering this callback, a consumer can
        silently stop receiving messages.

        Dispatched on the per-channel worker thread (same as delivery
        callbacks), so the callback may safely call any
        :class:`ThreadSafeChannel` method.  The RabbitMQ Java and .NET
        clients run this listener inline on the I/O thread; pika's
        wrapper deliberately diverges so a slow listener cannot stall
        heartbeats.

        Safe to call from any thread.

        :param callable callback:
            ``callback(method_frame)`` where *method_frame* contains a
            :class:`pika.spec.Basic.Cancel`.
        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _wrapped(method_frame) -> None:
            try:
                self._consumer_work_pool.submit(self._safe_dispatch,
                                                'cancel listener', callback,
                                                method_frame)
            except RuntimeError:
                LOGGER.debug(
                    'Server-initiated cancel dropped: work pool shut down')

        def _register() -> None:
            try:
                self._channel.add_on_cancel_callback(_wrapped)
            except Exception:
                LOGGER.warning('add_on_cancel_callback failed', exc_info=True)

        self._wrapper.add_callback_threadsafe(_register)

    def add_on_return_callback(self, callback) -> None:
        """Register a callback for messages returned by the broker.

        When a message is published with ``mandatory=True`` and cannot be
        routed to any queue, the broker returns it via a
        :class:`pika.spec.Basic.Return`.  The *callback* receives the
        returned message.

        Dispatched on the per-channel worker thread (same as delivery
        callbacks), so the callback may safely call any
        :class:`ThreadSafeChannel` method.  The RabbitMQ Java and .NET
        clients run this listener inline on the I/O thread; pika's
        wrapper deliberately diverges so a slow listener cannot stall
        heartbeats.

        Safe to call from any thread.

        :param callable callback:
            ``callback(channel, method, properties, body)`` where
            *channel* is this :class:`ThreadSafeChannel`, *method* is a
            :class:`pika.spec.Basic.Return`, *properties* is a
            :class:`pika.spec.BasicProperties`, and *body* is :class:`bytes`.
        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _wrapped(_raw_ch, method, properties, body) -> None:
            try:
                self._consumer_work_pool.submit(self._safe_dispatch,
                                                'return listener', callback,
                                                self, method, properties, body)
            except RuntimeError:
                LOGGER.debug('Returned message dropped: work pool shut down')

        def _register() -> None:
            try:
                self._channel.add_on_return_callback(_wrapped)
            except Exception:
                LOGGER.warning('add_on_return_callback failed', exc_info=True)

        self._wrapper.add_callback_threadsafe(_register)

    def confirm_delivery(self,
                         ack_nack_callback,
                         timeout: int = DEFAULT_RPC_TIMEOUT):
        """Enable publisher confirms and block until Confirm.SelectOk arrives.

        Idempotent: calling this method more than once on the same channel
        returns the original Confirm.SelectOk without sending a frame to
        the broker or resetting the delivery-tag counter, matching the
        behavior of the RabbitMQ Java and .NET clients.

        The *ack_nack_callback* is dispatched on the channel's worker
        thread (same as delivery callbacks), not the IOLoop thread.
        The RabbitMQ Java and .NET clients run this listener inline on
        the I/O thread; pika's wrapper deliberately diverges so a slow
        listener cannot stall heartbeats.

        Safe to call from any thread.

        :param callable ack_nack_callback:
            ``callback(method_frame)`` called for each Basic.Ack or
            Basic.Nack received from the broker.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Confirm.SelectOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        if self._confirm_select_ok is not None:
            return self._confirm_select_ok

        def _wrapped_ack_nack(method_frame) -> None:
            try:
                self._consumer_work_pool.submit(self._safe_dispatch,
                                                'publisher confirm callback',
                                                ack_nack_callback, method_frame)
            except RuntimeError:
                LOGGER.debug('Publisher confirm dropped: work pool shut down')

        result = self._blocking_rpc(
            'confirm_delivery',
            self._channel.confirm_delivery,
            timeout,
            ack_nack_callback=_wrapped_ack_nack,
        )
        self._next_publish_seq_no = 0
        self._confirm_select_ok = result
        return result

    def basic_consume(self,
                      queue,
                      on_message_callback,
                      auto_ack: bool = False,
                      exclusive: bool = False,
                      consumer_tag=None,
                      arguments=None,
                      timeout: int = DEFAULT_RPC_TIMEOUT):
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
        :param consumer_tag: Client-provided tag; generated if omitted.
        :param arguments: Additional AMQP arguments.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The consumer tag assigned by the broker.
        :rtype: str
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def _wrapped_callback(ch, method, properties, body) -> None:
            try:
                self._consumer_work_pool.submit(self._safe_dispatch,
                                                'consumer callback',
                                                on_message_callback, self,
                                                method, properties, body)
            except RuntimeError:
                LOGGER.debug('Consumer delivery dropped: work pool shut down')

        frame = self._blocking_rpc(
            'basic_consume',
            self._channel.basic_consume,
            timeout,
            queue=queue,
            on_message_callback=_wrapped_callback,
            auto_ack=auto_ack,
            exclusive=exclusive,
            consumer_tag=consumer_tag,
            arguments=arguments,
        )
        return frame.method.consumer_tag

    def basic_cancel(self,
                     consumer_tag,
                     timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Cancel a consumer and block until Basic.CancelOk arrives.

        Safe to call from any thread.

        :param str consumer_tag: Tag returned by :meth:`basic_consume`.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Basic.CancelOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'basic_cancel',
            self._channel.basic_cancel,
            timeout,
            consumer_tag=consumer_tag,
        )

    def queue_declare(self,
                      queue,
                      passive: bool = False,
                      durable: bool = False,
                      exclusive: bool = False,
                      auto_delete: bool = False,
                      arguments=None,
                      timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Declare a queue and block the calling thread until Queue.DeclareOk arrives.

        Safe to call from any thread.

        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Queue.DeclareOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        :param bool passive: If True, only check whether the queue or exchange exists
        :param bool durable: If True, the queue survives broker restart
        :param bool exclusive: If True, restrict access to the current connection
        :param bool auto_delete: If True, delete the queue or exchange when no longer in use
        """
        return self._blocking_rpc(
            'queue_declare',
            self._channel.queue_declare,
            timeout,
            queue=queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
        )

    def exchange_declare(self,
                         exchange,
                         exchange_type: str = 'direct',
                         passive: bool = False,
                         durable: bool = False,
                         auto_delete: bool = False,
                         internal: bool = False,
                         arguments=None,
                         timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Declare an exchange and block until Exchange.DeclareOk arrives.

        Safe to call from any thread.

        :param str exchange: The exchange name.
        :param str exchange_type: The exchange type (direct, fanout, topic, headers).
        :param bool passive: Only check if the exchange exists.
        :param bool durable: Survive broker restart.
        :param bool auto_delete: Delete when no queues are bound.
        :param bool internal: Can only be published to by other exchanges.
        :param arguments: Custom arguments for the exchange.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Exchange.DeclareOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'exchange_declare',
            self._channel.exchange_declare,
            timeout,
            exchange=exchange,
            exchange_type=exchange_type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            arguments=arguments,
        )

    def queue_bind(self,
                   queue,
                   exchange,
                   routing_key=None,
                   arguments=None,
                   timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Bind a queue to an exchange and block until Queue.BindOk arrives.

        Safe to call from any thread.

        :param str queue: The queue to bind.
        :param str exchange: The exchange to bind to.
        :param routing_key: The routing key to bind on.
            Defaults to the queue name.
        :param arguments: Custom arguments for the binding.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Queue.BindOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'queue_bind',
            self._channel.queue_bind,
            timeout,
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments,
        )

    def queue_unbind(self,
                     queue,
                     exchange=None,
                     routing_key=None,
                     arguments=None,
                     timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Unbind a queue from an exchange and block until Queue.UnbindOk arrives.

        Safe to call from any thread.

        :param str queue: The queue to unbind.
        :param exchange: The exchange to unbind from.
        :param routing_key: The routing key to unbind.
            Defaults to the queue name.
        :param arguments: Custom arguments for the unbinding.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Queue.UnbindOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'queue_unbind',
            self._channel.queue_unbind,
            timeout,
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments,
        )

    def queue_delete(self,
                     queue,
                     if_unused: bool = False,
                     if_empty: bool = False,
                     timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Delete a queue and block until Queue.DeleteOk arrives.

        Safe to call from any thread.

        :param str queue: The queue to delete.
        :param bool if_unused: Only delete if the queue has no consumers.
        :param bool if_empty: Only delete if the queue is empty.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Queue.DeleteOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'queue_delete',
            self._channel.queue_delete,
            timeout,
            queue=queue,
            if_unused=if_unused,
            if_empty=if_empty,
        )

    def queue_purge(self, queue, timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Purge all messages from a queue and block until Queue.PurgeOk arrives.

        Safe to call from any thread.

        :param str queue: The queue to purge.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Queue.PurgeOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'queue_purge',
            self._channel.queue_purge,
            timeout,
            queue=queue,
        )

    def exchange_bind(self,
                      destination,
                      source,
                      routing_key: str = '',
                      arguments=None,
                      timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Bind an exchange to another exchange and block until Exchange.BindOk.

        Safe to call from any thread.

        :param str destination: The destination exchange to bind.
        :param str source: The source exchange to bind to.
        :param str routing_key: The routing key to bind on.
        :param arguments: Custom arguments for the binding.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Exchange.BindOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'exchange_bind',
            self._channel.exchange_bind,
            timeout,
            destination=destination,
            source=source,
            routing_key=routing_key,
            arguments=arguments,
        )

    def exchange_unbind(self,
                        destination,
                        source,
                        routing_key: str = '',
                        arguments=None,
                        timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Unbind an exchange from another exchange and block until Exchange.UnbindOk.

        Safe to call from any thread.

        :param str destination: The destination exchange to unbind.
        :param str source: The source exchange to unbind from.
        :param str routing_key: The routing key to unbind.
        :param arguments: Custom arguments for the unbinding.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Exchange.UnbindOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'exchange_unbind',
            self._channel.exchange_unbind,
            timeout,
            destination=destination,
            source=source,
            routing_key=routing_key,
            arguments=arguments,
        )

    def exchange_delete(self,
                        exchange=None,
                        if_unused: bool = False,
                        timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        """Delete an exchange and block until Exchange.DeleteOk arrives.

        Safe to call from any thread.

        :param exchange: The exchange name.
        :param bool if_unused: Only delete if the exchange has no bindings.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Exchange.DeleteOk method frame.
        :rtype: pika.frame.Method
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        return self._blocking_rpc(
            'exchange_delete',
            self._channel.exchange_delete,
            timeout,
            exchange=exchange,
            if_unused=if_unused,
        )

    def close(self,
              reply_code: int = 0,
              reply_text: str = 'Normal shutdown',
              timeout: int = 10) -> None:
        """Close the channel and block until the Channel.CloseOk arrives.

        Shuts down the consumer work pool after the channel is closed,
        allowing any in-flight delivery callbacks to complete.

        If the channel is already closed or closing, returns immediately.
        Safe to call from any thread.

        :param int reply_code: Close reason code to send to the broker.
        :param str reply_text: Close reason text to send to the broker.
        :param timeout: Seconds to wait for Channel.CloseOk
            before treating the channel as closed regardless.  Defaults to
            10 seconds.  Pass ``None`` to wait indefinitely.
        :raises Exception: if the connection is closed or the channel is closed
            by the broker rather than by this client.
        """
        ready, error = self._register_waiter()

        def _close() -> None:
            if self._channel.is_closed or self._channel.is_closing:
                ready.set()
                return

            def _on_channel_close(channel, reason) -> None:
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

        try:
            if not ready.wait(timeout=timeout):
                LOGGER.warning('Channel %s close timed out after %s seconds',
                               self._channel.channel_number, timeout)
            self._shutdown_pool()
        finally:
            self._unregister_waiter(ready, error)

        if error[0] is not None:
            raise error[0]

    def abort(self,
              reply_code: int = 0,
              reply_text: str = 'Normal shutdown',
              timeout: int = 10) -> None:
        """Close the channel, swallowing any errors.

        Equivalent to :meth:`close` but never raises.  Useful in error-recovery
        paths where the channel may already be in a bad state and the caller
        only wants a best-effort shutdown.

        Safe to call from any thread.

        :param int reply_code: Close reason code to send to the broker.
        :param str reply_text: Close reason text to send to the broker.
        :param timeout: Seconds to wait for Channel.CloseOk
            before treating the channel as closed regardless.  Defaults to
            10 seconds.  Pass ``None`` to wait indefinitely.
        """
        try:
            self.close(reply_code=reply_code,
                       reply_text=reply_text,
                       timeout=timeout)
        except Exception:
            LOGGER.debug('channel abort() suppressed error', exc_info=True)

    @property
    def next_publish_seq_no(self):
        """The delivery tag that will be assigned to the next published message.

        Returns ``None`` if publisher confirms have not been enabled via
        :meth:`confirm_delivery`.  Once confirms are enabled, returns an
        integer starting at 1 that increments after each successful publish,
        matching the behavior of the RabbitMQ Java and .NET clients.

        This property is safe to read from any thread, but the value is
        only stable if no other thread is concurrently publishing (the
        counter advances on the IOLoop thread inside the scheduled publish
        callback).
        """
        if self._next_publish_seq_no is None:
            return None
        return self._next_publish_seq_no + 1

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
    :param on_open_error_callback:
        Called in the IOLoop thread if the connection cannot be established.
        Signature: ``on_open_error_callback(connection, exception)``
    :param on_close_callback:
        Called in the IOLoop thread when the connection is closed.
        Signature: ``on_close_callback(connection, reason)``
    :param timeout: Seconds to wait for the AMQP connection
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
                 timeout: int = DEFAULT_RPC_TIMEOUT) -> None:
        self._user_on_open_error_callback = on_open_error_callback
        self._user_on_close_callback = on_close_callback

        self._connect_error = None
        self._connected_event = threading.Event()

        self._instance_id = next(self._instance_counter)

        self._channel_waiters_lock = threading.Lock()
        self._closed_reason = None
        self._blocking_waiters: list[tuple[threading.Event,
                                           list[BaseException | None]]] = []
        self._channels: list[ThreadSafeChannel] = []

        # Single-worker pool for connection-level event callbacks
        # (Connection.Blocked / Unblocked).  Keeps user code off the
        # IOLoop thread so a slow listener cannot stall heartbeats.
        self._connection_work_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f'pika-conn-{self._instance_id}',
        )
        self._connection_pool_shutdown = False

        try:
            self._connection = SelectConnection(
                parameters=parameters,
                on_open_callback=self._on_connection_open,
                on_open_error_callback=self._on_connection_open_error,
                on_close_callback=self._on_connection_closed,
            )
        except Exception:
            # SelectConnection construction failed before the IOLoop thread
            # started, so _run_ioloop's cleanup tail will never run.  Shut
            # the connection-event pool down here to avoid leaking its
            # worker thread.
            self._shutdown_connection_pool()
            raise

        def _run_ioloop() -> None:
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
            self._shutdown_connection_pool()

        self._ioloop_thread = threading.Thread(
            target=_run_ioloop,
            name=f'pika-ioloop-{self._instance_id}',
            daemon=True,
        )
        self._ioloop_thread.start()

        # Block the calling thread until the connection is open or fails.
        if not self._connected_event.wait(timeout=timeout):
            self._connect_error = TimeoutError(
                f'connection attempt timed out after {timeout} seconds')
            self._connection.ioloop.add_callback_threadsafe(
                self._connection.ioloop.stop)
            # Wait for the IOLoop thread to exit so it can shut down the
            # consumer and connection pools before we raise.  Otherwise
            # those pools' worker threads outlive __init__.
            self._ioloop_thread.join(timeout=timeout)
            raise self._connect_error
        if self._connect_error is not None:
            # IOLoop thread already exited (open-error path stops the
            # IOLoop) but its cleanup tail may still be running; wait
            # briefly so pool shutdown completes before we raise.
            self._ioloop_thread.join(timeout=timeout)
            raise self._connect_error

    # ------------------------------------------------------------------
    # IOLoop-thread callbacks
    # ------------------------------------------------------------------

    def _on_connection_open(self, _connection) -> None:
        self._connected_event.set()

    def _on_connection_open_error(self, _connection, error) -> None:
        self._connect_error = error
        # Stop the IOLoop so the background thread can exit cleanly.
        self._connection.ioloop.stop()
        self._connected_event.set()
        if self._user_on_open_error_callback:
            self._user_on_open_error_callback(_connection, error)

    def _on_connection_closed(self, _connection, reason) -> None:
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

    def _shutdown_all_consumer_pools(self) -> None:
        """Shut down all tracked channel consumer pools."""
        with self._channel_waiters_lock:
            channels = list(self._channels)
        for ch in channels:
            ch._shutdown_pool()

    def _shutdown_connection_pool(self) -> None:
        """Shut down the connection-level event-callback pool."""
        if not self._connection_pool_shutdown:
            self._connection_pool_shutdown = True
            self._connection_work_pool.shutdown(wait=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def channel(self, timeout: int = DEFAULT_RPC_TIMEOUT) -> ThreadSafeChannel:
        """Open a new channel and return a :class:`ThreadSafeChannel`.

        Blocks the calling thread until the channel is open.  The returned
        channel's methods are safe to call from any thread.

        :param timeout: Seconds to wait for Channel.OpenOk.
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

        def _open() -> None:

            def _on_open(ch) -> None:
                result[0] = ch
                ready.set()

            try:
                self._connection.channel(on_open_callback=_on_open)
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._connection.ioloop.add_callback_threadsafe(_open)

        try:
            if not ready.wait(timeout=timeout):
                raise TimeoutError(
                    f'channel open timed out after {timeout} seconds')
        finally:
            with self._channel_waiters_lock:
                try:
                    self._blocking_waiters.remove((ready, error))
                except ValueError:
                    pass

        if error[0] is not None:
            raise error[0]

        # Race guard: the connection may have closed on the IOLoop thread
        # after _on_open fired but before we woke up.  Without this check,
        # _shutdown_all_consumer_pools() would have already run and the
        # newly-constructed channel's consumer pool would never be reached.
        with self._channel_waiters_lock:
            if self._closed_reason is not None:
                raise self._closed_reason
            ch = ThreadSafeChannel(result[0], self)
            self._channels.append(ch)
        return ch

    def close(self, timeout: int = 10) -> None:
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

        :param timeout: Seconds to wait for a clean close before
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

        def _safe_close() -> None:
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
            self._shutdown_connection_pool()

    def abort(self, timeout: int = 10) -> None:
        """Close the connection, swallowing any errors.

        Equivalent to :meth:`close` but never raises.  Currently
        :meth:`close` itself does not raise (it logs a warning on
        timeout), but ``abort`` is provided for API symmetry with
        other AMQP 0-9-1 clients and to make error-recovery intent
        explicit at the call site.

        Safe to call from any thread.

        :param timeout: Seconds to wait for a clean close before
            force-stopping the IOLoop.  Defaults to 10 seconds.
            Pass ``None`` to wait indefinitely.
        """
        try:
            self.close(timeout=timeout)
        except Exception:
            LOGGER.debug('connection abort() suppressed error', exc_info=True)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
        self.close()
        return False

    def add_callback_threadsafe(self, callback) -> None:
        """Schedule *callback* to run in the IOLoop thread.

        Safe to call from any thread.  Exposed so callers can schedule
        arbitrary work without going through the wrapper methods.

        :param callable callback: Zero-argument callable.
        """
        self._connection.ioloop.add_callback_threadsafe(callback)

    def add_on_connection_blocked_callback(self, callback) -> None:
        """Register a callback for ``Connection.Blocked`` notifications.

        RabbitMQ sends ``Connection.Blocked`` when the broker is running
        low on memory or disk.  In this state RabbitMQ stops processing
        incoming data, so a publisher receiving this notification should
        suspend publishing until the connection is unblocked.

        Dispatched on a connection-level worker thread (one per
        connection), so the callback may safely call any
        :class:`ThreadSafeChannel` method without stalling heartbeats.
        The RabbitMQ Java and .NET clients run this listener inline on
        the I/O thread; pika's wrapper deliberately diverges so a slow
        listener cannot stall heartbeats.

        Safe to call from any thread.

        :param callable callback:
            ``callback(connection, method_frame)`` where *connection* is
            this :class:`ThreadSafeConnection` and *method_frame* contains
            a :class:`pika.spec.Connection.Blocked`.
        :raises Exception: if the connection is already closed.
        """
        self._register_connection_event_callback(
            'add_on_connection_blocked_callback', callback)

    def add_on_connection_unblocked_callback(self, callback) -> None:
        """Register a callback for ``Connection.Unblocked`` notifications.

        Sent by RabbitMQ once a previously blocked connection is no
        longer resource-constrained, letting publishers resume.

        Dispatched on the connection-level worker thread (same as
        ``add_on_connection_blocked_callback``).  The RabbitMQ Java and
        .NET clients run this listener inline on the I/O thread; pika's
        wrapper deliberately diverges so a slow listener cannot stall
        heartbeats.

        Safe to call from any thread.

        :param callable callback:
            ``callback(connection, method_frame)`` where *connection* is
            this :class:`ThreadSafeConnection` and *method_frame* contains
            a :class:`pika.spec.Connection.Unblocked`.
        :raises Exception: if the connection is already closed.
        """
        self._register_connection_event_callback(
            'add_on_connection_unblocked_callback', callback)

    def _register_connection_event_callback(self, raw_method_name: str,
                                            callback) -> None:
        """Register a connection-level event callback via the IOLoop.

        Wraps the user callback so it dispatches on the connection work
        pool, then schedules registration with the underlying
        :class:`pika.connection.Connection` on the IOLoop thread.
        :param str raw_method_name: Unqualified AMQP method name (e.g. ``"Basic.Publish"``)
        """
        with self._channel_waiters_lock:
            if self._closed_reason is not None:
                raise self._closed_reason

        def _wrapped(_raw_conn, method_frame) -> None:
            try:
                self._connection_work_pool.submit(
                    ThreadSafeChannel._safe_dispatch, raw_method_name, callback,
                    self, method_frame)
            except RuntimeError:
                LOGGER.debug('Connection event dropped: work pool shut down')

        def _register() -> None:
            try:
                getattr(self._connection, raw_method_name)(_wrapped)
            except Exception:
                LOGGER.warning('%s failed', raw_method_name, exc_info=True)

        self._connection.ioloop.add_callback_threadsafe(_register)

    @property
    def is_open(self) -> bool:
        return self._connection.is_open

    @property
    def is_closed(self) -> bool:
        return self._connection.is_closed
