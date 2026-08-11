"""
Thread-safe connection wrapper for pika.

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

import itertools
import logging
import queue
import threading
import time
from threading import Event
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Literal, Self

from pika import spec
from pika.adapters.select_connection import SelectConnection
from pika.exceptions import ConnectionWrongStateError, WorkQueueFullError

LOGGER = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 10

# Matches MAX_QUEUE_LENGTH in the RabbitMQ Java client's WorkPool.
DEFAULT_WORK_QUEUE_MAXSIZE = 1000

# Default seconds the IOLoop thread waits for space on a full work queue
# before raising WorkQueueFullError and tearing down the connection.
# While the queue is full the IOLoop blocks - it sends no heartbeats and
# reads no frames - so a wedged consumer stalls the connection for up to
# this long.  Keep an override comfortably under the negotiated
# heartbeat-death window (twice the heartbeat interval) so the stall
# surfaces as a clean WorkQueueFullError before the broker drops the link
# with an opaque StreamLostError.
DEFAULT_WORK_QUEUE_PUT_TIMEOUT = 30.0


def _validate_put_timeout(put_timeout: float) -> float:
    """
    Validate a public work-queue put timeout.

    A ``None`` (infinite) timeout is rejected because it lets a full work queue block the IOLoop
    thread forever.  A non-positive timeout - including ``0`` and ``float('nan')`` - is rejected
    because it cannot bound the stall.

    :param put_timeout: Seconds the IOLoop thread waits for queue space before raising
        :class:`pika.exceptions.WorkQueueFullError`.
    :returns: *put_timeout* unchanged.
    :raises ValueError: if *put_timeout* is ``None`` or is not a positive number.
    """
    if put_timeout is None:
        raise ValueError(
            'work_queue_put_timeout must be a number of seconds, not None; '
            'an infinite timeout lets a full work queue block the IOLoop '
            'thread forever')
    # ``not put_timeout > 0`` (rather than ``put_timeout <= 0``) also
    # rejects float('nan'), for which every comparison is False.
    if not put_timeout > 0:
        raise ValueError(
            f'work_queue_put_timeout must be a positive number of seconds, '
            f'got {put_timeout!r}')
    return put_timeout


def _with_close_traceback(error: BaseException,
                          close_tb: TracebackType | None) -> BaseException:
    """
    Return *error* carrying the traceback it had when the close reason was recorded.

    A ``raise`` appends the raising frames to the exception and leaves them there, which is harmless
    for an exception that dies with the ``except`` clause that caught it.  The close reason does not:
    it stays reachable from ``_closed_reason`` for the life of the connection, so every caller that
    is rejected against a closed connection grows the same instance's traceback, and every appended
    frame pins that call's arguments - a published message body included.

    Restoring the traceback captured at close time bounds the depth.  Clearing it instead would too,
    but would discard frames worth keeping, such as the crash site of an exception that killed the
    IOLoop.  The instance is returned unchanged otherwise, because callers compare what they catch
    against the reason the connection recorded.

    :param error: The recorded close reason, about to be raised again.
    :param close_tb: The traceback snapshotted when the reason was recorded.
    :returns: *error*, with *close_tb* as its traceback.
    """
    return error.with_traceback(close_tb)


def _reraisable(error: BaseException, close_reason: BaseException | None,
                close_tb: TracebackType | None) -> BaseException:
    """
    Return an exception recorded for a blocking waiter, ready to be raised.

    A waiter woken by the connection closing is handed the shared close reason itself, so raising it
    grows that instance's traceback the way :func:`_with_close_traceback` avoids.  Any other value
    was raised for this call alone and is returned untouched - grafting the close-time traceback onto
    it would misreport where it came from.

    Returns the exception rather than raising it so the ``raise`` stays at the caller's own site,
    which is the frame worth reporting.

    :param error: The exception recorded for this waiter.
    :param close_reason: The connection's recorded close reason, if any.
    :param close_tb: The traceback snapshotted when that reason was recorded.
    :returns: *error*, with its close-time traceback restored if it is the shared close reason.
    """
    if error is not close_reason:
        return error
    return _with_close_traceback(error, close_tb)


def _submit_or_terminate(pool: _BoundedWorkPool, connection: SelectConnection,
                         drop_msg: str, fn: Callable[...,
                                                     None], *args: Any) -> None:
    """
    Submit *fn* to *pool*, tearing the connection down on queue overflow.

    Called on the IOLoop thread from the wrappers that hand user callbacks to a
    :class:`_BoundedWorkPool`.  A :class:`~pika.exceptions.WorkQueueFullError`
    means a slow callback let the queue stay full past ``work_queue_put_timeout``;
    it is routed through :meth:`~pika.connection.Connection._terminate_stream` so
    the connection closes with :class:`~pika.exceptions.WorkQueueFullError` as the
    reason the application observes, rather than being caught by the transport's
    generic read-error handler and relabeled as an opaque
    :class:`~pika.exceptions.StreamLostError`.  A ``RuntimeError`` means the pool
    is already shut down, so the callback is dropped.

    Once teardown has begun, ``connection._error`` is set synchronously (by the
    first :meth:`~pika.connection.Connection._terminate_stream` call) while the
    transport abort completes asynchronously on the IOLoop.  A broker that has
    buffered a burst of deliveries would otherwise keep dispatching them on this
    same thread before the abort runs, and each one would block for the full
    ``work_queue_put_timeout`` and re-trigger teardown - so a backlog of ``N``
    deliveries would take ``N * work_queue_put_timeout`` to close.  Short-circuit
    once ``_error`` is set so the connection tears down within one put timeout;
    the skipped deliveries were already lost to the overflow.

    :param pool: The work pool to submit to.
    :param connection: The underlying connection, torn down on overflow.
    :param drop_msg: Debug message logged when the pool is already shut down.
    :param fn: The callback to run on the pool worker.
    :param args: Positional arguments forwarded to *fn*.
    """
    if connection._error is not None:
        # Teardown already underway; do not block on a full queue or re-trigger
        # it.  The delivery is dropped along with the rest of the overflow.
        LOGGER.debug(drop_msg)
        return
    try:
        pool.submit(fn, *args)
    except WorkQueueFullError as exc:
        connection._terminate_stream(exc)
    except RuntimeError:
        LOGGER.debug(drop_msg)


class _BoundedWorkPool:
    """
    Single worker thread consuming from a bounded work queue.

    Replaces :class:`concurrent.futures.ThreadPoolExecutor`, whose internal
    queue is unbounded and grows without limit when user callbacks cannot
    keep up with incoming events (issue #1600).

    ``submit`` runs on the IOLoop thread.  While the queue is full it
    blocks, which stops frame reads from the socket and applies
    back-pressure to the broker via TCP flow control - the same strategy
    as the RabbitMQ Java client's ``WorkPool``.

    The worker thread starts lazily on first submit so that channels which
    never dispatch callbacks (e.g. publish-only channels) do not start one.

    :param maxsize: Maximum number of pending work items.  Pass ``0`` for
        an unbounded queue.
    :param put_timeout: Seconds :meth:`submit` waits for queue space before
        raising :class:`pika.exceptions.WorkQueueFullError`.  Must be
        finite; an infinite (``None``) timeout would let a full queue block
        the IOLoop thread forever.
    :param thread_name: Name of the worker thread.
    """

    # Best-effort wakeup ping enqueued by shutdown() to rouse a worker idling
    # in a blocking get().  The shutdown flag remains the source of truth; the
    # sentinel only nudges the worker to re-check it, so a dropped ping (full
    # queue) is harmless - a busy worker re-checks the flag between items.
    _SENTINEL = object()

    def __init__(self, maxsize: int, put_timeout: float,
                 thread_name: str) -> None:
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=maxsize)
        self._put_timeout = put_timeout
        self._thread_name = thread_name
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._shutdown = False

    def submit(self, fn: Callable[..., None], *args: Any) -> None:
        """
        Enqueue ``fn(*args)`` for execution on the worker thread.

        :raises RuntimeError: if the pool has been shut down.
        :raises WorkQueueFullError: if the queue stays full for *put_timeout* seconds.
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError('cannot submit work after pool shutdown')
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run_worker,
                    name=self._thread_name,
                    daemon=True,
                )
                self._thread.start()
        try:
            self._queue.put((fn, args), timeout=self._put_timeout)
        except queue.Full:
            raise WorkQueueFullError(
                f'work queue full for {self._put_timeout} seconds '
                f'(maxsize={self._queue.maxsize})') from None
        # shutdown() may have run between the flag check above and this put:
        # it sets the flag and the worker exits once it finds the queue empty,
        # so an item enqueued in that window could be stranded in a queue no
        # worker will ever service.  The flag is always set before the worker
        # can exit, so re-reading it after the put (lock-free, like the worker)
        # detects that case.  Report it as a rejected submit rather than
        # silently losing the work; a caller whose item actually still runs
        # only sees a spurious "dropped" debug log, never a lost delivery.
        if self._shutdown:
            raise RuntimeError('cannot submit work after pool shutdown')

    def _run_worker(self) -> None:
        while True:
            # Exit only once shutdown has been requested and every queued item
            # has drained.  Checking before the blocking get() means a worker
            # that finishes the last real item after shutdown() ran sees the
            # flag and the empty queue and exits without waiting for a ping.
            if self._shutdown and self._queue.empty():
                return
            item = self._queue.get()
            if item is self._SENTINEL:
                # Wakeup ping from shutdown(); re-check the flag at the loop top.
                continue
            fn, args = item
            fn(*args)

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> bool:
        """
        Shut down the pool, allowing in-flight work to finish.

        Idempotent: repeated calls are safe and subsequent :meth:`submit`
        calls raise :class:`RuntimeError`.  A repeat call still joins (so a
        caller whose first bounded call timed out can wait again for the
        worker), which is why the flag is set unconditionally rather than
        short-circuiting.

        The shutdown flag is the source of truth.  A best-effort
        :data:`_SENTINEL` ping is then enqueued via ``put_nowait`` purely to
        rouse a worker idling in a blocking ``get()``; if the queue is full the
        ping is dropped, which is harmless because a busy worker re-checks the
        flag between items.  Either way this call never blocks on a full queue,
        and the worker drains all pending work before exiting the next time it
        finds the queue empty with the flag set.

        When *wait* is true this joins the worker thread.  A finite *timeout*
        bounds that join: if the worker is still running callbacks when the
        timeout expires, the join returns anyway and the (daemon) worker is
        left to finish and exit on its own.  A wedged worker therefore cannot
        stall the caller past *timeout*, at the cost of possibly not draining
        queued work before this returns.

        :param wait: Block until the worker thread exits (subject to *timeout*).
        :param timeout: Seconds to wait for the worker to exit.  ``None`` waits
            indefinitely.  Ignored when *wait* is false.
        :returns: ``True`` if the worker has exited (or never started),
            ``False`` if *timeout* expired with the worker still alive.
            When called from the worker thread itself (a callback shutting down
            its own pool), returns ``True`` without joining even though the
            worker has not exited yet; it exits once the calling callback
            returns.
        """
        with self._lock:
            self._shutdown = True
            thread = self._thread
        if thread is None:
            return True
        # Wake a worker blocked in get(); dropped if the queue is full (a busy
        # worker re-checks the flag between items, so no wakeup is needed).
        try:
            self._queue.put_nowait(self._SENTINEL)
        except queue.Full:
            pass
        if thread is threading.current_thread():
            # A delivery callback is shutting down its own pool (e.g. it called
            # channel.close()).  The worker cannot join itself; the flag is
            # already set, so it exits once this callback returns.
            return True
        if not wait:
            return not thread.is_alive()
        thread.join(timeout=timeout)
        return not thread.is_alive()


class Channel:
    """
    Thread-safe wrapper around :class:`pika.channel.Channel`.

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
    - As long as the callback keeps up with deliveries, the IOLoop
      thread is not starved by consumer processing, so heartbeats are
      sent on time.
    - Messages are delivered to the callback **in order** (a single
      worker thread per channel).
    - All :class:`Channel` methods (:meth:`basic_ack`,
      :meth:`basic_nack`, :meth:`basic_reject`, :meth:`basic_publish`,
      :meth:`queue_declare`, etc.) are safe to call from within the
      callback.

    .. rubric:: Sustained-backlog caveat

    "Safe" assumes the callback keeps pace with incoming deliveries.
    The worker consumes from a bounded queue (see *work_queue_maxsize*).
    If a slow callback lets that queue fill, the IOLoop thread blocks
    while enqueuing the next delivery - sending no heartbeats during the
    stall - and, if the queue stays full for *work_queue_put_timeout*
    seconds, raises :class:`pika.exceptions.WorkQueueFullError`, which
    tears the connection down rather than silently dropping the event.
    A persistently slow consumer therefore does not stall heartbeats
    forever; it fails fast with an explicit error.  Keep callbacks
    faster than the sustained delivery rate, or raise *work_queue_maxsize*
    to absorb bursts.

    .. rubric:: IOLoop-thread callbacks

    Callables passed directly to
    :meth:`~Connection.add_callback_threadsafe` still run
    on the IOLoop thread.  These must return quickly and must not call
    blocking channel methods (which would deadlock).
    """

    def __init__(
            self,
            channel,
            wrapper,
            work_queue_maxsize: int = DEFAULT_WORK_QUEUE_MAXSIZE,
            work_queue_put_timeout: float = DEFAULT_WORK_QUEUE_PUT_TIMEOUT
    ) -> None:
        work_queue_put_timeout = _validate_put_timeout(work_queue_put_timeout)
        self._channel = channel
        self._wrapper = wrapper
        self._consumer_work_pool = _BoundedWorkPool(
            maxsize=work_queue_maxsize,
            put_timeout=work_queue_put_timeout,
            thread_name='pika-consumer',
        )
        self._pool_shutdown = False
        self._next_publish_seq_no: int | None = None
        self._confirm_select_ok = None

    def _check_not_closed(self) -> None:
        """
        Raise if the connection is known to be closed.

        Called from fire-and-forget methods to prevent silently dropping work when the connection is
        already gone.
        """
        with self._wrapper._channel_waiters_lock:
            reason = self._wrapper._closed_reason
            if reason is not None:
                raise _with_close_traceback(reason,
                                            self._wrapper._closed_reason_tb)

    @staticmethod
    def _safe_dispatch(label, callback, *args) -> None:
        """
        Execute *callback* on the pool worker, logging any exception.

        Used to wrap user callbacks before submitting them to a
        :class:`_BoundedWorkPool` so that exceptions are not silently lost
        and so that one failing dispatch does not prevent subsequent
        dispatches from being processed.

        :param label: Human-readable name of the callback for log lines.
        :param callback: The user callback.
        :param args: Positional arguments forwarded to *callback*.
        """
        try:
            callback(*args)
        except Exception:
            LOGGER.exception('Unhandled exception in %s', label)

    def _signal_pool_shutdown(self) -> None:
        """
        Tell the consumer pool to begin draining without joining its worker.

        Sets the pool's shutdown flag and wakes an idle worker, but returns immediately.  Used to
        signal every channel's pool up front so their workers drain concurrently before
        :meth:`_shutdown_pool` joins them one at a time under a shared budget.
        """
        self._consumer_work_pool.shutdown(wait=False)

    def _claim_pool_shutdown(self) -> bool:
        """
        Claim this channel's pool shutdown and drop it from connection tracking.

        Returns ``True`` if this call is the first to claim the shutdown - so the caller owns any
        follow-up work, such as joining the worker - and ``False`` if a prior close already claimed
        it.  Being idempotent lets a user :meth:`close`, the broker-close hook
        (:meth:`_on_broker_close`) and the connection-shutdown sweep race while each channel is
        dropped from ``_channels`` exactly once.

        The tracking list exists only so :meth:`~Connection._shutdown_all_consumer_pools` can reach
        every live pool, and a channel whose pool is shutting down has nothing left to drain; left
        in place the entries accumulate for the life of the connection, retaining each closed
        channel's pool, raw channel and per-RPC callbacks.

        :returns: ``True`` if this call claimed the shutdown, ``False`` if a prior close already did.
        """
        # Safe to mutate the list while a sweep is in flight:
        # _shutdown_all_consumer_pools snapshots it under this same lock before
        # iterating.
        with self._wrapper._channel_waiters_lock:
            if self._pool_shutdown:
                return False
            self._pool_shutdown = True
            try:
                self._wrapper._channels.remove(self)
            except ValueError:
                pass
        return True

    def _shutdown_pool(self, timeout: float | None = None) -> None:
        """
        Shut down the consumer work pool, joining its worker.

        Claims the shutdown and drops the channel from tracking via :meth:`_claim_pool_shutdown`,
        then joins the worker.  Used by the paths that can afford to wait off the IOLoop thread: a
        user :meth:`close` (on the caller's thread) and the connection-shutdown sweep (after the
        IOLoop has stopped).

        :param timeout: Seconds to wait for the worker to drain and exit. ``None`` waits
            indefinitely. A wedged worker that outlives a finite *timeout* is left running (it is a
            daemon thread) rather than stalling the caller; a warning is logged.
        """
        # The join stays outside the lock: it can block for the whole timeout,
        # and holding the connection-wide lock that long would stall every other
        # channel operation.
        if not self._claim_pool_shutdown():
            return
        if not self._consumer_work_pool.shutdown(wait=True, timeout=timeout):
            LOGGER.warning(
                'Channel %s consumer work pool did not drain within '
                '%s seconds; abandoning its worker thread',
                self._channel.channel_number, timeout)

    def _on_broker_close(self, _channel, reason) -> None:
        """
        Drop this channel from tracking when the broker closes it on its own.

        Registered once per channel, on the IOLoop thread, right after the channel opens.  A client
        close - a user :meth:`close`, or the per-channel closes a graceful :meth:`Connection.close`
        issues - reports :class:`~pika.exceptions.ChannelClosedByClient` and is left to
        :meth:`_shutdown_pool`, so this handles only broker- or error-initiated closes.  Without it
        those channels stay in ``_channels`` (with their pool, worker thread, raw channel and
        per-RPC callbacks) until the connection itself is torn down - the leak #1688 describes,
        reached through a close path :meth:`_shutdown_pool` never sees.

        Runs on the IOLoop thread, so it must not join the worker: joining a worker that is mid
        delivery-callback and waiting on this same IOLoop would deadlock, which is why pool joins
        are otherwise deferred off this thread (see :meth:`~Connection._on_connection_closed`).  It
        only signals the pool to drain; the daemon worker exits on its own once the channel is
        closed and no further deliveries arrive.  The connection-shutdown sweep, running after the
        IOLoop stops, joins any channel this has not already dropped.

        :param _channel: The raw channel reporting the close (unused).
        :param reason: The exception describing why the channel closed.
        """
        from pika.exceptions import ChannelClosedByClient
        if isinstance(reason, ChannelClosedByClient):
            return
        if self._claim_pool_shutdown():
            self._consumer_work_pool.shutdown(wait=False)

    def _register_waiter(self) -> tuple[Event, list[BaseException | None]]:
        """
        Create and register a blocking waiter.

        :returns: (ready, error) tuple for use with ``ready.wait()``
        :raises Exception: if the connection is already closed.
        """
        ready = threading.Event()
        error: list[BaseException | None] = [None]
        with self._wrapper._channel_waiters_lock:
            reason = self._wrapper._closed_reason
            if reason is not None:
                raise _with_close_traceback(reason,
                                            self._wrapper._closed_reason_tb)
            self._wrapper._blocking_waiters.append((ready, error))
        return ready, error

    def _unregister_waiter(self, ready: Event,
                           error: list[BaseException | None]) -> None:
        """
        Remove a waiter from the blocking list.

        :param ready: Threading event signalling that the RPC response has
            arrived
        :param error: list containing an exception if the RPC response was an
            error, or None if it was successful
        """
        with self._wrapper._channel_waiters_lock:
            try:
                self._wrapper._blocking_waiters.remove((ready, error))
            except ValueError:
                pass

    def _release_close_callback(self, on_chan_close, *extra) -> None:
        """
        Unregister the per-RPC channel callbacks now that the call has finished.

        :meth:`~pika.channel.Channel.add_on_close_callback` registers with ``one_shot=False`` and
        each RPC passes a distinct closure, so nothing ever collapses them: left in place they
        accumulate for the life of the channel, and ``CallbackManager.add`` rescans that growing
        list on every later registration.  A completed RPC has no use for its close callback, so
        drop it.

        Removal is scheduled rather than done inline because the channel's callback stack belongs to
        the IOLoop thread.  Ordering is safe: the ``_invoke`` that registers is queued before this,
        and the IOLoop drains its callback queue in FIFO order, so the removal never runs first.

        :param on_chan_close: The close callback registered for this RPC.
        :param extra: Zero-argument callables removing any other per-RPC callbacks.
        """

        def _remove() -> None:
            self._channel.remove_on_close_callback(on_chan_close)
            for remove_one in extra:
                remove_one()

        self._wrapper._schedule_unchecked(_remove)

    def _blocking_rpc(self,
                      method_name: str,
                      channel_method,
                      timeout: float | None,
                      *args,
                      on_sent: Callable[[], None] | None = None,
                      **kwargs) -> Any:
        """
        Execute a channel RPC and block until the broker responds.

        Handles the waiter lifecycle: registers the calling thread's event
        in ``_blocking_waiters``, schedules the RPC on the IOLoop thread,
        waits for the response (or timeout/error), unregisters, and returns
        the result or raises.

        *channel_method* is called on the IOLoop thread with (*args,
        **kwargs, callback=<success_cb>).  The success callback receives
        the broker's response frame and must be accepted as a keyword
        argument named ``callback``.

        :param method_name: Human-readable name for timeout messages.
        :param channel_method: Bound method on the raw channel.
        :param timeout: Seconds to wait.
        :param on_sent: Optional zero-argument callable run on the IOLoop thread
            immediately after *channel_method* writes its frame, ahead of any
            callback already queued behind it.  For state that must be ordered
            against the frame going out rather than against the broker's
            response coming back.
        :returns: The broker response frame.
        :raises TimeoutError: if *timeout* expires.
        :raises Exception: if the connection or channel closes first.
        """
        ready, error = self._register_waiter()
        result = [None]

        def _on_ok(method_frame) -> None:
            result[0] = method_frame
            ready.set()

        def _on_chan_close(ch, reason) -> None:
            if not ready.is_set():
                if error[0] is None:
                    error[0] = reason
                ready.set()

        def _invoke() -> None:
            try:
                self._channel.add_on_close_callback(_on_chan_close)
                channel_method(*args, **kwargs, callback=_on_ok)
                if on_sent is not None:
                    on_sent()
            except Exception as exc:
                error[0] = exc
                ready.set()

        self._wrapper._schedule_unchecked(_invoke)

        try:
            if not ready.wait(timeout=timeout):
                raise TimeoutError(
                    f'{method_name} timed out after {timeout} seconds')
        finally:
            self._unregister_waiter(ready, error)
            self._release_close_callback(_on_chan_close)

        if error[0] is not None:
            raise _reraisable(error[0], self._wrapper._closed_reason,
                              self._wrapper._closed_reason_tb)
        return result[0]

    def basic_publish(self,
                      exchange,
                      routing_key,
                      body,
                      properties=None,
                      mandatory: bool = False,
                      on_publish=None) -> None:
        """
        Schedule a publish in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :param exchange: The exchange to publish to.
        :param routing_key: The routing key to publish with.
        :param body: The message body to publish.
        :param properties: Properties for the message.
        :param mandatory: If True, return unroutable messages to the publisher
        :param on_publish: Optional callback invoked on the **IOLoop thread** immediately after the
            publish frame is written successfully, with the delivery tag (int) as its sole argument.
            Only meaningful when publisher confirms are enabled via :meth:`confirm_delivery`;
            ignored otherwise. Must return quickly (same contract as any
            :meth:`~Connection.add_callback_threadsafe` callback).
        :raises Exception: if the connection is already closed.
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

        self._wrapper._schedule_unchecked(_publish)

    def basic_ack(self, delivery_tag: int = 0, multiple: bool = False) -> None:
        """
        Schedule an acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :param delivery_tag: Server-assigned delivery tag
        :param multiple: If True, apply to all messages up to and including this delivery tag
        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _ack() -> None:
            try:
                self._channel.basic_ack(delivery_tag=delivery_tag,
                                        multiple=multiple)
            except Exception:
                LOGGER.warning('basic_ack failed (channel may have closed)',
                               exc_info=True)

        self._wrapper._schedule_unchecked(_ack)

    def basic_nack(self,
                   delivery_tag: int = 0,
                   multiple: bool = False,
                   requeue: bool = True) -> None:
        """
        Schedule a negative acknowledgement in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :param delivery_tag: Server-assigned delivery tag
        :param multiple: If True, apply to all messages up to and including this delivery tag
        :param requeue: If True, requeue the message on the broker
        :raises Exception: if the connection is already closed.
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

        self._wrapper._schedule_unchecked(_nack)

    def basic_reject(self, delivery_tag: int = 0, requeue: bool = True) -> None:
        """
        Schedule a rejection in the IOLoop thread (fire-and-forget).

        Safe to call from any thread simultaneously.

        :param delivery_tag: Server-assigned delivery tag
        :param requeue: If True, requeue the message on the broker
        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _reject() -> None:
            try:
                self._channel.basic_reject(delivery_tag=delivery_tag,
                                           requeue=requeue)
            except Exception:
                LOGGER.warning('basic_reject failed (channel may have closed)',
                               exc_info=True)

        self._wrapper._schedule_unchecked(_reject)

    def basic_qos(self,
                  prefetch_size: int = 0,
                  prefetch_count: int = 0,
                  global_qos: bool = False,
                  timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """Set channel QoS and block until Basic.QosOk arrives.

        Safe to call from any thread.

        :param prefetch_size: Prefetch window in octets (0 = no limit).
        :param prefetch_count: Prefetch window in whole messages (0 = no limit).
        :param global_qos: Apply QoS to all consumers on the channel.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Basic.QosOk method frame.
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

    def basic_get(
            self,
            queue,
            auto_ack: bool = False,
            timeout: float | None = DEFAULT_RPC_TIMEOUT) -> tuple[None, ...]:
        """
        Get a single message from the broker and block until it arrives.

        Returns a ``(method, properties, body)`` tuple if a message is available, or ``(None, None,
        None)`` if the queue is empty.

        Safe to call from any thread.

        :param queue: The queue to get a message from.
        :param auto_ack: Do not require acknowledgement.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns:``(method, properties, body)`` or ``(None, None, None)``.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        ready, error = self._register_waiter()
        result = [None, None, None]

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

        def _get() -> None:
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

        self._wrapper._schedule_unchecked(_get)

        try:
            if not ready.wait(timeout=timeout):
                raise TimeoutError(
                    f'basic_get timed out after {timeout} seconds')
        finally:
            self._unregister_waiter(ready, error)
            # A one-shot is only consumed if it fires, so the Basic.GetEmpty
            # callback outlives every get that actually returned a message.
            self._release_close_callback(
                _on_chan_close, lambda: self._channel.remove_callback(
                    _on_get_empty, [spec.Basic.GetEmpty]))

        if error[0] is not None:
            raise _reraisable(error[0], self._wrapper._closed_reason,
                              self._wrapper._closed_reason_tb)
        return tuple(result)

    def add_on_cancel_callback(self, callback) -> None:
        """
        Register a callback for server-initiated consumer cancellation.

        The broker sends ``Basic.Cancel`` when a consumer is cancelled by the server (for example,
        when the queue the consumer is bound to is deleted).  Without registering this callback, a
        consumer can silently stop receiving messages.

        Dispatched on the per-channel worker thread (same as delivery callbacks), so the callback
        may safely call any :class:`Channel` method.  The RabbitMQ Java and .NET clients run this
        listener inline on the I/O thread; pika's wrapper deliberately diverges so a slow listener
        cannot stall heartbeats.

        Safe to call from any thread.

        :param callback:``callback(method_frame)`` where *method_frame* contains a
            :class:`pika.spec.Basic.Cancel`.
        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _wrapped(method_frame) -> None:
            _submit_or_terminate(
                self._consumer_work_pool, self._wrapper._connection,
                'Server-initiated cancel dropped: work pool shut down',
                self._safe_dispatch, 'cancel listener', callback, method_frame)

        def _register() -> None:
            try:
                self._channel.add_on_cancel_callback(_wrapped)
            except Exception:
                LOGGER.warning('add_on_cancel_callback failed', exc_info=True)

        self._wrapper._schedule_unchecked(_register)

    def add_on_return_callback(self, callback) -> None:
        """
        Register a callback for messages returned by the broker.

        When a message is published with ``mandatory=True`` and cannot be routed to any queue, the
        broker returns it via a :class:`pika.spec.Basic.Return`.  The *callback* receives the
        returned message.

        Dispatched on the per-channel worker thread (same as delivery callbacks), so the callback
        may safely call any :class:`Channel` method.  The RabbitMQ Java and .NET clients run this
        listener inline on the I/O thread; pika's wrapper deliberately diverges so a slow listener
        cannot stall heartbeats.

        Safe to call from any thread.

        :param callback:``callback(channel, method, properties, body)`` where *channel* is this
            :class:`Channel`, *method* is a :class:`pika.spec.Basic.Return`, *properties* is a
            :class:`pika.spec.BasicProperties`, and *body* is :class:`bytes`.
        :raises Exception: if the connection is already closed.
        """
        self._check_not_closed()

        def _wrapped(_raw_ch, method, properties, body) -> None:
            _submit_or_terminate(
                self._consumer_work_pool, self._wrapper._connection,
                'Returned message dropped: work pool shut down',
                self._safe_dispatch, 'return listener', callback, self, method,
                properties, body)

        def _register() -> None:
            try:
                self._channel.add_on_return_callback(_wrapped)
            except Exception:
                LOGGER.warning('add_on_return_callback failed', exc_info=True)

        self._wrapper._schedule_unchecked(_register)

    def confirm_delivery(self,
                         ack_nack_callback,
                         timeout: float | None = DEFAULT_RPC_TIMEOUT):
        """
        Enable publisher confirms and block until Confirm.SelectOk arrives.

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

        :param ack_nack_callback:
            ``callback(method_frame)`` called for each Basic.Ack or
            Basic.Nack received from the broker.
        :param timeout: Seconds to wait for the response.
            Defaults to :data:`DEFAULT_RPC_TIMEOUT` (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Confirm.SelectOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        if self._confirm_select_ok is not None:
            return self._confirm_select_ok

        def _wrapped_ack_nack(method_frame) -> None:
            _submit_or_terminate(
                self._consumer_work_pool, self._wrapper._connection,
                'Publisher confirm dropped: work pool shut down',
                self._safe_dispatch, 'publisher confirm callback',
                ack_nack_callback, method_frame)

        def _arm_seq_no() -> None:
            # Runs on the IOLoop thread the moment Confirm.Select is written,
            # so it is ordered ahead of every publish the IOLoop has queued
            # behind it.  Arming from the calling thread once the RPC returns
            # instead would miss each publish issued during the round trip: the
            # broker numbers those from 1, leaving the counter permanently
            # behind the broker's delivery tags.
            #
            # Arm only once.  A confirm_delivery that timed out may still have
            # reached the broker, and the retry's second Confirm.Select does not
            # reset the broker's counter, so neither may this.
            if self._next_publish_seq_no is None:
                self._next_publish_seq_no = 0

        result = self._blocking_rpc(
            'confirm_delivery',
            self._channel.confirm_delivery,
            timeout,
            on_sent=_arm_seq_no,
            ack_nack_callback=_wrapped_ack_nack,
        )
        self._confirm_select_ok = result
        return result

    def basic_consume(self,
                      queue,
                      on_message_callback,
                      auto_ack: bool = False,
                      exclusive: bool = False,
                      consumer_tag=None,
                      arguments=None,
                      timeout: float | None = DEFAULT_RPC_TIMEOUT):
        """
        Register a consumer and block until Basic.ConsumeOk arrives.

        The *on_message_callback* is dispatched on the channel's worker thread, not the IOLoop
        thread.  All :class:`Channel` methods are safe to call from within the callback.

        Because the channel uses a single worker thread, deliveries are processed serially.  A
        callback that blocks (e.g. on a database write or a call to :meth:`queue_declare`) delays
        subsequent deliveries on the same channel until it returns.

        Safe to call from any thread.

        :param queue: Queue to consume from.
        :param on_message_callback:``callback(channel, method, properties, body)``
        :param auto_ack: Disable manual acknowledgement.
        :param exclusive: Request exclusive consumer access.
        :param consumer_tag: Client-provided tag; generated if omitted.
        :param arguments: Additional AMQP arguments.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The consumer tag assigned by the broker.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def _wrapped_callback(ch, method, properties, body) -> None:
            _submit_or_terminate(
                self._consumer_work_pool, self._wrapper._connection,
                'Consumer delivery dropped: work pool shut down',
                self._safe_dispatch, 'consumer callback', on_message_callback,
                self, method, properties, body)

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
                     timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Cancel a consumer and block until Basic.CancelOk arrives.

        Safe to call from any thread.

        :param consumer_tag: Tag returned by :meth:`basic_consume`.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Basic.CancelOk method frame.
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
                      timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Declare a queue and block the calling thread until Queue.DeclareOk arrives.

        Safe to call from any thread.

        :param queue: The queue name. If empty, the broker will generate a unique name.
        :param passive: If True, only check whether the queue or exchange exists
        :param durable: If True, the queue survives broker restart
        :param exclusive: If True, restrict access to the current connection
        :param auto_delete: If True, delete the queue or exchange when no longer in use
        :param arguments: Custom arguments for the queue declaration
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.DeclareOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
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
                         timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Declare an exchange and block until Exchange.DeclareOk arrives.

        Safe to call from any thread.

        :param exchange: The exchange name.
        :param exchange_type: The exchange type (direct, fanout, topic, headers).
        :param passive: Only check if the exchange exists.
        :param durable: Survive broker restart.
        :param auto_delete: Delete when no queues are bound.
        :param internal: Can only be published to by other exchanges.
        :param arguments: Custom arguments for the exchange.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.DeclareOk method frame.
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
                   timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Bind a queue to an exchange and block until Queue.BindOk arrives.

        Safe to call from any thread.

        :param queue: The queue to bind.
        :param exchange: The exchange to bind to.
        :param routing_key: The routing key to bind on. Defaults to the queue name.
        :param arguments: Custom arguments for the binding.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.BindOk method frame.
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
                     timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Unbind a queue from an exchange and block until Queue.UnbindOk arrives.

        Safe to call from any thread.

        :param queue: The queue to unbind.
        :param exchange: The exchange to unbind from.
        :param routing_key: The routing key to unbind. Defaults to the queue name.
        :param arguments: Custom arguments for the unbinding.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.UnbindOk method frame.
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
                     timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Delete a queue and block until Queue.DeleteOk arrives.

        Safe to call from any thread.

        :param queue: The queue to delete.
        :param if_unused: Only delete if the queue has no consumers.
        :param if_empty: Only delete if the queue is empty.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.DeleteOk method frame.
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

    def queue_purge(self,
                    queue,
                    timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Purge all messages from a queue and block until Queue.PurgeOk arrives.

        Safe to call from any thread.

        :param queue: The queue to purge.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.PurgeOk method frame.
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
                      timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Bind an exchange to another exchange and block until Exchange.BindOk.

        Safe to call from any thread.

        :param destination: The destination exchange to bind.
        :param source: The source exchange to bind to.
        :param routing_key: The routing key to bind on.
        :param arguments: Custom arguments for the binding.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.BindOk method frame.
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
                        timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Unbind an exchange from another exchange and block until Exchange.UnbindOk.

        Safe to call from any thread.

        :param destination: The destination exchange to unbind.
        :param source: The source exchange to unbind from.
        :param routing_key: The routing key to unbind.
        :param arguments: Custom arguments for the unbinding.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.UnbindOk method frame.
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
                        timeout: float | None = DEFAULT_RPC_TIMEOUT) -> None:
        """
        Delete an exchange and block until Exchange.DeleteOk arrives.

        Safe to call from any thread.

        :param exchange: The exchange name.
        :param if_unused: Only delete if the exchange has no bindings.
        :param timeout: Seconds to wait for the response. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.DeleteOk method frame.
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
              timeout: float | None = 10) -> None:
        """
        Close the channel and block until the Channel.CloseOk arrives.

        Shuts down the consumer work pool after the channel is closed, allowing any in-flight
        delivery callbacks to complete.

        If the channel is already closed or closing, returns immediately. Safe to call from any
        thread.

        *timeout* bounds the whole call: it caps the wait for Channel.CloseOk and then the leftover
        budget caps the consumer-pool drain, so a slow or wedged delivery callback cannot stall
        ``close`` past *timeout*.  A worker still running when the budget is exhausted is left to
        finish on its own (it is a daemon thread).

        :param reply_code: Close reason code to send to the broker.
        :param reply_text: Close reason text to send to the broker.
        :param timeout: Seconds to wait for Channel.CloseOk before treating the channel as closed
            regardless. Defaults to 10 seconds. Pass ``None`` to wait indefinitely.
        :raises Exception: if the connection is closed or the channel is closed by the broker rather
            than by this client.
        """
        ready, error = self._register_waiter()
        deadline = None if timeout is None else time.monotonic() + timeout

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

        self._wrapper._schedule_unchecked(_close)

        try:
            if not ready.wait(timeout=timeout):
                LOGGER.warning('Channel %s close timed out after %s seconds',
                               self._channel.channel_number, timeout)
            # Drain the pool within whatever remains of the caller's budget so
            # the whole close honors *timeout* rather than up to twice it.
            if deadline is None:
                pool_timeout: float | None = None
            else:
                pool_timeout = max(0.0, deadline - time.monotonic())
            self._shutdown_pool(timeout=pool_timeout)
        finally:
            self._unregister_waiter(ready, error)

        if error[0] is not None:
            raise _reraisable(error[0], self._wrapper._closed_reason,
                              self._wrapper._closed_reason_tb)

    def abort(self,
              reply_code: int = 0,
              reply_text: str = 'Normal shutdown',
              timeout: float | None = 10) -> None:
        """
        Close the channel, swallowing any errors.

        Equivalent to :meth:`close` but never raises.  Useful in error-recovery paths where the
        channel may already be in a bad state and the caller only wants a best-effort shutdown.

        Safe to call from any thread.

        :param reply_code: Close reason code to send to the broker.
        :param reply_text: Close reason text to send to the broker.
        :param timeout: Seconds to wait for Channel.CloseOk before treating the channel as closed
            regardless. Defaults to 10 seconds. Pass ``None`` to wait indefinitely.
        """
        try:
            self.close(reply_code=reply_code,
                       reply_text=reply_text,
                       timeout=timeout)
        except Exception:
            LOGGER.debug('channel abort() suppressed error', exc_info=True)

    @property
    def next_publish_seq_no(self) -> int | None:
        """
        The delivery tag that will be assigned to the next published message.

        Returns ``None`` if publisher confirms have not been enabled via :meth:`confirm_delivery`.
        Once confirms are enabled, returns an integer starting at 1 that increments after each
        successful publish, matching the behavior of the RabbitMQ Java and .NET clients.

        This property is safe to read from any thread, but the value is only stable if no other
        thread is concurrently publishing (the counter advances on the IOLoop thread inside the
        scheduled publish callback).
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


class Connection:
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

        conn = Connection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()

        # safe to call from any number of threads simultaneously
        threading.Thread(target=ch.basic_publish,
                         kwargs=dict(exchange='', routing_key='q',
                                     body='hello')).start()

        conn.close()

    Can also be used as a context manager::

        with Connection(pika.ConnectionParameters('localhost')) as conn:
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
    :param work_queue_maxsize: Maximum number of user-callback dispatches
        (deliveries, publisher confirms, returned messages, cancel and
        blocked/unblocked notifications) that may be pending on each
        worker's queue.  Defaults to :data:`DEFAULT_WORK_QUEUE_MAXSIZE`
        (1000, matching the RabbitMQ Java client).  While a queue is full
        the IOLoop blocks, applying back-pressure to the broker via TCP.
        Heartbeats are not sent while the IOLoop is blocked, so the
        broker's heartbeat timeout bounds how long a wedged consumer can
        stall the connection.  Pass ``0`` for an unbounded queue.
    :param work_queue_put_timeout: Seconds to wait for space on a full
        work queue.  Defaults to :data:`DEFAULT_WORK_QUEUE_PUT_TIMEOUT`
        (30 s); must be a positive number, and ``None`` (infinite) is
        rejected because it lets a full queue stall or deadlock the IOLoop
        thread.  Set an override comfortably below the negotiated
        heartbeat-death window (twice the heartbeat interval) so a stall
        surfaces here rather than as an opaque broker-side close.  On
        timeout :class:`pika.exceptions.WorkQueueFullError` is raised on
        the IOLoop thread, closing the connection rather than silently
        dropping the event (which would lose an auto-acked delivery or
        force redelivery of a manually-acked one).
    :raises ValueError: if *work_queue_put_timeout* is ``None`` or is not a
        positive number.
    :raises Exception: if the connection cannot be established.
    :raises TimeoutError: if *timeout* expires before the connection opens.
    """

    _instance_counter = itertools.count(1)

    def __init__(
            self,
            parameters,
            on_open_error_callback=None,
            on_close_callback=None,
            timeout: float | None = DEFAULT_RPC_TIMEOUT,
            work_queue_maxsize: int = DEFAULT_WORK_QUEUE_MAXSIZE,
            work_queue_put_timeout: float = DEFAULT_WORK_QUEUE_PUT_TIMEOUT
    ) -> None:
        self._user_on_open_error_callback = on_open_error_callback
        self._user_on_close_callback = on_close_callback
        self._work_queue_maxsize = work_queue_maxsize
        self._work_queue_put_timeout = _validate_put_timeout(
            work_queue_put_timeout)

        self._connect_error = None
        self._connected_event = threading.Event()

        self._instance_id = next(self._instance_counter)

        self._channel_waiters_lock = threading.Lock()
        self._closed_reason: BaseException | None = None
        # The traceback _closed_reason carried when it was recorded.  Raising
        # the shared instance appends frames to it permanently, so every raise
        # site restores this snapshot; see _with_close_traceback.
        self._closed_reason_tb: TracebackType | None = None
        self._blocking_waiters: list[tuple[threading.Event,
                                           list[BaseException | None]]] = []
        self._channels: list[Channel] = []

        # Single-worker pool for connection-level event callbacks
        # (Connection.Blocked / Unblocked).  Keeps user code off the
        # IOLoop thread so a slow listener cannot stall heartbeats.
        self._connection_work_pool = _BoundedWorkPool(
            maxsize=work_queue_maxsize,
            put_timeout=work_queue_put_timeout,
            thread_name=f'pika-conn-{self._instance_id}',
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
                LOGGER.exception('IOLoop thread crashed')
                with self._channel_waiters_lock:
                    if self._closed_reason is None:
                        self._record_closed_reason(exc)
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
            self._record_closed_reason(reason)
            for evt, err in self._blocking_waiters:
                err[0] = reason
                evt.set()
            self._blocking_waiters.clear()
        if self._user_on_close_callback:
            self._user_on_close_callback(_connection, reason)

    def _shutdown_all_consumer_pools(self,
                                     timeout: float | None = None) -> None:
        """
        Shut down all tracked channel consumer pools.

        :param timeout: Total seconds to wait across all channel pools. ``None`` waits indefinitely
            (the clean-shutdown default). A finite budget is split across channels so the whole
            sweep stays bounded.
        """
        with self._channel_waiters_lock:
            channels = list(self._channels)
        if not channels:
            return
        if timeout is None:
            for ch in channels:
                ch._shutdown_pool()
            return
        # Signal every pool to start draining before joining any of them, so
        # all workers drain concurrently.  Joining one at a time without this
        # would let an early wedged channel burn the whole budget while later
        # channels' workers had not even been told to stop, so those healthy
        # workers would be abandoned at timeout=0 with a spurious warning.
        for ch in channels:
            ch._signal_pool_shutdown()
        # Share the remaining budget across channels using a common deadline so
        # a slow early channel cannot consume the entire allowance and starve
        # later ones of their chance to drain.
        deadline = time.monotonic() + timeout
        for ch in channels:
            ch._shutdown_pool(timeout=max(0.0, deadline - time.monotonic()))

    def _shutdown_connection_pool(self, timeout: float | None = None) -> None:
        """
        Shut down the connection-level event-callback pool.

        :param timeout: Seconds to wait for the worker to drain and exit. ``None`` waits
            indefinitely (the clean-shutdown default). A wedged worker outliving a finite *timeout*
            is left running (daemon thread) rather than stalling the caller; a warning is logged.
        """
        # Atomically claim the shutdown so the IOLoop tail and a concurrent
        # close() do not both run the join.  The join stays outside the lock so
        # it cannot stall other channel operations for the whole timeout.
        with self._channel_waiters_lock:
            if self._connection_pool_shutdown:
                return
            self._connection_pool_shutdown = True
        if not self._connection_work_pool.shutdown(wait=True, timeout=timeout):
            LOGGER.warning(
                'Connection %s event work pool did not drain within '
                '%s seconds; abandoning its worker thread', self._instance_id,
                timeout)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def channel(self, timeout: float | None = DEFAULT_RPC_TIMEOUT) -> Channel:
        """
        Open a new channel and return a :class:`Channel`.

        Blocks the calling thread until the channel is open.  The returned channel's methods are
        safe to call from any thread.

        :param timeout: Seconds to wait for Channel.OpenOk. Defaults to :data:`DEFAULT_RPC_TIMEOUT`
            (10 s). Pass ``None`` to wait indefinitely.
        :raises Exception: if the connection is closed before the channel opens.
        :raises TimeoutError: if *timeout* expires before the channel opens.
        """
        ready = threading.Event()
        result = [None]
        error: list[BaseException | None] = [None]

        with self._channel_waiters_lock:
            reason = self._closed_reason
            if reason is not None:
                raise _with_close_traceback(reason, self._closed_reason_tb)
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
            raise _reraisable(error[0], self._closed_reason,
                              self._closed_reason_tb)

        # Race guard: the connection may have closed on the IOLoop thread
        # after _on_open fired but before we woke up.  Without this check,
        # _shutdown_all_consumer_pools() would have already run and the
        # newly-constructed channel's consumer pool would never be reached.
        with self._channel_waiters_lock:
            reason = self._closed_reason
            if reason is not None:
                raise _with_close_traceback(reason, self._closed_reason_tb)
            ch = Channel(result[0],
                         self,
                         work_queue_maxsize=self._work_queue_maxsize,
                         work_queue_put_timeout=self._work_queue_put_timeout)
            self._channels.append(ch)
        # Register the broker-close hook on the IOLoop thread, where the raw
        # channel's callback stack is safe to mutate.  A broker close arriving
        # in the brief gap before this runs simply falls back to the
        # connection-shutdown sweep, which still drops the channel from
        # tracking.
        self._schedule_unchecked(
            lambda: ch._channel.add_on_close_callback(ch._on_broker_close))
        return ch

    def close(self, timeout: float | None = 10) -> None:
        """
        Close the connection and block until the IOLoop thread exits.

        Schedules a clean Connection.Close handshake and waits up to *timeout* seconds for the
        IOLoop thread to finish.  If the thread is still alive after the timeout (e.g. the broker
        never sends Connection.CloseOk), the IOLoop is force-stopped via ``add_callback_threadsafe``
        and joined once more under a second *timeout* budget.  A worker still draining when that
        budget expires is left to finish on its own (it is a daemon thread), so a wedged delivery
        callback cannot hang ``close`` indefinitely.

        Safe to call from any thread including the IOLoop thread itself (e.g. from within a channel
        callback).  When called from the IOLoop thread the close is initiated synchronously and the
        method returns immediately without joining.

        Calling ``close()`` on an already-closed connection is a no-op.

        :param timeout: Seconds to wait for a clean close before force-stopping the IOLoop. Defaults
            to 10 seconds, which is sufficient for a healthy broker on any reasonable network. The
            pathological force path may wait up to twice this (once for the clean join, once for the
            forced join). Pass ``None`` to wait indefinitely.
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
            # The IOLoop thread runs its own pool-drain tail after
            # ioloop.start() returns; a wedged consumer callback can keep it
            # from exiting.  Bound the join (and the pool shutdowns below) by a
            # fresh budget so a stuck worker cannot hang close() forever - the
            # worker is a daemon thread and is left to finish on its own.
            force_deadline = (None if timeout is None else time.monotonic() +
                              timeout)

            def _remaining() -> float | None:
                if force_deadline is None:
                    return None
                return max(0.0, force_deadline - time.monotonic())

            self._ioloop_thread.join(timeout=_remaining())
            if self._ioloop_thread.is_alive():
                LOGGER.warning(
                    'Connection %s IOLoop thread did not exit within the close '
                    'timeout; abandoning it', self._instance_id)
            # _on_connection_closed may not have fired if the IOLoop was
            # force-stopped before the broker sent Connection.CloseOk.
            # Wake any threads still blocked so they do not hang forever.
            forced = Exception(
                'connection force-closed: broker did not respond to close')
            with self._channel_waiters_lock:
                if self._closed_reason is None:
                    self._record_closed_reason(forced)
                    for evt, err in self._blocking_waiters:
                        if err[0] is None:
                            err[0] = self._closed_reason
                        evt.set()
                    self._blocking_waiters.clear()
            # Normally the IOLoop tail already claimed these pools, making the
            # calls no-ops; bound them in case this thread reaches them first.
            self._shutdown_all_consumer_pools(timeout=_remaining())
            self._shutdown_connection_pool(timeout=_remaining())

    def abort(self, timeout: float | None = 10) -> None:
        """
        Close the connection, swallowing any errors.

        Equivalent to :meth:`close` but never raises.  Currently :meth:`close` itself does not raise
        (it logs a warning on timeout), but ``abort`` is provided for API symmetry with other AMQP
        0-9-1 clients and to make error-recovery intent explicit at the call site.

        Safe to call from any thread.

        :param timeout: Seconds to wait for a clean close before force-stopping the IOLoop. Defaults
            to 10 seconds. Pass ``None`` to wait indefinitely.
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
        """
        Schedule *callback* to run in the IOLoop thread.

        Safe to call from any thread.  Exposed so callers can schedule arbitrary work without going
        through the wrapper methods.

        Raises rather than accepting work that can never run: once the connection is closed the
        IOLoop thread has exited, so a queued callback would be silently dropped.  The caller gets
        the recorded close reason, the same exception every other method on the connection raises
        once closed, or a :class:`~pika.exceptions.ConnectionWrongStateError` when no reason was
        recorded yet.

        Calls from the IOLoop thread itself are exempt and never raise, mirroring :meth:`close`.
        A close callback runs on that thread while the connection is already closed, so raising
        would abort the rest of teardown rather than reach the application.  The callback is
        accepted and logged at debug level, but a stopping IOLoop will most likely never run it.

        :param callback: Zero-argument callable.
        :raises BaseException: the recorded close reason, if the connection is closed and this is
            not the IOLoop thread.
        :raises pika.exceptions.ConnectionWrongStateError: if the connection is closed but no reason
            was recorded yet, and this is not the IOLoop thread.
        """
        try:
            self._check_not_closed()
        except Exception:
            # Teardown runs on the IOLoop thread: _on_connection_closed records
            # the close reason and then invokes the user's on_close_callback,
            # whose natural idiom is to schedule follow-up work.  Raising there
            # would propagate out through CallbackManager.process and abort the
            # rest of teardown, so accept the callback from that thread instead.
            # The IOLoop is already stopping, so the callback most likely will
            # not run; that beats leaving the connection half torn down.
            if threading.current_thread() is not self._ioloop_thread:
                raise
            LOGGER.debug(
                'add_callback_threadsafe() called from the IOLoop thread on a '
                'closed connection; the callback may not run',
                exc_info=True)
        self._schedule_unchecked(callback)

    def _schedule_unchecked(self, callback) -> None:
        """
        Hand *callback* to the IOLoop without the closed-connection check.

        For internal callers that must not be rejected mid-teardown.  Several channel methods
        register a waiter before scheduling and unregister it in a ``finally``; raising here would
        skip that cleanup and leak the waiter, so they check whatever state they care about
        themselves and then schedule through this method.

        :param callback: Zero-argument callable.
        """
        self._connection.ioloop.add_callback_threadsafe(callback)

    def _record_closed_reason(self, reason: BaseException) -> None:
        """
        Record the connection's close reason and the traceback it arrived with.

        Callers must already hold ``_channel_waiters_lock``: the reason and its traceback are read
        together and must not be seen half-updated.

        :param reason: The exception every blocked caller will be given. A value that is not an
            exception carries no traceback to snapshot; pika always passes an exception, but
            ``_on_connection_closed`` is driven by the underlying connection and does not enforce
            that.
        """
        self._closed_reason = reason
        self._closed_reason_tb = getattr(reason, '__traceback__', None)

    def _check_not_closed(self) -> None:
        """
        Raise if the connection is known to be closed.

        Best-effort: the connection may still close between this check and the caller's use of the
        IOLoop, which no amount of locking can prevent.  It catches the case that matters in
        practice - work submitted to a connection the caller already closed.

        Raises the recorded ``_closed_reason`` itself, the same exception every other method on the
        class raises once closed, with its close-time traceback restored so re-raising the shared
        instance does not grow it (see :func:`_with_close_traceback`).  Only when the connection is
        closed but no reason was recorded yet does it raise a fresh
        :class:`~pika.exceptions.ConnectionWrongStateError`.

        :raises BaseException: ``_closed_reason`` if one was recorded.
        :raises pika.exceptions.ConnectionWrongStateError: if the underlying connection is closed
            but no reason was recorded yet, which is the brief window between the connection
            reaching the closed state and ``_on_connection_closed`` running.
        """
        with self._channel_waiters_lock:
            reason = self._closed_reason
            if reason is not None:
                raise _with_close_traceback(reason, self._closed_reason_tb)
        if self._connection.is_closed:
            raise ConnectionWrongStateError(
                'Connection.add_callback_threadsafe() called on '
                'closed connection.')

    def add_on_connection_blocked_callback(self, callback) -> None:
        """
        Register a callback for ``Connection.Blocked`` notifications.

        RabbitMQ sends ``Connection.Blocked`` when the broker is running
        low on memory or disk.  In this state RabbitMQ stops processing
        incoming data, so a publisher receiving this notification should
        suspend publishing until the connection is unblocked.

        Dispatched on a connection-level worker thread (one per
        connection), so the callback may safely call any
        :class:`Channel` method without stalling heartbeats.
        The RabbitMQ Java and .NET clients run this listener inline on
        the I/O thread; pika's wrapper deliberately diverges so a slow
        listener cannot stall heartbeats.

        Safe to call from any thread.

        :param callback:
            ``callback(connection, method_frame)`` where *connection* is
            this :class:`Connection` and *method_frame* contains
            a :class:`pika.spec.Connection.Blocked`.
        :raises Exception: if the connection is already closed.
        """
        self._register_connection_event_callback(
            'add_on_connection_blocked_callback', callback)

    def add_on_connection_unblocked_callback(self, callback) -> None:
        """
        Register a callback for ``Connection.Unblocked`` notifications.

        Sent by RabbitMQ once a previously blocked connection is no
        longer resource-constrained, letting publishers resume.

        Dispatched on the connection-level worker thread (same as
        ``add_on_connection_blocked_callback``).  The RabbitMQ Java and
        .NET clients run this listener inline on the I/O thread; pika's
        wrapper deliberately diverges so a slow listener cannot stall
        heartbeats.

        Safe to call from any thread.

        :param callback:
            ``callback(connection, method_frame)`` where *connection* is
            this :class:`Connection` and *method_frame* contains
            a :class:`pika.spec.Connection.Unblocked`.
        :raises Exception: if the connection is already closed.
        """
        self._register_connection_event_callback(
            'add_on_connection_unblocked_callback', callback)

    def _register_connection_event_callback(self, raw_method_name: str,
                                            callback) -> None:
        """
        Register a connection-level event callback via the IOLoop.

        Wraps the user callback so it dispatches on the connection work pool, then schedules
        registration with the underlying :class:`pika.connection.Connection` on the IOLoop thread.
        :param raw_method_name: Unqualified AMQP method name (e.g. ``"Basic.Publish"``)
        :param callback: User callback to dispatch on the connection work pool."
        """
        with self._channel_waiters_lock:
            reason = self._closed_reason
            if reason is not None:
                raise _with_close_traceback(reason, self._closed_reason_tb)

        def _wrapped(_raw_conn, method_frame) -> None:
            _submit_or_terminate(
                self._connection_work_pool, self._connection,
                'Connection event dropped: work pool shut down',
                Channel._safe_dispatch, raw_method_name, callback, self,
                method_frame)

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
