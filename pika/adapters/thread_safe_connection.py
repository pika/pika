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
import weakref
from threading import Event
from typing import TYPE_CHECKING, Any, Callable, TypeVar

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Literal, Self

    import pika.channel

from pika import frame, spec, validators
from pika.adapters.select_connection import SelectConnection
from pika.exceptions import ConnectionWrongStateError, WorkQueueFullError

LOGGER = logging.getLogger(__name__)

_R = TypeVar('_R')

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

# Default seconds the IOLoop thread waits, after its loop has exited, for
# every pool worker to finish the work already queued.  Long enough for a
# slow but healthy callback to complete, short enough that process exit is
# not visibly stalled by a wedged one, which is then abandoned on its daemon
# thread with a warning.  It compounds with the put timeout above: a wedged
# consumer can hold the IOLoop in `submit` for one budget and here for
# another.
DEFAULT_SHUTDOWN_DRAIN_TIMEOUT = 30.0

# How often a bounded wait rechecks whether the thing it waits for can still
# happen, rather than only whether the budget has run out.
_LIVENESS_POLL_INTERVAL = 0.05

# Floor on the wait for the close handshake, applied after the queued callbacks
# have had the caller's budget.  A fixed value is right here and wrong for the
# phases before it: the handshake is one round trip whose duration does not
# depend on the backlog, so a floor cannot cut short work that is merely slow,
# whereas a drain that spent the whole budget would otherwise leave the join
# nothing and abandon the connection half-closed (#1763).
_CLOSE_HANDSHAKE_FLOOR = 1.0


def _validate_shutdown_drain_timeout(drain_timeout: float) -> float:
    """
    Validate a public shutdown drain timeout.

    ``None`` is rejected for the reason `_validate_put_timeout` gives: a wedged callback would hold
    the IOLoop thread after its loop has exited, and nothing else bounds that wait. ``0`` is
    allowed, unlike a put timeout, because abandoning the queued work immediately is a coherent
    choice for a caller who wants the process gone.

    :param drain_timeout: Seconds the IOLoop thread waits for the queued work to finish.
    :returns: *drain_timeout* unchanged.
    :raises ValueError: if *drain_timeout* is ``None`` or is negative.
    """
    if drain_timeout is None:
        raise ValueError(
            'shutdown_drain_timeout must be a number of seconds, not None; an '
            'infinite timeout lets a wedged callback hold the IOLoop thread '
            'after its loop has exited')
    # ``not drain_timeout >= 0`` also rejects float('nan').
    if not drain_timeout >= 0:
        raise ValueError(f'shutdown_drain_timeout must not be negative, got '
                         f'{drain_timeout!r}')
    return drain_timeout


def _validate_put_timeout(put_timeout: float) -> float:
    """
    Validate a public work-queue put timeout.

    A ``None`` (infinite) timeout is rejected because it lets a full work queue block the IOLoop
    thread forever.  A non-positive timeout - including ``0`` and ``float('nan')`` - is rejected
    because it cannot bound the stall.

    :param put_timeout: Seconds the IOLoop thread waits for queue space before raising
        [`pika.exceptions.WorkQueueFullError`][pika.exceptions.WorkQueueFullError].
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
    grows that instance's traceback the way `_with_close_traceback` avoids.  Any other value
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
    `_BoundedWorkPool`.  A [`WorkQueueFullError`][pika.exceptions.WorkQueueFullError]
    means a slow callback let the queue stay full past ``work_queue_put_timeout``;
    it is routed through [`_terminate_stream`][pika.connection.Connection._terminate_stream] so
    the connection closes with [`WorkQueueFullError`][pika.exceptions.WorkQueueFullError] as the
    reason the application observes, rather than being caught by the transport's
    generic read-error handler and relabeled as an opaque
    [`StreamLostError`][pika.exceptions.StreamLostError].  A ``RuntimeError`` means the pool
    is already shut down, so the callback is dropped.

    Once teardown has begun, ``connection._error`` is set synchronously (by the
    first [`_terminate_stream`][pika.connection.Connection._terminate_stream] call) while the
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

    Replaces `concurrent.futures.ThreadPoolExecutor`, whose internal
    queue is unbounded and grows without limit when user callbacks cannot
    keep up with incoming events (issue #1600).

    ``submit`` runs on the IOLoop thread.  While the queue is full it
    blocks, which stops frame reads from the socket and applies
    back-pressure to the broker via TCP flow control - the same strategy
    as the RabbitMQ Java client's ``WorkPool``.

    An exception out of a submitted item is logged and the next item runs, so
    no submit site can cost the pool its worker and strand the queue behind it.
    pika's own sites still wrap user callbacks before submitting, which is what
    names the callback in the log line; this guard is what holds when one does
    not.

    The worker thread starts lazily on first submit so that channels which
    never dispatch callbacks (e.g. publish-only channels) do not start one.

    :param maxsize: Maximum number of pending work items.  Pass ``0`` for
        an unbounded queue.
    :param put_timeout: Seconds `submit` waits for queue space before
        raising [`pika.exceptions.WorkQueueFullError`][pika.exceptions.WorkQueueFullError].  Must be
        finite; an infinite (``None``) timeout would let a full queue block
        the IOLoop thread forever.
    :param thread_name: Name of the worker thread.
    :param owner: The `Connection` whose IOLoop thread this pool's worker must not join.  Held
        weakly and stamped on the worker thread, so a callback can ask whose worker it is in O(1).
        The per-channel pools pass their connection, not their channel.
    """

    # Best-effort wakeup ping enqueued by shutdown() to rouse a worker idling
    # in a blocking get().  The shutdown flag remains the source of truth; the
    # sentinel only nudges the worker to re-check it, so a dropped ping (full
    # queue) is harmless - a busy worker re-checks the flag between items.
    _SENTINEL = object()

    def __init__(self, maxsize: int, put_timeout: float, thread_name: str,
                 owner: Connection) -> None:
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=maxsize)
        self._put_timeout = put_timeout
        self._thread_name = thread_name
        # Both edges stay weak -- here, and the stamp in submit() holds this
        # ref rather than its referent: a running thread is a GC root, so
        # either held strongly would pin the connection and its socket (#1761).
        self._owner = weakref.ref(owner)
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
                # Read back by Connection._is_pool_worker_thread (#1761).
                self._thread._pika_pool_owner = self._owner  # type: ignore[attr-defined]
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
            try:
                fn(*args)
            except BaseException:
                # BaseException, because SystemExit off the main thread ends only
                # this worker. See docs/internal/thread-safe-connection.md, #1751.
                LOGGER.exception('Unhandled exception in %s work item',
                                 self._thread_name)

    def drain(self, timeout: float | None = None) -> bool:
        """
        Wait for the work already queued to run, without shutting the pool down.

        Submits a marker and waits for it.  The queue is FIFO and one worker services it, so the
        marker running means every item queued ahead of it has returned.  The pool stays open, so
        work arriving afterwards is still accepted rather than rejected, which is the difference
        that matters to a caller draining before a close, since an item rejected at submit is an
        item lost (#1763).

        The marker is enqueued under *timeout* rather than ``put_timeout``, so a full queue cannot
        stretch a close past the budget its caller was given.

        :param timeout: Seconds for the whole drain, the enqueue and the wait together. ``None``
            waits indefinitely.
        :returns:``True`` if the queued work completed. ``False`` if *timeout* expired, if the pool
            is already shut down, if the queue stayed full, or if called from the pool's own worker
            thread, where waiting could never succeed: the marker cannot run until the calling
            callback returns.
        """
        thread = self._thread
        if thread is None:
            # No worker ever started, so nothing was ever queued.
            return True
        if thread is threading.current_thread():
            return False
        deadline = None if timeout is None else time.monotonic() + timeout

        def _remaining() -> float | None:
            if deadline is None:
                return None
            return max(0.0, deadline - time.monotonic())

        done = threading.Event()
        with self._lock:
            if self._shutdown:
                return False
        # Both waits below are bounded by the worker still being able to run the
        # marker as well as by the budget: a worker killed by a BaseException
        # leaves a queue nothing will ever drain and no slot will ever free, so
        # either wait alone is a hang under close(timeout=None).  shutdown() has
        # always checked liveness; so must these.
        while True:
            try:
                self._queue.put((done.set, ()), timeout=_LIVENESS_POLL_INTERVAL)
                break
            except queue.Full:
                if not thread.is_alive():
                    return False
                budget = _remaining()
                if budget is not None and budget <= 0:
                    return False
        while not done.wait(timeout=_LIVENESS_POLL_INTERVAL):
            if not thread.is_alive():
                return done.is_set()
            budget = _remaining()
            if budget is not None and budget <= 0:
                return done.is_set()
        return True

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> bool:
        """
        Shut down the pool, allowing in-flight work to finish.

        Idempotent: repeated calls are safe and subsequent `submit`
        calls raise `RuntimeError`.  A repeat call still joins (so a
        caller whose first bounded call timed out can wait again for the
        worker), which is why the flag is set unconditionally rather than
        short-circuiting.

        The shutdown flag is the source of truth.  A best-effort
        `_SENTINEL` ping is then enqueued via ``put_nowait`` purely to
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
    Thread-safe wrapper around [`pika.channel.Channel`][pika.channel.Channel].

    Every write operation is routed through the parent connection's
    ``add_callback_threadsafe`` so that ``_tx_buffers`` is only ever
    touched from the IOLoop thread.

    .. rubric:: Consumer callback threading model

    The *on_message_callback* registered with [`basic_consume`][pika.adapters.thread_safe_connection.Channel.basic_consume] is
    dispatched on a per-channel worker thread, **not** the IOLoop
    thread.  This means:

    - Blocking operations (database writes, HTTP calls, even
      [`queue_declare`][pika.adapters.thread_safe_connection.Channel.queue_declare] / [`basic_qos`][pika.adapters.thread_safe_connection.Channel.basic_qos] / [`basic_cancel`][pika.adapters.thread_safe_connection.Channel.basic_cancel])
      are safe inside delivery callbacks.
    - As long as the callback keeps up with deliveries, the IOLoop
      thread is not starved by consumer processing, so heartbeats are
      sent on time.
    - Messages are delivered to the callback **in order** (a single
      worker thread per channel).
    - All [`Channel`][pika.adapters.thread_safe_connection.Channel] methods ([`basic_ack`][pika.adapters.thread_safe_connection.Channel.basic_ack],
      [`basic_nack`][pika.adapters.thread_safe_connection.Channel.basic_nack], [`basic_reject`][pika.adapters.thread_safe_connection.Channel.basic_reject], [`basic_publish`][pika.adapters.thread_safe_connection.Channel.basic_publish],
      [`queue_declare`][pika.adapters.thread_safe_connection.Channel.queue_declare], etc.) are safe to call from within the
      callback.

    .. rubric:: Sustained-backlog caveat

    "Safe" assumes the callback keeps pace with incoming deliveries.
    The worker consumes from a bounded queue (see *work_queue_maxsize*).
    If a slow callback lets that queue fill, the IOLoop thread blocks
    while enqueuing the next delivery - sending no heartbeats during the
    stall - and, if the queue stays full for *work_queue_put_timeout*
    seconds, raises [`pika.exceptions.WorkQueueFullError`][pika.exceptions.WorkQueueFullError], which
    tears the connection down rather than silently dropping the event.
    A persistently slow consumer therefore does not stall heartbeats
    forever; it fails fast with an explicit error.  Keep callbacks
    faster than the sustained delivery rate, or raise *work_queue_maxsize*
    to absorb bursts.

    .. rubric:: IOLoop-thread callbacks

    Callables passed directly to
    [`add_callback_threadsafe`][pika.adapters.thread_safe_connection.Connection.add_callback_threadsafe] still run
    on the IOLoop thread.  These must return quickly and must not call
    blocking channel methods (which would deadlock).
    """

    def __init__(
            self,
            channel: pika.channel.Channel,
            wrapper: Connection,
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
            owner=wrapper,
        )
        self._pool_shutdown = False
        self._next_publish_seq_no: int | None = None
        self._confirm_select_ok: frame.Method[
            spec.Confirm.SelectOk] | None = None
        # Serializes confirm_delivery so two threads cannot both pass the
        # _confirm_select_ok guard while the first is still waiting for
        # Confirm.SelectOk.  Per-channel rather than the connection-wide
        # _channel_waiters_lock: the call blocks for a broker round trip, which
        # must not stall operations on other channels.
        self._confirm_lock = threading.Lock()

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
        Run *callback*, logging any exception instead of letting it propagate.

        Wraps a user callback so one failing dispatch neither escapes into the caller nor prevents
        later dispatches from running.  Most callers submit the wrapped callback to a
        `_BoundedWorkPool`, so it runs on a pool worker; [`basic_publish`][pika.adapters.thread_safe_connection.Channel.basic_publish] calls it
        inline on the IOLoop thread. Either way the exception is contained here.

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
        [`_shutdown_pool`][pika.adapters.thread_safe_connection.Channel._shutdown_pool] joins them one at a time under a shared budget.
        """
        self._consumer_work_pool.shutdown(wait=False)

    def _claim_pool_shutdown(self) -> bool:
        """
        Claim this channel's pool shutdown and drop it from connection tracking.

        Returns ``True`` if this call is the first to claim the shutdown - so the caller owns any
        follow-up work, such as joining the worker - and ``False`` if a prior close already claimed
        it.  Being idempotent lets a user [`close`][pika.adapters.thread_safe_connection.Channel.close], the broker-close hook
        ([`_on_broker_close`][pika.adapters.thread_safe_connection.Channel._on_broker_close]) and the connection-shutdown sweep race while each channel is
        dropped from ``_channels`` exactly once.

        The tracking list exists only so [`_shutdown_all_consumer_pools`][pika.adapters.thread_safe_connection.Connection._shutdown_all_consumer_pools] can reach
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

        Claims the shutdown and drops the channel from tracking via [`_claim_pool_shutdown`][pika.adapters.thread_safe_connection.Channel._claim_pool_shutdown],
        then joins the worker.  Used by the paths that can afford to wait off the IOLoop thread: a
        user [`close`][pika.adapters.thread_safe_connection.Channel.close] (on the caller's thread) and the connection-shutdown sweep (after the
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

    def _cancel_consumers(self, deadline: float | None) -> None:
        """
        Cancel every consumer this channel still has, so the broker stops dispatching.

        The first half of `_quiesce`, separate so a connection-wide close can stop every channel
        before draining any of them.  Draining one channel while another still has a live consumer
        lets that other channel's queue grow during the drain, which is the starvation
        `Connection._shutdown_all_consumer_pools` avoids for the same reason.

        :param deadline:`time.monotonic` value the cancellations must finish by, or ``None`` to wait
            indefinitely.
        """

        def _remaining() -> float | None:
            if deadline is None:
                return None
            return max(0.0, deadline - time.monotonic())

        asked: list[set[str]] = [set()]
        issued = threading.Event()

        def _noop(_frame) -> None:
            """Make the cancel synchronous; completion is read from the channel."""

        def _cancel_all() -> None:
            # Read and cancel in one IOLoop callback so the set is current, and
            # skip tags already cancelling: a nowait cancel's retained tag never
            # leaves _consumers, so waiting on it would never end (#1763).
            try:
                for tag in [
                        t for t in list(self._channel._consumers)
                        if t not in self._channel._cancelled
                ]:
                    try:
                        self._channel.basic_cancel(consumer_tag=tag,
                                                   callback=_noop)
                        if tag in self._channel._cancelled:
                            asked[0].add(tag)
                    except Exception:
                        LOGGER.debug('Quiesce: cancelling consumer %s failed',
                                     tag,
                                     exc_info=True)
            finally:
                issued.set()

        self._wrapper._schedule_unchecked(_cancel_all)

        def _outstanding() -> set[str]:
            # The channel drops a tag from _consumers when its Basic.CancelOk
            # arrives, so its own bookkeeping says when this sweep is done.
            return {t for t in asked[0] if t in self._channel._consumers}

        # Liveness as well as the budget: the replies come from the IOLoop
        # thread, so if it has gone there is nothing left to wait for.  This is
        # the only wait here the broker's pace governs, and capping it below
        # that pace abandons live cancels and costs messages, so it is not
        # capped (#1763).
        while True:
            if issued.is_set() and not _outstanding():
                return
            if not self._wrapper._ioloop_thread.is_alive():
                return
            with self._wrapper._channel_waiters_lock:
                if self._wrapper._closed_reason is not None:
                    return
            if self._channel.is_closed:
                return
            budget = _remaining()
            if budget is not None and budget <= 0:
                LOGGER.warning(
                    'Channel %s did not finish cancelling its consumers '
                    'before the close budget expired',
                    self._channel.channel_number)
                return
            time.sleep(_LIVENESS_POLL_INTERVAL)

    def _drain_pool(self, deadline: float | None) -> None:
        """
        Run the callbacks already queued on this channel's pool.

        The second half of `_quiesce`.  The pool is left open rather than shut down, so a confirm or
        a return arriving between this and the close is still accepted; the caller's own post-close
        shutdown runs those.

        :param deadline:`time.monotonic` value the drain must finish by, or ``None`` to wait
            indefinitely.
        """
        remaining = (None if deadline is None else max(
            0.0, deadline - time.monotonic()))
        pool = self._consumer_work_pool
        if pool.drain(timeout=remaining):
            return
        if pool._shutdown:
            # Shut down already, by an earlier close or by _on_broker_close,
            # which sets the flag without draining.  Either way nothing here can
            # run the queue, and warning about a budget would be wrong.
            return
        if pool._thread is threading.current_thread():
            # A callback closing its own channel cannot wait for a marker queued
            # behind itself; the rest of its queue is left to the post-close
            # shutdown.
            LOGGER.debug(
                'Channel %s drained from its own worker; queued callbacks are '
                'left to the shutdown', self._channel.channel_number)
        else:
            LOGGER.warning(
                'Channel %s consumer work pool did not finish its queued '
                'callbacks before the close; anything left unacknowledged will '
                'be redelivered', self._channel.channel_number)

    def _quiesce(self, deadline: float | None) -> None:
        """
        Stop further deliveries, then drain the pool while the connection still works.

        `_cancel_consumers` followed by `_drain_pool`, under one deadline.  The broker may dispatch
        more deliveries before it answers a cancel, so the cancel does not by itself bound what
        arrives; what it gives is a point after which the broker sends no more, which is what lets
        the drain finish.  Draining then runs with the connection open, so the acknowledgements
        those callbacks issue still reach the broker.

        Draining after the close instead is what #1763 measured: the callbacks run, their
        acknowledgements never reach the broker, and it redelivers work that was already performed.
        Dropping the backlog instead would destroy the only copy of an ``auto_ack`` delivery or a
        returned message, since for those the queue holds the sole copy.

        The cancellations are not given a reserved share of the budget.  Their replies cannot arrive
        until the IOLoop has submitted the backlog, which takes as long as the worker needs to
        consume it, so capping them at a fraction caps them below the work they are waiting on:
        measured with a 25 ms callback and 2000 messages, a half-budget reserve timed the cancel out
        and lost 641 messages.  A broker that never answers does spend the whole budget here, and
        that costs nothing, because a drain cannot land an acknowledgement through a broker that is
        not answering either.

        :param deadline:`time.monotonic` value the whole quiesce must finish by, or ``None`` to wait
            indefinitely.
        """
        self._cancel_consumers(deadline)
        self._drain_pool(deadline)

    def _on_broker_close(self, _channel, reason) -> None:
        """
        Drop this channel from tracking when the broker closes it on its own.

        Registered once per channel, on the IOLoop thread, right after the channel opens.  A client
        close - a user [`close`][pika.adapters.thread_safe_connection.Channel.close], or the per-channel closes a graceful [`Connection.close`][pika.adapters.thread_safe_connection.Connection.close]
        issues - reports [`ChannelClosedByClient`][pika.exceptions.ChannelClosedByClient] and is left to
        [`_shutdown_pool`][pika.adapters.thread_safe_connection.Channel._shutdown_pool], so this handles only broker- or error-initiated closes.  Without it
        those channels stay in ``_channels`` (with their pool, worker thread, raw channel and
        per-RPC callbacks) until the connection itself is torn down - the leak #1688 describes,
        reached through a close path [`_shutdown_pool`][pika.adapters.thread_safe_connection.Channel._shutdown_pool] never sees.

        Runs on the IOLoop thread, so it must not join the worker: joining a worker that is mid
        delivery-callback and waiting on this same IOLoop would deadlock, which is why pool joins
        are otherwise deferred off this thread (see [`_on_connection_closed`][pika.adapters.thread_safe_connection.Connection._on_connection_closed]).  It
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

        [`add_on_close_callback`][pika.channel.Channel.add_on_close_callback] registers with ``one_shot=False`` and
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
                      send: Callable[[Callable[[_R], None]], object],
                      timeout: float | None,
                      on_sent: Callable[[], None] | None = None) -> _R:
        """
        Execute a channel RPC and block until the broker responds.

        Handles the waiter lifecycle: registers the calling thread's event
        in ``_blocking_waiters``, schedules the RPC on the IOLoop thread,
        waits for the response (or timeout/error), unregisters, and returns
        the result or raises.

        *send* is called on the IOLoop thread with the success callback and
        must pass it to the raw channel method as that method's ``callback``,
        so the reply type is checked against the channel's annotation.

        :param method_name: Human-readable name for timeout messages.
        :param send: Issues the RPC on the raw channel.
        :param timeout: Seconds to wait.
        :param on_sent: Optional zero-argument callable run on the IOLoop thread
            immediately after *send* writes the frame, ahead of any
            callback already queued behind it.  For state that must be ordered
            against the frame going out rather than against the broker's
            response coming back.
        :returns: The broker response frame.
        :raises TimeoutError: if *timeout* expires.
        :raises Exception: if the connection or channel closes first.
        """
        ready, error = self._register_waiter()
        result: list[_R] = []

        def _on_ok(method_frame: _R) -> None:
            result.append(method_frame)
            ready.set()

        def _on_chan_close(ch, reason) -> None:
            if not ready.is_set():
                if error[0] is None:
                    error[0] = reason
                ready.set()

        def _invoke() -> None:
            try:
                self._channel.add_on_close_callback(_on_chan_close)
                send(_on_ok)
            except Exception as exc:
                error[0] = exc
                ready.set()
                return
            # The frame is on the wire.  Run the post-send hook separately: if
            # send delivered its response synchronously (result and
            # ready already set) a raising hook must not clobber that result,
            # since _blocking_rpc checks error ahead of result.  Surface the
            # hook's failure only while the response is still outstanding.
            if on_sent is not None:
                try:
                    on_sent()
                except Exception as exc:
                    if not ready.is_set():
                        error[0] = exc
                        ready.set()
                    else:
                        LOGGER.exception(
                            'on_sent hook raised after %s completed',
                            method_name)

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
            Only meaningful when publisher confirms are enabled via [`confirm_delivery`][pika.adapters.thread_safe_connection.Channel.confirm_delivery];
            ignored otherwise. Must return quickly (same contract as any
            [`add_callback_threadsafe`][pika.adapters.thread_safe_connection.Connection.add_callback_threadsafe] callback). An exception raised by this
            callback is logged and suppressed rather than propagated, so it does not tear down the
            connection.
        :raises TypeError: if *on_publish* is not callable and not ``None``.
        :raises Exception: if the connection is already closed.
        """
        if on_publish is not None:
            validators.require_callback(on_publish, 'on_publish')

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
                    # Guard on_publish with _safe_dispatch so an exception
                    # from user code is logged and contained instead of
                    # propagating out of the IOLoop thread and killing the
                    # whole connection (#1687).  Unlike the consumer, confirm,
                    # return and cancel callbacks, which _submit_or_terminate
                    # hands to the work pool, on_publish runs inline here on
                    # the IOLoop thread as the basic_publish docstring
                    # documents; _safe_dispatch adds only the exception guard,
                    # not the pool dispatch.
                    self._safe_dispatch('publish callback', on_publish,
                                        self._next_publish_seq_no)

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

    def basic_qos(
        self,
        prefetch_size: int = 0,
        prefetch_count: int = 0,
        global_qos: bool = False,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Basic.QosOk]:
        """Set channel QoS and block until Basic.QosOk arrives.

        Safe to call from any thread.

        :param prefetch_size: Prefetch window in octets (0 = no limit).
        :param prefetch_count: Prefetch window in whole messages (0 = no limit).
        :param global_qos: Apply QoS to all consumers on the channel.
        :param timeout: Seconds to wait for the response.
            Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT] (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Basic.QosOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
                callback: Callable[[frame.Method[spec.Basic.QosOk]],
                                   None]) -> None:
            self._channel.basic_qos(prefetch_size=prefetch_size,
                                    prefetch_count=prefetch_count,
                                    global_qos=global_qos,
                                    callback=callback)

        return self._blocking_rpc('basic_qos', send, timeout)

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
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
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
        may safely call any [`Channel`][pika.adapters.thread_safe_connection.Channel] method.  The RabbitMQ Java client dispatches its cancel
        listener to the consumer work pool rather than running it inline on the I/O thread, which is
        the same choice made here.

        Safe to call from any thread.

        :param callback:``callback(method_frame)`` where *method_frame* contains a
            [`pika.spec.Basic.Cancel`][pika.spec.Basic.Cancel].
        :raises TypeError: if *callback* is not callable.
        :raises Exception: if the connection is already closed.
        """
        validators.require_callback(callback, 'callback')

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
        broker returns it via a [`pika.spec.Basic.Return`][pika.spec.Basic.Return].  The *callback* receives the
        returned message.

        Dispatched on the per-channel worker thread (same as delivery callbacks), so the callback
        may safely call any [`Channel`][pika.adapters.thread_safe_connection.Channel] method and heartbeats keep flowing while it works. The
        RabbitMQ Java and .NET clients run this listener inline on the I/O thread.  A listener that
        cannot keep pace still blocks the IOLoop once the shared queue fills; see
        *work_queue_maxsize*.

        Safe to call from any thread.

        :param callback:``callback(channel, method, properties, body)`` where *channel* is this
            [`Channel`][pika.adapters.thread_safe_connection.Channel], *method* is a [`pika.spec.Basic.Return`][pika.spec.Basic.Return], *properties* is a
            [`pika.spec.BasicProperties`][pika.spec.BasicProperties], and *body* is `bytes`.
        :raises TypeError: if *callback* is not callable.
        :raises Exception: if the connection is already closed.
        """
        validators.require_callback(callback, 'callback')

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

    def confirm_delivery(
        self,
        ack_nack_callback,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Confirm.SelectOk]:
        """
        Enable publisher confirms and block until Confirm.SelectOk arrives.

        Idempotent: calling this method more than once on the same
        channel - concurrently from several threads included - returns
        the original Confirm.SelectOk without sending a frame to the
        broker or resetting the delivery-tag counter.

        Concurrent callers serialize on a per-channel lock, and
        *timeout* bounds only the wait for the broker's response, not
        that queueing, so a second caller can block for the first
        caller's timeout on top of its own.

        The *ack_nack_callback* runs on the channel's worker thread, not
        the IOLoop thread, so it may call any `Channel` method.  It
        shares that thread and its bounded queue with delivery
        callbacks, so a listener that cannot keep up with the confirm
        rate blocks the IOLoop once the queue fills; see
        *work_queue_maxsize*.

        It receives one Basic.Ack or Basic.Nack frame.  There is no
        separate nack callback, so ``isinstance`` is how the two are
        told apart::

            def on_confirm(method_frame):
                tag = method_frame.method.delivery_tag
                multiple = method_frame.method.multiple
                if isinstance(method_frame.method, pika.spec.Basic.Ack):
                    ...   # `tag`, and every earlier tag if `multiple`
                else:
                    ...   # Basic.Nack: the broker rejected it

            channel.confirm_delivery(on_confirm)

        Take the tag to match against from *basic_publish*'s
        ``on_publish``.  Reading [`next_publish_seq_no`][pika.adapters.thread_safe_connection.Channel.next_publish_seq_no] after a
        publish does not give that publish's tag, because the counter
        advances later on the IOLoop thread.

        An ack is not proof the message reached a queue: an unroutable
        publish sent with ``mandatory=True`` is answered with
        Basic.Return *and then* Basic.Ack.  Register
        [`add_on_return_callback`][pika.adapters.thread_safe_connection.Channel.add_on_return_callback] as well when routing matters.

        Safe to call from any thread.

        A `TimeoutError` does not mean confirms are off.  The
        Confirm.Select frame may already have reached the broker, which
        then numbers publishes from 1, so the delivery-tag counter can
        be armed and a subsequent publish still fire its ``on_publish``.
        It may equally never have been written, since the send is
        scheduled on the IOLoop thread rather than performed inline, and
        a backed-up IOLoop can time this out before it runs.  Retry
        either way rather than assuming confirms were left disabled.

        ``examples/publisher_confirms_threaded.py`` shows the tag
        bookkeeping.

        :param ack_nack_callback:
            ``callback(method_frame)`` called for each Basic.Ack or
            Basic.Nack received from the broker.
        :param timeout: Seconds to wait for the response.
            Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT] (10 s).
            Pass ``None`` to wait indefinitely.
        :returns: The Confirm.SelectOk method frame.
        :raises TypeError: if *ack_nack_callback* is not callable.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        validators.require_callback(ack_nack_callback, 'ack_nack_callback')

        with self._confirm_lock:
            if self._confirm_select_ok is not None:
                return self._confirm_select_ok

            def _wrapped_ack_nack(method_frame) -> None:
                _submit_or_terminate(
                    self._consumer_work_pool, self._wrapper._connection,
                    'Publisher confirm dropped: work pool shut down',
                    self._safe_dispatch, 'publisher confirm callback',
                    ack_nack_callback, method_frame)

            def _arm_seq_no() -> None:
                # Runs on the IOLoop thread the moment Confirm.Select is
                # written, so it is ordered ahead of every publish the IOLoop
                # has queued behind it.  Arming from the calling thread once the
                # RPC returns instead would miss each publish issued during the
                # round trip: the broker numbers those from 1, leaving the
                # counter permanently behind the broker's delivery tags.
                #
                # Arm only once.  A confirm_delivery that timed out may still
                # have reached the broker, and the retry's second Confirm.Select
                # does not reset the broker's counter, so neither may this.
                if self._next_publish_seq_no is None:
                    self._next_publish_seq_no = 0

            def send(
                callback: Callable[[frame.Method[spec.Confirm.SelectOk]], None]
            ) -> None:
                self._channel.confirm_delivery(
                    ack_nack_callback=_wrapped_ack_nack, callback=callback)

            result = self._blocking_rpc('confirm_delivery',
                                        send,
                                        timeout,
                                        on_sent=_arm_seq_no)
            self._confirm_select_ok = result
            return result

    def basic_consume(self,
                      queue,
                      on_message_callback,
                      auto_ack: bool = False,
                      exclusive: bool = False,
                      consumer_tag=None,
                      arguments=None,
                      timeout: float | None = DEFAULT_RPC_TIMEOUT) -> str:
        """
        Register a consumer and block until Basic.ConsumeOk arrives.

        The *on_message_callback* is dispatched on the channel's worker thread, not the IOLoop
        thread.  All [`Channel`][pika.adapters.thread_safe_connection.Channel] methods are safe to call from within the callback.

        Because the channel uses a single worker thread, deliveries are processed serially.  A
        callback that blocks (e.g. on a database write or a call to [`queue_declare`][pika.adapters.thread_safe_connection.Channel.queue_declare]) delays
        subsequent deliveries on the same channel until it returns.

        Safe to call from any thread.

        :param queue: Queue to consume from.
        :param on_message_callback:``callback(channel, method, properties, body)``
        :param auto_ack: Disable manual acknowledgement.
        :param exclusive: Request exclusive consumer access.
        :param consumer_tag: Client-provided tag; generated if omitted.
        :param arguments: Additional AMQP arguments.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The consumer tag assigned by the broker.
        :raises TypeError: if *on_message_callback* is not callable.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """
        validators.require_callback(on_message_callback, 'on_message_callback')

        def _wrapped_callback(ch, method, properties, body) -> None:
            _submit_or_terminate(
                self._consumer_work_pool, self._wrapper._connection,
                'Consumer delivery dropped: work pool shut down',
                self._safe_dispatch, 'consumer callback', on_message_callback,
                self, method, properties, body)

        def send(
            callback: Callable[[frame.Method[spec.Basic.ConsumeOk]],
                               None]) -> None:
            self._channel.basic_consume(queue=queue,
                                        on_message_callback=_wrapped_callback,
                                        auto_ack=auto_ack,
                                        exclusive=exclusive,
                                        consumer_tag=consumer_tag,
                                        arguments=arguments,
                                        callback=callback)

        consume_ok = self._blocking_rpc('basic_consume', send, timeout)
        tag = consume_ok.method.consumer_tag
        # The broker echoes the client's tag, which is always a str.
        assert isinstance(tag, str)
        return tag

    def basic_cancel(
        self,
        consumer_tag,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Basic.CancelOk]:
        """
        Cancel a consumer and block until Basic.CancelOk arrives.

        Safe to call from any thread.

        :param consumer_tag: Tag returned by [`basic_consume`][pika.adapters.thread_safe_connection.Channel.basic_consume].
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Basic.CancelOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Basic.CancelOk]],
                               None]) -> None:
            self._channel.basic_cancel(consumer_tag=consumer_tag,
                                       callback=callback)

        return self._blocking_rpc('basic_cancel', send, timeout)

    def queue_declare(
        self,
        queue,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments=None,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Queue.DeclareOk]:
        """
        Declare a queue and block the calling thread until Queue.DeclareOk arrives.

        Safe to call from any thread.

        :param queue: The queue name. If empty, the broker will generate a unique name.
        :param passive: If True, only check whether the queue or exchange exists
        :param durable: If True, the queue survives broker restart
        :param exclusive: If True, restrict access to the current connection
        :param auto_delete: If True, delete the queue or exchange when no longer in use
        :param arguments: Custom arguments for the queue declaration
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.DeclareOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Queue.DeclareOk]],
                               None]) -> None:
            self._channel.queue_declare(queue=queue,
                                        passive=passive,
                                        durable=durable,
                                        exclusive=exclusive,
                                        auto_delete=auto_delete,
                                        arguments=arguments,
                                        callback=callback)

        return self._blocking_rpc('queue_declare', send, timeout)

    def exchange_declare(
        self,
        exchange,
        exchange_type: str = 'direct',
        passive: bool = False,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        arguments=None,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Exchange.DeclareOk]:
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
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.DeclareOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Exchange.DeclareOk]], None]
        ) -> None:
            self._channel.exchange_declare(exchange=exchange,
                                           exchange_type=exchange_type,
                                           passive=passive,
                                           durable=durable,
                                           auto_delete=auto_delete,
                                           internal=internal,
                                           arguments=arguments,
                                           callback=callback)

        return self._blocking_rpc('exchange_declare', send, timeout)

    def queue_bind(
        self,
        queue,
        exchange,
        routing_key=None,
        arguments=None,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Queue.BindOk]:
        """
        Bind a queue to an exchange and block until Queue.BindOk arrives.

        Safe to call from any thread.

        :param queue: The queue to bind.
        :param exchange: The exchange to bind to.
        :param routing_key: The routing key to bind on. Defaults to the queue name.
        :param arguments: Custom arguments for the binding.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.BindOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Queue.BindOk]],
                               None]) -> None:
            self._channel.queue_bind(queue=queue,
                                     exchange=exchange,
                                     routing_key=routing_key,
                                     arguments=arguments,
                                     callback=callback)

        return self._blocking_rpc('queue_bind', send, timeout)

    def queue_unbind(
        self,
        queue,
        exchange=None,
        routing_key=None,
        arguments=None,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Queue.UnbindOk]:
        """
        Unbind a queue from an exchange and block until Queue.UnbindOk arrives.

        Safe to call from any thread.

        :param queue: The queue to unbind.
        :param exchange: The exchange to unbind from.
        :param routing_key: The routing key to unbind. Defaults to the queue name.
        :param arguments: Custom arguments for the unbinding.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.UnbindOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Queue.UnbindOk]],
                               None]) -> None:
            self._channel.queue_unbind(queue=queue,
                                       exchange=exchange,
                                       routing_key=routing_key,
                                       arguments=arguments,
                                       callback=callback)

        return self._blocking_rpc('queue_unbind', send, timeout)

    def queue_delete(
        self,
        queue,
        if_unused: bool = False,
        if_empty: bool = False,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Queue.DeleteOk]:
        """
        Delete a queue and block until Queue.DeleteOk arrives.

        Safe to call from any thread.

        :param queue: The queue to delete.
        :param if_unused: Only delete if the queue has no consumers.
        :param if_empty: Only delete if the queue is empty.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.DeleteOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Queue.DeleteOk]],
                               None]) -> None:
            self._channel.queue_delete(queue=queue,
                                       if_unused=if_unused,
                                       if_empty=if_empty,
                                       callback=callback)

        return self._blocking_rpc('queue_delete', send, timeout)

    def queue_purge(
        self,
        queue,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Queue.PurgeOk]:
        """
        Purge all messages from a queue and block until Queue.PurgeOk arrives.

        Safe to call from any thread.

        :param queue: The queue to purge.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Queue.PurgeOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Queue.PurgeOk]],
                               None]) -> None:
            self._channel.queue_purge(queue=queue, callback=callback)

        return self._blocking_rpc('queue_purge', send, timeout)

    def exchange_bind(
        self,
        destination,
        source,
        routing_key: str = '',
        arguments=None,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Exchange.BindOk]:
        """
        Bind an exchange to another exchange and block until Exchange.BindOk.

        Safe to call from any thread.

        :param destination: The destination exchange to bind.
        :param source: The source exchange to bind to.
        :param routing_key: The routing key to bind on.
        :param arguments: Custom arguments for the binding.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.BindOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Exchange.BindOk]],
                               None]) -> None:
            self._channel.exchange_bind(destination=destination,
                                        source=source,
                                        routing_key=routing_key,
                                        arguments=arguments,
                                        callback=callback)

        return self._blocking_rpc('exchange_bind', send, timeout)

    def exchange_unbind(
        self,
        destination,
        source,
        routing_key: str = '',
        arguments=None,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Exchange.UnbindOk]:
        """
        Unbind an exchange from another exchange and block until Exchange.UnbindOk.

        Safe to call from any thread.

        :param destination: The destination exchange to unbind.
        :param source: The source exchange to unbind from.
        :param routing_key: The routing key to unbind.
        :param arguments: Custom arguments for the unbinding.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.UnbindOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Exchange.UnbindOk]], None]
        ) -> None:
            self._channel.exchange_unbind(destination=destination,
                                          source=source,
                                          routing_key=routing_key,
                                          arguments=arguments,
                                          callback=callback)

        return self._blocking_rpc('exchange_unbind', send, timeout)

    def exchange_delete(
        self,
        exchange=None,
        if_unused: bool = False,
        timeout: float | None = DEFAULT_RPC_TIMEOUT
    ) -> frame.Method[spec.Exchange.DeleteOk]:
        """
        Delete an exchange and block until Exchange.DeleteOk arrives.

        Safe to call from any thread.

        :param exchange: The exchange name.
        :param if_unused: Only delete if the exchange has no bindings.
        :param timeout: Seconds to wait for the response. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :returns: The Exchange.DeleteOk method frame.
        :raises Exception: if the connection is closed before the response arrives.
        :raises TimeoutError: if *timeout* expires before the response arrives.
        """

        def send(
            callback: Callable[[frame.Method[spec.Exchange.DeleteOk]], None]
        ) -> None:
            self._channel.exchange_delete(exchange=exchange,
                                          if_unused=if_unused,
                                          callback=callback)

        return self._blocking_rpc('exchange_delete', send, timeout)

    def close(self,
              reply_code: int = 0,
              reply_text: str = 'Normal shutdown',
              timeout: float | None = 10) -> None:
        """
        Close the channel and block until the Channel.CloseOk arrives.

        Cancels this channel's consumers and waits for each ``Basic.CancelOk``, runs the callbacks
        already queued, and only then closes, so the acknowledgements those callbacks issue still
        reach the broker (#1763).  Cancelling the consumers is an observable effect of closing a
        channel that is still consuming.

        If the channel is already closed or closing, returns immediately. Safe to call from any
        thread.

        *timeout* bounds the whole call, shared across the cancellations, the queued callbacks, the
        wait for Channel.CloseOk and the pool shutdown, so a slow or wedged delivery callback cannot
        stall ``close`` past *timeout* plus one second.  That second is a floor on the wait for
        Channel.CloseOk: the pool shutdown that follows makes the pool reject at ``submit``, and a
        delivery rejected there is discarded, so the channel has to be closed first.  No phase is
        given a reserved share of it: a cancellation's reply and a drain both take as long as the
        worker needs to consume the backlog, so a share caps them below the work they are waiting
        on.  A worker still running when the budget is exhausted is left to finish on its own (it is
        a daemon thread), and what it has not acknowledged the broker redelivers.

        :param reply_code: Close reason code to send to the broker.
        :param reply_text: Close reason text to send to the broker.
        :param timeout: Seconds to wait for Channel.CloseOk before treating the channel as closed
            regardless. Defaults to 10 seconds. Pass ``None`` to wait indefinitely.
        :raises Exception: if the connection is closed or the channel is closed by the broker rather
            than by this client.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        # Registered before the quiesce below, so a connection that is already
        # gone raises here rather than after a pointless cancel-and-drain, and
        # so a close arriving mid-quiesce is reported by this call.
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

        def _remaining() -> float | None:
            if deadline is None:
                return None
            return max(0.0, deadline - time.monotonic())

        try:
            # Cancel consumers and run the queued callbacks before closing, so
            # the acknowledgements they issue still have a live channel (#1763).
            self._quiesce(deadline)
            self._wrapper._schedule_unchecked(_close)
            # The phases share the caller's budget, except that the close itself
            # gets a floor: shutting the pool down below makes it reject at
            # submit, and a delivery rejected there is discarded, so the channel
            # has to be closed first.  A quiesce that spent the whole budget
            # would otherwise leave this zero and drop whatever the broker was
            # still dispatching (#1763).  The floor is one round trip, which
            # does not depend on the backlog.
            if deadline is None:
                close_wait: float | None = None
            else:
                close_wait = max(_CLOSE_HANDSHAKE_FLOOR,
                                 deadline - time.monotonic())
            if not ready.wait(timeout=close_wait):
                LOGGER.warning('Channel %s close timed out after %s seconds',
                               self._channel.channel_number, timeout)
            if ready.is_set():
                # The quiesce ran the backlog; this picks up anything that
                # arrived while the channel was closing.
                self._shutdown_pool(timeout=_remaining())
            else:
                # The channel is not closed, so the broker may still be
                # dispatching.  Shutting the pool down here would make submit
                # reject those deliveries, and a rejected one is discarded, so
                # leave it to the connection's own sweep once the IOLoop has
                # stopped and nothing more can arrive (#1763).
                LOGGER.warning(
                    'Channel %s did not close within the budget; its consumer '
                    'work pool is left for the connection to shut down',
                    self._channel.channel_number)
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

        Equivalent to [`close`][pika.adapters.thread_safe_connection.Channel.close] but never raises.  Useful in error-recovery paths where the
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

        Returns ``None`` if publisher confirms have not been enabled via [`confirm_delivery`][pika.adapters.thread_safe_connection.Channel.confirm_delivery].
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
    def channel_number(self) -> int:
        return self._channel.channel_number

    @property
    def is_open(self) -> bool:
        """
        Whether this channel can still be used.

        False once the connection has recorded a close reason, because a channel outlives neither
        its connection nor the IOLoop thread that serves it. See
        [`Connection.is_open`][pika.adapters.thread_safe_connection.Connection.is_open].

        :returns: True while the channel is usable.
        """
        return self._wrapper._closed_reason is None and self._channel.is_open

    @property
    def is_closed(self) -> bool:
        """
        Whether this channel is finished.

        True once this channel closes or the connection records a close reason.

        :returns: True when the channel can no longer be used.
        """
        return (self._wrapper._closed_reason is not None or
                self._channel.is_closed)


class Connection:
    """Pika connection that is safe to use from multiple threads.

    .. note:: Each instance starts a background thread named
        ``pika-ioloop-N`` (where *N* is a per-process sequence number)
        so that multiple connections are distinguishable in stack traces
        and thread listings.

    Internally wraps [`SelectConnection`][pika.adapters.select_connection.SelectConnection] and runs its
    IOLoop in a single dedicated background thread (the *IOLoop thread*).
    All channel operations submitted from external threads are routed
    through [`add_callback_threadsafe`][pika.adapters.thread_safe_connection.Connection.add_callback_threadsafe] so that ``_tx_buffers`` has
    a single owner, the IOLoop thread.

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
        to be established.  Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT] (10 s).
        Pass ``None`` to wait indefinitely (relying on the socket timeout
        in *parameters*).  On timeout the IOLoop is stopped and
        `TimeoutError` is raised.
    :param work_queue_maxsize: Maximum number of user-callback dispatches
        (deliveries, publisher confirms, returned messages, cancel and
        blocked/unblocked notifications) that may be pending on each
        worker's queue.  Defaults to [`DEFAULT_WORK_QUEUE_MAXSIZE`][pika.adapters.thread_safe_connection.DEFAULT_WORK_QUEUE_MAXSIZE]
        (1000, matching the RabbitMQ Java client).  While a queue is full
        the IOLoop blocks, applying back-pressure to the broker via TCP.
        Heartbeats are not sent while the IOLoop is blocked, so the
        broker's heartbeat timeout bounds how long a wedged consumer can
        stall the connection.  Pass ``0`` for an unbounded queue.
    :param work_queue_put_timeout: Seconds to wait for space on a full
        work queue.  Defaults to [`DEFAULT_WORK_QUEUE_PUT_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_WORK_QUEUE_PUT_TIMEOUT]
        (30 s); must be a positive number, and ``None`` (infinite) is
        rejected because it lets a full queue stall or deadlock the IOLoop
        thread.  Set an override comfortably below the negotiated
        heartbeat-death window (twice the heartbeat interval) so a stall
        surfaces here rather than as an opaque broker-side close.  On
        timeout [`pika.exceptions.WorkQueueFullError`][pika.exceptions.WorkQueueFullError] is raised on
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
            work_queue_put_timeout: float = DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
            shutdown_drain_timeout: float = DEFAULT_SHUTDOWN_DRAIN_TIMEOUT
    ) -> None:
        self._user_on_open_error_callback = on_open_error_callback
        self._user_on_close_callback = on_close_callback
        self._work_queue_maxsize = work_queue_maxsize
        self._work_queue_put_timeout = _validate_put_timeout(
            work_queue_put_timeout)
        self._shutdown_drain_timeout = _validate_shutdown_drain_timeout(
            shutdown_drain_timeout)

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
        # (Connection.Blocked / Unblocked).  Bounded like the per-channel
        # pools, so a listener that cannot keep pace blocks the IOLoop.
        self._connection_work_pool = _BoundedWorkPool(
            maxsize=work_queue_maxsize,
            put_timeout=work_queue_put_timeout,
            thread_name=f'pika-conn-{self._instance_id}',
            owner=self,
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
            self._drain_pools_after_ioloop()

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

    def _drain_pools_after_ioloop(self) -> None:
        """
        Run the queued pool work once the IOLoop has exited, under this connection's budget.

        Waiting here is safe because the loop has returned, but it is still bounded: a wedged
        callback would otherwise hold the IOLoop thread forever and no caller asked for that
        (#1761).  A worker still running when the budget expires is left to finish on its daemon
        thread, with a warning from the pool.
        """
        self._shutdown_all_consumer_pools(timeout=self._shutdown_drain_timeout)
        self._shutdown_connection_pool(timeout=self._shutdown_drain_timeout)

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

    def _quiesce_all_pools(self, deadline: float | None) -> None:
        """
        Cancel every consumer and run every pool's queued callbacks, with the connection open.

        The counterpart to `_shutdown_all_consumer_pools`, which runs after the IOLoop has
        stopped and so cannot carry an acknowledgement.  Running the backlog here instead is what
        keeps a close from repeating work the broker will redeliver, or discarding the only copy of
        an ``auto_ack`` delivery: #1763 measures both.

        Every channel is cancelled before any is drained, so no channel is still receiving while
        another drains; draining one at a time would let a later channel's queue grow throughout.

        One deadline covers the whole sweep, which does mean a channel whose cancel goes unanswered
        can spend it and leave later channels nothing: their consumers are not cancelled and their
        queued callbacks do not run.  Reserving a share per channel is worse, because a cancel's
        reply legitimately takes as long as the worker needs to consume that channel's backlog, so
        a share caps it below the work it waits on and abandons live cancels (#1763).

        :param deadline: `time.monotonic` value the whole sweep must finish by, or ``None`` to
            wait indefinitely.
        """
        with self._channel_waiters_lock:
            channels = list(self._channels)
        for ch in channels:
            ch._cancel_consumers(deadline)
        for ch in channels:
            ch._drain_pool(deadline)
        remaining = (None if deadline is None else max(
            0.0, deadline - time.monotonic()))
        if not self._connection_work_pool.drain(timeout=remaining):
            LOGGER.warning(
                'Connection %s event work pool did not finish its queued '
                'callbacks before the close', self._instance_id)

    def _is_pool_worker_thread(self) -> bool:
        """
        True if the calling thread is one of this connection's pool worker threads.

        Covers every channel's consumer worker plus the connection's own event-callback worker
        ([`_connection_work_pool`][pika.adapters.thread_safe_connection.Connection._connection_work_pool]).  [`close`][pika.adapters.thread_safe_connection.Channel.close] uses it to avoid joining
        [`_ioloop_thread`][pika.adapters.thread_safe_connection.Connection._ioloop_thread] from inside a delivery or event callback, which would deadlock
        against the IOLoop thread's own cleanup tail joining that same worker (issue #1686).

        The answer is a stamp each pool puts on its worker thread, not a search of any collection: a
        thread is one of our workers for as long as it runs, and a registry that gets pruned cannot
        express that.  See `docs/internal/thread-safe-connection.md` for the window this closes and
        the design it replaced (#1761).
        """
        owner = getattr(threading.current_thread(), '_pika_pool_owner', None)
        return owner is not None and owner() is self

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
        Open a new channel and return a [`Channel`][pika.adapters.thread_safe_connection.Channel].

        Blocks the calling thread until the channel is open.  The returned channel's methods are
        safe to call from any thread.

        :param timeout: Seconds to wait for Channel.OpenOk. Defaults to [`DEFAULT_RPC_TIMEOUT`][pika.adapters.thread_safe_connection.DEFAULT_RPC_TIMEOUT]
            (10 s). Pass ``None`` to wait indefinitely.
        :raises Exception: if the connection is closed before the channel opens.
        :raises TimeoutError: if *timeout* expires before the channel opens.
        """
        ready = threading.Event()
        result: list[pika.channel.Channel] = []
        error: list[BaseException | None] = [None]

        with self._channel_waiters_lock:
            reason = self._closed_reason
            if reason is not None:
                raise _with_close_traceback(reason, self._closed_reason_tb)
            self._blocking_waiters.append((ready, error))

        def _open() -> None:

            def _on_open(ch: pika.channel.Channel) -> None:
                result.append(ch)
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

        Cancels every channel's consumers and waits for each ``Basic.CancelOk``, runs the callbacks
        already queued on every pool, and only then schedules the close, so the acknowledgements
        those callbacks issue still reach the broker (#1763).  Cancelling the consumers is an
        observable effect of closing a connection that is still consuming.  That phase is skipped
        when ``close()`` is called from a pool worker or from the IOLoop thread, as described below,
        and on those paths the queued callbacks run after the teardown as they did before.

        It then waits up to the remainder of *timeout* for the IOLoop thread to finish.  If the
        thread is still alive after the timeout (e.g. the broker never sends Connection.CloseOk),
        the IOLoop is force-stopped via ``add_callback_threadsafe`` and joined once more under a
        second *timeout* budget.  A worker still draining when that budget expires is left to finish
        on its own (it is a daemon thread), so a wedged delivery callback cannot hang ``close``
        indefinitely.

        Safe to call from any thread including the IOLoop thread itself (e.g. from within a channel
        callback).  When called from the IOLoop thread the close is initiated synchronously and the
        method returns immediately without joining.

        Also safe to call from a delivery or connection-event callback running on one of this
        connection's pool worker threads (e.g. a consumer's ``on_message_callback``).  The IOLoop
        thread's own post-close cleanup joins every pool worker, including whichever one is running
        the callback that called ``close()``, so joining [`_ioloop_thread`][pika.adapters.thread_safe_connection.Connection._ioloop_thread] from there would
        deadlock the two threads against each other. In that case the close is scheduled and this
        method returns immediately without joining; the worker exits normally once the callback
        returns.  A finite *timeout* schedules an IOLoop timer to stop an unresponsive connection
        and wake blocked callers.

        Calling ``close()`` on an already-closed connection is a no-op.

        :param timeout: Seconds for a clean close before force-stopping the IOLoop, shared across
            the cancellations, the queued callbacks and the join. Defaults to 10 seconds, which is
            sufficient for a healthy broker on any reasonable network. The pathological force path
            may wait up to twice this plus one second (once for the clean phases and join, once for
            the forced join, and a floor on the close handshake so a drain that spent the budget
            cannot leave it nothing). Pass ``None`` to wait indefinitely. On a pool worker thread it
            is enforced asynchronously without blocking the worker, and the queued callbacks are not
            run first, since a worker cannot wait for its own queue.
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

        is_pool_worker = self._is_pool_worker_thread()
        close_deadline = (None if timeout is None else time.monotonic() +
                          timeout)
        # Separate from close_deadline: this one arms the IOLoop timer that is
        # the only bound on a pool worker's close (#1706), where close_deadline
        # bounds the phases this thread waits through itself. Merging the two
        # would arm the timer on every close and stop the IOLoop ahead of
        # _CLOSE_HANDSHAKE_FLOOR.
        worker_force_deadline = close_deadline if is_pool_worker else None

        def _force_close_on_timeout() -> None:
            if self._record_forced_close():
                LOGGER.warning(
                    'Connection %s broker did not respond to close within '
                    '%s seconds; force-stopping IOLoop', self._instance_id,
                    timeout)
                self._connection.ioloop.stop()

        if not is_pool_worker:
            # Run every pool's queued callbacks before the connection goes
            # away, so their acknowledgements still reach the broker (#1763).
            # Skipped on a pool worker: a worker cannot wait for its own queue.
            self._quiesce_all_pools(close_deadline)

        def _safe_close() -> None:
            try:
                self._connection.close()
            except Exception:
                # Already closing or closed; _on_connection_closed wakes waiters.
                LOGGER.debug(
                    'connection.close() raised (already closing or closed)',
                    exc_info=True)
            if worker_force_deadline is not None:
                self._connection.ioloop.call_later(
                    max(0.0, worker_force_deadline - time.monotonic()),
                    _force_close_on_timeout)

        self._connection.ioloop.add_callback_threadsafe(_safe_close)

        if is_pool_worker:
            # The IOLoop thread's cleanup tail joins this worker, so joining
            # that thread from here would deadlock both (#1686). The close is
            # already scheduled. What this forfeits is on
            # docs/internal/thread-safe-connection.md.
            return

        # Whether the queued callbacks left the handshake any time at all. If
        # not, the force path below is reached without the broker ever having
        # been asked, which the reason it records has to distinguish.
        budget_spent_before_handshake = (close_deadline is not None and
                                         close_deadline <= time.monotonic())
        self._ioloop_thread.join(
            timeout=None if close_deadline is
            None else max(_CLOSE_HANDSHAKE_FLOOR, close_deadline -
                          time.monotonic()))
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
            # The reason distinguishes the broker failing to answer from the
            # budget being spent before it was asked, which is reachable when
            # the queued callbacks take the whole of it.
            self._record_forced_close(
                'the close timeout expired while running queued callbacks, so '
                'the broker was never asked'
                if budget_spent_before_handshake else None)
            # Normally the IOLoop tail already claimed these pools, making the
            # calls no-ops; bound them in case this thread reaches them first.
            self._shutdown_all_consumer_pools(timeout=_remaining())
            self._shutdown_connection_pool(timeout=_remaining())

    def _record_forced_close(self, reason: str | None = None) -> bool:
        """
        Record a forced close, wake blocked callers and report whether it was recorded.

        :param reason: What to report instead of the broker failing to answer.
        """
        with self._channel_waiters_lock:
            if self._closed_reason is not None:
                return False
            self._record_closed_reason(
                Exception('connection force-closed: ' +
                          (reason or 'broker did not respond to close')))
            for event, error in self._blocking_waiters:
                if error[0] is None:
                    error[0] = self._closed_reason
                event.set()
            self._blocking_waiters.clear()
            return True

    def abort(self, timeout: float | None = 10) -> None:
        """
        Close the connection, swallowing any errors.

        Equivalent to [`close`][pika.adapters.thread_safe_connection.Channel.close] but never raises.  Currently [`close`][pika.adapters.thread_safe_connection.Channel.close] itself does not raise
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
        once closed, or a [`ConnectionWrongStateError`][pika.exceptions.ConnectionWrongStateError] when no reason was
        recorded yet.

        Calls from the IOLoop thread itself are exempt and never raise, mirroring [`close`][pika.adapters.thread_safe_connection.Channel.close].
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
        instance does not grow it (see `_with_close_traceback`).  Only when the connection is
        closed but no reason was recorded yet does it raise a fresh
        [`ConnectionWrongStateError`][pika.exceptions.ConnectionWrongStateError].

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
        [`Channel`][pika.adapters.thread_safe_connection.Channel] method and heartbeats keep flowing while it
        works.  The RabbitMQ Java and .NET clients run this listener
        inline on the I/O thread.  That pool is bounded like the
        per-channel ones, so a listener that cannot keep pace still
        blocks the IOLoop once its queue fills; see
        *work_queue_maxsize*.

        Safe to call from any thread.

        :param callback:
            ``callback(connection, method_frame)`` where *connection* is
            this [`Connection`][pika.adapters.thread_safe_connection.Connection] and *method_frame* contains
            a [`pika.spec.Connection.Blocked`][pika.spec.Connection.Blocked].
        :raises Exception: if the connection is already closed.
        """
        self._register_connection_event_callback(
            'add_on_connection_blocked_callback', callback)

    def add_on_connection_unblocked_callback(self, callback) -> None:
        """
        Register a callback for ``Connection.Unblocked`` notifications.

        Sent by RabbitMQ once a previously blocked connection is no
        longer resource-constrained, letting publishers resume.

        Dispatched on the connection-level worker thread, with the same
        back-pressure as ``add_on_connection_blocked_callback``.

        Safe to call from any thread.

        :param callback:
            ``callback(connection, method_frame)`` where *connection* is
            this [`Connection`][pika.adapters.thread_safe_connection.Connection] and *method_frame* contains
            a [`pika.spec.Connection.Unblocked`][pika.spec.Connection.Unblocked].
        :raises Exception: if the connection is already closed.
        """
        self._register_connection_event_callback(
            'add_on_connection_unblocked_callback', callback)

    def _register_connection_event_callback(self, raw_method_name: str,
                                            callback) -> None:
        """
        Register a connection-level event callback via the IOLoop.

        Wraps the user callback so it dispatches on the connection work pool, then schedules
        registration with the underlying [`pika.connection.Connection`][pika.connection.Connection] on the IOLoop thread.
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
        """
        Whether this connection can still be used.

        False once a close reason has been recorded, which includes an IOLoop thread that exited on
        an unhandled callback exception.  The underlying connection's own state does not move in
        that case, so asking it alone would report an open connection with no IOLoop behind it.

        :returns: True while the connection is usable.
        """
        # Read without `_channel_waiters_lock`: only the reason is needed here,
        # not the traceback it is paired with.  See #1743.
        return self._closed_reason is None and self._connection.is_open

    @property
    def is_closed(self) -> bool:
        """
        Whether this connection is finished.

        True once a close reason has been recorded, for the reason
        [`is_open`][pika.adapters.thread_safe_connection.Connection.is_open] gives.

        :returns: True when the connection can no longer be used.
        """
        return self._closed_reason is not None or self._connection.is_closed
