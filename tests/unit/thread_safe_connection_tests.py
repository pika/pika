"""Tests for pika.adapters.thread_safe_connection."""

import contextlib
import threading
import traceback
import unittest
from unittest.mock import ANY, MagicMock, patch

from pika import spec
from pika.adapters.thread_safe_connection import (
    DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
    Channel,
    Connection,
    _BoundedWorkPool,
    _reraisable,
    _submit_or_terminate,
    _with_close_traceback,
)
from pika.exceptions import (
    AMQPConnectionError,
    ConnectionClosedByClient,
    ConnectionWrongStateError,
    WorkQueueFullError,
)


class SubmitOrTerminateTests(unittest.TestCase):
    """Tests for the _submit_or_terminate submit-wrapper helper."""

    def test_submits_work_to_pool(self):
        """A normal submit must forward fn and args to the pool untouched."""
        pool = MagicMock()
        connection = MagicMock()
        connection._error = None
        fn = object()
        _submit_or_terminate(pool, connection, 'dropped', fn, 'a', 'b')
        pool.submit.assert_called_once_with(fn, 'a', 'b')
        connection._terminate_stream.assert_not_called()

    def test_overflow_terminates_connection_with_the_error(self):
        """A WorkQueueFullError must tear the connection down with that error."""
        exc = WorkQueueFullError('full')
        pool = MagicMock()
        pool.submit.side_effect = exc
        connection = MagicMock()
        connection._error = None
        _submit_or_terminate(pool, connection, 'dropped', lambda: None)
        connection._terminate_stream.assert_called_once_with(exc)

    def test_runtime_error_drops_work_without_terminating(self):
        """A RuntimeError (pool shut down) must drop the work, not terminate."""
        pool = MagicMock()
        pool.submit.side_effect = RuntimeError('pool shut down')
        connection = MagicMock()
        connection._error = None
        with patch('pika.adapters.thread_safe_connection.LOGGER') as logger:
            _submit_or_terminate(pool, connection, 'dropped msg', lambda: None)
        connection._terminate_stream.assert_not_called()
        logger.debug.assert_called_once_with('dropped msg')

    def test_skips_submit_when_teardown_already_underway(self):
        """When connection._error is set, the work must be dropped without a pool submit or a repeat
        teardown.
        """
        pool = MagicMock()
        connection = MagicMock()
        connection._error = WorkQueueFullError('already tearing down')
        with patch('pika.adapters.thread_safe_connection.LOGGER') as logger:
            _submit_or_terminate(pool, connection, 'dropped msg', lambda: None)
        pool.submit.assert_not_called()
        connection._terminate_stream.assert_not_called()
        logger.debug.assert_called_once_with('dropped msg')


def _tb_depth(exc):
    """
    Count the frames in *exc*'s traceback.

    :param exc: The exception to measure.
    :returns: Number of traceback frames.
    """
    depth, tb = 0, exc.__traceback__
    while tb is not None:
        depth += 1
        tb = tb.tb_next
    return depth


def _raised(exc):
    """
    Give *exc* a traceback by raising and catching it.

    :param exc: The exception to raise.
    :returns: *exc*, now carrying a traceback.
    """
    try:
        raise exc
    except BaseException as caught:
        return caught


@contextlib.contextmanager
def _expect_raised(test, exc_type):
    """
    Assert the block raises *exc_type*, leaving the exception's traceback intact.

    :meth:`unittest.TestCase.assertRaises` calls ``with_traceback(None)`` on what it catches, which
    erases exactly what the traceback-growth tests measure and makes them pass vacuously.

    :param test: The test case, used to fail when nothing is raised.
    :param exc_type: The exception type expected.
    :yields: A list that receives the caught exception.
    """
    caught = []
    try:
        yield caught
    except exc_type as exc:
        caught.append(exc)
    if not caught:
        test.fail(f'{exc_type.__name__} not raised')


class CloseTracebackHelperTests(unittest.TestCase):
    """Tests for the helpers that keep the shared close reason's traceback bounded."""

    def test_with_close_traceback_bounds_repeated_raises(self):
        """
        Re-raising through the helper must land at the same depth every time.

        A raise still appends its own frames, so the depth after one raise exceeds the snapshot;
        what matters is that the next raise starts from the snapshot again instead of from the grown
        traceback, which is what makes the depth stable rather than unbounded.
        """
        reason = _raised(Exception('closed'))
        close_tb = reason.__traceback__

        with _expect_raised(self, Exception):
            raise _with_close_traceback(reason, close_tb)
        after_first = _tb_depth(reason)

        for _ in range(50):
            with _expect_raised(self, Exception):
                raise _with_close_traceback(reason, close_tb)

        self.assertEqual(_tb_depth(reason), after_first)

    def test_with_close_traceback_preserves_the_recorded_frames(self):
        """The snapshot must keep the frames that show where the connection died."""

        def ioloop_crash_site():
            raise RuntimeError('ioloop died')

        try:
            ioloop_crash_site()
        except RuntimeError as exc:
            reason = exc
        close_tb = reason.__traceback__

        with _expect_raised(self, RuntimeError):
            raise _with_close_traceback(reason, close_tb)

        frames = [f.name for f in traceback.extract_tb(reason.__traceback__)]
        self.assertIn('ioloop_crash_site', frames)

    def test_reraisable_resets_only_the_shared_close_reason(self):
        """The close reason gets its snapshot back; a per-call exception is returned untouched."""
        reason = _raised(Exception('closed'))
        close_tb = reason.__traceback__

        self.assertIs(_reraisable(reason, reason, close_tb), reason)
        self.assertIs(reason.__traceback__, close_tb)

        own = _raised(ValueError('this call only'))
        own_tb = own.__traceback__
        self.assertIs(_reraisable(own, reason, close_tb), own)
        self.assertIs(own.__traceback__, own_tb)

    def test_reraisable_leaves_a_traceback_free_error_alone(self):
        """A per-call exception that never propagated must not gain the close-time frames."""
        reason = _raised(Exception('closed'))
        own = ValueError('never raised')

        self.assertIsNone(
            _reraisable(own, reason, reason.__traceback__).__traceback__)


class BoundedWorkPoolTests(unittest.TestCase):
    """Tests for the bounded work queue backing the dispatch pools."""

    def _make_blocked_pool(self,
                           maxsize,
                           put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT):
        """Return (pool, release) where the pool's worker is stuck in its first work item until
        *release* is set.
        """
        pool = _BoundedWorkPool(maxsize=maxsize,
                                put_timeout=put_timeout,
                                thread_name='test-pool')
        release = threading.Event()
        started = threading.Event()

        def block():
            started.set()
            release.wait(timeout=5)

        pool.submit(block)
        self.assertTrue(started.wait(timeout=5))
        return pool, release

    def test_submit_with_timeout_raises_when_queue_stays_full(self):
        """A submit() with put_timeout set must raise WorkQueueFullError when the queue stays
        full.
        """
        pool, release = self._make_blocked_pool(maxsize=1, put_timeout=0.05)
        try:
            pool.submit(lambda: None)  # fills the queue
            with self.assertRaises(WorkQueueFullError):
                pool.submit(lambda: None)
        finally:
            release.set()
            pool.shutdown(wait=True)

    def test_submit_blocks_until_queue_has_space(self):
        """A submit() on a full queue must block and proceed once the worker drains an item."""
        pool, release = self._make_blocked_pool(maxsize=1)
        pool.submit(lambda: None)  # fills the queue
        submitted = threading.Event()

        def blocked_submit():
            pool.submit(lambda: None)
            submitted.set()

        producer = threading.Thread(target=blocked_submit, daemon=True)
        producer.start()
        # The producer must be stuck in put() while the queue is full...
        self.assertFalse(submitted.wait(timeout=0.1))
        # ...and proceed once the worker drains an item.
        release.set()
        self.assertTrue(submitted.wait(timeout=5))
        producer.join(timeout=5)
        pool.shutdown(wait=True)

    def test_shutdown_drains_pending_work(self):
        """Pool shutdown must let already-enqueued work finish before returning."""
        pool, release = self._make_blocked_pool(maxsize=10)
        done = []
        for i in range(5):
            pool.submit(done.append, i)
        release.set()
        pool.shutdown(wait=True)
        self.assertEqual(done, list(range(5)))

    def test_shutdown_does_not_block_when_queue_full(self):
        """
        Shutdown() must not block when the queue is full and the worker is wedged.

        The worker holds its first item while a second fills the single queue slot, so no slot is
        free.  The shutdown flag is set unconditionally and the wakeup sentinel is enqueued with
        put_nowait, so a full queue drops the ping instead of blocking; shutdown must return
        promptly.
        """
        pool, release = self._make_blocked_pool(maxsize=1)
        pool.submit(lambda: None)  # fills the one queue slot

        returned = threading.Event()

        def do_shutdown():
            pool.shutdown(wait=False)
            returned.set()

        shutter = threading.Thread(target=do_shutdown, daemon=True)
        shutter.start()
        self.assertTrue(returned.wait(timeout=5),
                        'shutdown(wait=False) blocked on a full queue')
        shutter.join(timeout=5)

        # Releasing the worker lets it drain and exit cleanly.
        release.set()
        pool.shutdown(wait=True)

    def test_worker_idles_then_processes_later_work(self):
        """An idle worker blocked in get() must still process work submitted after it goes idle."""
        pool = _BoundedWorkPool(maxsize=10,
                                put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool')

        # Signal when the worker re-enters get() with an empty queue after the
        # first item has run: that is the worker going idle, detected without
        # a fixed sleep and without reaching into queue/condition internals.
        idle = threading.Event()
        first = threading.Event()
        real_get = pool._queue.get

        def signaling_get(*args, **kwargs):
            if first.is_set() and pool._queue.empty():
                idle.set()
            return real_get(*args, **kwargs)

        with patch.object(pool._queue, 'get', side_effect=signaling_get):
            pool.submit(first.set)
            self.assertTrue(first.wait(timeout=5))
            # The worker drains the first item, then blocks in get() on the
            # empty queue; wait for that idle state deterministically.
            self.assertTrue(idle.wait(timeout=5),
                            'worker never went idle in get()')

            # Handing it more work now proves the blocking get() wakes and
            # processes work submitted after the worker went idle.
            second = threading.Event()
            pool.submit(second.set)
            self.assertTrue(second.wait(timeout=5))
        pool.shutdown(wait=True)

    def test_shutdown_timeout_returns_false_when_worker_wedged(self):
        """Shutdown(timeout) must return False and not hang when the worker will not exit."""
        pool, release = self._make_blocked_pool(maxsize=1)
        try:
            # The worker is stuck in its first item, so it cannot observe the
            # shutdown flag until released.  A short timeout must expire.
            self.assertFalse(pool.shutdown(wait=True, timeout=0.1))
            self.assertTrue(pool._thread.is_alive())
        finally:
            release.set()
            self.assertTrue(pool.shutdown(wait=True, timeout=5))

    def test_shutdown_timeout_returns_true_when_worker_exits(self):
        """Shutdown(timeout) must return True once the worker drains and exits."""
        pool, release = self._make_blocked_pool(maxsize=1)
        release.set()
        self.assertTrue(pool.shutdown(wait=True, timeout=5))
        self.assertFalse(pool._thread.is_alive())

    def test_submit_after_shutdown_raises_runtime_error(self):
        """A submit() on a shut-down pool must raise RuntimeError."""
        pool = _BoundedWorkPool(maxsize=1,
                                put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool')
        pool.shutdown(wait=True)
        with self.assertRaises(RuntimeError):
            pool.submit(lambda: None)

    def test_submit_raises_when_shutdown_races_the_put(self):
        """
        A shutdown landing between the flag check and the put must reject the submit.

        Otherwise the item lands in a queue whose worker has already drained empty and exited,
        stranding the work with no exception to the caller. The put is made to flip the shutdown
        flag as a side effect, standing in for a concurrent shutdown() that ran in that exact
        window.
        """
        pool = _BoundedWorkPool(maxsize=10,
                                put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool')
        real_put = pool._queue.put

        def put_then_shutdown(*args, **kwargs):
            result = real_put(*args, **kwargs)
            pool._shutdown = True
            return result

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                patch.object(pool._queue, 'put', side_effect=put_then_shutdown))
            stack.enter_context(self.assertRaises(RuntimeError))
            pool.submit(lambda: None)

    def test_worker_thread_starts_lazily(self):
        """The worker thread must not start until the first submit."""
        pool = _BoundedWorkPool(maxsize=1,
                                put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool')
        self.assertIsNone(pool._thread)
        pool.submit(lambda: None)
        self.assertIsNotNone(pool._thread)
        pool.shutdown(wait=True)

    def test_shutdown_is_idempotent(self):
        """Calling shutdown() multiple times must not raise."""
        pool = _BoundedWorkPool(maxsize=1,
                                put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool')
        pool.submit(lambda: None)
        pool.shutdown(wait=True)
        pool.shutdown(wait=True)

    def test_shutdown_from_worker_thread_does_not_join_itself(self):
        """A work item that shuts its own pool down must not join the worker thread it runs on."""
        pool = _BoundedWorkPool(maxsize=1,
                                put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool')
        result: list[bool] = []

        def self_shutdown():
            # RuntimeError('cannot join current thread') if shutdown() tried to
            # join the worker from the worker itself.
            result.append(pool.shutdown(wait=True))

        pool.submit(self_shutdown)
        pool._thread.join(timeout=5)
        self.assertFalse(pool._thread.is_alive())
        # shutdown() returned True (worker exits on its own) rather than raising.
        self.assertEqual(result, [True])

    def test_connection_params_propagate_to_channel_pool(self):
        """Work-queue parameters passed to Channel must reach its pool."""
        raw_ch = MagicMock()
        wrapper = MagicMock()
        ch = Channel(raw_ch,
                     wrapper,
                     work_queue_maxsize=7,
                     work_queue_put_timeout=45.0)
        self.assertEqual(ch._consumer_work_pool._queue.maxsize, 7)
        self.assertEqual(ch._consumer_work_pool._put_timeout, 45.0)

    def test_channel_rejects_none_put_timeout(self):
        """Channel must reject a None put timeout (would block the IOLoop forever)."""
        with self.assertRaises(ValueError):
            Channel(MagicMock(), MagicMock(), work_queue_put_timeout=None)

    def test_channel_rejects_non_positive_put_timeout(self):
        """Channel must reject a zero, negative, or NaN put timeout."""
        for bad in (0, -1.0, float('nan')):
            with self.assertRaises(ValueError):
                Channel(MagicMock(), MagicMock(), work_queue_put_timeout=bad)

    def test_channel_accepts_small_positive_put_timeout(self):
        """A positive override below the default must be accepted (no floor)."""
        ch = Channel(MagicMock(), MagicMock(), work_queue_put_timeout=5.0)
        self.assertEqual(ch._consumer_work_pool._put_timeout, 5.0)

    def test_channel_accepts_default_put_timeout(self):
        """The default put timeout must satisfy its own validation."""
        ch = Channel(MagicMock(), MagicMock())
        self.assertEqual(ch._consumer_work_pool._put_timeout,
                         DEFAULT_WORK_QUEUE_PUT_TIMEOUT)

    def test_connection_rejects_invalid_put_timeout(self):
        """
        Connection must validate its put timeout before connecting.

        Validation happens ahead of SelectConnection construction, so a bad value raises without any
        connection machinery or patching.
        """
        for bad in (None, 0, -1.0, float('nan')):
            with self.assertRaises(ValueError):
                Connection(parameters='params', work_queue_put_timeout=bad)


class ChannelTests(unittest.TestCase):

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def test_basic_publish_routes_through_schedule_unchecked(self):
        ch, _raw_ch, wrapper = self._make_channel()
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'hello')
        wrapper._schedule_unchecked.assert_called_once()

    def test_basic_publish_does_not_call_raw_channel_directly(self):
        ch, raw_ch, _wrapper = self._make_channel()
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'hello')
        raw_ch.basic_publish.assert_not_called()

    def test_basic_publish_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'msg',
                         properties='props',
                         mandatory=True)
        # Extract and invoke the scheduled callback manually
        scheduled_cb = wrapper._schedule_unchecked.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_publish.assert_called_once_with(
            exchange='ex',
            routing_key='rk',
            body=b'msg',
            properties='props',
            mandatory=True,
        )

    def test_basic_publish_callback_swallows_channel_wrong_state_error(self):
        """ChannelWrongStateError inside the scheduled callback must not propagate to the IOLoop —
        that would crash the IOLoop thread.
        """
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_publish.side_effect = ChannelWrongStateError(
            'channel closed')
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')
        scheduled_cb = wrapper._schedule_unchecked.call_args[0][0]
        scheduled_cb()  # must not raise
        raw_ch.basic_publish.assert_called_once()

    def test_on_publish_not_called_when_confirms_not_enabled(self):
        """on_publish callback is ignored when confirm_delivery has not been called
        (_next_publish_seq_no is None).
        """
        ch, raw_ch, wrapper = self._make_channel()
        on_publish = MagicMock()
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'x',
                         on_publish=on_publish)
        scheduled_cb = wrapper._schedule_unchecked.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_publish.assert_called_once()
        on_publish.assert_not_called()

    def test_on_publish_called_with_delivery_tag_when_confirms_enabled(self):
        """After confirms are armed, on_publish receives monotonically increasing delivery tags
        starting at 1.
        """
        ch, _raw_ch, wrapper = self._make_channel()
        ch._next_publish_seq_no = 0  # simulate confirm_delivery success
        tags = []
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'a',
                         on_publish=lambda tag: tags.append(tag))
        wrapper._schedule_unchecked.call_args[0][0]()
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'b',
                         on_publish=lambda tag: tags.append(tag))
        wrapper._schedule_unchecked.call_args[0][0]()
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'c',
                         on_publish=lambda tag: tags.append(tag))
        wrapper._schedule_unchecked.call_args[0][0]()
        self.assertEqual(tags, [1, 2, 3])

    def test_on_publish_not_called_when_publish_raises(self):
        """If the raw basic_publish raises, the tag is not consumed and on_publish is not
        invoked.
        """
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        ch._next_publish_seq_no = 0
        raw_ch.basic_publish.side_effect = ChannelWrongStateError('closed')
        on_publish = MagicMock()
        ch.basic_publish(exchange='ex',
                         routing_key='rk',
                         body=b'x',
                         on_publish=on_publish)
        scheduled_cb = wrapper._schedule_unchecked.call_args[0][0]
        scheduled_cb()  # must not raise
        on_publish.assert_not_called()
        self.assertEqual(ch._next_publish_seq_no, 0)

    def test_seq_no_increments_without_on_publish_when_confirms_enabled(self):
        """
        The delivery tag counter advances on every successful publish when confirms are armed,
        regardless of whether on_publish is provided.

        This matches the RabbitMQ Java and .NET client behavior.
        """
        ch, _raw_ch, wrapper = self._make_channel()
        ch._next_publish_seq_no = 0
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')
        wrapper._schedule_unchecked.call_args[0][0]()
        self.assertEqual(ch._next_publish_seq_no, 1)

    def test_basic_ack_callback_swallows_channel_wrong_state_error(self):
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_ack.side_effect = ChannelWrongStateError('channel closed')
        ch.basic_ack(delivery_tag=1)
        wrapper._schedule_unchecked.call_args[0][0]()  # must not raise
        raw_ch.basic_ack.assert_called_once()

    def test_basic_nack_callback_swallows_channel_wrong_state_error(self):
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_nack.side_effect = ChannelWrongStateError('channel closed')
        ch.basic_nack(delivery_tag=1)
        wrapper._schedule_unchecked.call_args[0][0]()  # must not raise
        raw_ch.basic_nack.assert_called_once()

    def test_basic_reject_callback_swallows_channel_wrong_state_error(self):
        from pika.exceptions import ChannelWrongStateError
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.basic_reject.side_effect = ChannelWrongStateError(
            'channel closed')
        ch.basic_reject(delivery_tag=1)
        wrapper._schedule_unchecked.call_args[0][0]()  # must not raise
        raw_ch.basic_reject.assert_called_once()

    def test_basic_publish_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')

        self.assertIs(ctx.exception, reason)
        wrapper._schedule_unchecked.assert_not_called()

    def test_basic_publish_does_not_grow_close_reason_traceback(self):
        """
        Publishing into a closed connection must not accumulate frames on the shared close reason.

        The reason stays reachable from ``_closed_reason`` for the life of the connection, so each
        appended frame is permanent and pins that call's ``body``.
        """
        ch, _raw_ch, wrapper = self._make_channel()
        reason = _raised(Exception('closed'))
        wrapper._closed_reason = reason
        wrapper._closed_reason_tb = reason.__traceback__

        with _expect_raised(self, Exception):
            ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')
        after_first = _tb_depth(reason)

        for _ in range(50):
            with _expect_raised(self, Exception):
                ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')

        self.assertEqual(_tb_depth(reason), after_first)

    def test_blocking_method_does_not_grow_close_reason_traceback(self):
        """
        A waiter woken by the close reason must not append frames to it either.

        ``queue_declare`` gets the shared instance through ``error[0]``, so raising it directly
        would grow it once per blocked caller.  Only the first call takes that path; once the reason
        is recorded, later calls are rejected by the pre-registration guard instead, so the depth is
        sampled after both paths have run once.
        """
        ch, _raw_ch, wrapper = self._make_channel()
        reason = _raised(Exception('connection lost'))

        def close_while_waiting(_cb):
            with wrapper._channel_waiters_lock:
                if wrapper._closed_reason is None:
                    # _record_closed_reason snapshots the traceback once, at
                    # close time; re-snapshotting here would capture a grown
                    # one and mask the growth this test looks for.
                    wrapper._closed_reason = reason
                    wrapper._closed_reason_tb = reason.__traceback__
                for evt, err in wrapper._blocking_waiters:
                    err[0] = reason
                    evt.set()
                wrapper._blocking_waiters.clear()

        wrapper._schedule_unchecked.side_effect = close_while_waiting

        # First call: woken by the close reason through error[0].
        with _expect_raised(self, Exception) as caught:
            ch.queue_declare(queue='q')
        self.assertIs(caught[0], reason)
        # Second call: rejected up front, now that the reason is recorded.
        with _expect_raised(self, Exception):
            ch.queue_declare(queue='q')
        settled = _tb_depth(reason)

        for _ in range(50):
            with _expect_raised(self, Exception):
                ch.queue_declare(queue='q')

        self.assertEqual(_tb_depth(reason), settled)

    def test_basic_ack_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_ack(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper._schedule_unchecked.assert_not_called()

    def test_basic_nack_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_nack(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper._schedule_unchecked.assert_not_called()

    def test_basic_reject_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_reject(delivery_tag=1)

        self.assertIs(ctx.exception, reason)
        wrapper._schedule_unchecked.assert_not_called()

    def test_basic_ack_routes_through_schedule_unchecked(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_ack(delivery_tag=42, multiple=True)
        wrapper._schedule_unchecked.assert_called_once()
        raw_ch.basic_ack.assert_not_called()

    def test_basic_ack_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_ack(delivery_tag=42, multiple=True)
        scheduled_cb = wrapper._schedule_unchecked.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_ack.assert_called_once_with(delivery_tag=42, multiple=True)

    def test_basic_nack_routes_through_schedule_unchecked(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_nack(delivery_tag=7, multiple=False, requeue=False)
        wrapper._schedule_unchecked.assert_called_once()
        raw_ch.basic_nack.assert_not_called()

    def test_basic_nack_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_nack(delivery_tag=7, multiple=False, requeue=False)
        scheduled_cb = wrapper._schedule_unchecked.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_nack.assert_called_once_with(delivery_tag=7,
                                                  multiple=False,
                                                  requeue=False)

    def test_basic_reject_routes_through_schedule_unchecked(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_reject(delivery_tag=3, requeue=False)
        wrapper._schedule_unchecked.assert_called_once()
        raw_ch.basic_reject.assert_not_called()

    def test_basic_reject_callback_calls_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.basic_reject(delivery_tag=3, requeue=False)
        scheduled_cb = wrapper._schedule_unchecked.call_args[0][0]
        scheduled_cb()
        raw_ch.basic_reject.assert_called_once_with(delivery_tag=3,
                                                    requeue=False)

    def test_queue_declare_blocks_and_returns_result(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_queue_declare(queue, passive, durable, exclusive,
                                   auto_delete, arguments, callback):
                callback(mock_frame)

            raw_ch.queue_declare.side_effect = fake_queue_declare
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        result = ch.queue_declare(queue='my_queue', durable=True)

        self.assertIs(result, mock_frame)
        raw_ch.queue_declare.assert_called_once_with(
            queue='my_queue',
            passive=False,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments=None,
            callback=ANY,
        )

    def test_queue_declare_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('connection closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.queue_declare(queue='my_queue')

        self.assertIs(ctx.exception, reason)
        wrapper._schedule_unchecked.assert_not_called()

    def test_queue_declare_raises_when_connection_closes_while_waiting(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('connection lost')

        def close_while_waiting(cb):
            # Simulate the connection closing instead of Queue.DeclareOk arriving
            with wrapper._channel_waiters_lock:
                wrapper._closed_reason = reason
                for evt, err in wrapper._blocking_waiters:
                    err[0] = reason
                    evt.set()
                wrapper._blocking_waiters.clear()

        wrapper._schedule_unchecked.side_effect = close_while_waiting

        with self.assertRaises(Exception) as ctx:
            ch.queue_declare(queue='my_queue')

        self.assertIs(ctx.exception, reason)

    def test_basic_qos_blocks_and_returns_result(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_qos(prefetch_size, prefetch_count, global_qos, callback):
                callback(mock_frame)

            raw_ch.basic_qos.side_effect = fake_qos
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        result = ch.basic_qos(prefetch_count=10)

        self.assertIs(result, mock_frame)
        raw_ch.basic_qos.assert_called_once_with(
            prefetch_size=0,
            prefetch_count=10,
            global_qos=False,
            callback=ANY,
        )

    def test_basic_qos_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_qos()

        self.assertIs(ctx.exception, reason)

    def test_basic_consume_blocks_and_returns_consumer_tag(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        tag = ch.basic_consume(queue='q', on_message_callback=MagicMock())

        self.assertEqual(tag, 'ctag1')

    def test_basic_consume_wraps_callback_with_thread_safe_channel(self):
        """on_message_callback must receive the Channel, not the raw channel."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        received = []

        def my_callback(channel, method, properties, body):
            received.append(channel)

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', b'body')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)

        # Wait for the worker pool to drain so the assertion is deterministic.
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(len(received), 1)
        self.assertIsInstance(received[0], Channel)
        self.assertIs(received[0], ch)

    def test_basic_consume_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_consume(queue='q', on_message_callback=MagicMock())

        self.assertIs(ctx.exception, reason)

    def test_basic_cancel_blocks_and_returns_result(self):
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_cancel(consumer_tag, callback):
                callback(mock_frame)

            raw_ch.basic_cancel.side_effect = fake_cancel
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        result = ch.basic_cancel('ctag1')

        self.assertIs(result, mock_frame)
        raw_ch.basic_cancel.assert_called_once_with(
            consumer_tag='ctag1',
            callback=ANY,
        )

    def test_basic_cancel_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.basic_cancel('ctag1')

        self.assertIs(ctx.exception, reason)

    def test_channel_close_blocks_until_closed(self):
        from pika.exceptions import ChannelClosedByClient
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False

        def execute_and_fire(cb):
            close_callbacks = []
            raw_ch.add_on_close_callback.side_effect = close_callbacks.append
            raw_ch.close.side_effect = lambda reply_code, reply_text: close_callbacks[
                0](raw_ch, ChannelClosedByClient(reply_code, reply_text))
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.close()  # must not raise

        raw_ch.close.assert_called_once_with(reply_code=0,
                                             reply_text='Normal shutdown')

    def test_channel_close_raises_when_connection_already_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('connection closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.close()

        self.assertIs(ctx.exception, reason)

    def test_channel_close_is_noop_when_channel_already_closed(self):
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = True

        def execute_immediately(cb):
            cb()

        wrapper._schedule_unchecked.side_effect = execute_immediately

        ch.close()  # must not raise; channel is already closed

        raw_ch.close.assert_not_called()

    def test_channel_abort_swallows_close_errors(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('connection already closed')
        wrapper._closed_reason = reason

        # close() would raise; abort() must swallow.
        ch.abort()

    def test_channel_abort_calls_close(self):
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = True  # make close() a noop

        def execute_immediately(cb):
            cb()

        wrapper._schedule_unchecked.side_effect = execute_immediately

        ch.abort(reply_code=320, reply_text='shutting down', timeout=5)
        # Verify the close path ran (work pool was shut down via close())
        self.assertTrue(ch._pool_shutdown)

    def test_channel_close_honors_timeout_when_consumer_wedged(self):
        """Close() must return within its timeout even if a delivery callback is wedged."""
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = True  # make the close handshake an immediate no-op

        def execute_immediately(cb):
            cb()

        wrapper._schedule_unchecked.side_effect = execute_immediately

        # Wedge the consumer worker in a never-releasing callback so the pool
        # cannot drain and exit.
        release = threading.Event()
        started = threading.Event()

        def block():
            started.set()
            release.wait(timeout=5)

        try:
            ch._consumer_work_pool.submit(block)
            self.assertTrue(started.wait(timeout=5))

            done = threading.Event()

            def do_close():
                ch.close(timeout=0.1)
                done.set()

            closer = threading.Thread(target=do_close, daemon=True)
            closer.start()
            self.assertTrue(done.wait(timeout=5),
                            'close() hung on a wedged consumer worker')
            closer.join(timeout=5)
            self.assertTrue(ch._pool_shutdown)
        finally:
            release.set()
            ch._consumer_work_pool.shutdown(wait=True, timeout=5)

    def test_channel_close_with_none_timeout_drains_unbounded(self):
        """Close(timeout=None) must drain the pool with no bound (the None-budget branch)."""
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = True  # immediate no-op handshake

        def execute_immediately(cb):
            cb()

        wrapper._schedule_unchecked.side_effect = execute_immediately

        ch.close(timeout=None)
        self.assertTrue(ch._pool_shutdown)

    def test_properties_delegate_to_raw_channel(self):
        ch, raw_ch, _wrapper = self._make_channel()
        self.assertEqual(ch.channel_number, raw_ch.channel_number)
        self.assertEqual(ch.is_open, raw_ch.is_open)
        self.assertEqual(ch.is_closed, raw_ch.is_closed)

    def test_concurrent_publishes_all_scheduled(self):
        """All publishes from N threads must each schedule exactly one callback."""
        ch, _raw_ch, wrapper = self._make_channel()
        n = 20

        # Count scheduled callbacks via a lock-guarded list rather than
        # ``Mock.call_count``: the mock's internal bookkeeping is not
        # thread-safe, so concurrent calls can lose an increment and make
        # this assertion spuriously fail.
        scheduled = []
        scheduled_lock = threading.Lock()

        def record(cb):
            with scheduled_lock:
                scheduled.append(cb)

        wrapper._schedule_unchecked.side_effect = record

        barrier = threading.Barrier(n)

        def publish():
            barrier.wait()
            ch.basic_publish(exchange='', routing_key='q', body=b'x')

        threads = [threading.Thread(target=publish) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(scheduled), n)


class ConsumerWorkPoolTests(unittest.TestCase):
    """Tests for the per-channel consumer dispatch pool."""

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def test_callback_runs_on_worker_thread_not_ioloop(self):
        """on_message_callback must execute on the pool thread, not the thread that invoked the
        wrapped callback (which is the IOLoop thread).
        """
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        callback_thread = []

        def my_callback(channel, method, properties, body):
            callback_thread.append(threading.current_thread())

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', b'body')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(len(callback_thread), 1)
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-consumer', callback_thread[0].name)

    def test_callbacks_execute_in_order(self):
        """Multiple deliveries must be dispatched in FIFO order."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        order = []

        def my_callback(channel, method, properties, body):
            order.append(body)

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                for i in range(10):
                    on_message_callback(raw_ch, 'method', 'props', i)
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(order, list(range(10)))

    def test_callback_exception_does_not_kill_pool(self):
        """An exception in a delivery callback must not prevent subsequent deliveries from being
        processed.
        """
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        delivered = []

        def my_callback(channel, method, properties, body):
            delivered.append(body)
            if body == 'explode':
                raise RuntimeError('boom')

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', 'explode')
                on_message_callback(raw_ch, 'method', 'props', 'after')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=my_callback)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(delivered, ['explode', 'after'])

    def test_channel_close_waits_for_pending_callbacks(self):
        """Close() must wait for in-flight callbacks to complete before returning."""
        import time

        from pika.exceptions import ChannelClosedByClient

        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        completed = threading.Event()

        def slow_callback(channel, method, properties, body):
            time.sleep(0.05)
            completed.set()

        call_count = [0]

        def execute_and_fire(cb):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: basic_consume setup
                def fake_consume(queue, on_message_callback, auto_ack,
                                 exclusive, consumer_tag, arguments, callback):
                    on_message_callback(raw_ch, 'method', 'props', b'body')
                    callback(mock_frame)

                raw_ch.basic_consume.side_effect = fake_consume
                cb()
            else:
                # Second call: channel close
                close_callbacks = []
                raw_ch.add_on_close_callback.side_effect = close_callbacks.append
                raw_ch.close.side_effect = lambda reply_code, reply_text: close_callbacks[
                    -1](raw_ch, ChannelClosedByClient(reply_code, reply_text))
                cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=slow_callback)
        ch.close()

        self.assertTrue(completed.is_set())

    def test_pool_uses_single_worker(self):
        """The pool must use exactly one worker thread (for ordering)."""
        ch, _, _ = self._make_channel()
        pool = ch._consumer_work_pool
        threads = []
        for _ in range(5):
            pool.submit(lambda: threads.append(threading.current_thread()))
        pool.shutdown(wait=True)
        self.assertEqual(len(set(threads)), 1)

    def test_callback_exception_is_logged(self):
        """Exceptions in delivery callbacks must be logged, not swallowed silently."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'

        def exploding_callback(channel, method, properties, body):
            raise ValueError('test error')

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                on_message_callback(raw_ch, 'method', 'props', b'body')
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        with patch(
                'pika.adapters.thread_safe_connection.LOGGER') as mock_logger:
            ch.basic_consume(queue='q', on_message_callback=exploding_callback)
            ch._consumer_work_pool.shutdown(wait=True)
            mock_logger.exception.assert_called_once()
            self.assertIn('Unhandled exception',
                          mock_logger.exception.call_args[0][0])

    def test_submit_after_pool_shutdown_does_not_raise(self):
        """Delivery arriving after pool shutdown must not crash the IOLoop."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()
        mock_frame.method.consumer_tag = 'ctag1'
        wrapped_cb_holder = []

        def execute_and_fire(cb):

            def fake_consume(queue, on_message_callback, auto_ack, exclusive,
                             consumer_tag, arguments, callback):
                wrapped_cb_holder.append(on_message_callback)
                callback(mock_frame)

            raw_ch.basic_consume.side_effect = fake_consume
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.basic_consume(queue='q', on_message_callback=MagicMock())

        # Shut down the pool, then simulate a late delivery
        ch._shutdown_pool()
        # Must not raise RuntimeError
        wrapped_cb_holder[0](raw_ch, 'method', 'props', b'late')

    def test_shutdown_pool_is_idempotent(self):
        """Calling _shutdown_pool() multiple times must not raise."""
        ch, _, _ = self._make_channel()
        ch._shutdown_pool()
        ch._shutdown_pool()  # second call must be a no-op

    def test_add_on_return_callback_routes_through_schedule_unchecked(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.add_on_return_callback(MagicMock())
        wrapper._schedule_unchecked.assert_called_once()
        raw_ch.add_on_return_callback.assert_not_called()

    def test_add_on_return_callback_registers_on_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()

        def execute_immediately(cb):
            cb()

        wrapper._schedule_unchecked.side_effect = execute_immediately

        ch.add_on_return_callback(MagicMock())
        raw_ch.add_on_return_callback.assert_called_once()

    def test_add_on_return_callback_dispatches_on_worker_thread(self):
        """Returned messages must run on the per-channel worker, not IOLoop."""
        ch, raw_ch, wrapper = self._make_channel()
        callback_thread = []
        received = []

        def user_cb(channel, method, properties, body):
            callback_thread.append(threading.current_thread())
            received.append((channel, method, properties, body))

        def execute_and_fire(cb):
            cb()
            # Capture the wrapped callback the wrapper handed to raw_ch
            wrapped = raw_ch.add_on_return_callback.call_args[0][0]
            # Simulate the broker returning a message
            wrapped(raw_ch, 'method', 'props', b'returned-body')

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.add_on_return_callback(user_cb)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(len(received), 1)
        # First arg must be the Channel, not the raw channel
        self.assertIs(received[0][0], ch)
        self.assertEqual(received[0][1:], ('method', 'props', b'returned-body'))
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-consumer', callback_thread[0].name)

    def test_add_on_return_callback_after_pool_shutdown_logs_and_drops(self):
        """A returned message arriving after pool shutdown must not raise."""
        ch, raw_ch, wrapper = self._make_channel()
        wrapped_holder = []

        def execute_and_fire(cb):
            cb()
            wrapped_holder.append(raw_ch.add_on_return_callback.call_args[0][0])

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.add_on_return_callback(MagicMock())
        ch._shutdown_pool()
        # Late return must be silently dropped
        wrapped_holder[0](raw_ch, 'method', 'props', b'late')

    def test_add_on_return_callback_raises_when_connection_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.add_on_return_callback(MagicMock())

        self.assertIs(ctx.exception, reason)
        wrapper._schedule_unchecked.assert_not_called()

    def test_add_on_cancel_callback_routes_through_schedule_unchecked(self):
        ch, raw_ch, wrapper = self._make_channel()
        ch.add_on_cancel_callback(MagicMock())
        wrapper._schedule_unchecked.assert_called_once()
        raw_ch.add_on_cancel_callback.assert_not_called()

    def test_add_on_cancel_callback_registers_on_raw_channel(self):
        ch, raw_ch, wrapper = self._make_channel()

        def execute_immediately(cb):
            cb()

        wrapper._schedule_unchecked.side_effect = execute_immediately

        ch.add_on_cancel_callback(MagicMock())
        raw_ch.add_on_cancel_callback.assert_called_once()

    def test_add_on_cancel_callback_dispatches_on_worker_thread(self):
        """Server-initiated cancel must run on the per-channel worker."""
        ch, raw_ch, wrapper = self._make_channel()
        callback_thread = []
        received = []

        def user_cb(method_frame):
            callback_thread.append(threading.current_thread())
            received.append(method_frame)

        def execute_and_fire(cb):
            cb()
            wrapped = raw_ch.add_on_cancel_callback.call_args[0][0]
            wrapped('cancel-frame')

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.add_on_cancel_callback(user_cb)
        ch._consumer_work_pool.shutdown(wait=True)

        self.assertEqual(received, ['cancel-frame'])
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-consumer', callback_thread[0].name)

    def test_add_on_cancel_callback_after_pool_shutdown_logs_and_drops(self):
        ch, raw_ch, wrapper = self._make_channel()
        wrapped_holder = []

        def execute_and_fire(cb):
            cb()
            wrapped_holder.append(raw_ch.add_on_cancel_callback.call_args[0][0])

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        ch.add_on_cancel_callback(MagicMock())
        ch._shutdown_pool()
        wrapped_holder[0]('late-cancel')  # must not raise

    def test_add_on_cancel_callback_raises_when_connection_closed(self):
        ch, _raw_ch, wrapper = self._make_channel()
        reason = Exception('closed')
        wrapper._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            ch.add_on_cancel_callback(MagicMock())

        self.assertIs(ctx.exception, reason)
        wrapper._schedule_unchecked.assert_not_called()

    def test_return_listener_exception_is_logged_not_lost(self):
        """An exception raised inside a return listener must be logged (not silently dropped on an
        unobserved Future).
        """
        ch, raw_ch, wrapper = self._make_channel()

        def boom(channel, method, properties, body):
            raise RuntimeError('return-listener boom')

        def execute_and_fire(cb):
            cb()
            wrapped = raw_ch.add_on_return_callback.call_args[0][0]
            wrapped(raw_ch, 'method', 'props', b'body')

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            ch.add_on_return_callback(boom)
            ch._consumer_work_pool.shutdown(wait=True)

        self.assertTrue(
            any('return listener' in msg for msg in cm.output),
            msg=f'Expected "return listener" in logs, got: {cm.output}')

    def test_cancel_listener_exception_is_logged_not_lost(self):
        ch, raw_ch, wrapper = self._make_channel()

        def boom(method_frame):
            raise RuntimeError('cancel-listener boom')

        def execute_and_fire(cb):
            cb()
            wrapped = raw_ch.add_on_cancel_callback.call_args[0][0]
            wrapped('cancel-frame')

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            ch.add_on_cancel_callback(boom)
            ch._consumer_work_pool.shutdown(wait=True)

        self.assertTrue(
            any('cancel listener' in msg for msg in cm.output),
            msg=f'Expected "cancel listener" in logs, got: {cm.output}')

    def test_publisher_confirm_callback_exception_is_logged_not_lost(self):
        ch, raw_ch, wrapper = self._make_channel()
        ok_frame = MagicMock()

        def boom(method_frame):
            raise RuntimeError('confirm boom')

        def execute_and_fire(cb):

            def fake_confirm(ack_nack_callback, callback):
                ack_nack_callback('ack-frame')  # fire user cb via wrapper
                callback(ok_frame)

            raw_ch.confirm_delivery.side_effect = fake_confirm
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            ch.confirm_delivery(boom)
            ch._consumer_work_pool.shutdown(wait=True)

        self.assertTrue(
            any('publisher confirm callback' in msg for msg in cm.output),
            msg=
            f'Expected "publisher confirm callback" in logs, got: {cm.output}')

    def test_confirm_delivery_arms_publish_seq_no_counter(self):
        """After confirm_delivery succeeds, _next_publish_seq_no is 0 (armed) so subsequent
        publishes will track tags.
        """
        ch, raw_ch, wrapper = self._make_channel()
        self.assertIsNone(ch._next_publish_seq_no)
        ok_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_confirm(ack_nack_callback, callback):
                callback(ok_frame)

            raw_ch.confirm_delivery.side_effect = fake_confirm
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire
        ch.confirm_delivery(MagicMock())
        self.assertEqual(ch._next_publish_seq_no, 0)

    def test_confirm_delivery_is_idempotent(self):
        """Calling confirm_delivery a second time returns the cached Confirm.SelectOk without
        sending a frame or resetting the counter.
        """
        ch, raw_ch, wrapper = self._make_channel()
        ok_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_confirm(ack_nack_callback, callback):
                callback(ok_frame)

            raw_ch.confirm_delivery.side_effect = fake_confirm
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        result1 = ch.confirm_delivery(MagicMock())
        self.assertIs(result1, ok_frame)

        # Advance the counter to prove it is not reset on the second call
        ch._next_publish_seq_no = 5

        result2 = ch.confirm_delivery(MagicMock())
        self.assertIs(result2, ok_frame)
        self.assertEqual(ch._next_publish_seq_no, 5)
        # Only one RPC should have been sent.  Count the frames handed to the
        # raw channel rather than the callbacks scheduled on the IOLoop: a
        # single RPC also schedules its own callback cleanup.
        self.assertEqual(raw_ch.confirm_delivery.call_count, 1)

    def test_next_publish_seq_no_property_none_before_confirms(self):
        """next_publish_seq_no returns None when confirms are not enabled."""
        ch, _raw_ch, _wrapper = self._make_channel()
        self.assertIsNone(ch.next_publish_seq_no)

    def test_next_publish_seq_no_property_starts_at_one(self):
        """next_publish_seq_no returns 1 immediately after confirm_delivery."""
        ch, _raw_ch, _wrapper = self._make_channel()
        ch._next_publish_seq_no = 0
        self.assertEqual(ch.next_publish_seq_no, 1)

    def test_next_publish_seq_no_property_advances_after_publish(self):
        """next_publish_seq_no reflects the next tag to be assigned."""
        ch, _raw_ch, wrapper = self._make_channel()
        ch._next_publish_seq_no = 0
        ch.basic_publish(exchange='ex', routing_key='rk', body=b'x')
        wrapper._schedule_unchecked.call_args[0][0]()
        self.assertEqual(ch.next_publish_seq_no, 2)


class BlockingMethodTimeoutTests(unittest.TestCase):
    """Tests for timeout behavior on blocking methods."""

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def test_queue_declare_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            # Schedule the callback but the mock never fires _on_declare
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.queue_declare = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.queue_declare(queue='q', timeout=0.05)

        self.assertIn('queue_declare', str(ctx.exception))

    def test_basic_qos_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.basic_qos = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.basic_qos(prefetch_count=1, timeout=0.05)

        self.assertIn('basic_qos', str(ctx.exception))

    def test_basic_consume_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.basic_consume = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.basic_consume(queue='q',
                             on_message_callback=MagicMock(),
                             timeout=0.05)

        self.assertIn('basic_consume', str(ctx.exception))

    def test_basic_cancel_raises_timeout_error(self):
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.basic_cancel = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            ch.basic_cancel('ctag1', timeout=0.05)

        self.assertIn('basic_cancel', str(ctx.exception))

    def test_channel_close_does_not_raise_on_timeout(self):
        """Close() should log and continue, not raise TimeoutError."""
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.close = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        # Must not raise - close() treats timeout as "channel is dead"
        ch.close(timeout=0.05)

    def test_default_timeout_succeeds_when_response_is_immediate(self):
        """RPC methods succeed normally when the response arrives before timeout."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute_and_fire(cb):

            def fake_qos(prefetch_size, prefetch_count, global_qos, callback):
                callback(mock_frame)

            raw_ch.basic_qos.side_effect = fake_qos
            cb()

        wrapper._schedule_unchecked.side_effect = execute_and_fire

        result = ch.basic_qos(prefetch_count=1)
        self.assertIs(result, mock_frame)

    def test_timeout_unregisters_waiter(self):
        """A timed-out RPC must not leave a stale entry in _blocking_waiters."""
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.queue_declare = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        with self.assertRaises(TimeoutError):
            ch.queue_declare(queue='q', timeout=0.05)

        self.assertEqual(wrapper._blocking_waiters, [])

    def test_basic_get_timeout_unregisters_waiter(self):
        """basic_get timeout must not leak a waiter."""
        ch, raw_ch, wrapper = self._make_channel()

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.add_callback = MagicMock()
            raw_ch.basic_get = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        with self.assertRaises(TimeoutError):
            ch.basic_get(queue='q', timeout=0.05)

        self.assertEqual(wrapper._blocking_waiters, [])

    def test_close_timeout_unregisters_waiter(self):
        """Close() timeout must not leak a waiter."""
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.is_closed = False
        raw_ch.is_closing = False

        def never_respond(cb):
            raw_ch.add_on_close_callback = MagicMock()
            raw_ch.close = MagicMock()
            cb()

        wrapper._schedule_unchecked.side_effect = never_respond

        ch.close(timeout=0.05)
        self.assertEqual(wrapper._blocking_waiters, [])


class BlockingRPCPassthroughTests(unittest.TestCase):
    """
    Cover the eight thin RPC method wrappers that delegate to _blocking_rpc.

    Each method's only logic is to forward arguments to the raw channel via self._blocking_rpc and
    return its result.  These tests verify the success path of each wrapper end-to-end.
    """

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def _run_immediate_rpc(self, raw_method, *call_args, **call_kwargs):
        """Wire up wrapper/raw_method so the callback fires immediately."""
        ch, raw_ch, wrapper = self._make_channel()
        mock_frame = MagicMock()

        def execute(cb):
            cb()

        def fake_method(*args, **kwargs):
            kwargs['callback'](mock_frame)

        getattr(raw_ch, raw_method).side_effect = fake_method
        wrapper._schedule_unchecked.side_effect = execute
        return ch, raw_ch, wrapper, mock_frame

    def test_exchange_declare_returns_method_frame(self):
        ch, raw_ch, _w, frame = self._run_immediate_rpc('exchange_declare')
        result = ch.exchange_declare(exchange='ex', exchange_type='topic')
        self.assertIs(result, frame)
        raw_ch.exchange_declare.assert_called_once()
        kwargs = raw_ch.exchange_declare.call_args[1]
        self.assertEqual(kwargs['exchange'], 'ex')
        self.assertEqual(kwargs['exchange_type'], 'topic')

    def test_queue_bind_returns_method_frame(self):
        ch, raw_ch, _w, frame = self._run_immediate_rpc('queue_bind')
        result = ch.queue_bind(queue='q', exchange='ex', routing_key='rk')
        self.assertIs(result, frame)
        kwargs = raw_ch.queue_bind.call_args[1]
        self.assertEqual(kwargs['queue'], 'q')
        self.assertEqual(kwargs['exchange'], 'ex')
        self.assertEqual(kwargs['routing_key'], 'rk')

    def test_queue_unbind_returns_method_frame(self):
        ch, raw_ch, _w, frame = self._run_immediate_rpc('queue_unbind')
        result = ch.queue_unbind(queue='q', exchange='ex', routing_key='rk')
        self.assertIs(result, frame)
        kwargs = raw_ch.queue_unbind.call_args[1]
        self.assertEqual(kwargs['queue'], 'q')

    def test_queue_delete_returns_method_frame(self):
        ch, raw_ch, _w, frame = self._run_immediate_rpc('queue_delete')
        result = ch.queue_delete(queue='q', if_unused=True, if_empty=True)
        self.assertIs(result, frame)
        kwargs = raw_ch.queue_delete.call_args[1]
        self.assertTrue(kwargs['if_unused'])
        self.assertTrue(kwargs['if_empty'])

    def test_queue_purge_returns_method_frame(self):
        ch, raw_ch, _w, frame = self._run_immediate_rpc('queue_purge')
        result = ch.queue_purge(queue='q')
        self.assertIs(result, frame)
        self.assertEqual(raw_ch.queue_purge.call_args[1]['queue'], 'q')

    def test_exchange_bind_returns_method_frame(self):
        ch, raw_ch, _w, frame = self._run_immediate_rpc('exchange_bind')
        result = ch.exchange_bind(destination='d', source='s', routing_key='rk')
        self.assertIs(result, frame)
        kwargs = raw_ch.exchange_bind.call_args[1]
        self.assertEqual(kwargs['destination'], 'd')
        self.assertEqual(kwargs['source'], 's')

    def test_exchange_unbind_returns_method_frame(self):
        ch, _raw_ch, _w, frame = self._run_immediate_rpc('exchange_unbind')
        result = ch.exchange_unbind(destination='d',
                                    source='s',
                                    routing_key='rk')
        self.assertIs(result, frame)

    def test_exchange_delete_returns_method_frame(self):
        ch, raw_ch, _w, frame = self._run_immediate_rpc('exchange_delete')
        result = ch.exchange_delete(exchange='ex', if_unused=True)
        self.assertIs(result, frame)
        self.assertTrue(raw_ch.exchange_delete.call_args[1]['if_unused'])


class BlockingRPCErrorPathTests(unittest.TestCase):
    """
    Cover the two error paths inside Channel._blocking_rpc:

    * channel_method itself raises during _invoke (connection already
      torn down on the IOLoop thread).
    * channel close fires while the RPC is in flight, before the
      method's callback arrives.
    """

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def test_method_raise_propagates_to_caller(self):
        ch, raw_ch, wrapper = self._make_channel()
        boom = RuntimeError('connection torn down')
        raw_ch.queue_declare.side_effect = boom
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        with self.assertRaises(RuntimeError) as ctx:
            ch.queue_declare(queue='q', timeout=0.5)
        self.assertIs(ctx.exception, boom)

    def test_channel_close_during_rpc_propagates_reason(self):
        """Channel-close callback firing before _on_ok must surface the close reason to the blocked
        caller.
        """
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('channel closed by broker')
        captured_close_cb = {}

        def capture_close_cb(cb):
            captured_close_cb['cb'] = cb

        raw_ch.add_on_close_callback.side_effect = capture_close_cb

        # queue_declare never fires its own callback; instead we fire the
        # close callback that was registered first.
        def fire_close(*_args, **_kwargs):
            captured_close_cb['cb'](raw_ch, reason)

        raw_ch.queue_declare.side_effect = fire_close
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        with self.assertRaises(Exception) as ctx:
            ch.queue_declare(queue='q', timeout=0.5)
        self.assertIs(ctx.exception, reason)


class BasicGetTests(unittest.TestCase):
    """Cover Channel.basic_get success, empty, error and channel-close paths."""

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def test_basic_get_returns_message_tuple(self):
        ch, raw_ch, wrapper = self._make_channel()

        def fire_get_ok(*_args, **kwargs):
            kwargs['callback'](raw_ch, 'method', 'props', b'body')

        raw_ch.basic_get.side_effect = fire_get_ok
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        method, properties, body = ch.basic_get(queue='q', timeout=0.5)
        self.assertEqual(method, 'method')
        self.assertEqual(properties, 'props')
        self.assertEqual(body, b'body')

    def test_basic_get_returns_none_tuple_on_empty(self):
        ch, raw_ch, wrapper = self._make_channel()
        captured_empty_cb = {}

        def capture_empty(empty_cb, replies, one_shot):
            captured_empty_cb['cb'] = empty_cb

        raw_ch.add_callback.side_effect = capture_empty

        # basic_get registers callbacks but Basic.GetEmpty fires instead of
        # the GetOk callback.
        def fire_empty(*_args, **_kwargs):
            captured_empty_cb['cb']('empty-frame')

        raw_ch.basic_get.side_effect = fire_empty
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        result = ch.basic_get(queue='q', timeout=0.5)
        self.assertEqual(result, (None, None, None))

    def test_basic_get_method_raise_propagates(self):
        ch, raw_ch, wrapper = self._make_channel()
        boom = RuntimeError('basic_get raised')
        raw_ch.basic_get.side_effect = boom
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        with self.assertRaises(RuntimeError) as ctx:
            ch.basic_get(queue='q', timeout=0.5)
        self.assertIs(ctx.exception, boom)

    def test_basic_get_channel_close_propagates_reason(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('channel closed mid-get')
        captured_close_cb = {}

        def capture_close_cb(cb):
            captured_close_cb['cb'] = cb

        raw_ch.add_on_close_callback.side_effect = capture_close_cb

        def fire_close(*_args, **_kwargs):
            captured_close_cb['cb'](raw_ch, reason)

        raw_ch.basic_get.side_effect = fire_close
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        with self.assertRaises(Exception) as ctx:
            ch.basic_get(queue='q', timeout=0.5)
        self.assertIs(ctx.exception, reason)


class PerRPCCallbackReleaseTests(unittest.TestCase):
    """
    Cover the release of the per-RPC channel callbacks.

    ``add_on_close_callback`` registers with ``one_shot=False`` and every RPC passes a distinct
    closure, so any callback left behind accumulates for the life of the channel and slows every
    later registration down.  A finished RPC has to take its own callbacks back out.
    """

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return ThreadSafeChannel(raw_ch, wrapper), raw_ch, wrapper

    def test_blocking_rpc_removes_the_close_callback_it_registered(self):
        """A completed RPC must remove the exact close callback it added."""
        ch, raw_ch, wrapper = self._make_channel()

        def fire_ok(*_args, **kwargs):
            kwargs['callback']('Queue.DeclareOk')

        raw_ch.queue_declare.side_effect = fire_ok
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        ch.queue_declare(queue='q', timeout=0.5)

        registered = raw_ch.add_on_close_callback.call_args[0][0]
        raw_ch.remove_on_close_callback.assert_called_once_with(registered)

    def test_blocking_rpc_removes_the_close_callback_after_a_timeout(self):
        """A timed-out RPC must clean up too, or a flaky broker leaks one per call."""
        ch, raw_ch, wrapper = self._make_channel()
        # Never fire the ok callback, so the wait times out.
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        with self.assertRaises(TimeoutError):
            ch.queue_declare(queue='q', timeout=0.01)

        registered = raw_ch.add_on_close_callback.call_args[0][0]
        raw_ch.remove_on_close_callback.assert_called_once_with(registered)

    def test_basic_get_removes_both_of_its_callbacks(self):
        """
        basic_get must drop its close callback and its Basic.GetEmpty one-shot.

        A one-shot is only consumed if it fires, so a get that returns a message leaves its
        Basic.GetEmpty callback registered unless it is removed explicitly.
        """
        ch, raw_ch, wrapper = self._make_channel()

        def fire_get_ok(*_args, **kwargs):
            kwargs['callback'](raw_ch, 'method', 'props', b'body')

        raw_ch.basic_get.side_effect = fire_get_ok
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        ch.basic_get(queue='q', timeout=0.5)

        registered_close = raw_ch.add_on_close_callback.call_args[0][0]
        raw_ch.remove_on_close_callback.assert_called_once_with(
            registered_close)
        registered_empty = raw_ch.add_callback.call_args[0][0]
        raw_ch.remove_callback.assert_called_once_with(registered_empty,
                                                       [spec.Basic.GetEmpty])

    def test_release_is_scheduled_rather_than_run_inline(self):
        """Removal must go through the IOLoop, which owns the channel's callback stack."""
        ch, raw_ch, wrapper = self._make_channel()
        scheduled = []

        def fire_ok(*_args, **kwargs):
            kwargs['callback']('Queue.DeclareOk')

        raw_ch.queue_declare.side_effect = fire_ok

        def capture(cb):
            scheduled.append(cb)
            # Only run the RPC itself; leave the cleanup queued.
            if len(scheduled) == 1:
                cb()

        wrapper._schedule_unchecked.side_effect = capture

        ch.queue_declare(queue='q', timeout=0.5)

        # The cleanup was handed to the IOLoop, not executed on this thread.
        self.assertEqual(len(scheduled), 2)
        raw_ch.remove_on_close_callback.assert_not_called()
        scheduled[1]()
        raw_ch.remove_on_close_callback.assert_called_once()


class ChannelCloseErrorPathTests(unittest.TestCase):
    """Cover error branches inside Channel.close()."""

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        raw_ch.is_closing = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def test_close_propagates_broker_initiated_close_reason(self):
        ch, raw_ch, wrapper = self._make_channel()
        reason = Exception('broker closed channel during our close')
        captured_close_cb = {}

        def capture_close_cb(cb):
            captured_close_cb['cb'] = cb

        raw_ch.add_on_close_callback.side_effect = capture_close_cb

        def fire_close(*_args, **_kwargs):
            captured_close_cb['cb'](raw_ch, reason)

        raw_ch.close.side_effect = fire_close
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        with self.assertRaises(Exception) as ctx:
            ch.close(timeout=0.5)
        self.assertIs(ctx.exception, reason)

    def test_close_method_raise_propagates(self):
        ch, raw_ch, wrapper = self._make_channel()
        boom = RuntimeError('close raised')
        raw_ch.close.side_effect = boom
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        with self.assertRaises(RuntimeError) as ctx:
            ch.close(timeout=0.5)
        self.assertIs(ctx.exception, boom)


class CallbackRegistrationFailureTests(unittest.TestCase):
    """Cover the LOGGER.warning branches when raw-channel registration methods raise."""

    def _make_channel(self):
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        # A MagicMock would auto-create this as a Mock, which with_traceback()
        # rejects; the real attribute is None until a close reason is recorded.
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        # A healthy connection has no pending error; _submit_or_terminate
        # short-circuits when _error is set, so it must be None here.
        wrapper._connection._error = None
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    def test_add_on_cancel_callback_logs_when_raw_register_raises(self):
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.add_on_cancel_callback.side_effect = RuntimeError('boom')
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()
        with patch(
                'pika.adapters.thread_safe_connection.LOGGER') as mock_logger:
            ch.add_on_cancel_callback(MagicMock())
        mock_logger.warning.assert_called_once()
        self.assertIn('add_on_cancel_callback failed',
                      mock_logger.warning.call_args[0][0])

    def test_add_on_return_callback_logs_when_raw_register_raises(self):
        ch, raw_ch, wrapper = self._make_channel()
        raw_ch.add_on_return_callback.side_effect = RuntimeError('boom')
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()
        with patch(
                'pika.adapters.thread_safe_connection.LOGGER') as mock_logger:
            ch.add_on_return_callback(MagicMock())
        mock_logger.warning.assert_called_once()
        self.assertIn('add_on_return_callback failed',
                      mock_logger.warning.call_args[0][0])

    def test_publisher_confirm_drops_when_pool_shut_down(self):
        """The wrapped ack/nack callback logs and drops if the consumer pool has been shut down by
        the time the broker delivers the confirm.
        """
        ch, _raw_ch, _wrapper = self._make_channel()
        # Capture the wrapped callback by short-circuiting _blocking_rpc.
        captured = {}

        def fake_blocking_rpc(method_name, channel_method, timeout, **kwargs):
            captured['wrapped'] = kwargs['ack_nack_callback']
            return MagicMock()

        with patch.object(ch, '_blocking_rpc', side_effect=fake_blocking_rpc):
            ch.confirm_delivery(MagicMock())

        # Force the work pool to RuntimeError on submit.
        ch._consumer_work_pool = MagicMock()
        ch._consumer_work_pool.submit.side_effect = RuntimeError(
            'pool shut down')

        with patch(
                'pika.adapters.thread_safe_connection.LOGGER') as mock_logger:
            captured['wrapped'](MagicMock())
        mock_logger.debug.assert_called_once()
        self.assertIn('Publisher confirm dropped',
                      mock_logger.debug.call_args[0][0])


class ConsumerPoolConnectionIntegrationTests(unittest.TestCase):
    """Tests for connection-level consumer pool lifecycle management."""

    def _make_connection(self):
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False
            mock_conn.is_closing = False
            # A healthy connection has no pending error; _submit_or_terminate
            # short-circuits when _error is set, so it must be None here.
            mock_conn._error = None

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            conn = Connection(parameters='params')
            conn._ioloop_thread.join(timeout=1)

        return conn, mock_conn, mock_ioloop

    def test_connection_tracks_channels(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_scheduled(cb):
            mock_raw_ch = MagicMock()
            mock_conn.channel.side_effect = lambda on_open_callback: on_open_callback(
                mock_raw_ch)
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()
        self.assertIn(ch, conn._channels)

    def test_connection_close_shuts_down_channel_pools(self):
        """Pool shutdown happens after ioloop.start() returns, not inside _on_connection_closed
        (which runs on the IOLoop thread).
        """
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_scheduled(cb):
            mock_raw_ch = MagicMock()
            mock_conn.channel.side_effect = lambda on_open_callback: on_open_callback(
                mock_raw_ch)
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()

        # _on_connection_closed wakes waiters but does NOT shut down pools
        conn._on_connection_closed(mock_conn, Exception('gone'))
        self.assertFalse(ch._pool_shutdown)

        # Pool shutdown happens when _run_ioloop calls it after ioloop.start()
        # returns. Simulate that by calling it directly.
        conn._shutdown_all_consumer_pools()
        self.assertTrue(ch._pool_shutdown)

    def test_bounded_sweep_signals_all_pools_before_joining_any(self):
        """
        The bounded sweep must signal every pool before it blocks joining the first.

        Otherwise a wedged early channel's blocking join runs while later
        channels have not been told to drain, so their workers only start
        draining after the budget is already spent.  The invariant checked here
        is order-based and deterministic: when the first (wedged) channel's
        blocking join begins, every other channel's pool flag is already set.
        """
        conn, _mock_conn, _mock_ioloop = self._make_connection()

        wedged = Channel(MagicMock(), conn)
        healthy = Channel(MagicMock(), conn)
        with conn._channel_waiters_lock:
            conn._channels.extend([wedged, healthy])

        release = threading.Event()
        started = threading.Event()
        self.addCleanup(release.set)

        def block():
            started.set()
            release.wait(timeout=5)

        # Wedge the first channel's worker so its blocking join stalls.
        wedged._consumer_work_pool.submit(block)
        self.assertTrue(started.wait(timeout=5))

        # Record the healthy pool's flag state at the moment the wedged pool's
        # blocking join (wait=True) begins.  With the up-front signal sweep it
        # is already set; without it the healthy pool is still unsignalled.
        real_shutdown = wedged._consumer_work_pool.shutdown
        healthy_flag_at_wedged_join = []

        def recording_shutdown(*args, **kwargs):
            if kwargs.get('wait'):
                healthy_flag_at_wedged_join.append(
                    healthy._consumer_work_pool._shutdown)
            return real_shutdown(*args, **kwargs)

        with patch.object(wedged._consumer_work_pool,
                          'shutdown',
                          side_effect=recording_shutdown):
            conn._shutdown_all_consumer_pools(timeout=0.2)

        self.assertEqual(healthy_flag_at_wedged_join, [True])
        self.assertTrue(healthy._pool_shutdown)
        self.assertTrue(wedged._pool_shutdown)

    def test_concurrent_channel_close_runs_shutdown_once(self):
        """Two concurrent close() calls must not both run the pool join."""
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        ch = Channel(MagicMock(), conn)

        calls = []
        real_shutdown = ch._consumer_work_pool.shutdown

        def counting_shutdown(*args, **kwargs):
            calls.append(1)
            return real_shutdown(*args, **kwargs)

        with patch.object(ch._consumer_work_pool,
                          'shutdown',
                          side_effect=counting_shutdown):
            barrier = threading.Barrier(2)

            def do_shutdown():
                barrier.wait(timeout=5)
                ch._shutdown_pool()

            threads = [
                threading.Thread(target=do_shutdown, daemon=True)
                for _ in range(2)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5)

        self.assertTrue(ch._pool_shutdown)
        self.assertEqual(len(calls), 1)

    def test_ioloop_crash_shuts_down_channel_pools(self):
        """If the IOLoop crashes, all channel pools must be shut down."""
        crash = RuntimeError('IOLoop crash')
        gate = threading.Event()

        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            def ioloop_start():
                gate.wait(timeout=5)
                raise crash

            mock_ioloop.start.side_effect = ioloop_start
            conn = Connection(parameters='params')

        # Manually add a channel to the tracking list
        ch = Channel(MagicMock(), conn)
        with conn._channel_waiters_lock:
            conn._channels.append(ch)

        # Trigger the IOLoop crash
        gate.set()
        conn._ioloop_thread.join(timeout=2)

        self.assertTrue(ch._pool_shutdown)

    def test_force_stop_shuts_down_channel_pools(self):
        """Force-stop path in close() must shut down channel pools."""
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = True

        ch = Channel(MagicMock(), conn)
        with conn._channel_waiters_lock:
            conn._channels.append(ch)

        conn.close(timeout=0.1)

        self.assertTrue(ch._pool_shutdown)

    def test_force_stop_with_none_timeout_shuts_down_pools(self):
        """Force-stop path with timeout=None must drain pools with an unbounded budget."""
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        # First join returns with the thread still alive to enter the force
        # path; after force-stop the second join sees it exited.
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.side_effect = [True, False]

        ch = Channel(MagicMock(), conn)
        with conn._channel_waiters_lock:
            conn._channels.append(ch)

        conn.close(timeout=None)

        self.assertTrue(ch._pool_shutdown)
        self.assertTrue(conn._connection_pool_shutdown)

    def test_shutdown_connection_pool_warns_when_worker_wedged(self):
        """A wedged connection-event worker must be abandoned (not hang) with a warning logged."""
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        # _make_connection runs the IOLoop tail, which already shut the
        # connection pool down.  Replace it with a fresh pool wedged in a
        # never-releasing callback and reset the guard so the shutdown runs.
        release = threading.Event()
        started = threading.Event()

        def block():
            started.set()
            release.wait(timeout=5)

        pool = _BoundedWorkPool(maxsize=10,
                                put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-conn-pool')
        conn._connection_work_pool = pool
        conn._connection_pool_shutdown = False

        try:
            pool.submit(block)
            self.assertTrue(started.wait(timeout=5))

            done = threading.Event()

            def do_shutdown():
                with patch('pika.adapters.thread_safe_connection.LOGGER'
                          ) as mock_logger:
                    conn._shutdown_connection_pool(timeout=0.1)
                    self.warned = mock_logger.warning.called
                done.set()

            shutter = threading.Thread(target=do_shutdown, daemon=True)
            shutter.start()
            self.assertTrue(
                done.wait(timeout=5),
                '_shutdown_connection_pool hung on a wedged worker')
            shutter.join(timeout=5)
            self.assertTrue(conn._connection_pool_shutdown)
            self.assertTrue(self.warned,
                            'expected a warning when abandoning the worker')
        finally:
            release.set()
            pool.shutdown(wait=True, timeout=5)

    def test_init_raises_timeout_error(self):
        """Constructor must raise TimeoutError if connection is not established within the specified
        timeout.
        """
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                # Never call on_open_callback - simulates stalled handshake
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn
            mock_ioloop.start = MagicMock()  # IOLoop does nothing

            with self.assertRaises(TimeoutError) as ctx:
                Connection(parameters='params', timeout=0.05)

            self.assertIn('timed out', str(ctx.exception))
            mock_ioloop.add_callback_threadsafe.assert_called()

    def test_init_failure_shuts_down_connection_pool(self):
        """
        If SelectConnection construction raises, the connection-event pool must be shut down before
        __init__ propagates the exception.

        Otherwise its worker thread leaks for the lifetime of the process.
        """
        boom = RuntimeError('boom')
        with patch('pika.adapters.thread_safe_connection.SelectConnection',
                   side_effect=boom):
            captured = []

            # Capture the pool reference before __init__ raises so we can
            # assert it shut down.  We patch _BoundedWorkPool to record
            # the instance that __init__ created.
            def capturing_pool(*a, **kw):
                inst = _BoundedWorkPool(*a, **kw)
                captured.append(inst)
                return inst

            with patch('pika.adapters.thread_safe_connection._BoundedWorkPool',
                       side_effect=capturing_pool), self.assertRaises(
                           RuntimeError) as ctx:
                Connection(parameters='params')
            self.assertIs(ctx.exception, boom)
            self.assertEqual(len(captured), 1)
            # The captured pool must be shut down.  shutdown(wait=True)
            # will be a no-op if already shut down, but submit() raises
            # RuntimeError on a shut-down pool; use that as the probe.
            with self.assertRaises(RuntimeError):
                captured[0].submit(lambda: None)

    def test_init_timeout_joins_ioloop_thread(self):
        """When __init__ raises TimeoutError it must wait for the IOLoop thread to exit so
        consumer/connection pools are shut down.
        """
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            stop_event = threading.Event()

            def slow_start():
                # Block until __init__ raises and signals us; then exit so
                # _run_ioloop's cleanup tail runs.
                stop_event.wait(timeout=2)

            def stop_signal(_cb):
                # The post-timeout add_callback_threadsafe(stop) lands here.
                stop_event.set()

            mock_ioloop.start = slow_start
            mock_ioloop.add_callback_threadsafe.side_effect = stop_signal

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            try:
                Connection(parameters='params', timeout=0.05)
            except TimeoutError as exc:
                # By the time the TimeoutError reaches us, the IOLoop
                # thread must have been joined - i.e., it is no longer
                # alive.  Find it via thread enumeration; the daemon flag
                # is set, so it would otherwise outlive __init__.
                live_ioloop_threads = [
                    t for t in threading.enumerate()
                    if t.name.startswith('pika-ioloop-') and t.is_alive()
                ]
                self.assertEqual(
                    live_ioloop_threads, [],
                    msg=f'IOLoop thread still alive after TimeoutError: '
                    f'{live_ioloop_threads}')
                self.assertIn('timed out', str(exc))
            else:
                self.fail('Expected TimeoutError')

    def test_channel_raises_timeout_error(self):
        """Channel() must raise TimeoutError if Channel.OpenOk never arrives."""
        conn, mock_conn, mock_ioloop = self._make_connection()

        def never_respond(cb):
            # Schedule the callback but Channel.OpenOk never fires
            mock_conn.channel = MagicMock()
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError) as ctx:
            conn.channel(timeout=0.05)

        self.assertIn('channel open', str(ctx.exception))

    def test_channel_timeout_unregisters_waiter(self):
        """Channel() timeout must not leak a waiter."""
        conn, mock_conn, mock_ioloop = self._make_connection()

        def never_respond(cb):
            mock_conn.channel = MagicMock()
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = never_respond

        with self.assertRaises(TimeoutError):
            conn.channel(timeout=0.05)

        self.assertEqual(conn._blocking_waiters, [])

    def test_channel_raises_if_connection_closes_during_open(self):
        """
        Channel() must raise (not return a leaked channel) if the connection closes between
        Channel.OpenOk and the caller waking.

        Without the race guard, a Channel would be appended to _channels after
        _shutdown_all_consumer_pools() had already run, leaking the per-channel consumer pool.
        """
        conn, mock_conn, mock_ioloop = self._make_connection()
        mock_raw_ch = MagicMock()
        reason = Exception('connection lost')

        def execute_and_close(cb):

            def fake_channel(on_open_callback):
                # Channel.OpenOk fires...
                on_open_callback(mock_raw_ch)
                # ...and then the connection closes before the caller wakes.
                conn._closed_reason = reason

            mock_conn.channel.side_effect = fake_channel
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_close

        with self.assertRaises(Exception) as ctx:
            conn.channel()

        self.assertIs(ctx.exception, reason)
        # Channel must NOT have been appended to _channels.
        self.assertEqual(conn._channels, [])


class ConnectionTests(unittest.TestCase):

    def _make_connection(self,
                         on_open_error_callback=None,
                         on_close_callback=None):
        """
        Construct a Connection with a mocked SelectConnection.

        The mock immediately fires on_open_callback so __init__ unblocks.
        """
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False
            mock_conn.is_closing = False
            # A healthy connection has no pending error; _submit_or_terminate
            # short-circuits when _error is set, so it must be None here.
            mock_conn._error = None

            # Capture the on_open_callback passed to SelectConnection
            # and call it immediately so _connected_event gets set.
            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            conn = Connection(
                parameters='params',
                on_open_error_callback=on_open_error_callback,
                on_close_callback=on_close_callback,
            )
            # Stop the background thread immediately since ioloop.start is mocked
            conn._ioloop_thread.join(timeout=1)

        return conn, mock_conn, mock_ioloop

    def test_ioloop_thread_is_started(self):
        _conn, _, mock_ioloop = self._make_connection()
        mock_ioloop.start.assert_called_once()

    def test_is_open_delegates_to_inner_connection(self):
        conn, mock_conn, _ = self._make_connection()
        mock_conn.is_open = True
        self.assertTrue(conn.is_open)

    def test_is_closed_delegates_to_inner_connection(self):
        conn, mock_conn, _ = self._make_connection()
        mock_conn.is_closed = True
        self.assertTrue(conn.is_closed)

    def test_add_callback_threadsafe_delegates(self):
        conn, _mock_conn, mock_ioloop = self._make_connection()
        cb = MagicMock()
        conn.add_callback_threadsafe(cb)
        mock_ioloop.add_callback_threadsafe.assert_called_once_with(cb)

    def test_schedule_unchecked_bypasses_closed_connection_guard(self):
        """
        ``_schedule_unchecked`` must hand the callback straight to the IOLoop even when closed.

        Channel methods register a waiter and then schedule through this method, unregistering in a
        ``finally``.  If it applied the closed-connection guard, the raise would jump over that
        cleanup and leak the waiter, so the guard belongs only on the public
        ``add_callback_threadsafe``.
        """
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._on_connection_closed(mock_conn,
                                   ConnectionClosedByClient(200, 'shutdown'))
        mock_ioloop.add_callback_threadsafe.reset_mock()
        cb = MagicMock()

        conn._schedule_unchecked(cb)

        mock_ioloop.add_callback_threadsafe.assert_called_once_with(cb)

    def test_add_callback_threadsafe_raises_after_connection_closed(self):
        """
        Scheduling on a closed connection raises the close reason.

        The IOLoop thread is gone once the connection closes, so a callback accepted here would
        never run.  The caller must be told instead of being left to assume it was scheduled.  The
        reported exception is ``_closed_reason`` itself, the same exception every other method on
        the connection raises once closed.
        """
        conn, mock_conn, mock_ioloop = self._make_connection()
        reason = ConnectionClosedByClient(200, 'Normal shutdown')
        conn._on_connection_closed(mock_conn, reason)
        mock_ioloop.add_callback_threadsafe.reset_mock()
        cb = MagicMock()

        with self.assertRaises(ConnectionClosedByClient) as ctx:
            conn.add_callback_threadsafe(cb)

        self.assertIs(ctx.exception, reason)
        mock_ioloop.add_callback_threadsafe.assert_not_called()
        cb.assert_not_called()

    def test_add_callback_threadsafe_does_not_grow_close_reason_traceback(self):
        """
        Repeated rejections must not accumulate tracebacks on the close reason.

        ``add_callback_threadsafe`` re-raises the shared ``_closed_reason`` like every other method
        here, so bounding its depth depends on restoring the close-time traceback rather than on
        raising something fresh.  Without that, each rejection would append a frame to the shared
        instance and keep that call's arguments alive.
        """
        conn, mock_conn, _mock_ioloop = self._make_connection()
        reason = ConnectionClosedByClient(200, 'Normal shutdown')
        conn._on_connection_closed(mock_conn, reason)

        with _expect_raised(self, ConnectionClosedByClient) as caught:
            conn.add_callback_threadsafe(MagicMock())
        self.assertIs(caught[0], reason)
        after_first = _tb_depth(reason)

        for _ in range(50):
            with _expect_raised(self, ConnectionClosedByClient):
                conn.add_callback_threadsafe(MagicMock())

        self.assertEqual(_tb_depth(reason), after_first)

    def test_channel_does_not_grow_close_reason_traceback(self):
        """
        ``channel()`` on a closed connection re-raises the close reason, so it must not grow it.

        Like every method here, this path raises ``_closed_reason`` itself.  Bounding it depends on
        restoring the traceback captured at close time rather than on raising something fresh.
        """
        conn, mock_conn, _mock_ioloop = self._make_connection()
        reason = ConnectionClosedByClient(200, 'Normal shutdown')
        conn._on_connection_closed(mock_conn, reason)

        with _expect_raised(self, ConnectionClosedByClient) as caught:
            conn.channel()
        self.assertIs(caught[0], reason)
        after_first = _tb_depth(reason)

        for _ in range(50):
            with _expect_raised(self, ConnectionClosedByClient):
                conn.channel()

        self.assertEqual(_tb_depth(reason), after_first)

    def test_add_callback_threadsafe_raises_wrong_state_without_close_reason(
            self):
        """
        Closed connection with no recorded reason must still raise.

        Covers the window between the underlying connection reaching the closed state and
        ``_on_connection_closed`` setting ``_closed_reason``.
        """
        conn, mock_conn, mock_ioloop = self._make_connection()
        mock_conn.is_closed = True
        self.assertIsNone(conn._closed_reason)

        with self.assertRaises(ConnectionWrongStateError) as ctx:
            conn.add_callback_threadsafe(MagicMock())

        self.assertIn('add_callback_threadsafe', str(ctx.exception))
        mock_ioloop.add_callback_threadsafe.assert_not_called()

    def test_add_callback_threadsafe_from_ioloop_thread_is_exempt(self):
        """
        A callback scheduled from the IOLoop thread on a closed connection must be accepted.

        ``_on_connection_closed`` records the reason and then runs the user close callback on the
        IOLoop thread; scheduling follow-up work from there is the natural idiom.  Raising would
        propagate out through ``CallbackManager.process`` and abort the rest of teardown, so the
        IOLoop thread is exempt even though the callback most likely will not run.
        """
        conn, mock_conn, mock_ioloop = self._make_connection()
        reason = ConnectionClosedByClient(200, 'Normal shutdown')
        conn._on_connection_closed(mock_conn, reason)
        mock_ioloop.add_callback_threadsafe.reset_mock()
        # Make current_thread() is self._ioloop_thread true.
        conn._ioloop_thread = threading.current_thread()
        cb = MagicMock()

        with patch(
                'pika.adapters.thread_safe_connection.LOGGER') as mock_logger:
            conn.add_callback_threadsafe(cb)

        mock_ioloop.add_callback_threadsafe.assert_called_once_with(cb)
        mock_logger.debug.assert_called_once()

    def test_channel_schedules_open_via_add_callback_threadsafe(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        # Simulate the IOLoop executing the scheduled callback synchronously
        def execute_scheduled(cb):
            # The callback calls connection.channel(on_open_callback=...)
            # We intercept that and immediately fire on_open_callback.
            mock_raw_ch = MagicMock()

            def fake_channel(on_open_callback):
                on_open_callback(mock_raw_ch)

            mock_conn.channel.side_effect = fake_channel
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()
        self.assertIsInstance(ch, Channel)

    def test_channel_wraps_self_not_inner_connection(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_scheduled(cb):
            mock_raw_ch = MagicMock()

            def fake_channel(on_open_callback):
                on_open_callback(mock_raw_ch)

            mock_conn.channel.side_effect = fake_channel
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_scheduled

        ch = conn.channel()
        self.assertIs(ch._wrapper, conn)

    def test_channel_raises_when_connection_already_closed(self):
        conn, _mock_conn, _ = self._make_connection()
        reason = Exception('already closed')
        conn._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            conn.channel()

        self.assertIs(ctx.exception, reason)

    def test_channel_raises_when_connection_closes_while_waiting(self):
        conn, mock_conn, _ = self._make_connection()
        reason = Exception('connection lost')

        def close_while_waiting(cb):
            # Simulate the connection closing instead of the channel opening
            conn._on_connection_closed(mock_conn, reason)

        mock_conn.ioloop.add_callback_threadsafe.side_effect = close_while_waiting

        with self.assertRaises(Exception) as ctx:
            conn.channel()

        self.assertIs(ctx.exception, reason)

    def test_ioloop_crash_wakes_blocked_channel_waiter(self):
        """A thread blocked in channel() must be unblocked if the IOLoop crashes."""
        crash = RuntimeError('simulated IOLoop crash')
        gate = threading.Event()

        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False
            mock_conn.is_closing = False
            # A healthy connection has no pending error; _submit_or_terminate
            # short-circuits when _error is set, so it must be None here.
            mock_conn._error = None

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            # ioloop.start() blocks until gate is set, then crashes.
            def ioloop_start():
                gate.wait(timeout=5)
                raise crash

            mock_ioloop.start.side_effect = ioloop_start

            conn = Connection(parameters='params')

        exc_holder = [None]

        def try_channel():
            try:
                conn.channel()
            except Exception as exc:
                exc_holder[0] = exc

        t = threading.Thread(target=try_channel)
        t.start()
        # Give the thread time to enter channel() and register its waiter.
        import time
        time.sleep(0.05)
        gate.set()
        t.join(timeout=2)

        self.assertFalse(t.is_alive(),
                         'channel() thread must not hang after IOLoop crash')
        self.assertIs(exc_holder[0], crash)

    def test_on_connection_open_error_sets_event_and_stops_ioloop(self):
        error = Exception('refused')
        captured = {}

        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                captured['on_open_error_callback'] = on_open_error_callback
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            # Fire the error callback from within ioloop.start() so that
            # self._connection is already assigned when the callback runs.
            def ioloop_start():
                captured['on_open_error_callback'](mock_conn, error)

            mock_ioloop.start.side_effect = ioloop_start

            with self.assertRaises(Exception) as ctx:
                Connection(parameters='params')

        self.assertIs(ctx.exception, error)
        mock_ioloop.stop.assert_called_once()

    def test_on_connection_closed_stops_ioloop(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._on_connection_closed(mock_conn, 'reason')
        mock_ioloop.stop.assert_called_once()

    def test_on_connection_closed_calls_user_callback(self):
        user_cb = MagicMock()
        conn, mock_conn, _ = self._make_connection(on_close_callback=user_cb)
        conn._on_connection_closed(mock_conn, 'reason')
        user_cb.assert_called_once_with(mock_conn, 'reason')

    def test_close_schedules_close_and_joins_thread(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = False
        conn.close()
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        # The scheduled callback must delegate to the inner connection's close.
        mock_ioloop.add_callback_threadsafe.call_args[0][0]()
        mock_conn.close.assert_called_once()
        conn._ioloop_thread.join.assert_called_once_with(timeout=10)

    def test_close_force_stops_ioloop_after_timeout(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = True
        conn.close(timeout=0.1)
        self.assertEqual(mock_ioloop.add_callback_threadsafe.call_count, 2)
        # First call: graceful close wrapper
        mock_ioloop.add_callback_threadsafe.call_args_list[0][0][0]()
        mock_conn.close.assert_called_once()
        # Second call: force-stop the IOLoop
        self.assertIs(
            mock_ioloop.add_callback_threadsafe.call_args_list[1][0][0],
            mock_ioloop.stop)
        self.assertEqual(conn._ioloop_thread.join.call_count, 2)
        # Force-stop must set _closed_reason so subsequent callers do not hang.
        self.assertIsNotNone(conn._closed_reason)

    def test_close_force_stop_wakes_blocked_waiters(self):
        """Threads blocked when the IOLoop is force-stopped must be woken."""
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = True

        waiter_event = threading.Event()
        waiter_error: list[BaseException | None] = [None]
        conn._blocking_waiters.append((waiter_event, waiter_error))

        conn.close(timeout=0.1)

        self.assertTrue(waiter_event.is_set())
        self.assertIsNotNone(waiter_error[0])
        self.assertIsNotNone(conn._closed_reason)
        self.assertEqual(conn._blocking_waiters, [])

    def test_close_safe_close_suppresses_connection_wrong_state_error(self):
        """_safe_close must swallow ConnectionWrongStateError so it does not propagate through the
        IOLoop and crash the IOLoop thread.
        """
        from pika.exceptions import ConnectionWrongStateError
        conn, mock_conn, _ = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = False
        mock_conn.close.side_effect = ConnectionWrongStateError(
            'already closing')

        def run_callback_inline(cb):
            cb()  # execute synchronously — must not raise

        mock_conn.ioloop.add_callback_threadsafe.side_effect = run_callback_inline
        conn.close()  # must not raise
        mock_conn.close.assert_called_once()

    def test_close_is_noop_when_already_closed(self):
        conn, _mock_conn, mock_ioloop = self._make_connection()
        conn._closed_reason = Exception('already closed')
        conn._ioloop_thread = MagicMock()
        conn.close()
        mock_ioloop.add_callback_threadsafe.assert_not_called()
        conn._ioloop_thread.join.assert_not_called()

    def test_close_from_ioloop_thread_calls_connection_close_directly(self):
        conn, mock_conn, _mock_ioloop = self._make_connection()
        # Simulate close() being called from within the IOLoop thread itself
        conn._ioloop_thread = threading.current_thread()
        conn.close()
        mock_conn.close.assert_called_once()

    def test_abort_swallows_close_errors(self):
        conn, _mock_conn, _ = self._make_connection()
        # Force conn.close() to raise
        with patch.object(conn, 'close', side_effect=Exception('boom')):
            conn.abort()  # must not raise

    def test_abort_delegates_to_close(self):
        conn, _mock_conn, _ = self._make_connection()
        with patch.object(conn, 'close') as mock_close:
            conn.abort(timeout=5)
            mock_close.assert_called_once_with(timeout=5)

    def test_blocked_callback_routes_through_add_callback_threadsafe(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        # Wipe call count from setup
        mock_ioloop.add_callback_threadsafe.reset_mock()
        conn.add_on_connection_blocked_callback(MagicMock())
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        mock_conn.add_on_connection_blocked_callback.assert_not_called()

    def test_blocked_callback_registers_on_raw_connection(self):
        conn, mock_conn, mock_ioloop = self._make_connection()

        def execute_immediately(cb):
            cb()

        mock_ioloop.add_callback_threadsafe.side_effect = execute_immediately
        conn.add_on_connection_blocked_callback(MagicMock())
        mock_conn.add_on_connection_blocked_callback.assert_called_once()

    def test_blocked_callback_dispatches_on_worker_thread(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        # Recreate the work pool because _make_connection lets _run_ioloop
        # exit, which shuts the pool down.
        conn._connection_work_pool = _BoundedWorkPool(
            maxsize=1000,
            put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
            thread_name='pika-conn-test')
        conn._connection_pool_shutdown = False
        callback_thread = []
        received = []

        def user_cb(connection, method_frame):
            callback_thread.append(threading.current_thread())
            received.append((connection, method_frame))

        def execute_and_fire(cb):
            cb()
            wrapped = mock_conn.add_on_connection_blocked_callback.call_args[0][
                0]
            wrapped(mock_conn, 'blocked-frame')

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        conn.add_on_connection_blocked_callback(user_cb)
        conn._shutdown_connection_pool()

        self.assertEqual(len(received), 1)
        self.assertIs(received[0][0], conn)
        self.assertEqual(received[0][1], 'blocked-frame')
        self.assertIsNot(callback_thread[0], threading.current_thread())
        self.assertIn('pika-conn', callback_thread[0].name)

    def test_blocked_callback_after_pool_shutdown_logs_and_drops(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        wrapped_holder = []

        def execute_and_fire(cb):
            cb()
            wrapped_holder.append(
                mock_conn.add_on_connection_blocked_callback.call_args[0][0])

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        conn.add_on_connection_blocked_callback(MagicMock())
        conn._shutdown_connection_pool()
        wrapped_holder[0](mock_conn, 'late-blocked')  # must not raise

    def test_blocked_callback_raises_when_connection_closed(self):
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        reason = Exception('closed')
        conn._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            conn.add_on_connection_blocked_callback(MagicMock())

        self.assertIs(ctx.exception, reason)

    def test_unblocked_callback_routes_through_add_callback_threadsafe(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        mock_ioloop.add_callback_threadsafe.reset_mock()
        conn.add_on_connection_unblocked_callback(MagicMock())
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        mock_conn.add_on_connection_unblocked_callback.assert_not_called()

    def test_unblocked_callback_dispatches_on_worker_thread(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._connection_work_pool = _BoundedWorkPool(
            maxsize=1000,
            put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
            thread_name='pika-conn-test')
        conn._connection_pool_shutdown = False
        received = []

        def user_cb(connection, method_frame):
            received.append((connection, method_frame))

        def execute_and_fire(cb):
            cb()
            wrapped = mock_conn.add_on_connection_unblocked_callback.call_args[
                0][0]
            wrapped(mock_conn, 'unblocked-frame')

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        conn.add_on_connection_unblocked_callback(user_cb)
        conn._shutdown_connection_pool()

        self.assertEqual(len(received), 1)
        self.assertIs(received[0][0], conn)
        self.assertEqual(received[0][1], 'unblocked-frame')

    def test_unblocked_callback_raises_when_connection_closed(self):
        conn, _mock_conn, _mock_ioloop = self._make_connection()
        reason = Exception('closed')
        conn._closed_reason = reason

        with self.assertRaises(Exception) as ctx:
            conn.add_on_connection_unblocked_callback(MagicMock())

        self.assertIs(ctx.exception, reason)

    def test_connection_pool_shutdown_is_idempotent(self):
        conn, _, _ = self._make_connection()
        conn._shutdown_connection_pool()
        conn._shutdown_connection_pool()  # second call must be a no-op

    def test_close_force_stop_shuts_down_connection_pool(self):
        """When close() force-stops the IOLoop, the connection pool must be shut down so no callback
        worker thread is left dangling.
        """
        conn, _mock_conn, _ = self._make_connection()
        conn._ioloop_thread = MagicMock()
        # First join() returns with thread still alive, second returns dead
        conn._ioloop_thread.is_alive.return_value = True
        conn.close(timeout=0.01)
        self.assertTrue(conn._connection_pool_shutdown)

    def test_connection_event_listener_exception_is_logged_not_lost(self):
        """A blocked-listener exception must be logged, not silently lost on an unobserved
        Future.
        """
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._connection_work_pool = _BoundedWorkPool(
            maxsize=1000,
            put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
            thread_name='pika-conn-test')
        conn._connection_pool_shutdown = False

        def boom(connection, method_frame):
            raise RuntimeError('blocked-listener boom')

        def execute_and_fire(cb):
            cb()
            wrapped = mock_conn.add_on_connection_blocked_callback.call_args[0][
                0]
            wrapped(mock_conn, 'blocked-frame')

        mock_ioloop.add_callback_threadsafe.side_effect = execute_and_fire

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR') as cm:
            conn.add_on_connection_blocked_callback(boom)
            conn._shutdown_connection_pool()

        self.assertTrue(
            any('add_on_connection_blocked_callback' in msg
                for msg in cm.output),
            msg=f'Expected listener label in logs, got: {cm.output}')

    def test_context_manager(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        conn._ioloop_thread = MagicMock()
        conn._ioloop_thread.is_alive.return_value = False
        with conn as c:
            self.assertIs(c, conn)
        mock_ioloop.add_callback_threadsafe.assert_called_once()
        mock_ioloop.add_callback_threadsafe.call_args[0][0]()
        mock_conn.close.assert_called_once()


class ConnectionInitErrorPathTests(unittest.TestCase):
    """Cover the IOLoop-thread error paths in Connection.__init__."""

    def test_ioloop_crash_before_open_propagates_to_init(self):
        """If ioloop.start() raises before on_open_callback fires, __init__ must surface the
        exception (it cannot silently hang forever).
        """
        crash = RuntimeError('IOLoop crashed before open')
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            MockSelectConn.return_value = mock_conn
            # ioloop.start() raises immediately, before on_open_callback fires.
            mock_ioloop.start.side_effect = crash

            with self.assertRaises(RuntimeError) as ctx:
                Connection(parameters='params')
        self.assertIs(ctx.exception, crash)

    def test_on_connection_open_error_invokes_user_callback(self):
        """When the broker rejects the open, the user-supplied on_open_error_callback must be called
        with the error.
        """
        error = AMQPConnectionError('refused')
        captured = {}
        user_cb = MagicMock()

        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                captured['on_open_error_callback'] = on_open_error_callback
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            def ioloop_start():
                captured['on_open_error_callback'](mock_conn, error)

            mock_ioloop.start.side_effect = ioloop_start

            with self.assertRaises(AMQPConnectionError):
                Connection(parameters='params', on_open_error_callback=user_cb)

        user_cb.assert_called_once_with(mock_conn, error)


class ConnectionCloseFromIOLoopTests(unittest.TestCase):
    """Cover the close-from-IOLoop-thread path in Connection.close()."""

    def _make_connection(self):
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False
            mock_conn.is_closing = False
            # A healthy connection has no pending error; _submit_or_terminate
            # short-circuits when _error is set, so it must be None here.
            mock_conn._error = None

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            conn = Connection(parameters='params')
            conn._ioloop_thread.join(timeout=1)
        return conn, mock_conn, mock_ioloop

    def test_close_from_ioloop_thread_suppresses_connection_close_error(self):
        """When close() runs on the IOLoop thread and connection.close() raises (e.g. already
        closing), the exception must be swallowed and logged, not propagated.
        """
        conn, mock_conn, _ = self._make_connection()
        # Make `current_thread() is self._ioloop_thread` true.
        conn._ioloop_thread = threading.current_thread()
        mock_conn.close.side_effect = RuntimeError('already closing')

        # Must not raise.
        conn.close()
        mock_conn.close.assert_called_once()


class ConnectionEventRegistrationFailureTests(unittest.TestCase):
    """Cover the LOGGER.warning branch when the raw connection's blocked/unblocked registration
    method raises.
    """

    def _make_connection(self):
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False
            mock_conn.is_closing = False
            # A healthy connection has no pending error; _submit_or_terminate
            # short-circuits when _error is set, so it must be None here.
            mock_conn._error = None

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            conn = Connection(parameters='params')
            conn._ioloop_thread.join(timeout=1)
        return conn, mock_conn, mock_ioloop

    def test_blocked_registration_failure_is_logged(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        mock_conn.add_on_connection_blocked_callback.side_effect = RuntimeError(
            'register boom')
        mock_ioloop.add_callback_threadsafe.side_effect = lambda cb: cb()

        with patch(
                'pika.adapters.thread_safe_connection.LOGGER') as mock_logger:
            conn.add_on_connection_blocked_callback(MagicMock())
        mock_logger.warning.assert_called_once()
        self.assertIn('add_on_connection_blocked_callback',
                      mock_logger.warning.call_args[0][1])


class ChannelOpenExceptionPathTests(unittest.TestCase):
    """Cover the exception path inside Connection.channel() when the inner connection.channel() call
    itself raises.
    """

    def _make_connection(self):
        with patch('pika.adapters.thread_safe_connection.SelectConnection'
                  ) as MockSelectConn:
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = True
            mock_conn.is_closed = False
            mock_conn.is_closing = False
            # A healthy connection has no pending error; _submit_or_terminate
            # short-circuits when _error is set, so it must be None here.
            mock_conn._error = None

            def fake_init(parameters, on_open_callback, on_open_error_callback,
                          on_close_callback):
                on_open_callback(mock_conn)
                return mock_conn

            MockSelectConn.side_effect = fake_init
            MockSelectConn.return_value = mock_conn

            conn = Connection(parameters='params')
            conn._ioloop_thread.join(timeout=1)
        return conn, mock_conn, mock_ioloop

    def test_channel_open_propagates_inner_exception(self):
        conn, mock_conn, mock_ioloop = self._make_connection()
        boom = RuntimeError('connection.channel raised')
        mock_conn.channel.side_effect = boom
        mock_ioloop.add_callback_threadsafe.side_effect = lambda cb: cb()

        with self.assertRaises(RuntimeError) as ctx:
            conn.channel(timeout=0.5)
        self.assertIs(ctx.exception, boom)


if __name__ == '__main__':
    unittest.main()
