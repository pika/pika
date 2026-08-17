# Design: Auto-recovery for pika's ThreadSafeConnection

> Status: **proposal**. This is a design doc for review and approval before
> implementation begins. Nothing described here has been built yet. It is
> being circulated to get sign-off on the public API, the concurrency model,
> and the scope of what "recovery" covers before writing code.

## Problem statement

pika has no built-in recovery today: an unexpected connection or channel
loss wakes every blocked caller with an exception and tears the connection
down (`ThreadSafeConnection._on_connection_closed`). Nothing redials, and
nothing remembers what topology or consumers existed. Every pika user
currently hand-rolls reconnect logic (see
`examples/asynchronous_consumer_example.py`,
`examples/blocking_consume_recover_multiple_hosts_retry.py`).

RabbitMQ's Go client (`amqp091-go`) closed the equivalent gap with its
`recovery.go` / `lifecycle.go` design: a topology ledger plus pluggable
reconnection plus per-entity skip/abort error handling. We propose porting
that shape to pika, adapted to pika's concurrency model.

## Scope

The target adapter is **`ThreadSafeConnection` and `ThreadSafeChannel`**
only. `BlockingConnection`
is deprecated with removal planned for pika 2.0, so we don't propose adding
recovery there. Other adapters (asyncio, gevent, select, tornado, twisted)
are out of scope for this proposal.

`ThreadSafeConnection`'s concurrency model differs materially from what a
straight port of amqp091-go would suggest: it runs `SelectConnection`'s
IOLoop on one dedicated background thread, and every blocking call from a
caller thread (`ThreadSafeChannel._blocking_rpc`) works by registering a
`(threading.Event, error-slot)` pair in `self._blocking_waiters`,
scheduling the real work onto the IOLoop thread via
`add_callback_threadsafe`, and blocking the caller thread on the event.
`_on_connection_closed` already has exactly one "wake every blocked caller
with this exception" mechanism, guarded by `self._channel_waiters_lock`.
We propose that recovery **coexist with, rather than replace**, that
mechanism, and that recovery state live in **one coordinator object**
rather than scattered flags across `Channel`/`Connection`, reusing the
existing lock rather than adding a second one.

## Goals

The concrete UX bar we want to hit, and intend to verify with an acceptance
test (`TestConsumeContinuityAcrossRecovery`): an app consuming via
`basic_consume(queue, callback)` keeps receiving messages on the *same*
`callback` after a connection drop and automatic recovery, with **no
application code changes**.

Recovery must be **opt-in** — default behavior stays unchanged unless the
caller passes a `recovery=` config. We intend to cover this with a
regression test (`TestDefaultBehaviorUnchangedWithoutRecoveryConfig`).

## Non-goals

- No changes to `BlockingConnection` or any adapter other than
  `ThreadSafeConnection`.
- Recovery does not attempt to make in-flight synchronous RPCs survive a
  drop transparently (see "In-flight calls" below).
- No multi-host/cluster failover logic beyond what `AMQPConnectionWorkflow`
  already supports for the initial connect (iterating a sequence of
  `Parameters` objects, one per candidate host). The redial loop retries
  against the single `Parameters` the connection was originally opened
  with; it does not walk a host list.

## Proposed public API

**`pika/recovery.py`** (new module):

```python
class TopologyRecoveryMode(enum.Enum):
    """Which topology entities are redeclared after a reconnect."""

    #: Recover all tracked topology: exchanges, queues, bindings,
    #: exchange-to-exchange bindings, and active consumers. The default.
    ALL = 'all'

    #: Recover only connection-scoped (transient) entities: queues declared
    #: as exclusive and/or auto-delete (which includes server-named
    #: queues), auto-delete exchanges, and any bindings that reference one
    #: of those transient entities. Active consumers are still
    #: re-subscribed, since consumer subscriptions are always lost on
    #: reconnect regardless of queue durability. Durable, non-auto-delete
    #: exchanges and queues (and bindings purely between them) are skipped,
    #: since the broker retains them across a network interruption — use
    #: this mode when durable topology is managed declaratively or
    #: out-of-band and only the connection-scoped entities need restoring.
    ONLY_TRANSIENT = 'only_transient'

    #: Disable topology recovery entirely. Neither entities nor consumers
    #: are recovered; connection/channel recovery still happens if
    #: otherwise enabled.
    DISABLED = 'disabled'

@dataclass
class RecoveryConfig:
    max_attempts: int = 5
    initial_interval: float = 1.0
    max_interval: float = 30.0
    backoff_multiplier: float = 2.0
    topology_recovery_mode: TopologyRecoveryMode = TopologyRecoveryMode.ALL
    on_topology_recovery_error: (
        Callable[[ThreadSafeConnection, TopologyRecoveryEntity], bool] | None) = None
    # True (or None, default) -> skip entity, continue recovery.
    # False -> abort this recovery attempt, fall through to the outer retry loop.

    def next_interval(self, attempt: int) -> float: ...   # exponential backoff, capped at max_interval
    def should_skip(self, connection: ThreadSafeConnection,
                     entity: TopologyRecoveryEntity) -> bool: ...

class RecoveryState(enum.Enum):
    IDLE = 'idle'
    RECONNECTING = 'reconnecting'
    FAILED = 'failed'

@dataclass
class TopologyRecoveryEntity:
    entity_type: str          # 'exchange' | 'queue' | 'binding' | 'exchange_binding' | 'consumer' | 'qos'
    name: str
    channel_number: int       # the ThreadSafeChannel this entity's declare/bind/consume call was made on
    secondary_name: str = ''  # exchange for a queue binding, destination for an exchange binding
    routing_key: str = ''
    error: Exception | None = None
```

`topology_recovery_mode` takes a `TopologyRecoveryMode`. Every downstream
call site (`_recover_topology`, the transient-name filtering described in
"Topology ledger" below) reads `config.topology_recovery_mode` and
branches on `TopologyRecoveryMode`.

`TopologyRecoveryEntity.channel_number` and `on_topology_recovery_error`'s
`connection` argument give the app everything it needs to act on a
recovery failure — e.g. to look up the right `ThreadSafeChannel` and
re-issue `basic_consume` itself for a consumer that recovery gave up on.
Every entity type except exchanges is inherently tied to the specific
channel its declare/bind/consume call was made on, so `channel_number` is
what lets an app identify which of potentially many open channels a failed
consumer/binding/queue belonged to; `connection` is what it looks that
`ThreadSafeChannel` up on (e.g. via `connection._channels`). `should_skip`
takes `connection` for the same reason, since it's what forwards to
`on_topology_recovery_error`.

`ThreadSafeConnection.__init__` gains one new kwarg:
`recovery: RecoveryConfig | None = None`. We propose *not* placing this on
`Parameters` — `Parameters` is adapter-neutral and consumed only by the
initial-connect-only `AMQPConnectionWorkflow`; putting a post-open-recovery
config there would silently no-op on every other adapter.

New callback registration methods on `ThreadSafeConnection` (plain lists,
invoked directly — no channel-based listener framework needed):

```python
add_on_close_callback(callback)                # callback(connection, reason)
add_on_open_callback(callback)                 # callback(connection)
add_on_recovery_started_callback(callback)     # callback(connection, reason)
add_on_recovery_succeeded_callback(callback)   # callback(connection, skipped: list[TopologyRecoveryEntity])
add_on_recovery_failed_callback(callback)      # callback(connection, error)
```

`add_on_close_callback` fires whenever the connection closes, for any
reason — an explicit `close()`, a drop with no `RecoveryConfig`
attached, or recovery exhausting its retry budget — the same "any
cause" role `ThreadSafeChannel.add_on_close_callback` plays for a
channel, and available regardless of whether recovery is configured at
all. It complements, rather than replaces, the single `on_close_callback`
constructor kwarg `ThreadSafeConnection` already accepts: that one
callback keeps firing exactly as it does today, from the same
`_finalize_closed`/teardown path, and `add_on_close_callback` is simply
the list-based way to register additional listeners for the same event.
`add_on_open_callback` fires each time `_try_reconnect_once` swaps in a
freshly dialed connection, mirroring the channel-level one — this only
happens on a successful redial, so it never fires on a connection
without `RecoveryConfig`.

The three `add_on_recovery_*_callback` methods would each raise
`ValueError` if called on a connection that wasn't constructed with a
`RecoveryConfig`; `add_on_close_callback`/`add_on_open_callback` do not
have that restriction — the former is meaningful with or without
recovery, and the latter simply never fires without it.

New callback registration methods on `ThreadSafeChannel`, following the
same plain-list pattern used for `add_on_return_callback`/
`add_on_cancel_callback` (dispatched on the channel's own consumer work
pool, not the IOLoop thread):

```python
add_on_close_callback(callback)             # callback(channel, reason)
add_on_open_callback(callback)              # callback(channel)
add_on_recovery_started_callback(callback)  # callback(channel, reason)
add_on_recovery_succeeded_callback(callback)  # callback(channel, skipped)
add_on_recovery_failed_callback(callback)   # callback(channel, error)
```

`add_on_close_callback` fires whenever this channel closes, for any
reason: broker soft-error, app `close()`, or a connection-wide drop.
`add_on_open_callback` fires each time a fresh raw channel is swapped in
by recovery, whether by a connection-level or single-channel pass.

The three `add_on_recovery_*_callback` methods mirror the
connection-level ones above in signature, but **`_recover_channel` fires
these channel-scoped lists instead of the connection-wide ones** — it
never appends to `coordinator.on_recovery_started`/`_succeeded`/`_failed`.
Symmetrically, `_recover_connection` never touches a channel's lists.
This is a deliberate partition, not an additive convenience: firing each
pass's outcome only on the lists belonging to the object it actually
happened to means a handler registered on the connection only ever hears
about connection-wide passes, and a handler registered on a channel only
ever hears about that channel's own passes, with the channel identity
implicit in *which* list fired rather than needing to be read out of an
argument. This removes the ambiguity structurally, rather than papering
over it with an optional channel argument or a `None`-sentinel on the
connection-level signatures, which every connection-level handler would
then need to branch on.

No new exception type gates calls made while reconnecting — auto-recovery
does not intercept or alter any existing method's behavior, and calls
made during a reconnect are evaluated exactly as they always are, against
whatever the connection or channel currently is (see "Calling into the
connection during recovery" below). An app that wants to hold off on new
work until recovery finishes should coordinate through the `on_recovery_*`
callbacks (we'd ship this pattern in a new
`examples/thread_safe_recovery_example.py`).

## Recovery coordinator (single source of truth)

We propose `ThreadSafeConnection._recovery: RecoveryCoordinator | None`,
`None` when the caller didn't opt in — this one attribute would gate every
recovery code path. `RecoveryCoordinator` (in `pika/recovery.py`) would
hold:

- `config: RecoveryConfig`
- `state: RecoveryState`
- `attempt: int`
- `topology: dict[int, ChannelTopology]` — **the single, connection-wide
  topology store, keyed by channel number.** See "Topology ledger" below for
  why this must live here rather than on each `ThreadSafeChannel`.
- `on_recovery_started` / `on_recovery_succeeded` / `on_recovery_failed`:
  plain callback lists

Server-generated queue renames need a name-replacement map too (old
broker-assigned name → new one), but only for the duration of the single
phased pass that produces the rename — the queue-recovery phase notes it,
the binding/consumer-recovery phase later in the *same* pass consults it
to patch stale references, and once the pass returns every reference has
already been rewritten directly into `topology`. Nothing outside that one
pass ever needs the mapping again, so it is a local `dict[str, str]`
inside `_recover_topology(channels)`, not a `RecoveryCoordinator` field —
unlike `topology` itself, which is read and written continuously across
the connection's whole lifetime, this map has no state to hold once its
one pass ends. Keeping it local also means it needs no locking of its own:
`_recover_topology`'s declare/bind/consume RPCs already run outside
`_channel_waiters_lock` (see above), and only the thread running that pass
ever touches its own local map.

We propose no `RecoveryState.CLOSING` — `ThreadSafeConnection.close()`
would keep its own explicit-close path (`_closed_reason` set directly, no
coordinator involvement); the coordinator would only ever represent
"recovering or not." Coordinator mutations would happen under the existing
`_channel_waiters_lock` (reused, not a new lock) — but only for the
mutation itself. `_recover_topology` must snapshot (deep-copy) `topology`
under the lock, release it, and only then issue the blocking
`ch.exchange_declare(...)`/`ch.queue_declare(...)`/etc. RPCs against that
snapshot. Holding `_channel_waiters_lock` across those calls would risk
deadlocking against any other thread concurrently blocked in
`_register_waiter` for an unrelated RPC, since that's the same lock.

**Why one lock, not a dedicated one for the coordinator:** a second lock
would not reduce contention here — every critical section this design
adds is a dict/list mutation, never an RPC (the snapshot-then-release
pattern above is what keeps it that way), so there's no long hold for a
second lock to route around; it would just add a second acquisition per
call for the same amount of held time. It would, however, introduce a
real risk: `_recover_channel`'s abandonment check (see "Preventing
re-entrant and cross-level channel recovery" below) reads
`_closed_reason`, `ch._closed`, and `coordinator.state` together as one
atomic snapshot, not as three independently-locked reads that could
interleave with a state transition landing in between. Splitting
coordinator state onto its own lock would mean that check, and every
other site that reads connection-level and coordinator-level state
together, has to acquire both locks in a consistently maintained order,
forever — exactly the kind of two-lock discipline that is easy to get
right once and easy to violate later when a new call site is added.
Reusing the single existing lock removes that failure mode by
construction, and gives `_recover_topology`'s snapshot the same atomicity
against concurrent `record_*`/`remove_*`
calls it would need from a dedicated lock anyway, at no extra cost.

**Why `topology` lives on the coordinator and not per-channel:** AMQP
exchanges, queues, and bindings are scoped to the *connection* (vhost), not
to the channel that happened to declare them — a binding created on
channel B can reference an exchange declared on channel A, and either
channel can legally delete an entity the other one created. A store
scoped to an individual `ThreadSafeChannel` cannot represent that
correctly: recovery order would become dependent on which channel happens to
be processed first, and removal issued on one channel could not reach a
record living under another channel's entry. See "Topology ledger" below
for the concrete failure modes this must avoid, and why this is a Phase 4
correctness requirement, not an optional refinement.

## Where recovery hooks in

`ThreadSafeConnection._on_connection_closed` is the single funnel for all
connection death today. We propose it branch as follows:

```python
def _on_connection_closed(self, _connection, reason):
    _connection.ioloop.stop()          # stop the REPORTING connection's own ioloop
    if _connection is not self._connection:
        return                          # stale notification from an already-superseded connection
    if self._recovery is not None and not isinstance(reason, ConnectionClosedByClient):
        # ... plant RECONNECTING state (idempotently), wake blocking waiters,
        # fire on_recovery_started, spawn the recovery thread ...
        return
    # existing behavior, unchanged: wake waiters, set _closed_reason, user close callback
```

Two details are worth flagging up front, since they're easy to get wrong
and we want reviewers to weigh in before we build against them:

1. **Stop the reporting connection's ioloop, not necessarily
   `self._connection`'s.** If a stale/delayed close notification arrives
   from a connection that recovery has already superseded (e.g. right after
   a successful reconnect swapped `self._connection` to a new, healthy
   object), naively stopping "whatever `self._connection` currently is"
   would incorrectly stop the *new* connection's ioloop, manufacturing a
   spurious second "drop." The proposed fix is to operate on the
   `_connection` parameter and return early if it isn't the current one.
2. **Recovery trigger condition:** we propose `not isinstance(reason,
   ConnectionClosedByClient)` rather than an enumerated allow-list (e.g.
   `StreamLostError`, `ConnectionClosedByBroker`, `AMQPHeartbeatTimeout`,
   `ConnectionBlockedTimeout`). Recovering on "everything except an
   explicit client close" is simpler to reason about and more robust
   against future exception types than maintaining an allow-list.

**Existing in-flight blocking waiters would still be woken immediately**
with the close reason when recovery starts — we do not propose making a
synchronous RPC blocked mid-network-drop resume transparently. What
recovery would make transparent is: (a) *future* calls succeeding again
once reconnected, and (b) consumer deliveries resuming on the *same*
callback without the app re-issuing `basic_consume`.

### Calling into the connection during recovery

Auto-recovery does not intercept, block, or otherwise alter the behavior
of any existing public method: `_check_not_closed`, `_register_waiter`,
and `channel()` read exactly the same state they always have
(`_closed_reason`), whether or not a `RecoveryConfig` is attached, and
none of them read `coordinator.state`. A call made while reconnecting runs
exactly as it would on a healthy connection, against whatever the
connection or channel happens to be at that moment — the same code path
recovery's own topology replay uses to redeclare things.

The consequence worth documenting for app authors, mirroring the
equivalent note in `amqp091-go`'s own connection docs: a call issued while
a `RecoveryConfig`-enabled connection is reconnecting is evaluated against
a connection or channel that may be mid-teardown or not yet re-established,
so it may not produce the result the app expects until recovery
completes. That's an expected consequence of auto-recovery being purely
additive rather than a gap to patch — the alternative would mean
`ThreadSafeConnection` behaves differently depending on whether a
`RecoveryConfig` happens to be attached, for every existing method, which
we'd rather not introduce. Concretely, such a call lands in one of two
ordinary places, neither of which corrupts anything:

- If the redial hasn't reconnected yet, the call schedules its work onto
  the *old* connection's IOLoop, which has already stopped polling; it
  just blocks until its own `timeout` elapses and raises an ordinary
  `TimeoutError`.
- If the redial has already reconnected but this particular channel
  hasn't been reopened yet, the call is bound to the channel's stale raw
  object and gets pika's own ordinary "channel is closed" exception —
  the same exception any already-closed channel produces.

Apps that use `recovery=` should coordinate through
`on_recovery_started`/`on_recovery_succeeded`/`on_recovery_failed` and
hold off on issuing anything but `close()`/`abort()` while a reconnect is
in progress, the same discipline `amqp091-go` documents for its own
`NotifyStateChange`. This also means the recovery pass itself needs no
special-casing: `_recover_topology` simply calls
`ch.queue_declare(...)`, `ch.basic_consume(...)`, etc. — the exact same
methods an app calls.

**`close()` on both the connection and the channel is unconditional**,
matching `amqp091-go`'s own model: each just flips an atomic "closing"
signal and does its normal job, and whatever recovery loop is running is
solely responsible for noticing that signal and unwinding itself —
`close()` never asks recovery for permission or waits on it, and never
reads `coordinator.state`.

`ThreadSafeConnection.close()` sets `_closed_reason` (idempotently, under
`_channel_waiters_lock`; returns immediately if already set) and calls
`self._recovery_cv.notify_all()` in that same locked block — this *is*
the signal, waking the redial loop immediately if it's mid-backoff rather
than leaving it to notice only when the current interval elapses on its
own (see "Waking a sleeping backoff early" below). It then
unconditionally best-effort schedules `self._connection.close()` via
`add_callback_threadsafe`, and joins whichever of
`self._recovery_thread`/`self._ioloop_thread` is actually alive right
now. If `self._connection` happens to be stale (mid-redial, its IOLoop
already stopped), the scheduled callback silently never runs — harmless,
since `_closed_reason` is already set and the (now awake) redial loop
will notice on its next check regardless. If `self._connection` happens
to already be the fresh, live one (recovery is mid-topology-replay), the
close attempt actually goes out and interrupts that replay directly — a
faster abort than waiting for the next poll, not a bug.

`ThreadSafeChannel.close()`'s first line would be `self._closed = True`,
unconditionally, followed immediately by the same
`self._wrapper._recovery_cv.notify_all()` — that *is* its signal, and
`_recover_channel`'s retry loop checks it every iteration, now woken
immediately rather than on its next natural backoff expiry. If
the channel happens to be live right now, the real `Channel.Close` goes
out immediately, same as any other time; if it isn't (recovery hasn't
reopened it yet, or the connection is mid-redial), `is_closed`/`is_closing`
is already true and `close()` returns almost at once.

### Redial loop

We propose it run on a **new dedicated thread** (`pika-recovery-{id}`) — it
cannot reuse the dying `_ioloop_thread`/`SelectConnection` since that IOLoop
is being stopped. The thread object is stored as `self._recovery_thread`
at spawn time (alongside `state = RECONNECTING`, in `_on_connection_closed`,
under `_channel_waiters_lock`) — this is the reference `close()` joins
when this thread, rather than `_ioloop_thread`, is the one actually alive.
Proposed as `ThreadSafeConnection._recover_connection(reason)`:

1. Loop up to `config.max_attempts`, waiting up to `config.next_interval(attempt)`
   (exponential backoff capped at `max_interval`) before each attempt via
   `self._recovery_cv`'s deadline-loop wait rather than `time.sleep(...)` —
   see "Waking a sleeping backoff early" below — with a `_closed_reason`
   check before and on every wake during the wait, so an explicit
   `close()` aborts promptly without waiting for the rest of the current
   backoff interval to elapse.
2. Each attempt calls `_try_reconnect_once()`, which:
   - Builds a fresh `SelectConnection` and a fresh IOLoop thread. Open
     design question: should this share construction logic with
     `__init__` via extracted `_open_inner_connection`/
     `_start_ioloop_thread` helpers, or should the redial path be
     self-contained? Sharing avoids duplication but means retrofitting
     `__init__`; a self-contained path is more isolated but duplicates
     some connection-construction logic. We lean toward extraction if it
     doesn't complicate `__init__`, but want input before committing.
   - On success, swaps `self._connection`/`self._ioloop_thread` under
     `_channel_waiters_lock`, and fires `add_on_open_callback(connection)`
     right after the swap — before `_reopen_channels_and_recover_topology`
     runs, so a listener sees "we have a live connection again" as its own
     event, distinct from and earlier than "topology is fully recovered"
     (`on_recovery_succeeded`). Existing `ThreadSafeChannel`/
     `ThreadSafeConnection` references the app holds would stay valid.
   - Calls `_reopen_channels_and_recover_topology()` (see below).
3. On success: `state = IDLE`, `attempt = 0`, fire
   `on_recovery_succeeded(connection, skipped)`. There is no
   `_closed_reason` to clear here — reconnecting never sets it.
4. On exhaustion: `state = FAILED`, `_closed_reason = last_error`, wake any
   remaining blocking waiters, run the original pool-shutdown/close-callback
   teardown, then fire `on_recovery_failed(connection, last_error)`.

**Waking a sleeping backoff early.** A plain `time.sleep(interval)` can't
be interrupted — if an app calls `close()` while the redial loop is
mid-backoff, the loop has no way to notice until the current interval
elapses on its own, up to `max_interval` (30 seconds by default) after
`close()` was called. `close()` itself only blocks up to its own
`timeout` joining `self._recovery_thread`, so a backoff longer than that
would mean `close()` returns while the thread is still asleep in the
background. We propose `self._recovery_cv: threading.Condition`, built
over the existing `_channel_waiters_lock` (`threading.Condition(self._channel_waiters_lock)`,
created only when a `RecoveryConfig` is supplied), in place of a plain
sleep, notified by both `close()` methods right after they set their own
flag (`_closed_reason` / `ch._closed`), under the same lock.

Since this one condition variable is shared by the redial loop and every
concurrent `_recover_channel` loop, a `notify_all()` wakes all of them,
not just the one whose flag actually changed — closing channel 1 while
channel 2 is independently backing off wakes channel 2's loop too, even
though nothing about channel 2 changed. A single `wait(timeout=interval)`
call per attempt would handle that wrong: channel 2's loop would wake,
find its own abandonment condition still false, and fall straight through
to an early retry — cutting its backoff short every time some unrelated
channel happens to close, which defeats the point of backing off at all.
We propose looping the wait against a deadline instead of calling it
once, the standard pattern for a condition variable guarding a timed
wait: compute `deadline = time.monotonic() + config.next_interval(attempt)`
once, then repeatedly wait for whatever time remains and re-check this
loop's *own* abandonment condition on every wake, only proceeding to the
next attempt once the deadline has actually passed. A wake caused by an
unrelated channel's `notify_all()` finds nothing to abandon for and goes
back to waiting out the remaining time, unchanged; a wake caused by this
channel's own `close()` (or the connection's) finds its abandonment
condition true and exits immediately, regardless of how much of the
interval was left.

`_reopen_channels_and_recover_topology()` would:
- Reopen every `ThreadSafeChannel` in `self._channels` that the app hasn't
  explicitly closed (tracked via a proposed `ThreadSafeChannel._closed`
  flag, set by its own `close()` — needed because `self._channels` never
  removes entries). Reopening would call `self._connection.channel(...)`
  directly and repoint `ch._channel` in place — closing the previous raw
  channel first if it's still open, rather than only overwriting the
  attribute, so a channel that was already reopened by something else
  (see "Preventing re-entrant and cross-level channel recovery" below)
  doesn't get orphaned on the broker.
- If `config.topology_recovery_mode` is `TopologyRecoveryMode.DISABLED`, return
  without recovering anything (channels are still reopened, so basic
  pub/sub keeps working; nothing is redeclared/resubscribed).
- Otherwise (`ALL` or `ONLY_TRANSIENT`) call `_recover_topology(channels)`,
  which redeclares topology through the same public
  `queue_declare`/`basic_consume`/etc. methods an app would call, with no
  special-casing needed — see "Calling into the connection during
  recovery" above.

### Channel-level recovery (broker-initiated single-channel close)

A broker soft-error (e.g. 406 `PRECONDITION_FAILED`, 404 `NOT_FOUND`) can
close one channel while the connection stays healthy. We propose
`ThreadSafeChannel.__init__` register a permanent close listener
(`_register_recovery_close_listener`, scheduled via
`add_callback_threadsafe`) whenever `wrapper._recovery is not None`, wired
to `ThreadSafeConnection._on_channel_closed_for_recovery`.

**Design risk to flag:** checking `coordinator.state` alone (skip if
`RECONNECTING`) is *not* sufficient to avoid a channel-level recovery pass
racing a connection-level one. pika notifies every channel of a
connection-wide loss via this same close-listener callback *before*
invoking `_on_connection_closed` — by the time each channel's listener
fires, `coordinator.state` is still `IDLE` (the connection-level path
hasn't set it to `RECONNECTING` yet). A state check alone would let a
channel-level recovery pass spawn concurrently with the connection-level
one, corrupting the connection's frame stream and causing a genuine second
TCP reset. We propose guarding with a check of `self._connection.is_open`
*first*: if the underlying connection is already dead, this is a
connection-wide event that the connection-level pass will handle, and the
channel-level path returns immediately without touching
`coordinator.state` at all. We plan to validate this ordering assumption
against a real broker before relying on it.

`_recover_channel(ch, reason)` would mirror `_recover_connection`'s retry
loop but scoped to one channel: fire `ch.on_recovery_started(ch, reason)`,
reopen just this channel, call `_recover_topology([ch])`. On success it
would fire `ch.on_recovery_succeeded(ch, skipped)`; on exhaustion
`ch.on_recovery_failed(ch, reason)` — **never** the connection-wide
`coordinator.on_recovery_*` lists, which stay reserved for
`_recover_connection`. Exhaustion still does **not** tear down the whole
connection (it's still healthy) — that one channel would simply be left
closed. Note that `_recover_topology([ch])` still recovers against the
full connection-wide `coordinator.topology` (filtered to entries relevant
to `ch`), not a store scoped to `ch` alone — see "Topology ledger" below.
`_reopen_channel(ch)`, wherever it is called from (here, in
`_reopen_channels_and_recover_topology`, or in the skip-and-continue path
below), would fire `ch`'s `add_on_open_callback` list once the fresh raw
channel is in place; `_on_channel_closed_for_recovery` would fire `ch`'s
`add_on_close_callback` list on every invocation, regardless of which
branch below it takes, so the app sees a close notification even for
closes this design otherwise treats as internal no-ops (e.g. one racing an
already-in-flight connection-level pass).

### Preventing re-entrant and cross-level channel recovery

Two distinct races follow from a channel-level pass and a connection-level
pass both being able to touch the same `ThreadSafeChannel`. Each needs its
own guard; neither is covered by the `is_open`-first check above, which
only stops a channel-level pass from *starting* while a connection-level
one is already running — it says nothing about a channel-level pass
that's already in flight when a connection-level one begins, or about a
channel-level pass racing a second copy of itself.

**Scenario A — a channel-level pass retriggers itself.**
`_recover_channel`'s own `_recover_topology([ch])` call can hit a
broker-side protocol error that closes the channel again mid-pass — the
same situation the "skip-and-continue must reopen the channel" section
below describes — and that close fires
`_register_recovery_close_listener`'s callback exactly as an unrelated
soft-error would, spawning a *second* `_recover_channel` thread for a
channel a first one already owns. When the conflicting entity is
permanent (e.g. redeclaring an exchange with mismatched durability), every
pass hits it again, gets closed again, and spawns another pass in turn —
an unbounded chain, even though `should_skip` lets each individual pass
report success. This is the same failure shape `amqp091-go` fixed with a
per-channel `recoveringTopology` flag (commit `2219a1fb`): a recovery pass
owning a channel must suppress that channel's own close-notification for
the pass's duration, or the notification re-triggers a redundant pass that
hits the same conflict forever.

We propose the same shape for pika, with the flag set and checked
atomically in the same place the spawn decision is made, not inside
`_recover_channel`'s own body: `ThreadSafeChannel` gains a private
`_recovering: bool`, and `_on_channel_closed_for_recovery` — while still
holding `_channel_waiters_lock` for the guard checks described above — sets
`ch._recovering = True` in the same locked block where it decides to spawn
the `_recover_channel` thread, *before* `thread.start()` returns control:

```python
with self._channel_waiters_lock:
    if ch._closed or ch._recovering or isinstance(reason, ChannelClosedByClient):
        return
    ch._recovering = True
# thread.start() happens after releasing the lock
```

Setting the flag inside `_recover_channel`'s body instead (its first
statement, on the newly spawned thread) would leave a window — probably
never hit in practice, since no second close can land on `ch` before it's
reopened, but not provably closed either — between the thread being
spawned and the flag actually landing. Setting it under the same lock and
in the same branch as the spawn decision removes that window by
construction, and mirrors the approach proposed above for
`_on_connection_closed`, which plants `state = RECONNECTING` before
spawning `_recover_connection`, rather than letting `_recover_connection`
set its own state after it starts running.
`_recover_channel` clears `ch._recovering` in a `finally` spanning its
entire retry loop — every backoff sleep and every attempt, not just the
`_recover_topology` call inside one attempt — so it comes back `False` on
every exit path: success, exhaustion, or an unexpected exception.

We propose scoping this flag to the channel rather than putting an
equivalent boolean on the coordinator — a connection-wide flag would
incorrectly suppress an unrelated channel's independent recovery while
this one is still mid-pass. The race it guards against (Scenario A) is
about a second `_recover_channel` *thread* spawning for a channel a first
one already owns, which is a fact about concurrent threads, not about a
call tripping a check — so `_on_channel_closed_for_recovery` has to
consult this flag at the exact point the second thread would be spawned;
there is no way to make the check unnecessary by construction the way
"Calling into the connection during recovery" removes the equivalent
question for ordinary RPCs.

**Scenario B — a connection-level pass starts while a channel-level pass
already owns a channel.** The reverse direction from Scenario A: thread A
is mid-`_recover_channel` for `ch` (backing off, or blocked inside a
`_recover_topology([ch])` RPC) when the *entire* connection drops for
real. `_on_connection_closed` fires, `state` flips to `RECONNECTING`, and
a separate `_recover_connection` thread starts. Its
`_reopen_channels_and_recover_topology()` reopens *every* channel in
`self._channels` the app hasn't explicitly closed — with no check of
`ch._recovering` before doing so. Nothing stops thread A and the
connection-level pass from both calling `_reopen_channel(ch)` for the same
channel at overlapping times, racing on `ch._channel`/`ch._generation` and
potentially leaking an orphaned raw channel from whichever pass loses.

We propose closing this from both sides:

- `_recover_channel`'s abandonment check described above
  (`self._closed_reason is not None or ch._closed`, checked at the top of
  every retry-loop iteration) gains a third condition: `self._recovery.state ==
  RecoveryState.RECONNECTING`. Once a connection-level pass has taken
  over, the channel-level pass's own work is moot — it bails out at its
  next iteration boundary (bounded by the current `next_interval(attempt)`
  backoff, the same bound that already governs how quickly it notices an
  explicit `close()`), clears `ch._recovering` in its `finally`, and logs
  at `info` rather than firing `ch.on_recovery_failed` — this is a
  hand-off, not a failure the app needs to hear about as one.
- `_reopen_channels_and_recover_topology()` — before calling
  `_reopen_channel(ch)` for a given channel — waits for that channel's
  `_recovering` flag to clear, polling under `_channel_waiters_lock` with
  a short sleep between checks. The wait is bounded by the same backoff
  interval that bounds the channel-level pass's own abandonment check
  above, so this cannot hang indefinitely: the channel-level pass is
  always the one that yields, since the connection-level pass represents
  a real, total loss the channel-level pass cannot recover from on its
  own (it would be retrying against a connection that no longer exists).

**The flag clearing doesn't mean the channel-level pass failed — it might
have finished.** `ch._recovering` clearing tells
`_reopen_channels_and_recover_topology()` only "the channel-level pass is
no longer running," not *why* it stopped. Two different things can be
true when it clears: the pass noticed `state == RECONNECTING` and yielded
before finishing (the case above), or it had already reopened the channel
and redeclared its topology successfully — its own abandonment check only
runs once per retry-loop *iteration*, not continuously, so an attempt
already in flight when `state` flips can still complete, against whatever
`self._connection` happens to be at that moment, including the very
connection the connection-level pass just dialed. Either way,
`_reopen_channels_and_recover_topology()` proceeds to reopen and redeclare
that channel again as part of its own complete sweep — it doesn't try to
detect "was this one already handled," since that would mean the
connection-level pass's completeness depends on knowing what an unrelated
pass did moments earlier. That redundant work is wasteful but not
incorrect on its own, with one exception worth closing: if the
channel-level pass actually succeeded, its raw channel is still open on
the broker when `_reopen_channel(ch)` replaces `ch._channel` with a fresh
one. Simply overwriting the reference would orphan that channel number —
never sent a `Channel.Close`, but no longer tracked by anything client-side
— until the whole connection eventually goes away. We propose
`_reopen_channel(ch)` explicitly close any existing, still-open
`ch._channel` before installing its replacement, rather than only
overwriting the attribute. That closes the leak uniformly regardless of
which of the two cases above produced the redundant reopen, rather than
requiring the design to distinguish them.

`_recover_topology(channels)` is a **connection-level, phased pass over the
union of `coordinator.topology`'s entries** — all exchanges (across every
channel) before any queue, all queues before any binding, all bindings
before any consumer. This ordering is required because AMQP
exchanges/queues/bindings are vhost-scoped, not channel-scoped: a binding
declared on channel B can reference an exchange declared on channel A, so
recovering one channel's entries start-to-finish before moving to the next
would let channel B's bind 404 against an exchange channel A hasn't
redeclared yet. Because `topology` is one shared, connection-wide store
(see "Recovery coordinator" above), the phased pass naturally spans every
channel regardless of which one originally declared each entity. This is a
Phase 4 acceptance requirement, covered by
`TestMultiChannelTopologyRecoveryOrdering` below, not an item to defer.

## Topology ledger

**This section specifies the connection-wide store the previous two
sections referenced — its shape is a Phase 4 correctness requirement, not
a refinement to bolt on after the fact.** AMQP exchanges, queues, and
bindings are scoped to the connection (vhost), not to whichever channel
happened to declare them, and any channel can reference or delete an
entity another channel created. A store that isolated each channel's
entries from every other channel's would risk two concrete,
broker-reproducible failure modes that any topology-recovery design must
rule out up front:

1. **Ordering failure.** Channel A declares exchange `X` and queue `Q`;
   channel B declares a binding from `Q` to `X`. If entries were recovered
   channel-by-channel (all of B's entries, then all of A's, or vice versa
   depending on iteration order) and B happened to be processed before A,
   B's `queue_bind` would 404 against an exchange and queue that don't
   exist yet — recovery failing non-deterministically depending on
   iteration/dict order, for topology that was perfectly valid before the
   drop.
2. **Split-brain removal.** Channel A declares queue `X`; channel B later
   calls `queue_delete('X')`. If removal only searched the calling
   channel's own records, B's delete would find nothing (it never had a
   record for `X`, since A declared it) and silently no-op against the
   store, even though the broker-side deletion succeeds. `X` would remain
   tracked under A's entry and get incorrectly redeclared on the next
   recovery, as a queue the app had explicitly deleted.

`amqp091-go` rules out both failure modes by storing topology once, per
connection, keyed by channel ID (`Connection.topologyConfiguration
map[uint16]*TopologyConfiguration`, guarded by one `topologyM` mutex), and
by making removal operate on that whole map regardless of which channel
issued the delete (`removeQueue(name)` and `removeBinding(bc)` take no
channel argument — they scan every channel's bucket). We propose the same
shape for pika: `coordinator.topology: dict[int, ChannelTopology]`, keyed
by `channel_number`, lives on `RecoveryCoordinator` (see "Recovery
coordinator" above) as the single source of truth — `ThreadSafeChannel`
itself holds no topology state. `ChannelTopology` holds:

```python
@dataclass
class ExchangeRecord: name, exchange_type, durable, auto_delete, internal, arguments
@dataclass
class QueueRecord: declared_name, actual_name, durable, exclusive, auto_delete, arguments
@dataclass
class BindingRecord: queue, exchange, routing_key, arguments
@dataclass
class ExchangeBindingRecord: destination, source, routing_key, arguments
@dataclass
class ConsumerRecord: queue, consumer_tag, on_message_callback, auto_ack, exclusive, arguments

@dataclass
class ChannelTopology:
    exchanges: dict[str, ExchangeRecord] = field(default_factory=dict)
    queues: dict[str, QueueRecord] = field(default_factory=dict)
    bindings: list[BindingRecord] = field(default_factory=list)
    exchange_bindings: list[ExchangeBindingRecord] = field(default_factory=list)
    consumers: dict[str, ConsumerRecord] = field(default_factory=dict)
    qos: tuple[int, int, bool] | None = None
    confirm_select: bool = False
    confirm_ack_nack_callback: Callable[[Any], None] | None = None
```

We propose the field name `exchange_type` (matching pika's own parameter
name throughout `ThreadSafeChannel`) rather than a generic `kind`.

**Recording:** `ThreadSafeChannel.exchange_declare`, `queue_declare`,
`queue_bind`, `exchange_bind`, `basic_consume`, `basic_qos`,
`confirm_delivery` each call a coordinator method —
`self._connection._recovery.record_exchange(self.channel_number, record)`
and so on — guarded by `if self._connection._recovery is not None:`, after
the broker ack succeeds. Each coordinator `record_*` method takes the lock
briefly, fetches-or-creates that channel's `ChannelTopology` bucket in
`topology`, and upserts — the channel itself never owns the data, it only
ever asks the connection to mutate or read it.

**Removal:** `exchange_delete`, `queue_delete`, `queue_unbind`,
`exchange_unbind`, `basic_cancel` symmetrically call coordinator methods —
`remove_exchange(name)`, `remove_queue(name)`, etc. — that take **no
channel argument** and scan every bucket in `topology`. `remove_queue`/
`remove_exchange` additionally cascade: deleting a queue removes any
binding referencing it from every bucket, returning the exchanges those
bindings sourced from so an auto-delete exchange left with no remaining
bindings can be forgotten too.

**`TopologyRecoveryMode.ONLY_TRANSIENT` narrows what `_recover_topology`
redeclares.** Before the phased pass begins, it unions transient queue and
exchange names across *every* channel's bucket in `coordinator.topology` —
a queue is transient if `exclusive` or `auto_delete` is set (this includes
server-named queues, which are always both), an exchange is transient if
`auto_delete` is set — then filters each phase down to: transient
exchanges only; transient queues only; bindings/exchange-bindings that
reference at least one transient queue or exchange (redeclaring a
transient queue drops all of its bindings, including ones to durable
exchanges, so those bindings must still be recreated). Consumers, QoS, and
confirm mode are *never* filtered by this mode — they're lost with the
channel on every reconnect regardless of the durability of the queue
behind them, so they're always recovered. Durable, non-auto-delete
exchanges/queues (and bindings purely between them) are skipped entirely
under this mode, since the broker already retained them across the
network interruption. The cross-channel union step matters for the same
reason phased-pass ordering does above: a binding declared on channel B
may reference a transient queue declared on channel A, and the filter has
to recognize that queue as transient even though it isn't in channel B's
own bucket.

`basic_consume`'s `_wrapped_callback` closes over the caller-supplied
`on_message_callback` value directly, not over any mutable pika-internal
state — so recovering via `ch.basic_consume(queue, on_message_callback,
consumer_tag=tag, ...)` with the stored `ConsumerRecord` fields should
re-create an identical `_wrapped_callback` closure around the *same*
Python callback object. This is the mechanism the core UX requirement
(`TestConsumeContinuityAcrossRecovery`) depends on: the app should never
see a new callback, never re-call `basic_consume`, and messages should
keep flowing through the same per-channel `_consumer_work_pool` worker
thread as before.

Server-generated queue names: `queue_declare('')` would record
`declared_name=''`, `actual_name=<broker-assigned>`. On recovery, we'd
redeclare with `declared_name=''` again (getting a new broker name), note
the rename in `_recover_topology`'s local name-replacement map, and
rewrite the `actual_name` on that one record directly in
`coordinator.topology`, plus any `BindingRecord`/`ConsumerRecord` across
*every* bucket still referencing the old name — one shared-map edit,
rather than a rename call repeated per channel — before bindings/consumers
are recovered.

### Skip-and-continue must reopen the channel

**Design risk to flag:** a broker-side protocol error during topology
recovery (e.g. a 404 on a binding referencing a since-deleted queue)
closes the **entire channel**, not just the offending entity — AMQP
channel-level errors always close the channel. We propose
`_recover_topology`'s `_skip_or_abort` helper check `ch.is_closed` after
recording a skip and, if so, call `_reopen_channel(ch)` before continuing
with the channel's remaining entities. Without this, the first skipped
entity would silently abort recovering everything else on that channel
while still reporting overall success. We plan to validate this against a
real broker (`TestDeletedQueueSkipAndContinue`, below) before considering
the design settled.

## Proposed file-by-file changes

- **`pika/recovery.py`** (new): `RecoveryConfig`, `RecoveryState`,
  `RecoveryCoordinator` (holding `topology: dict[int, ChannelTopology]` and
  the `record_*`/`remove_*` methods described in "Topology ledger" above),
  `ChannelTopology`, the `*Record` dataclasses, `TopologyRecoveryEntity`.
- **`pika/adapters/thread_safe_connection.py`**: `ThreadSafeConnection.__init__`
  gains `recovery=`, `self._recovery`, `self._recovery_thread`,
  `self._recovery_cv` (a `threading.Condition` over `_channel_waiters_lock`,
  used to wake a sleeping backoff early — see "Waking a sleeping backoff
  early" above), `self._parameters`, `self._connect_timeout`; `add_on_close_callback`,
  `add_on_open_callback`, `add_on_recovery_*_callback` methods; extended
  `_on_connection_closed`; `_recover_connection`, `_try_reconnect_once`,
  `_reopen_channels_and_recover_topology`, `_reopen_channel`,
  `_recover_topology`, `_on_channel_closed_for_recovery`, `_recover_channel`.
  `ThreadSafeChannel` gains `self._closed`, `self._recovering` (guards
  against a second `_recover_channel` pass for the same channel, and
  against `_reopen_channels_and_recover_topology` reopening a channel a
  `_recover_channel` pass still owns, see "Preventing re-entrant and
  cross-level channel recovery" above), `_register_recovery_close_listener`,
  `add_on_close_callback`, `add_on_open_callback`,
  `add_on_recovery_started_callback`, `add_on_recovery_succeeded_callback`,
  `add_on_recovery_failed_callback` (own plain callback lists, populated
  only by `_recover_channel` — never by `_recover_connection`), plus
  recording/removal call sites in the declare/bind/consume/delete/unbind/cancel
  methods that delegate to `self._connection._recovery.record_*`/`remove_*` —
  `ThreadSafeChannel` itself holds no topology state (no `self._ledger`).
  Ten existing method signatures (`basic_qos`, `basic_cancel`,
  `queue_declare`, `exchange_declare`, `queue_bind`, `queue_unbind`,
  `queue_delete`, `exchange_bind`, `exchange_unbind`, `exchange_delete`)
  will need their declared return type corrected from `-> None` to
  `-> Any` — they already return the response frame at runtime (per their
  own docstrings), but nothing has previously consumed the return value,
  so the type mismatch has gone unnoticed until now; the topology-recovery
  code would become the first caller to rely on it.
- **`examples/thread_safe_recovery_example.py`** (new): a
  `ThreadSafeConnection` with `recovery=RecoveryConfig()`, a background
  publisher thread that holds off on publishing until
  `on_recovery_succeeded` fires after a drop, a consumer registered via
  `basic_consume`, and `on_recovery_*` callbacks logging state
  transitions.
- **`tests/acceptance/thread_safe_recovery_test.py`** (new, requires
  RabbitMQ): acceptance tests, listed in the test plan below.
- **`tests/unit/recovery_tests.py`** (new, mock-based): unit tests covering
  config/coordinator/ledger logic and the guard behavior.
- **`tests/unit/thread_safe_connection_tests.py`**: existing bare
  `MagicMock()` fixtures used as `ThreadSafeChannel` wrappers will need to
  explicitly set `wrapper._recovery = None`. A plain `MagicMock()` is
  truthy for any attribute access, so `wrapper._recovery is not None`
  would evaluate `True` by default — this would break existing tests as
  soon as `ThreadSafeChannel.__init__` starts checking that attribute. We
  expect to need to touch every such fixture.

`pika/spec.py` would remain untouched — no protocol/spec changes are
needed, since recovery is pure client-side orchestration of existing AMQP
methods.

## Proposed test plan

### Integration (`tests/acceptance/thread_safe_recovery_test.py`, real broker)

We propose simulating drops with **`ForwardServer`** (an existing test
helper that proxies TCP to the real broker in a subprocess) rather than
`shutdown(socket.SHUT_RDWR)` on the impl connection's transport —
`ForwardServer` already supports the stop/restart-on-the-same-port cycle
these tests need, and is the same technique
`tests/acceptance/thread_safe_connection_test.py` already uses for its own
drop tests. Queue/exchange names would be uuid-suffixed. We propose the
following cases:

1. `TestPublishContinuityAcrossRecovery` — publish before a drop, confirm
   it landed (via passive declare) *before* triggering the drop — a
   fire-and-forget publish with no confirms can legitimately race the drop
   and never land, which is normal AMQP behavior, not something recovery
   is responsible for — then publish again after recovery succeeds (via a
   retrying helper, since a publish attempted in the narrow window around a
   drop can still fail) and assert both messages are present. Uses a
   durable, non-exclusive queue so the assertion is about publish
   continuity specifically, not exclusive-queue redeclare (which has its
   own test).
2. `TestConsumeContinuityAcrossRecovery` — **the core requirement**:
   register `basic_consume` once, drop the connection, publish more
   messages from a second, independent connection after recovery
   completes, assert they arrive at the *original* callback with zero
   additional app calls.
3. `TestExclusiveQueueRecovery` — an exclusive queue (deleted by the broker
   on disconnect) should be transparently redeclared; verify via a passive
   declare on the same reopened channel after recovery succeeds.
4. `TestDeletedQueueSkipAndContinue` — a binding to a queue declared on a
   separate, untracked connection; that external queue is deleted while
   the primary connection is down, so the binding-recovery step gets a real
   404 on reconnect. Assert `on_topology_recovery_error` fires with a
   `'binding'` entity whose `channel_number` matches the channel that
   originally declared the binding, `on_recovery_succeeded`'s `skipped`
   list contains it, and everything else (the exchange, a second surviving
   queue, its binding) is still fully functional despite the one skipped
   entity.
5. `TestTopologyRecoveryDisabled` — `topology_recovery_mode=
   TopologyRecoveryMode.DISABLED`: connection reconnects but a queue
   declared before the drop is not redeclared (verify via a passive
   declare from a separate connection raising `ChannelClosedByBroker`).
6. `TestOnlyTransientTopologyRecovery` — `topology_recovery_mode=
   TopologyRecoveryMode.ONLY_TRANSIENT`: a durable queue/exchange pair and
   a separate exclusive (transient) queue bound to a durable exchange are
   declared before the drop. After recovery, assert the exclusive queue
   and its binding are redeclared (passive declare succeeds) while the
   durable queue is *not* redeclared by the client (verify by deleting it
   out-of-band before the drop and asserting recovery does not recreate
   it — a passive declare from a separate connection should raise
   `ChannelClosedByBroker`).
7. `TestRetryExhaustionRaisesCleanly` — `max_attempts=2` against a
   forwarder that is never restarted; assert `on_recovery_failed` fires,
   the original close callback still fires, and `coordinator.state ==
   FAILED`, with no hang.
8. `TestExplicitCloseDoesNotTriggerRecovery` — `connection.close()` from
   the app never fires `on_recovery_started`.
9. `TestDefaultBehaviorUnchangedWithoutRecoveryConfig` — regression guard:
   omitting `recovery=` should reproduce today's exact wake-all/teardown
   behavior on a forced drop.
10. `TestChannelLevelRecoveryWithoutFullConnectionReconnect` — redeclaring
    an existing exchange with mismatched durability triggers a broker-side
    406 that closes only that channel; assert the channel's own
    `add_on_recovery_succeeded_callback` fires while the connection-wide
    `on_recovery_started`/`on_recovery_succeeded` never fire at all.
11. `TestMultiChannelTopologyRecoveryOrdering` — channel 1 declares a
    transient exchange and a server-named exclusive queue; channel 2
    declares the binding and the consumer (mirroring `amqp091-go`'s
    `TestConnectionRecoveryMultiChannelTopology`). After a drop and
    recovery, assert the full exchange → binding → queue → consumer chain
    is functional on both channels regardless of which channel's entries
    the recovery pass processes first.
12. `TestCrossChannelDeletionRemovesStaleTopology` — channel A declares a
    queue; channel B (on the same connection) deletes it via
    `queue_delete`. Drop the connection and recover; assert a passive
    declare of that queue name from a separate connection raises
    `ChannelClosedByBroker` (i.e. it was *not* incorrectly redeclared from
    channel A's side of the topology store).
13. `TestChannelRecoveryDoesNotDuplicatePassOnPermanentConflict` — redeclare
    an exchange with mismatched durability on a channel so every recovery
    attempt hits the same broker-side 406 and gets skipped via
    `should_skip`; assert only one `_recover_channel` pass is ever active
    for that channel at a time (e.g. by counting concurrent
    `on_recovery_succeeded` firings for that channel against an expected
    upper bound) rather than an unbounded, ever-multiplying chain of
    passes.
14. `TestChannelCloseAndOpenCallbacksFireOnRecovery` — register
    `add_on_close_callback`/`add_on_open_callback` on a channel, force a
    broker-initiated single-channel close, and assert close fires with the
    broker's reason and open fires once the channel is usable again, with
    no `on_recovery_*` (connection-level) firing in between.
15. `TestFullReconnectDoesNotFireChannelRecoveryCallbacks` — the inverse of
    #10: force a full connection drop (not an isolated channel error) on a
    connection with channels that have `add_on_recovery_succeeded_callback`
    registered; assert the connection-wide `on_recovery_succeeded` fires
    exactly once while none of the per-channel `on_recovery_*` lists fire
    for any channel, confirming `_recover_connection`'s reopen path never
    touches a channel's own recovery callback lists.
16. `TestConnectionCloseAndOpenCallbacksFireOnRecovery` — register
    `add_on_close_callback`/`add_on_open_callback` on the connection, force
    a full drop, and assert `add_on_open_callback` fires once the redial
    succeeds and *before* `on_recovery_succeeded` fires (topology recovery
    hasn't started yet at that point); separately, assert
    `add_on_close_callback` fires on an explicit `close()` even with no
    `RecoveryConfig` at all, confirming it is not gated on recovery being
    configured.
17. `TestConnectionRecoverySupersedesInFlightChannelRecovery` — trigger an
    isolated channel-level recovery (e.g. a 406 on one channel), and while
    it is still backing off, force a full connection drop (kill the
    forwarder entirely). Assert: the channel-level pass's
    `on_recovery_failed` never fires (it yields rather than failing), the
    connection-level pass's `on_recovery_succeeded` fires once recovery
    completes, and the channel in question ends up open and fully
    recovered exactly once rather than twice or not at all — the concrete
    symptom Scenario B in "Preventing re-entrant and cross-level channel
    recovery" would otherwise produce.
18. `TestSupersededChannelRecoverySucceedsAnyway` — the timing variant of
    #17 where the channel-level pass's in-flight attempt completes
    successfully right as the connection-level pass takes over (tight
    enough timing that this may need a unit-level reproduction with mocks
    rather than a reliable real-broker window): assert the channel ends up
    with exactly one live raw channel afterward — no channel number left
    open on the broker that nothing client-side still references.

We'd want these run multiple times in CI to check for flakiness before
merging, given the timing-sensitive nature of drop simulation.

### Unit (`tests/unit/recovery_tests.py`, mocked)

- Config/entity/coordinator: backoff math, `should_skip` default/callback/
  exception-swallowing behavior (asserting the `connection` and `entity`
  arguments `on_topology_recovery_error` receives are the ones passed
  through, and `entity.channel_number` matches the channel the failing
  call was made on), `RecoveryConfig.topology_recovery_mode` defaulting to
  `TopologyRecoveryMode.ALL`, `__str__` formatting, coordinator defaults.
- Topology store: record/remove semantics for every entity type; a removal
  issued via one channel number must reach records stored under a
  *different* channel number's bucket (`remove_queue`/`remove_exchange`
  take no channel argument and must scan every bucket); cascade removal of
  bindings/consumers when their exchange/queue is removed;
  binding/exchange-binding dedup; server-generated-name rename propagating
  into bindings/consumers stored under any channel's bucket, not just the
  one that declared the queue.
- Close-races-recovery abort: `close()` planting `_closed_reason` while
  `_recover_connection` is mid-attempt is observed by the redial loop at
  its next check, and pool teardown runs exactly once regardless of which
  side notices first.
- Backoff wake-up: with a mocked clock/large `next_interval`, calling
  `close()` (connection or channel) while a loop is parked mid-backoff
  causes that loop to wake and exit immediately once its own abandonment
  condition is true, rather than only after the full interval elapses —
  asserted via wall-clock elapsed time staying well under the configured
  interval.
- Backoff wake-up does not leak across unrelated loops: with two channels
  independently backing off in `_recover_channel`, closing one must not
  shorten the other's backoff — the untouched channel's loop, woken by
  the shared condition variable's `notify_all()`, must go back to waiting
  out its own remaining interval rather than falling through to an early
  retry attempt.
- Channel-recovery reentrancy guard: `_on_channel_closed_for_recovery`
  does not spawn a second `_recover_channel` thread for a channel whose
  `_recovering` flag is already set, even when `coordinator.state` is
  still `IDLE` (Scenario A in "Preventing re-entrant and cross-level
  channel recovery"); `ch._recovering` is set atomically with the spawn
  decision, under `_channel_waiters_lock`, not as `_recover_channel`'s
  first statement on the new thread.
- Cross-level supersession (Scenario B): with a mocked `_recover_channel`
  loop parked mid-backoff, flipping `coordinator.state` to `RECONNECTING`
  is observed at the next abandonment check and the pass exits without
  firing `on_recovery_failed`; `_reopen_channels_and_recover_topology`
  blocks on a channel whose `_recovering` flag is set and proceeds only
  after it clears, rather than calling `_reopen_channel` concurrently.
- `_reopen_channel` closes a still-open previous raw channel before
  installing its replacement, rather than only overwriting `ch._channel`
  — exercised directly (call it twice on a channel that's still open in
  between) rather than only through the Scenario B timing that motivates
  it, since that timing is otherwise hard to hit reliably in a test.
- Channel-level callbacks: `add_on_close_callback`/`add_on_open_callback`
  fire with the right arguments for an app-initiated close, a broker
  soft-error close, and a `_reopen_channel` swap; exceptions raised inside
  one registered callback do not prevent the rest from running.
- Recovery-callback partition: `_recover_channel` appends only to a
  channel's own `on_recovery_*` lists and never to
  `coordinator.on_recovery_*`; `_recover_connection` is the reverse —
  asserted directly against both coordinator and channel state after each,
  with mocks, so the split doesn't silently regress to the old
  shared-list behavior.
- `_on_connection_closed` recovery triggers: fires for non-client closes,
  does not fire for `ConnectionClosedByClient`, and — the reentrancy case
  — a second close event while already `RECONNECTING` does not spawn a
  second recovery thread or re-fire `on_recovery_started`.
- Close-during-recovery: an app thread calls `close()` while the recovery
  loop is (conceptually) backing off; assert `close()` plants
  `_closed_reason` as a `ConnectionClosedByClient`, joins
  `self._recovery_thread` rather than blocking on a stale or
  already-exited `self._ioloop_thread`, and is a no-op if called again —
  all without `close()` itself ever reading `self._recovery` or `state`.
  This race is difficult to exercise against a live broker
  deterministically, so we propose covering it at the unit level with
  mocks instead.

### CI gates before merge

Standard project gates apply and should all be green before merge:
`hatch run fmt-check`, `hatch run lint-check`, `hatch run docfmt-check`,
`hatch run typecheck`, `hatch run unit`, and the acceptance suite against a
real broker.

## Open questions

**Querying recovery status.** As specified, there is no public,
synchronous way to ask "is this connection (or this channel) recovering
right now?" The only signal is the `on_recovery_*` callbacks, which are
edge-triggered and missed if registered after the fact — since calls made
during recovery aren't rejected (see "Calling into the connection during
recovery" above), there is nothing an app can catch reactively either. We
should decide whether to add a public `state` property or
`is_recovering()` method on `ThreadSafeConnection`/`ThreadSafeChannel`
before implementation.

## Next steps

Pending sign-off on the API above, implementation would proceed in
phases, each closed out with its own unit and integration coverage before
moving to the next, rather than deferring all testing to one acceptance
pass at the end:

1. **Core types** — `RecoveryConfig`, `RecoveryState`, `RecoveryCoordinator`,
   `TopologyRecoveryEntity` in `pika/recovery.py`.
   - Unit: backoff math, `should_skip` behavior, coordinator defaults.
   - Integration: none (no broker interaction yet).
2. **Connection-level redial path** — `_on_connection_closed` branching,
   `_recover_connection`, `_try_reconnect_once` (calling
   `queue_declare`/`basic_consume`/etc. with no special-casing, per
   "Calling into the connection during recovery" above), `close()`
   reworked to never inspect `self._recovery`/`state` at all (join
   whichever of `self._recovery_thread`/`self._ioloop_thread` is alive,
   unconditionally), `add_on_close_callback`/`add_on_open_callback` on
   `ThreadSafeConnection`.
   - Unit: recovery-trigger conditions, close-during-recovery, and that
     `close()` itself never reads `state`
     (`OnConnectionClosedRecoveryTests`, `CloseDuringRecoveryTests`);
     `add_on_open_callback` firing after the
     connection swap but before topology replay; `add_on_close_callback`
     firing on an explicit close with no `RecoveryConfig` present.
   - Integration: `TestRetryExhaustionRaisesCleanly`,
     `TestExplicitCloseDoesNotTriggerRecovery`,
     `TestDefaultBehaviorUnchangedWithoutRecoveryConfig`,
     `TestConnectionCloseAndOpenCallbacksFireOnRecovery` — these only need
     the redial loop, not the topology ledger, so they can run as soon as
     this phase lands.
3. **Channel-level recovery** — `_register_recovery_close_listener`,
   `_on_channel_closed_for_recovery`, `_recover_channel`, the
   `is_open`-first guard against racing connection-level recovery, and
   `add_on_close_callback`/`add_on_open_callback`. **Both guards in
   "Preventing re-entrant and cross-level channel recovery" must land in
   the same commit as `_recover_channel` itself** — Scenario A's
   `_recovering` flag (set atomically with the spawn decision in
   `_on_channel_closed_for_recovery`) and Scenario B's abandonment
   condition plus wait in `_reopen_channels_and_recover_topology`.
   Shipping the retry loop first and either guard as a follow-up would let
   a permanent per-entity conflict spawn an unbounded chain of concurrent
   recovery threads (Scenario A, the exact failure `amqp091-go` had to
   patch after the fact) or let a channel-level and connection-level pass
   race on the same raw channel (Scenario B) in the interim. This phase
   also gives `ThreadSafeChannel` its own
   `add_on_recovery_started_callback`/`_succeeded`/`_failed` lists, fired
   exclusively by `_recover_channel`, so a channel-scoped recovery event
   never fires the connection-wide `coordinator.on_recovery_*` lists and
   vice versa — resolving the ambiguity a shared list would otherwise
   leave, without adding a channel argument to the connection-level
   signatures proposed above.
   - Unit: guard ordering (state-check vs. `is_open`-check) with mocked
     connection/channel objects; the Scenario A reentrancy case including
     the atomic-set-under-lock timing; the Scenario B supersession case
     (abandonment on `state == RECONNECTING`, and
     `_reopen_channels_and_recover_topology` waiting on `_recovering`);
     close/open callback firing; the recovery-callback partition (channel
     passes never touch the coordinator's lists, connection passes never
     touch a channel's lists).
   - Integration: `TestChannelLevelRecoveryWithoutFullConnectionReconnect`,
     `TestChannelRecoveryDoesNotDuplicatePassOnPermanentConflict`,
     `TestChannelCloseAndOpenCallbacksFireOnRecovery`,
     `TestFullReconnectDoesNotFireChannelRecoveryCallbacks`,
     `TestConnectionRecoverySupersedesInFlightChannelRecovery`.
4. **Topology ledger and recovery** — `coordinator.topology`, `ChannelTopology`
   and the `*Record` dataclasses, `record_*`/`remove_*` coordinator methods
   and their call sites in `ThreadSafeChannel`, `_recover_topology`,
   server-generated-name rename handling, skip-and-continue reopen logic.
   **The topology store must be connection-wide (keyed by channel number,
   owned by the coordinator) from the first commit of this phase — a
   per-channel store is not an acceptable intermediate step**, since it
   produces the ordering and split-brain-removal failures described in
   "Topology ledger" above; a phase that lands with a per-channel store and
   "fixes cross-channel ordering later" would ship recovery that silently
   corrupts topology on exactly the multi-channel apps recovery exists to
   help. `TestMultiChannelTopologyRecoveryOrdering` and
   `TestCrossChannelDeletionRemovesStaleTopology` are acceptance criteria
   for closing this phase, not follow-up work.
   - Unit: `TopologyStoreTests` (record/remove semantics including
     cross-channel removal, cascade removal, rename propagation across
     buckets).
   - Integration: `TestPublishContinuityAcrossRecovery`,
     `TestConsumeContinuityAcrossRecovery`, `TestExclusiveQueueRecovery`,
     `TestDeletedQueueSkipAndContinue`, `TestTopologyRecoveryDisabled`,
     `TestOnlyTransientTopologyRecovery`,
     `TestMultiChannelTopologyRecoveryOrdering`,
     `TestCrossChannelDeletionRemovesStaleTopology`.
5. **Hardening pass** — run the full acceptance suite repeatedly to check
   for flakiness, run `fmt-check`/`lint-check`/`docfmt-check`/`typecheck`
   across all changed files.
6. **Example and docs** — `examples/thread_safe_recovery_example.py`,
   docstrings, changelog entry.

Each phase should be its own reviewable PR (or a small stack of PRs) rather
than one large PR at the end, so reviewers can weigh in on the redial path
before the topology ledger is built on top of it.
