# Design: Auto-recovery for pika's ThreadSafeConnection

Before reading: `README.md` in this directory records the pika 2.0.0 constraints that govern this design - one `Connection`/`Channel` type, all other adapters removed, and the API free to change. They override anything below that assumes otherwise.

> Status: proposal, for review before implementation begins. Recovery is a first-class `RECOVERING` state on the adapter `Connection`/`Channel` lifecycle, driven by the connection's own persistent IOLoop rather than a separate thread. This follows the state-machine framing explored in `design-state-machine.md`, grounded in the empirical client behavior recorded in `findings.md`.

## Editing this document

Guidance for anyone editing this file, AI or human. These are the rules from `~/genai/git/GIT.md` and the repository's own conventions that actually bite here; a change that breaks one is wrong even if the prose is good.

- **Never hard-wrap.** Every paragraph, list item and table row is a single line, however long. This file was hard-wrapped for most of its life and has been unwrapped; do not reflow it back. The rendered output is identical either way, so the cost of wrapping is entirely to the tools that read the file as text: line-anchored citation, `grep -n`, and per-line diff review all become unreliable when one sentence spans several lines and a one-word edit reflows the paragraph.
- **ASCII punctuation only.** No em-dashes and no arrow characters. Use `-` or `--` for a dash and `->` for a transition. This is measured repository convention, not preference: across the 58 tracked `.md` files outside `design/` there are five em-dashes in total.
- **Bare commit SHAs, never in backticks.** GitHub auto-links a bare SHA and backticks suppress that.
- **One H1, the title above.** Any section pasted into a pull request, issue, review or comment body must not carry an H1, because the forge renders those oversized and the title is already redundant there.
- **No trailing whitespace, and blank lines are truly empty.**
- **Commit messages are the one place hard-wrap is required**: 50-70 character subject in active voice and present tense, body wrapped at 72. Write them to a file and use `git commit -F <file>` rather than `-m`, because shell escaping of apostrophes and backticks silently corrupts text.
- **Post forge bodies with `--body-file`, never an inline `--body`.** A double-quoted string containing backticks executes them as command substitution and the text simply disappears, exit code zero.
- **Prose derived from this document and posted under a maintainer's account needs an AI-authorship disclosure** if an AI drafted it, placed at the top of the body, stating only what the human actually did. Ask before adding one rather than adding it unilaterally or omitting it silently.

Two conventions specific to this document rather than to git. Claims about pika's current behavior carry the `file:line` they were checked against, so a reader can re-verify rather than trust; if you cannot cite it, say it is unverified. And where this design diverges from the RabbitMQ Java, .NET or Go clients, say so explicitly and give the reason, because a reviewer who knows those clients will otherwise assume the divergence was an oversight.

## Glossary

Terms this document uses in a specific sense. Where a term names something in the code, the reference is given so a reader can check it rather than infer it.

**adapter `Connection` / `Channel`** - `pika.adapters.thread_safe_connection.Connection` and `.Channel`: the stable handles an application holds. Their identity survives a reconnect, which is what makes them the place recovery state can live. Renamed from `ThreadSafeConnection` / `ThreadSafeChannel` in 1.5.0 by #1617.

**base `Connection` / `Channel`** - `pika.connection.Connection` and `pika.channel.Channel`: one transport session and the channels on it. A base `Connection` reaches CLOSED and never reopens, so recovery constructs a new one rather than reviving it. Unqualified, "the connection" in this document means the adapter `Connection`.

**inner connection** - the base `SelectConnection` the adapter currently holds in `self._connection`. Replaced wholesale on every redial, which is why nothing durable may live on it.

**persistent loop** - the `pika.adapters.select_connection.IOLoop` instance, owned by the adapter `Connection` and outliving every inner connection. Today it is not persistent: `SelectConnection.__init__` resolves `SelectorIOServicesAdapter(custom_ioloop or IOLoop())` (`select_connection.py:126-129`) and the adapter passes no `custom_ioloop`, so each inner connection constructs and owns a fresh loop whose lifetime is that one transport session's. Under this proposal the adapter constructs one `IOLoop` in `__init__`, the background thread runs `self._ioloop.start()` directly rather than `self._connection.ioloop.start()`, and every inner connection is built with `custom_ioloop=self._ioloop` and so borrows a loop it does not own. Only ownership and lifetime change: same class, same thread count, same polling. The consequence that matters throughout this document is that `get_native_ioloop()` returns the object it was handed (`selector_ioloop_adapter.py:176-177`), so `_connection.ioloop is self._ioloop` holds for every generation and there is no longer any such thing as "the reporting connection's own loop".

**loop thread** - the single background thread running the persistent loop, named `pika-ioloop-N`. All protocol I/O and every callback run here, one at a time, which is what removes the need for most locking. Nothing on it may block; see "What the persistent loop costs".

**caller thread** - any application thread that calls a public method. A blocking wrapper call blocks the caller thread on a `threading.Event` while the loop thread performs the work and sets it.

**pool worker** - a `_BoundedWorkPool` thread: one per channel for consumer callbacks, one per connection for connection events. Distinct from the loop thread, which is precisely why a consumer callback can still be holding a delivery tag while a redial completes.

**the funnel** - the adapter's `on_close_callback`, `_on_connection_closed`, through which an inner connection that had reached OPEN reports its death. It is not the only death path: an inner connection that never opened reports through `on_open_error_callback` instead, and that is the path every failed redial takes. See "The funnel is not the only death path".

**redial** - one attempt to construct a new inner connection and bring it to OPEN. A recovery pass performs one or more, with backoff between them.

**recovery pass** - one traversal from `RECOVERING` to either `OPEN` or `CLOSED`, comprising the redial attempts and the topology replay that follows a successful one. Connection-level and channel-level passes are distinct and can interleave; see "Composing channel-level and connection-level recovery".

**guarded call** - a public method that consults `_state` before acting and raises `ConnectionRecovering` or `ChannelRecovering` when it is `RECOVERING`. Contrast the internal calls topology replay makes, which bypass the guard by design.

**topology ledger** - `coordinator.topology`, the recorded declarations that replay reissues. "Recorded" means the ledger holds it, which happens only after the broker acknowledged the declaration; it does not mean the entity currently exists on the broker, and the two diverge whenever something is deleted out of band.

**delivery-tag offset** - the per-channel value added to every wire delivery tag before the application sees it, and subtracted from every tag the application acknowledges. It gives the application one continuous tag space across any number of recoveries, and it is what makes a tag from a superseded generation detectable rather than ambiguous. See "Consumer delivery tags across recovery".

**stale tag** - a delivery tag issued by a superseded channel generation, detectable as `delivery_tag - _delivery_tag_offset <= 0`. The message it referred to has already been requeued by the broker, so discarding the acknowledgement loses nothing.

## Problem statement

pika has no built-in recovery today: an unexpected connection or channel loss wakes every blocked caller with an exception and tears the connection down (`ThreadSafeConnection._on_connection_closed`). Nothing redials, and nothing remembers what topology or consumers existed. Every pika user currently hand-rolls reconnect logic (see `examples/asynchronous_consumer_example.py`, `examples/blocking_consume_recover_multiple_hosts_retry.py`).

RabbitMQ's Go client (`amqp091-go`) closed the equivalent gap with its `recovery.go` / `lifecycle.go` design: a topology ledger plus pluggable reconnection plus per-entity skip/abort error handling. We propose porting that shape to pika. Where we part ways with a straight port is the concurrency model: `amqp091-go` and the reference clients measured in `findings.md` all represent "currently reconnecting" as an observable, gate-able state, and we adopt that instead of treating recovery as invisible plumbing underneath an unchanged call surface.

## Direction

Give the adapter `Connection` and `Channel` a single authoritative lifecycle state that includes `RECOVERING`, alongside `OPEN`, `CLOSING`, and `CLOSED`. Every public operation guards on that state; an operation attempted while `RECOVERING` raises a dedicated, catchable exception (`ConnectionRecovering` / `ChannelRecovering`, each subclassing the existing wrong-state error). Recovery runs on the connection's own persistent IOLoop, not a second thread. State is both observable (a synchronous property and ordered listeners) and reactive (the guard), so an app can ask "am I recovering?" and also catch the fact that it is. This follows the framing explored in `design-state-machine.md`.

Fail-fast during recovery matches how existing clients actually behave: `findings.md` shows neither the RabbitMQ Java client nor `amqp091-go` lets a call during recovery run as it would on a healthy connection - both fail fast (`AlreadyClosedException` / `ErrClosed`), and `amqp091-go` documents a reconnect-handshake hazard where publishing blindly during recovery can interleave a frame with the `channel.open` handshake and cause a protocol violation. Both expect the application to gate on connection state.

Running recovery on the connection's existing IOLoop, rather than a dedicated thread, removes an entire class of concurrency machinery that a second thread would otherwise require - reentrancy flags, cross-thread atomicity guards, condition-variable backoff - because state transitions and the redial sequence itself execute on the same thread as every other piece of connection state. See "Composing channel-level and connection-level recovery" below for what concurrency risk remains once that's true.

## Scope

The target adapter is **`ThreadSafeConnection` and `ThreadSafeChannel`** only. `BlockingConnection` is deprecated with removal planned for pika 2.0, so recovery is not proposed there. Other adapters (asyncio, gevent, select, tornado, twisted) are out of scope for this proposal, though see "Scope of the state contract" under Open Questions below for why the state/guard *contract* is worth sharing with them even though this proposal only implements a driver for the thread-safe adapter.

`ThreadSafeConnection` today runs `SelectConnection`'s IOLoop on one dedicated background thread, 1:1 with the inner connection: when the inner connection dies, that thread's `self._connection.ioloop.start()` call returns and the thread exits. We propose decoupling them: the IOLoop thread and the `IOLoop` instance it runs become properties of the **adapter** `ThreadSafeConnection` itself - constructed once in `__init__` and outliving any single inner connection - while `self._connection` (the inner `SelectConnection`) is what gets rebuilt on each redial. See "Persistent IOLoop and the redial sequence" below for how.

Every blocking call from a caller thread (`ThreadSafeChannel._blocking_rpc`) still works exactly as it does today: registering a `(threading.Event, error-slot)` pair in `self._blocking_waiters`, scheduling the real work onto the IOLoop thread via `add_callback_threadsafe`, and blocking the caller thread on the event. `_on_connection_closed` remains the single "wake every blocked caller with this exception" mechanism, guarded by `self._channel_waiters_lock`; recovery **coexists with, rather than replaces**, that mechanism. Recovery's own state lives directly on the adapter `Connection`/`Channel` objects, as the same kind of state value `_check_not_closed` already reads today - see "Where state lives" below.

## Goals

The concrete UX bar we want to hit, and intend to verify with an acceptance test (`TestConsumeContinuityAcrossRecovery`): an app consuming via `basic_consume(queue, callback)` keeps receiving messages on the *same* `callback` after a connection drop and automatic recovery, with **no application code changes**.

Recovery must be **opt-in** - default behavior stays unchanged unless the caller passes a `recovery=` config. We intend to cover this with a regression test (`TestDefaultBehaviorUnchangedWithoutRecoveryConfig`).

A call issued while the connection or channel is `RECOVERING` must fail **synchronously and catchably**, not time out or surface a generic already-closed error. We intend to verify this with `TestOperationDuringRecoveryRaisesDedicatedException` (below): a `basic_publish` (or any guarded call) issued between the drop and the `RECOVERING -> OPEN` transition raises `ConnectionRecovering` / `ChannelRecovering`, and an app can `except` that specific type.

## Non-goals

- No changes to `BlockingConnection` or any adapter other than `ThreadSafeConnection`.
- Recovery does not attempt to make in-flight synchronous RPCs survive a drop transparently (see "Existing in-flight blocking waiters" below) - a waiter blocked mid-call when the drop happens is woken with the close reason, exactly as it is today; it is not silently retried.
- No multi-host/cluster failover logic beyond what `AMQPConnectionWorkflow` already supports for the initial connect (iterating a sequence of `Parameters` objects, one per candidate host). The redial sequence retries against the single `Parameters` the connection was originally opened with; it does not walk a host list.
- No blocking/waiting mode for guarded calls. A call made during `RECOVERING` always fails fast with the dedicated exception; there is no opt-in "block until open" behavior in this proposal (see Open Questions).

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
    #: since the broker retains them across a network interruption - use
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
    on_topology_entity_error: (
        Callable[[ThreadSafeConnection, TopologyRecoveryEntity], bool] | None) = None
    # True (or None, default) -> skip entity, continue recovery.
    # False -> abort this recovery attempt, fall through to the outer retry loop.

    def next_interval(self, attempt: int) -> float: ...   # exponential backoff, capped at max_interval
    def should_skip(self, connection: ThreadSafeConnection,
                     entity: TopologyRecoveryEntity) -> bool: ...

@dataclass
class TopologyRecoveryEntity:
    entity_type: str          # 'exchange' | 'queue' | 'binding' | 'exchange_binding' | 'consumer' | 'qos'
    name: str
    channel_number: int       # the ThreadSafeChannel this entity's declare/bind/consume call was made on
    secondary_name: str = ''  # exchange for a queue binding, destination for an exchange binding
    routing_key: str = ''
    error: Exception | None = None
```

`RecoveryConfig` and `TopologyRecoveryEntity` carry the recovery policy and per-entity failure reporting; both are orthogonal to how state is represented. There is deliberately no separate `RecoveryState` enum on `RecoveryCoordinator` - "is the connection recovering" is answered by the connection's own lifecycle state, described next.

### Lifecycle state

```python
class ConnectionState(enum.Enum):
    OPEN = 'open'
    RECOVERING = 'recovering'
    CLOSING = 'closing'
    CLOSED = 'closed'

class ChannelState(enum.Enum):
    OPEN = 'open'
    RECOVERING = 'recovering'
    CLOSING = 'closing'
    CLOSED = 'closed'
```

These are new types, distinct from base `pika.connection.Connection`'s own `CONNECTION_CLOSED/INIT/PROTOCOL/START/TUNE/OPEN/CLOSING` and base `pika.channel.Channel`'s `CLOSED/OPENING/OPEN/CLOSING`. The base classes already guard on their states (`ConnectionWrongStateError`, `ChannelWrongStateError`) and are unaffected - a base `Connection` is 1:1 with a transport session, reaches `CLOSED`, and never comes back; recovery means constructing a new one. `ConnectionState`/`ChannelState` are the adapter-level states that make that construction-of-a-new-one process observable on the *stable* handle the app holds. `ThreadSafeConnection` does not have an equivalent state value today (only the informal `_closed_reason is None` check); this proposal adds one, specifically so `RECOVERING` has somewhere to live.

**Terminal rule**, borrowed from the AMQP 1.0 Java client's `ResourceBase`: once `CLOSING` or `CLOSED` is reached, only `CLOSED` may follow. A recovery attempt that succeeds after the app has already called `close()` cannot resurrect the handle back to `OPEN`.

Exhausting `config.max_attempts` transitions straight to `CLOSED` (with `_closed_reason` set to the last redial error), not to a separate `FAILED` value. From the app's perspective there is no meaningful difference between "the app closed this" and "recovery gave up" other than the reason attached, and folding them into one terminal state means the terminal rule above has only one destination to reason about.

### Guard and exceptions

```python
class ConnectionRecovering(ConnectionWrongStateError): ...
class ChannelRecovering(ChannelWrongStateError): ...
```

Every public operation that today calls `_check_not_closed()` (on the connection) or the channel's equivalent not-open guard checks `self._state` and raises `ConnectionRecovering`/`ChannelRecovering` when it is `RECOVERING` - fail-fast, matching `findings.md`, not block-until-open. Subclassing the existing wrong-state errors keeps `except ConnectionWrongStateError`/`except ChannelWrongStateError` code working unchanged; recovery-aware code can additionally do `except ChannelRecovering: wait_for_open(); republish()`.

This guard applies to calls made through the public wrapper API. `_recover_topology`'s own internal calls take a different path - see "Topology replay must not use the blocking wrapper API" below for why.

### Observability

```python
add_state_change_listener(callback)   # callback(obj, old_state, new_state, reason=None)
```

on both `ThreadSafeConnection` and `ThreadSafeChannel`: one ordered listener list per object, the equivalent of `amqp091-go`'s `NotifyStateChange` and the AMQP 1.0 Java client's `StateListener`, fired for every transition (not just ones related to recovery). `state` and `is_recovering` are synchronous properties, giving a synchronous, gate-able answer to "is this recovering right now" - see `findings.md` for why that matters.

**`is_open`/`is_closed` are redefined in terms of `_state`.** Today both properties delegate straight through to whichever inner object happens to be installed (`self._connection.is_open` on the connection wrapper, `self._channel.is_open` on the channel wrapper), reading the base `pika.connection.Connection`/`pika.channel.Channel`'s own state. That delegation is retired in favor of `self._state == ConnectionState.OPEN` / `== ConnectionState.CLOSED` (and the `ChannelState` equivalent), because the inner object is no longer a reliable read target once redial can rebuild it out from under the wrapper: mid-`RECOVERING` there may be no live inner connection at all, or a freshly constructed one that hasn't finished its own handshake, and delegating would answer from whichever of those happens to be installed at read time rather than from the wrapper's own state. `_state` is deliberately always present whether or not recovery is configured (see "Where state lives" below) specifically so `is_open`/`is_closed`, like their `_check_not_closed` replacement, have one unconditional source of truth regardless of what the inner object is doing underneath.

**Every other read of the raw inner object's state needs the same migration, or this reintroduces a split-brain hazard.** A narrow version of this pattern already exists today: `_check_not_closed` (`ThreadSafeConnection`, current code) checks `self._closed_reason` under lock and then falls through to `self._connection.is_closed` unlocked, documented as covering only the brief window between the connection reaching the closed state and `_on_connection_closed` running. That framing stops being true once `_state` is meant to be authoritative - any remaining direct read of the raw inner object's `is_open`/`is_closed`/`is_closing` is no longer a narrow race-window fallback, it is a second source of truth that can disagree with `_state` for the entire, possibly multi-step duration of a redial. A known concrete site: `ThreadSafeChannel.close()`'s loop-thread body (current code) short-circuits with `if self._channel.is_closed or self._channel.is_closing: ready.set(); return` against the *raw* inner channel before doing anything else. If the old raw channel already reports closed (broker dropped it) while the wrapper's `_state == ChannelState.RECOVERING`, this returns immediately as if `close()` succeeded, instead of applying the terminal rule or raising `ChannelRecovering` - silently swallowing an app-issued `close()` during recovery. Before implementation, every direct read of the inner `Connection`/`Channel` object's `is_open`/`is_closed`/`is_closing` in `thread_safe_connection.py` needs auditing - not just the two public properties above - and either migrated to `_state` or justified in a comment why that specific read must stay narrow, the same way today's `_check_not_closed` docstring justifies its fallback.

Five callback-registration methods (`add_on_close_callback`, `add_on_open_callback`, `add_on_recovery_started_callback`, `add_on_recovery_succeeded_callback`, `add_on_recovery_failed_callback`) are provided on both the connection and the channel as **convenience wrappers over state transitions**, not a second, independent notification path:

| Callback | Fires on transition |
|---|---|
| `add_on_open_callback(obj)` | any `-> OPEN` |
| `add_on_close_callback(obj, reason)` | any `-> CLOSED` |
| `add_on_recovery_started_callback(obj, reason)` | `OPEN -> RECOVERING` |
| `add_on_recovery_succeeded_callback(obj, skipped)` | `RECOVERING -> OPEN` |
| `add_on_recovery_failed_callback(obj, error)` | `RECOVERING -> CLOSED` |

**"Convenience wrapper over state transitions" is not quite literal for the last two rows.** `add_state_change_listener`'s callback shape is `(obj, old_state, new_state, reason=None)` - there is no `skipped` or `error` slot anywhere in that tuple. `add_on_close_callback(obj, reason)` and `add_on_recovery_started_callback(obj, reason)` really are pure filters over that signature (`reason` is already there). But `add_on_recovery_succeeded_callback(obj, skipped)` and `add_on_recovery_failed_callback(obj, error)` need a payload the generic transition event doesn't carry: which topology entities got skipped during replay, or which error ended the last redial attempt. Implementing these as "just a filtered listener" requires the recovery driver to stash that result somewhere readable at the moment it fires the `RECOVERING -> OPEN` / `RECOVERING -> CLOSED` transition (e.g. on `RecoveryCoordinator`, alongside `attempt`), and the two convenience methods read it from there rather than from anything `add_state_change_listener` itself provides. Deciding whether that extra state lives on `RecoveryCoordinator` or is threaded through as an enriched `reason` object on the transition itself is needed before implementation - the latter would make the sugar literal, at the cost of giving every listener a payload shape that varies by transition.

`add_on_recovery_*_callback` raise `ValueError` if called on a connection/channel not constructed with a `RecoveryConfig`, since those transitions cannot occur without one; `add_on_close_callback`, `add_on_open_callback`, and `add_state_change_listener` carry no such restriction. `add_on_close_callback`'s "any cause" behavior - explicit `close()`, a drop with no `RecoveryConfig`, or recovery exhausting its budget - falls out for free from being sugar over "any transition into `CLOSED`," rather than needing independent special-casing across separate teardown paths.

Connection-level and channel-level recovery notifications stay partitioned, and the partition falls out of the state machine structurally: `_recover_channel` transitions *that channel's own* `_state`, so that channel's listeners - both `add_state_change_listener` and the `add_on_recovery_*_callback` sugar - fire from that literal transition. A connection-wide drop only ever transitions the *connection's* `_state`; it does not individually transition each channel's `_state`. So a connection-wide drop fires the connection's own listeners, full stop - no listener registered directly on a channel (neither `add_state_change_listener` nor the `add_on_recovery_*_callback` sugar) fires, since nothing ever assigns a new value to that channel's `_state`. This is what `TestFullReconnectDoesNotFireChannelRecoveryCallbacks` locks in, and it keeps the two notification paths answering two different questions: a channel's own listeners mean "did *this specific channel* have an isolated episode," a connection's listeners mean "did the whole connection drop and come back" - see "Channel-level recovery" above.

**This creates a gap worth resolving before implementation, not papering over.** "Composing channel-level and connection-level recovery" defines a channel's *effective* state during a connection-wide drop as derived from the connection's state (`effective_state = CONNECTION.RECOVERING implies RECOVERING else ch._state`) - but only for the *guard* that decides whether to raise `ChannelRecovering` on a call. It says nothing about `ch.is_recovering` or `ch.state`, which, per the above, are read directly off the literal `ch._state` field. The result: during a connection-wide drop, `ch.is_recovering` reads `False` (nothing transitioned `ch._state`), yet calling any guarded method on that same channel raises `ChannelRecovering` (the guard reads the derived effective state). An app polling `ch.is_recovering` before deciding whether to call would see a stale "not recovering" right up until the call itself raises. Whether `is_recovering`/`state` on a channel should also fold in the connection's effective state, or whether this asymmetry between the query properties and the guard is acceptable, needs deciding - it is the same class of gap as the `is_open`/`is_closed` split-brain issue under "Observability" above, just between two read paths on the same object instead of between the wrapper and the raw inner object.

`ThreadSafeConnection.__init__` gains one new kwarg, `recovery: RecoveryConfig | None = None` - not on `Parameters`, which is adapter-neutral and consumed only by the initial-connect-only `AMQPConnectionWorkflow`.

## Where state lives, and who owns the topology ledger

`ThreadSafeConnection._recovery: RecoveryCoordinator | None` is `None` when the caller doesn't opt in. Lifecycle state does not live on the coordinator: it is `ThreadSafeConnection._state: ConnectionState` (and `ThreadSafeChannel._state: ChannelState`) directly, always present whether or not recovery is configured - a connection without `RecoveryConfig` simply never transitions to `RECOVERING`, but it still has `_state`, since the `_check_not_closed` replacement needs somewhere to read from regardless. `RecoveryCoordinator` (in `pika/recovery.py`) holds only:

- `config: RecoveryConfig`
- `attempt: int`
- `topology: dict[int, ChannelTopology]` - **the single, connection-wide topology store, keyed by channel number.** See "Topology ledger" below for why this must live here rather than on each `ThreadSafeChannel`.

Server-generated queue renames need a name-replacement map scoped locally to the single phased pass that produces the rename (`dict[str, str]` inside `_recover_topology(channels)`), not a `RecoveryCoordinator` field, since nothing outside that one pass ever needs it again.

**Locking model.** `_state` and `topology` are both read and written from multiple app-caller threads regardless of recovery - concurrent `queue_declare`/`exchange_declare` calls from different threads already need `topology`'s mutations serialized, and `state` needs to be safely readable from any thread via the `state` property. Both reuse the existing `_channel_waiters_lock`, the same lock `_closed_reason` is guarded by today: with state transitions driven from the loop thread and no second recovery thread able to race a caller thread, one lock is enough for the ordinary reason a single lock is enough for any other piece of connection state read cross-thread.

`_recover_topology` must snapshot (deep-copy) `topology` under the lock, release it, and only then issue the redeclare calls against that snapshot - not because of a competing recovery thread, but because `_recover_topology` runs as a sequence of non-blocking, callback-chained operations on the loop thread (see below), and holding a lock across a suspended callback chain would block every other loop activity, including the callbacks that would eventually resume the chain.

**Why `topology` lives on the coordinator and not per-channel:** AMQP exchanges, queues, and bindings are scoped to the *connection* (vhost), not to the channel that happened to declare them - a binding created on channel B can reference an exchange declared on channel A, and either channel can legally delete an entity the other one created. See "Topology ledger" below for the concrete failure modes a per-channel store would introduce.

## Where recovery hooks in

The adapter `Connection._on_connection_closed` is the funnel for the death of an inner connection that had reached OPEN. It is not the only death path - see "The funnel is not the only death path" below - but it is the one an established connection drops through:

```python
def _on_connection_closed(self, _connection, reason):
    _connection.ioloop.stop()          # stop the REPORTING connection's own ioloop
    if _connection is not self._connection:
        return                          # stale notification from an already-superseded connection
    if self._recovery is not None and not isinstance(reason, ConnectionClosedByClient):
        self._transition(ConnectionState.RECOVERING, reason)  # wakes blocking waiters, fires listeners
        self._schedule_reconnect_attempt(delay=0)              # loop timer, not a new thread
        return
    # existing behavior, unchanged: wake waiters, transition to CLOSED, user close callback
```

Two details matter here:

1. **Stop the reporting connection's ioloop, not necessarily `self._connection`'s** - operate on the `_connection` parameter and return early if it isn't the current one, so a stale notification from an already-superseded connection can't stop the new one's loop.
2. **Recovery trigger condition** stays `not isinstance(reason, ConnectionClosedByClient)` rather than an enumerated allow-list, for robustness against non-standard close reasons.

**Existing in-flight blocking waiters are still woken immediately** with the close reason when recovery starts - a synchronous RPC blocked mid-network-drop is not resumed transparently; it fails, as it does today. What differs from today is only that *subsequent* calls made before reconnection completes now fail immediately and catchably (`ConnectionRecovering`/`ChannelRecovering`) instead of hanging until their own timeout against a stopped loop.

### The funnel is not the only death path

The adapter wires two death callbacks on every inner connection:

```python
on_open_error_callback=self._on_connection_open_error,
on_close_callback=self._on_connection_closed,
```

and `pika.connection.Connection._on_stream_terminated` branches between them on `self._opened`: a connection that never reached OPEN reports through `ON_CONNECTION_ERROR`, one that did reports through `ON_CONNECTION_CLOSED`. They are mutually exclusive, and only together do they cover all connection death.

This matters because **a redial that fails to connect has never opened**, so it arrives at `_on_connection_open_error`, not at the funnel. During an outage that is the common case: a redial typically fails several times before one succeeds, so the retry cycle spends most of its life on the path the funnel does not cover. Its body today does three things that are correct for a failed *initial* connect and wrong for a failed redial:

1. `self._connection.ioloop.stop()` - the same call the funnel makes, with the same consequence under a shared loop. Suppressing it in one place and not the other leaves recovery working only when the first redial succeeds.
2. `self._user_on_open_error_callback(_connection, error)` fires. That is a documented public constructor parameter, whose signature is `on_open_error_callback(connection, exception)` and whose meaning today is "the connection you asked me to open could not be opened". Invoking it once per failed retry silently redefines it.
3. `self._connect_error = error` and `self._connected_event.set()` - both exist to hand the outcome of `__init__` back to the constructing thread, and neither has a meaning once that thread has moved on.

Item 1 is settled: recovery suppresses it, exactly as in the funnel. Item 3 is settled: the redial path must not touch either, since the constructing thread is long gone. **Item 2 is a fork this proposal does not resolve**, and it is a public-contract question rather than a mechanism one:

- Leave `on_open_error_callback` meaning only "initial connect failed", and report failed redials solely through the recovery listeners (`on_recovery_failed` once attempts are exhausted). Existing applications see no behavior change. The cost is that a caller watching that callback gets no per-attempt signal.
- Fire it per failed redial, so one callback covers "could not connect" whenever it happens. The cost is that an application which logs an alert or aborts startup from that callback now does so repeatedly, mid session, for a connection that may well recover.

Recommendation: the first. A per-attempt signal is worth having, but it belongs on a new recovery listener where its meaning is unambiguous, not bolted onto a constructor callback whose documented contract is about opening.

### Calling into the connection during recovery

Every guarded public method - `_check_not_closed`'s replacement, `_register_waiter`, `channel()`, and the equivalent channel-level guard - reads `self._state` and raises the dedicated exception when it is `RECOVERING`. This fail-fast behavior matches `findings.md`: neither reference client lets a call during recovery run as it would on a healthy connection, and both require the application to gate on connection state rather than assume transparency.

The recovery pass's own internal calls are the one place that does *not* go through the public wrapper API - see "Topology replay must not use the blocking wrapper API" below for why.

**`close()` on both the connection and the channel is unconditional**: it transitions state to `CLOSING`->`CLOSED` (idempotently) and schedules `self._connection.close()`/the raw channel's close via `add_callback_threadsafe`, regardless of what recovery is doing. The terminal rule (`CLOSING`/`CLOSED` -> only `CLOSED`) makes this safe without any handshake between `close()` and an in-flight redial: if a redial attempt is in flight on the loop when `close()` schedules its own callback, that callback runs on the same loop, sees the terminal state already set, and the redial's own next step (whichever loop callback would have resumed it) finds the same terminal state and stops. Because backoff is a loop timer (`ioloop.call_later`) rather than a blocking sleep on a separate thread, there is nothing analogous to waking a sleeping backoff early: `close()`'s transition simply makes the timer's own callback a no-op when it fires, or the pending timer can be cancelled outright at `close()` time via the handle `ioloop.call_later` returns.

## Persistent IOLoop and the redial sequence

`pika.adapters.select_connection.SelectConnection.__init__` already accepts a `custom_ioloop` parameter (also true of the asyncio/tornado connection adapters) and wraps it as the connection's `nbio` service - the mechanism this design needs already exists in pika, just unused by `ThreadSafeConnection` today. We propose `ThreadSafeConnection.__init__` construct one `IOLoop` instance up front (`self._ioloop`), start the background thread running `self._ioloop.start()` (not `self._connection.ioloop.start()` as today, which ties the thread's lifetime to the inner connection), and construct the first `SelectConnection` with `custom_ioloop=self._ioloop`. A redial then constructs a **new** `SelectConnection(parameters, ..., on_open_callback=..., custom_ioloop=self._ioloop)` bound to the *same* persistent loop and thread - the thread never stops and never gets rebuilt across a reconnect, which is what makes driving recovery on it (instead of a second thread) possible in the first place.

Redial therefore does not need shared `_open_inner_connection`/ `_start_ioloop_thread` helpers extracted for `__init__` to call: there is no "fresh IOLoop thread" to build on redial, only a fresh inner `SelectConnection` object bound to the loop that already exists - a small, self-contained change scoped to `__init__` and the redial path.

`ThreadSafeConnection._recover_connection(reason)`, run as a sequence of loop-scheduled steps rather than a thread body:

1. If `attempt >= config.max_attempts`: transition to `CLOSED` with the last error as `_closed_reason`, fire `on_recovery_failed`, run the usual pool-shutdown/close-callback teardown. Done.
2. Otherwise, schedule `_try_reconnect_once` via `self._ioloop.call_later(config.next_interval(attempt), ...)`. There is no condition variable to wait on and no backoff to interrupt early: if `close()` runs before the timer fires, the terminal-state check inside the scheduled callback (or cancelling the timer handle at `close()` time) is sufficient, since both `close()` and the timer callback execute on the same loop and cannot race each other.
3. `_try_reconnect_once()`:
   - Constructs a fresh `SelectConnection(self._parameters, ..., custom_ioloop=self._ioloop)`.
   - On success, swaps `self._connection` (still under `_channel_waiters_lock`, for the benefit of cross-thread readers of e.g. `is_open`), fires `add_on_open_callback` - before topology replay runs, so a listener sees "we have a live connection again" as distinct from and earlier than "topology is fully recovered." The app's existing `ThreadSafeChannel`/`ThreadSafeConnection` references stay valid.
   - Calls `_reopen_channels_and_recover_topology()` (below). On its completion: `attempt = 0`, transition to `OPEN`, fire `on_recovery_succeeded(connection, skipped)`.
   - On failure: `attempt += 1`, go to step 1.

`_reopen_channels_and_recover_topology()` reopens every `ThreadSafeChannel` in `self._channels` the app hasn't explicitly closed (tracked via `ThreadSafeChannel._closed`, since `self._channels` never removes entries), closing any still-open previous raw channel first rather than only overwriting the reference; if `topology_recovery_mode` is `DISABLED`, stop there; otherwise call `_recover_topology(channels)`.

### What the persistent loop costs

Driving recovery on the connection's own loop is a deliberate divergence from every reference client. All three drive recovery on a separate thread of control:

- The RabbitMQ Java client tears its I/O loop down *first* (`AMQConnection.closeMainLoopThreadIfNecessary`, called immediately before `notifyRecoveryCanBeginListeners`), then recovers on another thread. `AutorecoveringConnection.beginAutomaticRecovery` is `synchronized` and blocks on `wait(delay)` and `Thread.sleep`, so it could not run on an I/O thread even if it wanted to.
- The .NET client runs an explicit recovery loop on its own task (`AutorecoveringConnection.Recovery.cs`: `_recoveryTask = Task.Run(RecoverConnectionAsync)`), with a matching `StopRecoveryLoopAsync`.
- `amqp091-go` recovers in a goroutine that blocks on `time.After(RetryInterval() + jitter)`.

We diverge because a second thread has to synchronize with the state machine it mutates, and that synchronization is the complexity this design exists to avoid: a re-entrancy flag, a cross-level race guard, and condition-variable backoff, all protecting invariants the loop already enforces for free by running one callback at a time.

The divergence does not remove complexity. It relocates it, and the price is two obligations that simply do not arise when recovery owns a thread:

1. **Nothing on the loop thread may block.** A blocking call there waits on an event that only a future iteration of the same loop can set, and the loop cannot reach that iteration while blocked. The next section covers this for topology replay; the teardown table below covers it for pool joins, which is the same hazard reached by a different path.
2. **Recovery must undo the teardown the drop path already performed.** pika's drop path assumes the loop is going away and latches state on that assumption. Once the loop persists, every one of those latches is a bug.

Both are absolute rather than best-effort, and they fail in opposite directions. Violating the first hangs the connection with no exception to catch and no timer left running to notice. Violating the second leaves a connection that reports itself successfully recovered while refusing all work.

### Teardown the drop path performs, and what recovery must undo

On any unexpected drop, pika performs an adapter-wide teardown before `_on_connection_closed` returns. Every step below is reached today, in this order, and each assumes the connection is not coming back. Recovery must suppress or reverse each one; the reset paths for items 2, 4 and 6 currently exist only in `__init__`.

| # | Site | What it latches | What recovery must do |
|---|---|---|---|
| 1 | `pika.connection.Connection._on_stream_terminated` | Calls `_on_close_meta(self._error)` on every channel **before** dispatching `ON_CONNECTION_CLOSED` | Nothing directly, but note the ordering: step 2 has already run for every channel by the time the funnel sees the drop |
| 2 | `Channel._on_broker_close` -> `_claim_pool_shutdown` | `_pool_shutdown = True`; removes the channel from `wrapper._channels`; shuts the consumer work pool (`_shutdown = True`) | Suppress. A `StreamLostError` is not a client close, and the wrapper has to survive in order to be recovered |
| 3 | `Connection._on_connection_closed` | `self._connection.ioloop.stop()` | Suppress. Under `custom_ioloop` this *is* the shared loop, so stopping it ends the thread recovery runs on |
| 4 | `Connection._on_connection_closed` | `_record_closed_reason(reason)`, read by roughly sixteen sites including `close()`'s early return | Suppress, or reverse on the `RECOVERING -> OPEN` transition |
| 5 | `_run_ioloop` tail | `_shutdown_all_consumer_pools()` and `_shutdown_connection_pool()`, both unbounded thread joins | Nothing: it never runs while the loop persists, which is the point. The requirement is that it must not be relocated into a loop callback, which obligation 1 forbids |
| 6 | `Channel._arm_seq_no` | Leaves `_next_publish_seq_no` armed on the surviving wrapper, guarded by "arm only once" | Reverse. See "Publisher confirms hook" |
| 7 | `Connection._on_connection_open_error` | `ioloop.stop()`, plus `_connect_error`, `_connected_event` and the user's `on_open_error_callback` | Suppress the first three. The callback is a public-contract fork: see "The funnel is not the only death path" |

Item 7 is not reached by the drop that starts recovery; it is reached by every redial that fails to connect, which is the common case during an outage. It is in this table because it latches the same state, and because suppressing item 3 without also suppressing item 7 yields a recovery that works only when the first redial succeeds.

Item 2 is the one most easily missed, because its consequence is silent. A recovered channel whose pool stayed shut still accepts a `basic_consume`, and the broker still delivers to it; every delivery then fails `submit()` with `RuntimeError`, which `_submit_or_terminate` swallows at `debug`. The connection looks healthy, the consumer looks subscribed, and no message is ever handed to the application.

### Topology replay must not use the blocking wrapper API

**This needs flagging before implementation starts.** `ThreadSafeChannel.queue_declare` and friends work by blocking the *calling* thread on a `threading.Event` that only gets set when the IOLoop thread processes the broker's reply and calls `add_callback_threadsafe`'s corresponding wakeup. That's fine when the caller is an app thread - some other thread is blocked, and the IOLoop thread is free to run and eventually set the event. It self-deadlocks if the *IOLoop thread itself* calls it: the loop thread would block waiting on an event that only its own future iteration (processing the reply frame) can set, and it can never reach that future iteration because it's blocked.

Because recovery runs on the loop thread itself, `_recover_topology` cannot call `ch.exchange_declare(...)` / `ch.queue_declare(...)` the way an app would. It must instead drive the underlying raw `Channel`'s non-blocking, callback-based API directly (`raw_channel.exchange_declare( exchange=..., callback=on_declared)`), chaining each phase's entities through their own reply callbacks rather than sequential blocking calls. The phased ordering requirement (all exchanges before any queue, all queues before any binding, all bindings before any consumer - see "Topology ledger" below) is unaffected; only the mechanism for advancing from one entity to the next changes, from "the call returns" to "the callback fires." This is a hard correctness requirement: a callback-chained implementation that accidentally reintroduces a blocking call on the loop thread would hang the connection the first time recovery has more than a trivial amount of topology to replay, silently, with no exception to catch it (the loop simply stops servicing everything, including the timers that would otherwise detect the stall).

### Consumer delivery tags across recovery

Delivery tags are channel-scoped and restart from 1 on a reopened channel, so every tag an application holds when the socket drops is invalid afterwards. This is not a narrow window: a consumer callback runs on a `_BoundedWorkPool` worker, not the loop thread, so a long-running callback can still be holding a tag while the redial completes. `basic_ack` resolves `self._channel` inside the closure it schedules, so an ack issued from that callback lands on the **replacement** channel. Either the new channel's counter has already reached that value, in which case a different message is acknowledged and the loss is silent and undetectable, or it has not, in which case the broker answers 406 `unknown delivery tag` and closes the channel recovery just rebuilt - which re-enters channel recovery, driven by the application's own acks.

The Java and .NET clients both solve this with a delivery-tag offset, and we adopt it. `amqp091-go` has no equivalent: `Channel.Ack` transmits the tag exactly as given, with no staleness check anywhere in the package. Since this proposal otherwise ports `amqp091-go`'s recovery shape, that absence is worth stating plainly - following our primary reference here would reproduce the bug.

The mechanism. The `Channel` wrapper survives the swap, so it holds `_delivery_tag_offset` and `_max_seen_delivery_tag`, both starting at zero:

- `_wrapped_callback` updates `_max_seen_delivery_tag` to `max(tag, _max_seen_delivery_tag)`, then hands the user callback a `Basic.Deliver` carrying `tag + _delivery_tag_offset`. The application therefore sees one continuous, monotonically increasing tag space across any number of recoveries. Whether the offset is applied by mutating the decoded frame or by copying it is an implementation detail to settle against how `Basic.Deliver` is decoded per delivery.
- `basic_ack`, `basic_nack` and `basic_reject` compute `real = delivery_tag - _delivery_tag_offset`. When `real <= 0` the tag predates the current channel generation: return without transmitting, logging at `debug`. Otherwise transmit `real`.
- On reopen, `_delivery_tag_offset += _max_seen_delivery_tag` and `_max_seen_delivery_tag = 0`.
- `basic_ack(0, multiple=True)` and `basic_nack(0, multiple=True)` keep their protocol meaning of "everything outstanding" and pass through rather than being treated as stale. `basic_reject` takes no `multiple` argument, so it needs only the `real <= 0` guard.

Dropping a stale acknowledgement is correct rather than lossy. The broker requeues every unacknowledged delivery when it detects the connection loss, so the message a stale tag referred to has already been returned to its queue and will be redelivered; acknowledging it is meaningless and the only alternative is a guaranteed channel exception. The offset is what makes the drop *precise* rather than a guess: without it, a stale tag is indistinguishable from a legitimate tag on the new channel.

The consequence for applications is the one `findings.md` already records: delivery is at-least-once and consumers must be idempotent. A message being processed when the connection dropped is redelivered whether or not that processing completed. This is the single place where the "no application code changes" goal does not hold, and it does not hold for any client - it is a property of the protocol, not of the design.

### Publisher confirms hook

The `RECOVERING -> OPEN` transition is the natural place to signal "the confirm sequence has reset; treat outstanding publishes as unknown" - `findings.md` shows both reference clients reset delivery/confirm tags across recovery (unconfirmed set discarded in the Java client, pending `DeferredConfirmation`s nacked in `amqp091-go`). A confirm-tracking helper can subscribe via `add_state_change_listener` (or the `add_on_recovery_succeeded_callback` sugar) to fail and republish outstanding confirms at exactly that transition, giving the at-least-once pattern `findings.md` demonstrates a clean anchor instead of ad hoc detection. Consumers on the republish side must be idempotent - recovery can produce a genuine duplicate delivery when a message reached the broker but its confirmation was lost to the drop, the same as both reference clients.

### Channel-level recovery (broker-initiated single-channel close)

A broker soft-error (e.g. 406 `PRECONDITION_FAILED`, 404 `NOT_FOUND`) can close one channel while the connection stays healthy. `ThreadSafeChannel.__init__` registers a permanent close listener (`_register_recovery_close_listener`) whenever `wrapper._recovery is not None`, wired to `ThreadSafeConnection._on_channel_closed_for_recovery`.

`_recover_channel(ch, reason)` mirrors `_recover_connection`'s shape but scoped to one channel and driven the same way - loop-scheduled steps, not a thread body: transition `ch._state` to `RECOVERING` (firing `ch`'s own `on_recovery_started`), reopen just this channel, call `_recover_topology([ch])`. On success, transition `ch._state` to `OPEN` (firing `ch.on_recovery_succeeded`); on exhaustion, transition to `CLOSED` (firing `ch.on_recovery_failed`) - **never** the connection-wide listeners, which stay reserved for `_recover_connection`, for the structural reason given under "Observability" above. Exhaustion does **not** tear down the whole connection - that one channel is simply left `CLOSED`. `_recover_topology([ch])` still recovers against the full connection-wide `coordinator.topology` (filtered to entries relevant to `ch`), not a store scoped to `ch` alone.

## Composing channel-level and connection-level recovery

Channel-level and connection-level recovery passes can interact in ways that need explicit handling:

- **Scenario A**: a channel-level recovery pass retriggering itself - a second broker-initiated close notification for the same channel arriving while its own `_recover_channel` pass is already in flight.
- **Scenario B**: a connection-level pass starting while a channel-level pass already owns a channel - the whole connection drops while one channel is mid-recovery from its own soft error.

Because everything relevant runs on one loop thread, neither scenario involves two callers actually executing at the same instant - only one callback body ever runs at a time - but callback chains can still interleave across their suspended points, so both still need guarding.

**A channel's effective state derives from the connection's state.** Rather than tracking "is this specific channel mid-recovery" as an independent boolean, a channel is `RECOVERING` if either its own `_state` says so, *or* the connection's `_state` is `RECOVERING`. Concretely, the guard a public channel method checks is `effective_state = CONNECTION.RECOVERING implies RECOVERING else ch._state`. This answers Scenario B by construction: once the connection transitions to `RECOVERING`, every channel is observably `RECOVERING` too, immediately, with no separate flag to propagate and nothing to wait for.

**What single-threaded execution does not remove: staleness across a suspended callback chain.** `_recover_channel`'s own `_recover_topology([ch])` call is a chain of callbacks, each waiting on a broker reply - it is suspended, not actively running, between those replies. The connection can still drop for real while a channel-level pass is suspended mid-chain, because the drop is itself just another callback the loop will run next. The two are never *concurrent* (only one callback body ever executes at a time), but they can still **interleave**: the channel-level pass's next callback can fire after the connection-level transition has already happened. Every callback in `_recover_channel`'s chain checks `self._connection._state == ConnectionState.RECOVERING` (or worse) before proceeding to its next step, and yields - clearing its own bookkeeping, logging at `info`, **not** firing `ch.on_recovery_failed` - if a connection-level pass has taken over. This is a hand-off, not a failure the app needs to hear about as one.

**Scenario A** reduces to a per-step check the same way: because only one callback body runs at a time, `_on_channel_closed_for_recovery` firing again for a channel that already has a `_recover_channel` chain in flight can simply check `ch._state == ChannelState.RECOVERING` and return early - there is no window between "decide to start a pass" and "the pass's state change actually lands" for a second invocation to slip through, because both the decision and the state change happen in the same, uninterrupted callback.

**The redundant-reopen / orphaned-raw-channel risk is real regardless of thread model**: if a channel-level pass's replay completes successfully in the same window a connection-level pass's own `_reopen_channels_and_recover_topology()` reaches that channel, the connection-level pass still reopens and redeclares it again as part of its complete sweep - it does not try to detect "was this one already handled." `_reopen_channel(ch)` must explicitly close any existing, still-open `ch._channel` before installing its replacement, or that redundant reopen leaks a channel number that was never sent a `Channel.Close` but is no longer tracked client-side.

## Topology ledger

AMQP exchanges, queues, and bindings are scoped to the connection (vhost), not to whichever channel happened to declare them, and any channel can reference or delete an entity another channel created. A store that isolated each channel's entries from every other channel's would risk two concrete, broker-reproducible failure modes:

1. **Ordering failure.** Channel A declares exchange `X` and queue `Q`; channel B declares a binding from `Q` to `X`. If entries were recovered channel-by-channel and B happened to be processed before A, B's `queue_bind` would 404 against an exchange and queue that don't exist yet.
2. **Split-brain removal.** Channel A declares queue `X`; channel B later calls `queue_delete('X')`. If removal only searched the calling channel's own records, B's delete would find nothing, and `X` would get incorrectly redeclared on the next recovery as a queue the app had explicitly deleted.

We propose the same shape as `amqp091-go`'s `Connection. topologyConfiguration map[uint16]*TopologyConfiguration`: `coordinator.topology: dict[int, ChannelTopology]`, keyed by `channel_number`, on `RecoveryCoordinator` - `ThreadSafeChannel` itself holds no topology state. `ChannelTopology` holds:

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

We propose the field name `exchange_type` (matching pika's own parameter name throughout `ThreadSafeChannel`) rather than a generic `kind`.

**Recording:** `ThreadSafeChannel.exchange_declare`, `queue_declare`, `queue_bind`, `exchange_bind`, `basic_consume`, `basic_qos`, `confirm_delivery` each call a coordinator method - `self._connection._recovery.record_exchange(self.channel_number, record)` and so on - guarded by `if self._connection._recovery is not None:`, after the broker ack succeeds, under `_channel_waiters_lock` as described above.

**Removal:** `exchange_delete`, `queue_delete`, `queue_unbind`, `exchange_unbind`, `basic_cancel` symmetrically call `remove_exchange(name)`, `remove_queue(name)`, etc. - no channel argument, scanning every bucket in `topology`. `remove_queue`/`remove_exchange` cascade: deleting a queue removes any binding referencing it from every bucket, returning the exchanges those bindings sourced from so an auto-delete exchange left with no remaining bindings can be forgotten too.

**`TopologyRecoveryMode.ONLY_TRANSIENT`** narrows what `_recover_topology` redeclares, unioning transient queue/exchange names across every channel's bucket before the phased pass begins: a queue is transient if `exclusive` or `auto_delete` is set, an exchange is transient if `auto_delete` is set; consumers, QoS, and confirm mode are never filtered by this mode, since they're lost with the channel on every reconnect regardless of queue durability.

`basic_consume`'s `_wrapped_callback` closes over the caller-supplied `on_message_callback` value directly, so recovering via `ch.basic_consume(queue, on_message_callback, consumer_tag=tag, ...)` re-creates an identical closure around the *same* Python callback object - the mechanism `TestConsumeContinuityAcrossRecovery` depends on.

Server-generated queue names: `queue_declare('')` records `declared_name=''`, `actual_name=<broker-assigned>`; on recovery we redeclare with `declared_name=''` again, note the rename in `_recover_topology`'s local name-replacement map, and rewrite `actual_name` plus any `BindingRecord`/`ConsumerRecord` across every bucket still referencing the old name.

### Skip-and-continue must reopen the channel

A broker-side protocol error during topology recovery (e.g. a 404 on a binding referencing a since-deleted queue) closes the entire channel, not just the offending entity. `_recover_topology`'s `_skip_or_abort` helper checks `ch.is_closed` after recording a skip and, if so, calls `_reopen_channel(ch)` before continuing with the channel's remaining entities - driven through the same callback-chained mechanism described above, not a blocking reopen call.

## Proposed file-by-file changes

- **`pika/recovery.py`** (new): `RecoveryConfig`, `TopologyRecoveryMode`, `RecoveryCoordinator` (holding `config`, `attempt`, and `topology: dict[int, ChannelTopology]`, plus the `record_*`/`remove_*` methods described in "Topology ledger" above), `ChannelTopology`, the `*Record` dataclasses, `TopologyRecoveryEntity`.
- **`pika/exceptions.py`**: `ConnectionRecovering(ConnectionWrongStateError)`, `ChannelRecovering(ChannelWrongStateError)`.
- **`pika/adapters/thread_safe_connection.py`**:
  - `ThreadSafeConnection.__init__` gains `recovery=`, `self._recovery`, `self._state: ConnectionState`, `self._ioloop` (constructed once, no longer tied to a single inner `SelectConnection`), `self._parameters`, `self._connect_timeout`.
  - `_state`-guarded replacement for `_check_not_closed`; `state`/ `is_recovering` properties; `add_state_change_listener`, `add_on_close_callback`, `add_on_open_callback`, `add_on_recovery_*_callback` methods (the latter three implemented as listeners on `add_state_change_listener`, per the table above).
  - `is_open`/`is_closed` on both `ThreadSafeConnection` and `ThreadSafeChannel` changed from delegating to the inner `Connection`/`Channel` object to reading `self._state` directly (see "Observability" above).
  - Extended `_on_connection_closed`; `_recover_connection`, `_try_reconnect_once` (constructing new inner `SelectConnection` instances bound to the persistent `self._ioloop`), `_reopen_channels_and_recover_topology`, `_reopen_channel`, `_recover_topology` (driving the raw `Channel`'s non-blocking API directly - see "Topology replay must not use the blocking wrapper API" above), `_on_channel_closed_for_recovery`, `_recover_channel`.
  - `ThreadSafeChannel` gains `self._closed`, `self._state: ChannelState`, an effective-state check that also consults the connection's state (see "Composing channel-level and connection-level recovery" above), `_register_recovery_close_listener`, `add_state_change_listener`, `add_on_close_callback`, `add_on_open_callback`, `add_on_recovery_started_callback`, `add_on_recovery_succeeded_callback`, `add_on_recovery_failed_callback` (own listener list, populated only by `_recover_channel` - never by `_recover_connection`), plus recording/removal call sites in the declare/bind/consume/delete/unbind/ cancel methods that delegate to `self._connection._recovery.record_*`/ `remove_*`.
  - Ten existing method signatures (`basic_qos`, `basic_cancel`, `queue_declare`, `exchange_declare`, `queue_bind`, `queue_unbind`, `queue_delete`, `exchange_bind`, `exchange_unbind`, `exchange_delete`) need their declared return type corrected from `-> None` to `-> Any`.
- **`examples/thread_safe_recovery_example.py`** (new): a `ThreadSafeConnection` with `recovery=RecoveryConfig()`, a background publisher thread that catches `ConnectionRecovering`/`ChannelRecovering` and retries after `on_recovery_succeeded` fires, a consumer registered via `basic_consume`, and state-change listeners logging transitions.
- **`tests/acceptance/thread_safe_recovery_test.py`** (new, requires RabbitMQ): acceptance tests, listed in the test plan below.
- **`tests/unit/recovery_tests.py`** (new, mock-based): unit tests covering config/coordinator/ledger logic and the guard behavior.
- **`tests/unit/thread_safe_connection_tests.py`**: existing bare `MagicMock()` fixtures used as `ThreadSafeChannel` wrappers will need to explicitly set `wrapper._recovery = None` (a plain `MagicMock()` is truthy for any attribute access, so an un-set `_recovery` would look like an opted-in `RecoveryCoordinator`).

`pika/spec.py` remains untouched - no protocol/spec changes are needed, since recovery is pure client-side orchestration of existing AMQP methods.

## Proposed test plan

### Integration (`tests/acceptance/thread_safe_recovery_test.py`, real broker)

We propose simulating drops with **`ForwardServer`** (an existing test helper that proxies TCP to the real broker in a subprocess), the same technique `tests/acceptance/thread_safe_connection_test.py` already uses. Queue/exchange names would be uuid-suffixed.

1. `TestPublishContinuityAcrossRecovery` - publish before a drop, confirm it landed (via passive declare), publish again after recovery succeeds (via a retrying helper), assert both messages are present. Durable, non-exclusive queue.
2. `TestConsumeContinuityAcrossRecovery` - **the core requirement**: register `basic_consume` once, drop the connection, publish more messages from a second, independent connection after recovery completes, assert they arrive at the *original* callback with zero additional app calls.
3. `TestExclusiveQueueRecovery` - an exclusive queue (deleted by the broker on disconnect) is transparently redeclared; verify via a passive declare on the reopened channel after recovery succeeds.
4. `TestDeletedQueueSkipAndContinue` - a binding to a queue declared on a separate, untracked connection is deleted while the primary connection is down, so binding recovery gets a real 404. Assert `on_topology_entity_error` fires with a `'binding'` entity whose `channel_number` matches, `on_recovery_succeeded`'s `skipped` list contains it, and everything else recovers fully.
5. `TestTopologyRecoveryDisabled` - `topology_recovery_mode=DISABLED`: reconnects but a previously-declared queue is not redeclared.
6. `TestOnlyTransientTopologyRecovery` - `ONLY_TRANSIENT`: an exclusive queue and its binding to a durable exchange are redeclared; a durable queue deleted out-of-band before the drop is not recreated.
7. `TestRetryExhaustionRaisesCleanly` - `max_attempts=2` against a forwarder that's never restarted; assert `on_recovery_failed` fires, the original close callback fires, `connection.state == ConnectionState.CLOSED` (not a separate `FAILED` value), with no hang.
8. `TestExplicitCloseDoesNotTriggerRecovery` - `connection.close()` never fires `on_recovery_started`.
9. `TestDefaultBehaviorUnchangedWithoutRecoveryConfig` - regression guard: omitting `recovery=` reproduces today's exact wake-all/teardown behavior on a forced drop.
10. `TestOperationDuringRecoveryRaisesDedicatedException` - drop the connection, and while `state == ConnectionState.RECOVERING` (before the redial completes), issue a `basic_publish` and assert it raises `ConnectionRecovering` (or `ChannelRecovering`, for a channel-scoped call) synchronously rather than timing out. Repeat for a channel-scoped soft-error recovery episode.
11. `TestChannelLevelRecoveryWithoutFullConnectionReconnect` - redeclaring an existing exchange with mismatched durability triggers a 406 that closes only that channel; assert the channel's own `add_on_recovery_succeeded_callback` fires while the connection-wide ones never fire.
12. `TestMultiChannelTopologyRecoveryOrdering` - channel 1 declares a transient exchange and a server-named exclusive queue; channel 2 declares the binding and the consumer. After a drop and recovery, assert the full exchange -> binding -> queue -> consumer chain is functional on both channels regardless of processing order.
13. `TestCrossChannelDeletionRemovesStaleTopology` - channel A declares a queue; channel B deletes it. Drop and recover; assert the queue is *not* incorrectly redeclared from channel A's side of the store.
14. `TestChannelRecoveryDoesNotDuplicatePassOnPermanentConflict` - a permanent per-entity conflict, skipped via `should_skip` every attempt, produces exactly one active `_recover_channel` episode for that channel at a time, not an unbounded chain.
15. `TestChannelCloseAndOpenCallbacksFireOnRecovery` - register `add_on_close_callback`/`add_on_open_callback` on a channel, force a broker-initiated single-channel close, assert close fires with the broker's reason and open fires once usable again, with no connection-level `on_recovery_*` firing in between.
16. `TestFullReconnectDoesNotFireChannelRecoveryCallbacks` - force a full connection drop on a connection whose channels have `add_on_recovery_succeeded_callback` registered; assert the connection-wide callback fires exactly once while no per-channel `on_recovery_*` fires for any channel.
17. `TestConnectionCloseAndOpenCallbacksFireOnRecovery` - register `add_on_close_callback`/`add_on_open_callback` on the connection; assert `add_on_open_callback` fires once the redial succeeds and *before* `on_recovery_succeeded`; assert `add_on_close_callback` fires on an explicit `close()` even with no `RecoveryConfig` at all.
18. `TestConnectionRecoverySupersedesInFlightChannelRecovery` - trigger an isolated channel-level recovery, and while it's mid-backoff, force a full connection drop. Assert the channel-level pass's `on_recovery_failed` never fires (it yields), the connection-level pass's `on_recovery_succeeded` fires once, and the channel ends up open and recovered exactly once.
19. `TestSupersededChannelRecoverySucceedsAnyway` - the timing variant of
    #18 where the channel-level pass's in-flight attempt completes right as
    the connection-level pass takes over: assert the channel ends up with exactly one live raw channel afterward.
20. `TestPersistentIOLoopSurvivesReconnect` - assert the same `self._ioloop`/thread identity is used before and after a forced drop and successful redial (e.g. by tagging the thread object and comparing identity, not just liveness), confirming the redial did not spin up a second thread.

We'd want these run multiple times in CI to check for flakiness before merging, given the timing-sensitive nature of drop simulation.

### Unit (`tests/unit/recovery_tests.py`, mocked)

- Config/entity/coordinator: backoff math, `should_skip` behavior, `RecoveryConfig.topology_recovery_mode` defaulting to `ALL`, coordinator defaults (`RecoveryCoordinator` has no `state` field; lifecycle state lives on the adapter object, per "Where state lives" above).
- Topology store: record/remove semantics for every entity type (cross-channel removal, cascade removal, binding/exchange-binding dedup, server-generated-name rename propagation across buckets).
- State machine: every guarded public method raises `ConnectionRecovering`/`ChannelRecovering` (not a generic wrong-state error) when `state == RECOVERING`; the terminal rule (`CLOSING`/`CLOSED` -> only `CLOSED` may follow, even if a redial callback tries to set `OPEN` after `close()` ran); `state`/`is_recovering` properties reflect the current value under concurrent access from another thread.
- Callback-sugar correctness: each `add_on_*_callback` fires exactly on the transition table in "Observability" above, and not on any other transition; `add_state_change_listener` fires on every transition, including ones none of the named callbacks cover.
- Recovery-callback partition: `_recover_channel` fires only a channel's own listeners, never the coordinator/connection-wide ones, and vice versa for `_recover_connection` - asserted directly, so the split doesn't regress to a shared-list design.
- Composing channel/connection recovery: a channel's effective state reads `RECOVERING` while the *connection* is `RECOVERING`, even if the channel's own `_state` hasn't been individually transitioned; a `_recover_channel` callback chain that finds the connection has become `RECOVERING` mid-chain yields without firing `on_recovery_failed`; a second `_on_channel_closed_for_recovery` invocation for a channel whose `_state` is already `RECOVERING` returns immediately without starting a second chain.
- `_reopen_channel` closes a still-open previous raw channel before installing its replacement - exercised directly (call it twice on a channel that's still open in between).
- `_on_connection_closed` recovery triggers: fires for non-client closes, not for `ConnectionClosedByClient`; a second close event while already `RECOVERING` does not start a second recovery sequence or re-fire `on_recovery_started`.
- Close-during-recovery: `close()` sets the terminal state and is a no-op if called again; a pending reconnect timer either becomes a no-op or is cancelled outright, without ever blocking on a thread join beyond `self._ioloop_thread`, which `close()` already knows how to join today.
- Deadlock regression guard: `_recover_topology` never calls a `ThreadSafeChannel` blocking wrapper method (`queue_declare`, `exchange_declare`, etc.) from the loop thread - asserted by patching those methods to raise if invoked from the recovery code path, so a future change that accidentally reintroduces a blocking call during replay fails a unit test instead of hanging an acceptance test.

### CI gates before merge

Standard project gates apply and should all be green before merge: `hatch run fmt-check`, `hatch run lint-check`, `hatch run docfmt-check`, `hatch run typecheck`, `hatch run unit`, and the acceptance suite against a real broker.

## Open questions

These questions are raised in `design-state-machine.md` and remain open here:

- **Exact adapter state set.** This proposal uses `{OPEN, RECOVERING, CLOSING, CLOSED}` for both `ConnectionState` and `ChannelState`. Whether an `OPENING` value is also needed on the adapter (mirroring the base classes) for the initial-connect path, distinct from a post-drop `RECOVERING`, is open.
- **Scope of the state contract.** Should `ConnectionState`/`ChannelState` and the guard/exception pattern be a base-level concept shared by all adapters (asyncio/tornado/twisted already have a persistent external loop and could plausibly implement the same driver pattern with less new work than the thread-safe adapter needs), or thread-safe-adapter-only as scoped here? Recommendation: share the contract (states, guard, exceptions, listeners) at the level where the stable handle lives, with per-adapter recovery drivers. Deciding this changes where the code lands, so it's worth resolving before implementation rather than after.
- **Opt-in block-until-open.** Non-goals rules a blocking mode out of this proposal: a call during `RECOVERING` always fails fast. Whether to add an opt-in "block until open, with a timeout" mode later, for callers who would rather wait than handle the exception, is open. It would be purely additive, leaving fail-fast as the default.
- **Migration/compatibility**: confirming the new exceptions subclassing the existing wrong-state errors preserves current behavior for code that doesn't opt into recovery at all (it should, since a connection without `RecoveryConfig` never transitions to `RECOVERING`, but this needs an explicit regression test - see `TestDefaultBehaviorUnchangedWithoutRecoveryConfig`).

## Honest unknowns

- The claim that recovery can run entirely on the persistent loop with no extra thread rests on `SelectConnection`'s existing `custom_ioloop` support, confirmed to exist in `pika/adapters/select_connection.py`, but the full `_run_ioloop`/thread-lifecycle refactor in `ThreadSafeConnection.__init__` needed to decouple the thread from the inner connection has not been prototyped end-to-end. A minimal prototype - persistent `self._ioloop`, one forced redial, one `RECOVERING`-gated `basic_publish` raising the new exception - would settle the remaining risk before the full phased build-out below.
- The callback-chained rewrite of `_recover_topology` (driving the raw `Channel`'s non-blocking API instead of the blocking wrapper methods) touches every entity type's declare/bind/consume call site, not just the phased-ordering logic that sits on top of it - a larger mechanical change than the topology ledger's data model alone suggests.

## Next steps

Pending sign-off on the direction above, implementation proceeds in phases, each closed out with its own unit and integration coverage:

1. **Core state machine** - `ConnectionState`/`ChannelState`, `ConnectionRecovering`/`ChannelRecovering`, `state`/`is_recovering` properties, `add_state_change_listener` and the `add_on_*_callback` sugar, guard integration into every existing public method's not-closed check. No topology, no reconnection yet - this phase makes "is it recovering" answerable and enforced even before recovery can succeed at anything.
   - Unit: guard raises the dedicated exception per state; terminal-rule enforcement; callback-sugar-fires-on-correct-transition tests.
   - Integration: none yet (no reconnection exists to exercise).
2. **Persistent IOLoop and connection-level redial** - decouple `self._ioloop`/thread from the inner `SelectConnection` in `__init__`; `_on_connection_closed` branching; `_recover_connection`, `_try_reconnect_once` bound to `custom_ioloop=self._ioloop`; `close()` reworked to rely on the terminal rule rather than a condition variable.
   - Unit: recovery-trigger conditions; close-during-recovery; persistent loop identity across a forced redial.
   - Integration: `TestRetryExhaustionRaisesCleanly`, `TestExplicitCloseDoesNotTriggerRecovery`, `TestDefaultBehaviorUnchangedWithoutRecoveryConfig`, `TestConnectionCloseAndOpenCallbacksFireOnRecovery`, `TestPersistentIOLoopSurvivesReconnect`, `TestOperationDuringRecoveryRaisesDedicatedException` (connection-level half).
3. **Channel-level recovery and state composition** - `_register_recovery_close_listener`, `_on_channel_closed_for_recovery`, `_recover_channel`, the effective-state derivation rule from "Composing channel-level and connection-level recovery." Both the Scenario A guard (a per-step state check) and the Scenario B guard (effective-state derivation) land in the same commit as `_recover_channel` itself: shipping the retry logic first and adding either guard later would leave a window where a permanent per-entity conflict spawns an unbounded chain of recovery episodes, or a channel-level and connection-level pass double-reopen the same channel.
   - Unit: effective-state derivation; per-step staleness checks; recovery-callback partition.
   - Integration: `TestChannelLevelRecoveryWithoutFullConnectionReconnect`, `TestChannelRecoveryDoesNotDuplicatePassOnPermanentConflict`, `TestChannelCloseAndOpenCallbacksFireOnRecovery`, `TestFullReconnectDoesNotFireChannelRecoveryCallbacks`, `TestConnectionRecoverySupersedesInFlightChannelRecovery`, `TestSupersededChannelRecoverySucceedsAnyway`, `TestOperationDuringRecoveryRaisesDedicatedException` (channel-level half).
4. **Topology ledger and callback-chained replay** - `coordinator.topology`, `ChannelTopology` and the `*Record` dataclasses, `record_*`/`remove_*` methods and call sites, `_recover_topology` rewritten against the raw `Channel`'s non-blocking API (see "Topology replay must not use the blocking wrapper API" above), server-generated- name rename handling, skip-and-continue reopen logic. **The topology store must be connection-wide from the first commit of this phase**, for the ordering/split-brain-removal reasons in "Topology ledger" above. **The callback-chained replay mechanism must also land complete in this phase**, not as a "blocking calls for now, convert later" intermediate step - a blocking call issued from the loop thread hangs the connection outright rather than degrading gracefully, so there is no safe partial version of this phase to ship.
   - Unit: `TopologyStoreTests`; the deadlock regression guard described above.
   - Integration: `TestPublishContinuityAcrossRecovery`, `TestConsumeContinuityAcrossRecovery`, `TestExclusiveQueueRecovery`, `TestDeletedQueueSkipAndContinue`, `TestTopologyRecoveryDisabled`, `TestOnlyTransientTopologyRecovery`, `TestMultiChannelTopologyRecoveryOrdering`, `TestCrossChannelDeletionRemovesStaleTopology`.
5. **Publisher confirms hook** - the `RECOVERING -> OPEN` reset point described above, plus an example helper showing the fail-and-republish pattern from `findings.md`.
6. **Hardening pass** - run the full acceptance suite repeatedly to check for flakiness, run `fmt-check`/`lint-check`/`docfmt-check`/`typecheck` across all changed files.
7. **Example and docs** - `examples/thread_safe_recovery_example.py`, docstrings, changelog entry.

Each phase should be its own reviewable PR (or a small stack of PRs) rather than one large PR at the end, so reviewers can weigh in on the state machine and the persistent-loop refactor before the topology ledger is built on top of them.
