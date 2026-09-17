# Design: Auto-recovery for pika's Connection

Before reading: `README.md` in this directory records the pika 2.0.0 constraints that govern this design - one `Connection`/`Channel` type, all other adapters removed, and the API free to change. They override anything below that assumes otherwise.

> Status: proposal, for review before implementation begins. Recovery is a first-class `RECOVERING` state on the adapter `Connection`/`Channel` lifecycle, driven by the connection's own persistent IOLoop rather than a separate thread. This follows the state-machine framing explored in `design-state-machine.md`, grounded in the empirical client behavior recorded in `findings.md`.

## Editing this document

Guidance for anyone editing this file, AI or human. These are the rules from `~/genai/git/GIT.md` and the repository's own conventions that actually bite here; a change that breaks one is wrong even if the prose is good.

- **Never hard-wrap.** Every paragraph, list item and table row is a single line, however long. This file was hard-wrapped for most of its life and has been unwrapped; do not reflow it back. The rendered output is identical either way, so the cost of wrapping is entirely to the tools that read the file as text: line-anchored citation, `grep -n`, and per-line diff review all become unreliable when one sentence spans several lines and a one-word edit reflows the paragraph.
- **ASCII punctuation only.** No em-dashes and no arrow characters. Use `-` or `--` for a dash and `->` for a transition. This is measured repository convention, not preference: across the 58 tracked `.md` files outside `design/` there are six em-dashes in total.
- **Bare commit SHAs, never in backticks.** GitHub auto-links a bare SHA and backticks suppress that.
- **One H1, the title above.** Any section pasted into a pull request, issue, review or comment body must not carry an H1, because the forge renders those oversized and the title is already redundant there.
- **No trailing whitespace, and blank lines are truly empty.**
- **Commit messages are the one place hard-wrap is required**: 50-70 character subject in active voice and present tense, body wrapped at 72. Write them to a file and use `git commit -F <file>` rather than `-m`, because shell escaping of apostrophes and backticks silently corrupts text.
- **Post forge bodies with `--body-file`, never an inline `--body`.** A double-quoted string containing backticks executes them as command substitution and the text simply disappears, exit code zero.
- **Prose derived from this document and posted under a maintainer's account needs an AI-authorship disclosure** if an AI drafted it, placed at the top of the body, stating only what the human actually did. Ask before adding one rather than adding it unilaterally or omitting it silently.

**Run `python3 design/check_docs.py` before handing any change over.** It exits non-zero on the defect classes reviews of this document have actually caught, rather than restating them as advice: a backticked `Class.member` that does not exist on the real class, a `file:line` citation past the end of the file, a quoted cross-reference matching no heading anywhere in the tree, a fenced Python block that does not compile, non-ASCII characters, trailing whitespace, more than one H1, and hard-wrapped paragraphs. Every check has been proven to fail on a planted defect; one of them was dead on arrival and only the planting revealed it. Members this design proposes but that do not exist yet are declared in the script's `PROPOSED` set, so adding one is deliberate and a typo in an existing name is still an error. A long `PROPOSED` list is a signal the design has drifted from the code.

**After correcting any error, grep for the class of it rather than the instance.** This is the one discipline the checker cannot enforce, and it is the failure that has recurred most in this document's history: a receiver corrected in one place and left wrong in three others, a mechanism reassigned for QoS and left misassigned for the confirm counter, a rule inferred from a method's name without reading its body. Correcting the instance you were shown is half the work.

Two conventions specific to this document rather than to git. Claims about pika's current behavior carry the `file:line` they were checked against, so a reader can re-verify rather than trust; if you cannot cite it, say it is unverified. And where this design diverges from the RabbitMQ Java, .NET or Go clients, say so explicitly and give the reason, because a reviewer who knows those clients will otherwise assume the divergence was an oversight.

## Glossary

Terms this document uses in a specific sense. Where a term names something in the code, the reference is given so a reader can check it rather than infer it.

**adapter `Connection` / `Channel`** - `pika.adapters.thread_safe_connection.Connection` and `.Channel`: the stable handles an application holds. Their identity survives a reconnect, which is what makes them the place recovery state can live. Renamed from `ThreadSafeConnection` / `ThreadSafeChannel` by #1617, which is merged on `main` against the 1.5.0 milestone; 1.5.0 is unreleased, so `pika.__version__` still reads 1.4.0.

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

pika has no built-in recovery today: an unexpected connection or channel loss wakes every blocked caller with an exception and tears the connection down (`Connection._on_connection_closed`). Nothing redials, and nothing remembers what topology or consumers existed. Every pika user currently hand-rolls reconnect logic (see `examples/asynchronous_consumer_example.py`, `examples/blocking_consume_recover_multiple_hosts_retry.py`).

RabbitMQ's Go client (`amqp091-go`) closed the equivalent gap with its `recovery.go` / `lifecycle.go` design: a topology ledger plus pluggable reconnection plus per-entity skip/abort error handling. We propose porting that shape to pika. Where we part ways with a straight port is the concurrency model: `amqp091-go` and the reference clients measured in `findings.md` all represent "currently reconnecting" as an observable, gate-able state, and we adopt that instead of treating recovery as invisible plumbing underneath an unchanged call surface.

## Direction

Give the adapter `Connection` and `Channel` a single authoritative lifecycle state that includes `RECOVERING`, alongside `OPEN`, `CLOSING`, and `CLOSED`. Every public operation guards on that state; an operation attempted while `RECOVERING` raises a dedicated, catchable exception (`ConnectionRecovering` / `ChannelRecovering`, each subclassing the existing wrong-state error). Recovery runs on the connection's own persistent IOLoop, not a second thread. State is both observable (a synchronous property and ordered listeners) and reactive (the guard), so an app can ask "am I recovering?" and also catch the fact that it is. This follows the framing explored in `design-state-machine.md`.

Fail-fast during recovery matches how existing clients actually behave: `findings.md` shows neither the RabbitMQ Java client nor `amqp091-go` lets a call during recovery run as it would on a healthy connection - both fail fast (`AlreadyClosedException` / `ErrClosed`), and `amqp091-go` documents a reconnect-handshake hazard where publishing blindly during recovery can interleave a frame with the `channel.open` handshake and cause a protocol violation. Both expect the application to gate on connection state.

Running recovery on the connection's existing IOLoop, rather than a dedicated thread, removes an entire class of concurrency machinery that a second thread would otherwise require - reentrancy flags, cross-thread atomicity guards, condition-variable backoff - because state transitions and the redial sequence itself execute on the same thread as every other piece of connection state. See "Composing channel-level and connection-level recovery" below for what concurrency risk remains once that's true.

## Scope

Recovery targets the adapter `Connection` and `Channel`, which in 2.0 are the only connection and channel types pika ships. `BlockingConnection` and the asyncio, gevent, tornado and twisted adapters are all removed in 2.0, and `SelectConnection` survives only as the internal machinery backing `Connection`. So there is no second adapter to scope this against, no shared state contract to negotiate, and no per-adapter recovery driver to design: whatever this proposal specifies is what pika does. See `README.md` for the constraints this rests on.

The adapter `Connection` today runs `SelectConnection`'s IOLoop on one dedicated background thread, 1:1 with the inner connection: when the inner connection dies, that thread's `self._connection.ioloop.start()` call returns and the thread exits. We propose decoupling them: the IOLoop thread and the `IOLoop` instance it runs become properties of the **adapter** `Connection` itself - constructed once in `__init__` and outliving any single inner connection - while `self._connection` (the inner `SelectConnection`) is what gets rebuilt on each redial. See "Persistent IOLoop and the redial sequence" below for how.

Every blocking call from a caller thread (`Channel._blocking_rpc`) still works exactly as it does today: registering a `(threading.Event, error-slot)` pair in `self._blocking_waiters`, scheduling the real work onto the IOLoop thread via `add_callback_threadsafe`, and blocking the caller thread on the event. `_on_connection_closed` remains the single "wake every blocked caller with this exception" mechanism, guarded by `self._channel_waiters_lock`; recovery **coexists with, rather than replaces**, that mechanism. Recovery's own state lives directly on the adapter `Connection`/`Channel` objects, as the same kind of state value `_check_not_closed` already reads today - see "Where state lives, and who owns the topology ledger" below.

## Goals

The concrete UX bar we want to hit, and intend to verify with an acceptance test (`TestConsumeContinuityAcrossRecovery`): an app consuming via `basic_consume(queue, callback)` keeps receiving messages on the *same* `callback` after a connection drop and automatic recovery, with **no application code changes**.

Recovery must be **opt-in** - default behavior stays unchanged unless the caller passes a `recovery=` config. We intend to cover this with a regression test (`TestDefaultBehaviorUnchangedWithoutRecoveryConfig`).

A call issued while the connection or channel is `RECOVERING` must fail **synchronously and catchably**, not time out or surface a generic already-closed error. We intend to verify this with `TestOperationDuringRecoveryRaisesDedicatedException` (below): a `basic_publish` (or any guarded call) issued between the drop and the `RECOVERING -> OPEN` transition raises `ConnectionRecovering` / `ChannelRecovering`, and an app can `except` that specific type.

## Non-goals

- No recovery for connections that do not opt in. A `Connection` built without a `RecoveryConfig` behaves exactly as it does today, never transitions to `RECOVERING`, and so never raises the new exceptions. `TestDefaultBehaviorUnchangedWithoutRecoveryConfig` is the regression guard for this.
- Recovery does not attempt to make in-flight synchronous RPCs survive a drop transparently (see "Where recovery hooks in" below, which specifies how in-flight waiters are handled) - a waiter blocked mid-call when the drop happens is woken with the close reason, exactly as it is today; it is not silently retried.
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
        Callable[[Connection, TopologyRecoveryEntity], bool] | None) = None
    # True (or None, default) -> skip entity, continue recovery.
    # False -> abort this recovery attempt, fall through to the outer retry loop.

    def next_interval(self, attempt: int) -> float: ...   # exponential backoff, capped at max_interval
    def should_skip(self, connection: Connection,
                     entity: TopologyRecoveryEntity) -> bool: ...

@dataclass
class TopologyRecoveryEntity:
    entity_type: str          # 'exchange' | 'queue' | 'binding' | 'exchange_binding' | 'consumer'
    name: str
    channel_number: int       # the Channel this entity's declare/bind/consume call was made on
    secondary_name: str = ''  # exchange for a queue binding, destination for an exchange binding
    routing_key: str = ''
    error: Exception | None = None
```

`RecoveryConfig` and `TopologyRecoveryEntity` carry the recovery policy and per-entity failure reporting; both are orthogonal to how state is represented. There is deliberately no separate `RecoveryState` enum on `RecoveryCoordinator` - "is the connection recovering" is answered by the connection's own lifecycle state, described next.

### Lifecycle state

```python
class LifecycleState(enum.Enum):
    OPEN = 'open'
    RECOVERING = 'recovering'
    CLOSING = 'closing'
    CLOSED = 'closed'
```

**One shared enum, deliberately, rather than a `ConnectionState` and a `ChannelState`.** An earlier draft defined two, with byte-identical members. Splitting them buys no type safety and creates a silent trap: distinct `Enum` classes never compare equal, so `ch._state == ConnectionState.RECOVERING` would be `False` no matter what the channel's state was. That is exactly the shape the effective-state rule produces, since deriving `<connection recovering> ? RECOVERING : ch._state` yields a value typed as the union of the two enums, and whichever member a caller then compares against, one arm is dead.

Do not expect the type checker to catch it if anyone reintroduces the split. `mypy.ini` sets `strict_equality = True`, and mypy does flag the direct form: `Non-overlapping equality check (left operand type: "ChannelState", right operand type: "Literal[ConnectionState.RECOVERING]")`. Run against the union-typed ternary it reports nothing at all. Verified by running mypy over both forms in one file: one error, on the direct comparison only.

So a single `LifecycleState` serves both the adapter `Connection` and `Channel`. This also matches the AMQP 1.0 Java client this design borrows from, where `ResourceBase` holds one `AtomicReference<State>` shared by every resource type rather than a per-resource enum.

These are new types, distinct from base `pika.connection.Connection`'s own `CONNECTION_CLOSED/INIT/PROTOCOL/START/TUNE/OPEN/CLOSING` and base `pika.channel.Channel`'s `CLOSED/OPENING/OPEN/CLOSING`. The base classes already guard on their states (`ConnectionWrongStateError`, `ChannelWrongStateError`) and are unaffected - a base `Connection` is 1:1 with a transport session, reaches `CLOSED`, and never comes back; recovery means constructing a new one. `LifecycleState` is the adapter-level state that makes that construction-of-a-new-one process observable on the *stable* handle the app holds. `Connection` does not have an equivalent state value today (only the informal `_closed_reason is None` check); this proposal adds one, specifically so `RECOVERING` has somewhere to live.

**Terminal rule**, borrowed from the AMQP 1.0 Java client's `ResourceBase`: once `CLOSING` or `CLOSED` is reached, only `CLOSED` may follow. A recovery attempt that succeeds after the app has already called `close()` cannot resurrect the handle back to `OPEN`.

Exhausting `config.max_attempts` transitions straight to `CLOSED` (with `_closed_reason` set to the last redial error), not to a separate `FAILED` value. From the app's perspective there is no meaningful difference between "the app closed this" and "recovery gave up" other than the reason attached, and folding them into one terminal state means the terminal rule above has only one destination to reason about.

### Guard and exceptions

```python
class ConnectionRecovering(ConnectionWrongStateError): ...
class ChannelRecovering(ChannelWrongStateError): ...
```

Every public operation that today calls `_check_not_closed()` gains a `_state` test that raises `ConnectionRecovering`/`ChannelRecovering` when the effective state is `RECOVERING` - fail-fast, matching `findings.md`, not block-until-open. **It gains the test; it does not replace what is there.** An earlier draft said "replacement", inferring from the method's name that it was a channel-state check. It is not: `Channel._check_not_closed`'s docstring reads "Raise if the *connection* is known to be closed", and its body tests only `self._wrapper._closed_reason`. Replacing it would delete the closed-connection test from `basic_publish`, `basic_ack`, `basic_nack`, `basic_reject`, `add_on_cancel_callback` and `add_on_return_callback` - and would do so unconditionally, including on connections that never opt into recovery, breaking the Non-goal that such a connection behaves exactly as it does today. So the closed-connection test stays and the state test is added in front of it. Two methods are exempt, `close()` and the acknowledgement family; both exemptions are specified below where the reasons live. Recovery-aware code can catch the precise case: `except ChannelRecovering: wait_for_open(); republish()`. Note that subclassing the wrong-state errors is **not** a backward-compatibility measure, whatever it looks like. This adapter's guards do not raise those types today: `_check_not_closed` re-raises the recorded close reason itself, `ConnectionWrongStateError` is raised at exactly one site (the narrow window where the connection is closed but no reason was recorded), and `ChannelWrongStateError` is never raised anywhere in the adapter. An application catching what this adapter actually raises - `ConnectionClosed`, `StreamLostError`, or `AMQPConnectionError` - will not catch `ConnectionRecovering`, and a channel call that used to surface the connection's `AMQPConnectionError` now raises an `AMQPChannelError` instead. 2.0 permits that break; the bases are chosen on semantic merit, and the choice is recorded under Open questions.

This guard applies to calls made through the public wrapper API. `_recover_topology`'s own internal calls take a different path - see "Topology replay must not use the blocking wrapper API" below for why.

### Observability

```python
add_state_change_listener(callback)   # callback(obj, old_state, new_state, reason=None)
```

on both the adapter `Connection` and `Channel`: one ordered listener list per object, the equivalent of `amqp091-go`'s `NotifyStateChange` and the AMQP 1.0 Java client's `StateListener`, fired for every transition (not just ones related to recovery). `state` and `is_recovering` are synchronous properties, giving a synchronous, gate-able answer to "is this recovering right now" - see `findings.md` for why that matters.

**`is_open`/`is_closed` fold `_state` in alongside the existing delegation.** Today both properties delegate straight through to whichever inner object happens to be installed (`self._connection.is_open` on the connection wrapper, `self._channel.is_open` on the channel wrapper), reading the base `pika.connection.Connection`/`pika.channel.Channel`'s own state. That delegation is **not** retired; `_state` is folded in alongside it. An earlier draft did retire it, and that was wrong in a way worth recording: the design deliberately never transitions a channel's `_state` on a connection-wide event, so a channel would have reported `is_open == True` and `is_closed == False` for ever after any drop, recovery configured or not, and `while ch.is_open:` would spin on a dead connection. Today the delegation is correct precisely because `pika.connection.Connection._on_stream_terminated` runs `_on_close_meta` for every channel, which transitions the raw channel to CLOSED before the adapter hears about the drop. So `is_open` means "usable right now": the effective state is `OPEN` *and* the inner object reports open. `is_closed` is its complement plus the terminal state. That keeps today's behavior exactly, which three existing unit tests assert (`tests/unit/thread_safe_connection_tests.py` at 1078, 2980 and 2985 - two of which would have become vacuous passes rather than failures, so CI would not have caught the change), while making `RECOVERING` observable through `is_recovering` and `state` rather than by overloading `is_open`. The inner object stays a read target once redial can rebuild it out from under the wrapper: mid-`RECOVERING` there may be no live inner connection at all, or a freshly constructed one that hasn't finished its own handshake, and delegating would answer from whichever of those happens to be installed at read time rather than from the wrapper's own state. `_state` is deliberately always present whether or not recovery is configured (see "Where state lives, and who owns the topology ledger" below) specifically so `is_open`/`is_closed`, like their `_check_not_closed` replacement, have one unconditional source of truth regardless of what the inner object is doing underneath.

**Reads of the raw inner object's state need auditing, not blanket removal.** The distinction matters: where the inner object is the authority on whether the transport is usable, reading it is correct and removing it is the bug, as above. Where a read is being used to answer "is this handle usable", it has to consult `_state` too, because during `RECOVERING` the inner object may be a live connection whose topology is not yet replayed. A narrow version of this pattern already exists today: `_check_not_closed` (adapter `Connection`, current code; the adapter `Channel` has a same-named method with different semantics) checks `self._closed_reason` under lock and then falls through to `self._connection.is_closed` unlocked, documented as covering only the brief window between the connection reaching the closed state and `_on_connection_closed` running. That framing stops being true once `_state` is meant to be authoritative - any remaining direct read of the raw inner object's `is_open`/`is_closed`/`is_closing` is no longer a narrow race-window fallback, it is a second source of truth that can disagree with `_state` for the entire, possibly multi-step duration of a redial. A known concrete site: `Channel.close()`'s loop-thread body (current code) short-circuits with `if self._channel.is_closed or self._channel.is_closing: ready.set(); return` against the *raw* inner channel before doing anything else. If the old raw channel already reports closed (broker dropped it) while the wrapper's `_state == LifecycleState.RECOVERING`, this returns immediately as if `close()` succeeded, instead of applying the terminal rule or raising `ChannelRecovering` - silently swallowing an app-issued `close()` during recovery. Before implementation, every direct read of the inner `Connection`/`Channel` object's `is_open`/`is_closed`/`is_closing` in `thread_safe_connection.py` needs auditing - not just the two public properties above - and either migrated to `_state` or justified in a comment why that specific read must stay narrow, the same way today's `_check_not_closed` docstring justifies its fallback.

Five callback-registration methods (`add_on_close_callback`, `add_on_open_callback`, `add_on_recovery_started_callback`, `add_on_recovery_succeeded_callback`, `add_on_recovery_failed_callback`) are provided on both the connection and the channel as **convenience wrappers over state transitions**, not a second, independent notification path:

| Callback | Fires on transition |
|---|---|
| `add_on_open_callback(obj)` | any `-> OPEN` |
| `add_on_close_callback(obj, reason)` | any `-> CLOSED` |
| `add_on_recovery_started_callback(obj, reason)` | `OPEN -> RECOVERING` |
| `add_on_recovery_succeeded_callback(obj, skipped)` | `RECOVERING -> OPEN` |
| `add_on_recovery_failed_callback(obj, error)` | `RECOVERING -> CLOSED` |

**"Convenience wrapper over state transitions" is not quite literal for the last two rows.** `add_state_change_listener`'s callback shape is `(obj, old_state, new_state, reason=None)` - there is no `skipped` or `error` slot anywhere in that tuple. `add_on_close_callback(obj, reason)` and `add_on_recovery_started_callback(obj, reason)` really are pure filters over that signature (`reason` is already there). But `add_on_recovery_succeeded_callback(obj, skipped)` and `add_on_recovery_failed_callback(obj, error)` need a payload the generic transition event doesn't carry: which topology entities got skipped during replay, or which error ended the last redial attempt. Implementing these as "just a filtered listener" requires the recovery driver to stash that result somewhere readable at the moment it fires the `RECOVERING -> OPEN` / `RECOVERING -> CLOSED` transition, and the two convenience methods read it from there. That somewhere must be the pass, not the coordinator, for the reason given under "Every pass owns its retry budget" rather than from anything `add_state_change_listener` itself provides. Deciding whether that extra state lives on the pass object or is threaded through as an enriched `reason` object on the transition itself is needed before implementation - the latter would make the sugar literal, at the cost of giving every listener a payload shape that varies by transition.

`add_on_recovery_*_callback` raise `ValueError` if called on a connection/channel not constructed with a `RecoveryConfig`, since those transitions cannot occur without one; `add_on_close_callback`, `add_on_open_callback`, and `add_state_change_listener` carry no such restriction. `add_on_close_callback`'s "any cause" behavior - explicit `close()`, a drop with no `RecoveryConfig`, or recovery exhausting its budget - falls out for free from being sugar over "any transition into `CLOSED`," rather than needing independent special-casing across separate teardown paths.

Connection-level and channel-level recovery notifications stay partitioned, and the partition falls out of the state machine structurally: `_recover_channel` transitions *that channel's own* `_state`, so that channel's listeners - both `add_state_change_listener` and the `add_on_recovery_*_callback` sugar - fire from that literal transition. A connection-wide drop only ever transitions the *connection's* `_state`; it does not individually transition each channel's `_state`. So a connection-wide drop fires the connection's own listeners, full stop - no listener registered directly on a channel (neither `add_state_change_listener` nor the `add_on_recovery_*_callback` sugar) fires, since nothing ever assigns a new value to that channel's `_state`. This is what `TestFullReconnectDoesNotFireChannelRecoveryCallbacks` locks in, and it keeps the two notification paths answering two different questions: a channel's own listeners mean "did *this specific channel* have an isolated episode," a connection's listeners mean "did the whole connection drop and come back" - see "Channel-level recovery (broker-initiated single-channel close)" below.

**This creates a gap worth resolving before implementation, not papering over.** "Composing channel-level and connection-level recovery" defines a channel's *effective* state during a connection-wide drop as derived from the connection's state (`effective_state = CONNECTION.RECOVERING implies RECOVERING else ch._state`) - but only for the *guard* that decides whether to raise `ChannelRecovering` on a call. It says nothing about `ch.is_recovering` or `ch.state`, which, per the above, are read directly off the literal `ch._state` field. The result: during a connection-wide drop, `ch.is_recovering` reads `False` (nothing transitioned `ch._state`), yet calling any guarded method on that same channel raises `ChannelRecovering` (the guard reads the derived effective state). An app polling `ch.is_recovering` before deciding whether to call would see a stale "not recovering" right up until the call itself raises. Whether `is_recovering`/`state` on a channel should also fold in the connection's effective state, or whether this asymmetry between the query properties and the guard is acceptable, needs deciding - it is the same class of gap as the `is_open`/`is_closed` split-brain issue under "Observability" above, just between two read paths on the same object instead of between the wrapper and the raw inner object.

`Connection.__init__` gains one new kwarg, `recovery: RecoveryConfig | None = None` - not on `Parameters`, which is adapter-neutral and consumed only by the initial-connect-only `AMQPConnectionWorkflow`.

## Where state lives, and who owns the topology ledger

`Connection._recovery: RecoveryCoordinator | None` is `None` when the caller doesn't opt in. Lifecycle state does not live on the coordinator: it is `Connection._state` and `Channel._state`, both `LifecycleState`, directly, always present whether or not recovery is configured - a connection without `RecoveryConfig` simply never transitions to `RECOVERING`, but it still has `_state`, since the added guard needs somewhere to read from regardless. `RecoveryCoordinator` (in `pika/recovery.py`) holds only:

- `config: RecoveryConfig`
- no shared attempt counter; each recovery pass carries its own, see "Every pass owns its retry budget" below
- `topology: dict[int, ChannelTopology]` - **the single, connection-wide topology store, keyed by channel number.** See "Topology ledger" below for why this must live here rather than on each `Channel`.

Server-generated queue renames need a name-replacement map scoped locally to the single phased pass that produces the rename (`dict[str, str]` inside `_recover_topology(channels)`), not a `RecoveryCoordinator` field, since nothing outside that one pass ever needs it again.

**Locking model.** `_state` and `topology` are both read and written from multiple app-caller threads regardless of recovery - concurrent `queue_declare`/`exchange_declare` calls from different threads already need `topology`'s mutations serialized, and `state` needs to be safely readable from any thread via the `state` property. Both reuse the existing `_channel_waiters_lock`, the same lock `_closed_reason` is guarded by today: with state transitions driven from the loop thread and no second recovery thread able to race a caller thread, one lock is enough for the ordinary reason a single lock is enough for any other piece of connection state read cross-thread.

`_recover_topology` must snapshot `topology` under the lock, release it, and only then issue the redeclare calls against that snapshot. The snapshot is a **shallow structural copy** - new dicts and lists holding the same record objects - and explicitly not a deep copy. `copy.deepcopy` would traverse into `ConsumerRecord.on_message_callback`, and for the most ordinary pika consumer shape, a bound method of an object that owns a `threading.Lock`, that raises `TypeError: cannot pickle '_thread.lock' object`. Java takes the same shallow approach: `Utility.copy` returns `new LinkedHashMap<>(map)` under the container's own monitor, copying the container and not the elements - not because of a competing recovery thread, but because `_recover_topology` runs as a sequence of non-blocking, callback-chained operations on the loop thread (see below), and holding a lock across a suspended callback chain would block every other loop activity, including the callbacks that would eventually resume the chain.

**Why `topology` lives on the coordinator and not per-channel:** AMQP exchanges, queues, and bindings are scoped to the *connection* (vhost), not to the channel that happened to declare them - a binding created on channel B can reference an exchange declared on channel A, and either channel can legally delete an entity the other one created. See "Topology ledger" below for the concrete failure modes a per-channel store would introduce.

## Where recovery hooks in

The adapter `Connection._on_connection_closed` is the funnel for the death of an inner connection that had reached OPEN. It is not the only death path - see "The funnel is not the only death path" below - but it is the one an established connection drops through:

```python
def _on_connection_closed(self, _connection, reason):
    if _connection is not self._connection:
        return                          # stale notification from a superseded generation
    if self._recovery is not None and not isinstance(reason, ConnectionClosedByClient):
        self._transition(LifecycleState.RECOVERING, reason)  # wakes blocking waiters, fires listeners
        self._recover_connection(reason)                     # owns the budget check and the backoff
        return
    # No recovery: this connection is not coming back, so the loop must end.
    # Stopping it here is also what makes _run_ioloop's tail reachable, which
    # is where the pool joins run.  See "Teardown the drop path performs".
    self._connection.ioloop.stop()
    # otherwise unchanged: wake waiters, transition to CLOSED, user close callback
```

Two details matter here:

1. **The loop is stopped only on the terminal path.** An earlier draft stopped it unconditionally as the first statement, carried over from the pre-persistent-loop design where each inner connection owned its own loop. Under `custom_ioloop` there is only one loop, so that call ends the very thread recovery runs on, leaving `_state` stuck at `RECOVERING` with no thread left to transition it and the backoff timer queued on a dead loop. The stale-generation check moves to the top and is now purely a generation filter, not a guard on whose loop gets stopped.
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

Every guarded public method - `_check_not_closed` with its added state test, `_register_waiter`, `channel()`, and the equivalent channel-level guard - reads `self._state` and raises the dedicated exception when it is `RECOVERING`, with two exemptions: `close()`, for the reason given below, and `basic_ack`/`basic_nack`/`basic_reject`, for the reason under "Acknowledgements are exempt from the guard". This fail-fast behavior matches `findings.md`: neither reference client lets a call during recovery run as it would on a healthy connection, and both require the application to gate on connection state rather than assume transparency.

The recovery pass's own internal calls are the one place that does *not* go through the public wrapper API - see "Topology replay must not use the blocking wrapper API" below for why.

**`close()` on both the connection and the channel is unconditional**: it transitions state to `CLOSING`->`CLOSED` (idempotently) and schedules `self._connection.close()`/the raw channel's close via `add_callback_threadsafe`, regardless of what recovery is doing. This requires an explicit exemption rather than falling out for free, because `Channel.close()`'s first executable statement is `ready, error = self._register_waiter()`, and `_register_waiter` is on the guarded list above. Left unexempted, `ch.close()` during `RECOVERING` would raise `ChannelRecovering` and never reach its own scheduling: the channel would stay open, the terminal rule would never land, and `_shutdown_pool` would never run, leaking the consumer worker. So the guard has to be reachable from the application's calls and bypassed on the close path, not simply installed in `_register_waiter`. The terminal rule (`CLOSING`/`CLOSED` -> only `CLOSED`) makes this safe without any handshake between `close()` and an in-flight redial: if a redial attempt is in flight on the loop when `close()` schedules its own callback, that callback runs on the same loop, sees the terminal state already set, and the redial's own next step (whichever loop callback would have resumed it) finds the same terminal state and stops. Because backoff is a loop timer (`ioloop.call_later`) rather than a blocking sleep on a separate thread, there is nothing analogous to waking a sleeping backoff early: `close()`'s transition simply makes the timer's own callback a no-op when it fires, or the pending timer can be cancelled outright at `close()` time via the handle `ioloop.call_later` returns.

## Persistent IOLoop and the redial sequence

`pika.adapters.select_connection.SelectConnection.__init__` already accepts a `custom_ioloop` parameter and wraps it as the connection's `nbio` service - the mechanism this design needs already exists in pika, just unused by the adapter `Connection` today. We propose `Connection.__init__` construct one `IOLoop` instance up front (`self._ioloop`), start the background thread running `self._ioloop.start()` (not `self._connection.ioloop.start()` as today, which ties the thread's lifetime to the inner connection), and construct the first `SelectConnection` with `custom_ioloop=self._ioloop`. A redial then constructs a **new** `SelectConnection(parameters, ..., on_open_callback=..., custom_ioloop=self._ioloop)` bound to the *same* persistent loop and thread - the thread never stops and never gets rebuilt across a reconnect, which is what makes driving recovery on it (instead of a second thread) possible in the first place.

Redial therefore does not need shared `_open_inner_connection`/ `_start_ioloop_thread` helpers extracted for `__init__` to call: there is no "fresh IOLoop thread" to build on redial, only a fresh inner `SelectConnection` object bound to the loop that already exists - a small, self-contained change scoped to `__init__` and the redial path.

`Connection._recover_connection(reason)`, run as a sequence of loop-scheduled steps rather than a thread body:

1. If this pass's `attempt` has reached `config.max_attempts`: transition to `CLOSED` with the last error as `_closed_reason`, fire `on_recovery_failed`, then **stop the persistent loop**. Done. Stopping it is what runs the teardown rather than a substitute for it: `_run_ioloop` performs `_shutdown_all_consumer_pools()` and `_shutdown_connection_pool()` after `ioloop.start()` returns, outside the `try/except`, so those unbounded joins execute on the dying thread and not on a loop callback - which is what obligation 1 forbids and what teardown table row 5 warns against relocating. The loop only has to persist while recovery is live; once a pass has terminally failed the connection is dead and the loop's lifetime ends exactly as it does on the no-recovery path.
2. Otherwise, schedule `_try_reconnect_once` via `self._ioloop.call_later(config.next_interval(attempt), ...)`, where `attempt` is this pass's own counter and not shared state. There is no condition variable to wait on and no backoff to interrupt early: if `close()` runs before the timer fires, the terminal-state check inside the scheduled callback (or cancelling the timer handle at `close()` time) is sufficient, since both `close()` and the timer callback execute on the same loop and cannot race each other.
3. `_try_reconnect_once()`:
   - Constructs a fresh `SelectConnection(self._parameters, ..., custom_ioloop=self._ioloop)`.
   - On success, swaps `self._connection` under `_channel_waiters_lock`, since a caller thread can read it concurrently. The app's existing adapter `Channel`/`Connection` references stay valid. State remains `RECOVERING` here: the socket is back but the topology is not, and guarded calls must keep failing until replay finishes.

   **`add_on_open_callback` does not fire at this point, and an earlier draft had it doing so.** Three statements in this document could not all hold at once: the transition table makes `add_on_open_callback` a filter over `any -> OPEN`; this step had it firing at redial success; and test 17 asserts it fires before `on_recovery_succeeded`. At redial success the state is still `RECOVERING`, so there is no `-> OPEN` transition to filter, and firing an "open" callback while every guarded call still raises `ConnectionRecovering` is precisely the half-open state the fail-fast contract exists to avoid. The table wins: `add_on_open_callback` fires on the `RECOVERING -> OPEN` transition, after replay, ordered ahead of `on_recovery_succeeded` within that same transition.

   That leaves "the socket is back, replay is pending" unexposed, which is a real thing an application might want to observe. If it is worth exposing, it needs its own name rather than overloading `add_on_open_callback`; Java has exactly this shape, with `notifyRecoveryListenersStarted` and `notifyRecoveryListenersComplete` as separate signals. Recorded under Open questions rather than decided here.
   - Calls `_reopen_channels_and_recover_topology()` (below). On its completion: transition to `OPEN` and fire `on_recovery_succeeded(connection, skipped)`. The pass ends here and its counter goes with it; there is no counter to reset.
   - On failure: increment this pass's `attempt` and go to step 1.

`_reopen_channels_and_recover_topology()` reopens every `Channel` the app hasn't explicitly closed (tracked via `Channel._closed`), reopening each wrapper's raw channel as described under "How a wrapper's raw channel is reopened" below; if `topology_recovery_mode` is `DISABLED`, stop there; otherwise call `_recover_topology(channels)`.

### What the persistent loop costs

Driving recovery on the connection's own loop is a deliberate divergence from every reference client. All three drive recovery on a separate thread of control:

- The RabbitMQ Java client tears its I/O loop down *first* (`AMQConnection.closeMainLoopThreadIfNecessary`, called immediately before `notifyRecoveryCanBeginListeners`), then recovers on another thread. `AutorecoveringConnection.beginAutomaticRecovery` is `synchronized` and blocks on `wait(delay)` and `Thread.sleep`, so it could not run on an I/O thread even if it wanted to.
- The .NET client runs an explicit recovery loop on its own task (`AutorecoveringConnection.Recovery.cs`: `_recoveryTask = Task.Run(RecoverConnectionAsync)`), with a matching `StopRecoveryLoopAsync`.
- `amqp091-go` recovers in a goroutine that blocks on `time.After(RetryInterval() + jitter)`.

We diverge because a second thread has to synchronize with the state machine it mutates, and that synchronization is the complexity this design exists to avoid: a re-entrancy flag, a cross-level race guard, and condition-variable backoff, all protecting invariants the loop already enforces for free by running one callback at a time.

The divergence does not remove complexity. It relocates it, and the price is three obligations that simply do not arise when recovery owns a thread:

1. **Nothing on the loop thread may block.** A blocking call there waits on an event that only a future iteration of the same loop can set, and the loop cannot reach that iteration while blocked. The next section covers this for topology replay; the teardown table below covers it for pool joins, which is the same hazard reached by a different path.
2. **Recovery must undo the teardown the drop path already performed.** pika's drop path assumes the loop is going away and latches state on that assumption. Once the loop persists, every one of those latches is a bug.
3. **Nothing on the loop thread may raise.** An unhandled exception from any loop callback propagates out of `ioloop.start()` and kills the thread. `_run_ioloop` catches it, logs "IOLoop thread crashed", records `_closed_reason` and wakes blocked waiters - but that handler predates lifecycle state and never touches `_state`, so the handle is left `RECOVERING` permanently: `is_open` and `is_closed` are both False, `on_recovery_failed` never fires because there was no `RECOVERING -> CLOSED` transition, `add_on_close_callback` never fires for the same reason, every guarded call raises for the life of the process, and the only thread that could transition anything is gone. This is not hypothetical: `RecoveryConfig.should_skip` invokes the application's `on_topology_entity_error` on the loop thread, and test 4 deliberately produces a real 404 that fires it. So every recovery callback and every user callback recovery invokes goes through `_safe_dispatch`, the same containment deliveries already get, and `_run_ioloop`'s crash handler additionally transitions `_state` to `CLOSED` so that a callback which escapes containment still ends in a terminal state rather than a permanent limbo.

All three are absolute rather than best-effort, and they fail in different directions. Violating the first hangs the connection with no exception to catch and no timer left running to notice. Violating the second leaves a connection that reports itself successfully recovered while refusing all work. Violating the third strands it in `RECOVERING` with no thread alive to leave that state.

### Teardown the drop path performs, and what recovery must undo

On any unexpected drop, pika performs an adapter-wide teardown before `_on_connection_closed` returns. Every step below is reached today, in this order, and each assumes the connection is not coming back. Recovery must suppress or reverse each one; the reset paths for items 2, 4 and 6 currently exist only in `__init__`.

| # | Site | What it latches | What recovery must do |
|---|---|---|---|
| 1 | `pika.connection.Connection._on_stream_terminated` | Calls `_on_close_meta(self._error)` on every channel **before** dispatching `ON_CONNECTION_CLOSED` | Nothing directly, but note the ordering: step 2 has already run for every channel by the time the funnel sees the drop |
| 2 | `Channel._on_broker_close` -> `_claim_pool_shutdown` | `_pool_shutdown = True`; removes the channel from `wrapper._channels`; shuts the consumer work pool (`_shutdown = True`) | Suppress. A `StreamLostError` is not a client close, and the wrapper has to survive in order to be recovered |
| 3 | `Connection._on_connection_closed` | `self._connection.ioloop.stop()` | Suppress. Under `custom_ioloop` this *is* the shared loop, so stopping it ends the thread recovery runs on |
| 4 | `Connection._on_connection_closed` | `_record_closed_reason(reason)`, read by roughly sixteen sites including `close()`'s early return | **Suppress**, and only suppress; see below for why reversing it later is not an alternative |
| 5 | `_run_ioloop` tail | `_shutdown_all_consumer_pools()` and `_shutdown_connection_pool()`, both unbounded thread joins | Nothing while recovery is live, which is the point. It must not be relocated into a loop callback, which obligation 1 forbids; instead the terminal paths stop the loop so this tail runs off the loop thread as it does today |
| 6 | `Channel.confirm_delivery`'s `_arm_seq_no` hook | Leaves `_next_publish_seq_no` armed on the surviving wrapper, guarded by "arm only once". Note this is a closure passed as `on_sent`, not a method the drop path executes: the field simply persists because the wrapper does | Reverse. See "The confirm counter must be re-armed, and does not get an offset" |
| 7 | `Channel.confirm_delivery` | `_confirm_select_ok` is set once and short-circuits the method thereafter (`if self._confirm_select_ok is not None: return self._confirm_select_ok`), reset only in `__init__` | Reverse on reopen. Otherwise a post-recovery `confirm_delivery()` returns the dead generation's `Confirm.SelectOk` without ever sending `Confirm.Select` on the new channel |
| 8 | `Channel.confirm_delivery`, `add_on_return_callback`, `add_on_cancel_callback`, `Connection.add_on_connection_blocked_callback` / `_unblocked_callback` | Callbacks registered on the raw channel or raw connection, destroyed by `callbacks.cleanup` on channel close or by the whole `CallbackManager` going away with the inner connection | Re-register. See "Re-registration covers more than user channel callbacks" |
| 9 | `Connection._on_connection_open_error` | `ioloop.stop()`, plus `_connect_error`, `_connected_event` and the user's `on_open_error_callback` | Suppress the first three. The callback is a public-contract fork: see "The funnel is not the only death path" |

**Every blocking wrapper call captures the raw channel on the caller's thread, and the swap invalidates it.** `_blocking_rpc(method_name, channel_method, timeout, ...)` takes the raw channel's bound method as an argument, and all thirteen call sites evaluate `self._channel.<method>` on the *calling* thread before scheduling the work. The bound method therefore pins raw channel generation N. If a drop and a successful redial land between that evaluation and the moment the loop thread runs the scheduled callback, the RPC is issued against a dead channel object while `self._channel` already points at generation N+1. Today this is survivable, because a drop means everything fails anyway; with recovery it becomes a silent misdirection in the window recovery is specifically designed to make survivable.

`basic_get` is the counter-example and shows the fix: it resolves `self._channel` *inside* the closure it schedules, so it always reaches whichever generation is current when the closure runs. Rather than editing thirteen call sites, make the change once at the mechanism. `_blocking_rpc` already receives `method_name`, so it can take that alone and resolve `getattr(self._channel, method_name)` itself on the loop thread, dropping the `channel_method` parameter entirely. It already resolves the current channel two lines away when registering `add_on_close_callback`, so this is consistent rather than novel, and no future call site can reintroduce the bug.

Late binding alone is not sufficient, which is worth saying because it is easy to stop at the fix above. Some arguments are themselves generation-scoped: a pre-drop `consumer_tag` handed to `basic_cancel` is meaningless on the recovered channel and draws a 406, so late-binding the method while keeping a stale argument only moves the error. Calls carrying generation-scoped arguments must also fail when the generation changed under them, which the guard covers for the window it covers and does not cover for a swap completing between the caller's check and the loop's execution.

**An exhausted channel must not be resurrected by the next connection-level pass.** When a channel-level pass exhausts its budget it leaves that channel `CLOSED` while the connection stays up. But `_closed` is still False, because the application never closed it, and teardown row 2 suppresses the removal from `_channels` - so the next connection-wide drop finds it in the sweep, reopens it and replays its topology, resurrecting a channel that recovery had already given up on and whose `on_recovery_failed` the application has already been told about. The reopen sweep therefore skips any channel whose `_state` is terminal, not merely those the application closed. That is the terminal rule applied where it was written for the connection and not carried down to the channel: once `CLOSED`, only `CLOSED` may follow, whoever set it.

**Row 4 has only one safe option, and an earlier draft offered two.** Recording the close reason and reversing it on the `RECOVERING -> OPEN` transition looks equivalent to suppressing it, and is not. While it is set, `close()` takes its early return - `if self._closed_reason is not None: return` - so an application closing during recovery gets no state transition, no scheduled close, and no pool shutdown. Recovery then completes, reverses the field, transitions to `OPEN` and replays every consumer: the resurrection the terminal rule exists to forbid, reached without ever violating the terminal rule, because no transition to `CLOSING` ever happened. The same arm also makes every guarded call during recovery raise the recorded `StreamLostError` instead of `ConnectionRecovering`, since `_check_not_closed` tests `_closed_reason` before anything else - which defeats the dedicated-exception goal outright. Suppression is the requirement, not a preference.

**Timers belonging to a superseded connection: safe when it died, leaked when it was abandoned.** A persistent loop raises the obvious worry that a timer scheduled by an inner connection outlives it. For a connection that *died*, the answer is that it is cleaned up: `_on_stream_terminated` calls `_remove_heartbeat()` and ends by calling `_init_connection_state`, which removes `_blocked_conn_timer`. The cancellation happens after the funnel has dispatched, so recovery is scheduled while the old timer is nominally still live, but both run on the same loop within the same callback and nothing can interleave.

An inner connection **abandoned while still open** is a different case, and it is the one that leaks. `_remove_heartbeat()` is reached from exactly two places, `_terminate_stream` and `_on_stream_terminated`, and an abandoned-but-open connection reaches neither. Its `HeartbeatChecker` keeps rescheduling `_send_timer` and `_check_timer` through `_adapter_call_later` on the shared loop, so it goes on writing heartbeats and eventually decides its peer is unresponsive and calls `_terminate_stream` - on a connection nothing is listening to, while a healthy successor is running on the same loop. See "A failed replay must not abandon a live connection" for where that arises and how it is avoided.

Item 9 is not reached by the drop that starts recovery; it is reached by every redial that fails to connect, which is the common case during an outage. It is in this table because it latches the same state, and because suppressing item 3 without also suppressing item 9 yields a recovery that works only when the first redial succeeds.

Item 2 is the one most easily missed, because its consequence is silent. A recovered channel whose pool stayed shut still accepts a `basic_consume`, and the broker still delivers to it; every delivery then fails `submit()` with `RuntimeError`, which `_submit_or_terminate` swallows at `debug`. The connection looks healthy, the consumer looks subscribed, and no message is ever handed to the application.

### Topology replay must not use the blocking wrapper API

**This needs flagging before implementation starts.** `Channel.queue_declare` and friends work by blocking the *calling* thread on a `threading.Event` that only gets set when the IOLoop thread processes the broker's reply and calls `add_callback_threadsafe`'s corresponding wakeup. That's fine when the caller is an app thread - some other thread is blocked, and the IOLoop thread is free to run and eventually set the event. It self-deadlocks if the *IOLoop thread itself* calls it: the loop thread would block waiting on an event that only its own future iteration (processing the reply frame) can set, and it can never reach that future iteration because it's blocked.

Because recovery runs on the loop thread itself, `_recover_topology` cannot call `ch.exchange_declare(...)` / `ch.queue_declare(...)` the way an app would. It must instead drive the underlying raw `Channel`'s non-blocking, callback-based API directly (`raw_channel.exchange_declare( exchange=..., callback=on_declared)`), chaining each phase's entities through their own reply callbacks rather than sequential blocking calls. The phased ordering requirement (all exchanges before any queue, all queues before any binding, all bindings before any consumer - see "Topology ledger" below) is unaffected; only the mechanism for advancing from one entity to the next changes, from "the call returns" to "the callback fires." This is a hard correctness requirement: a callback-chained implementation that accidentally reintroduces a blocking call on the loop thread would hang the connection the first time recovery has more than a trivial amount of topology to replay, silently, with no exception to catch it (the loop simply stops servicing everything, including the timers that would otherwise detect the stall).

### Consumer delivery tags across recovery

Delivery tags are channel-scoped and restart from 1 on a reopened channel, so every tag an application holds when the socket drops is invalid afterwards. This is not a narrow window: a consumer callback runs on a `_BoundedWorkPool` worker, not the loop thread, so a long-running callback can still be holding a tag while the redial completes. `basic_ack` resolves `self._channel` inside the closure it schedules, so an ack issued from that callback lands on the **replacement** channel. Either the new channel's counter has already reached that value, in which case a different message is acknowledged and the loss is silent and undetectable, or it has not, in which case the broker answers 406 `unknown delivery tag` and closes the channel recovery just rebuilt - which re-enters channel recovery, driven by the application's own acks.

The Java and .NET clients both solve this with a delivery-tag offset, and we adopt it. `amqp091-go` has no equivalent: `Channel.Ack` transmits the tag exactly as given, with no staleness check anywhere in the package. Since this proposal otherwise ports `amqp091-go`'s recovery shape, that absence is worth stating plainly - following our primary reference here would reproduce the bug.

The mechanism. The `Channel` wrapper survives the swap, so it holds `_delivery_tag_offset` and `_max_seen_delivery_tag`, both starting at zero:

- `_wrapped_callback` updates `_max_seen_delivery_tag` to `max(tag, _max_seen_delivery_tag)`, then hands the user callback a `Basic.Deliver` carrying `tag + _delivery_tag_offset`. The application therefore sees one continuous, monotonically increasing tag space across any number of recoveries. Whether the offset is applied by mutating the decoded frame or by copying it is an implementation detail to settle against how `Basic.Deliver` is decoded per delivery.
- `basic_ack`, `basic_nack` and `basic_reject` compute `real = delivery_tag - _delivery_tag_offset` **on the loop thread, inside the closure they already schedule**, not on the calling thread. When `real <= 0` the tag predates the current channel generation: return without transmitting, logging at `debug`. Otherwise transmit `real`.
- On reopen, `_delivery_tag_offset += _max_seen_delivery_tag` and `_max_seen_delivery_tag = 0`.
- `basic_ack(0, multiple=True)` and `basic_nack(0, multiple=True)` keep their protocol meaning of "everything outstanding" and pass through rather than being treated as stale. `basic_reject` takes no `multiple` argument, so it needs only the `real <= 0` guard.

Dropping a stale acknowledgement is correct rather than lossy. The broker requeues every unacknowledged delivery when it detects the connection loss, so the message a stale tag referred to has already been returned to its queue and will be redelivered; acknowledging it is meaningless and the only alternative is a guaranteed channel exception. The offset is what makes the drop *precise* rather than a guess: without it, a stale tag is indistinguishable from a legitimate tag on the new channel.

The consequence for applications is the one `findings.md` already records: delivery is at-least-once and consumers must be idempotent. A message being processed when the connection dropped is redelivered whether or not that processing completed. This is the single place where the "no application code changes" goal does not hold, and it does not hold for any client - it is a property of the protocol, not of the design.

### Acknowledgements are exempt from the guard

`basic_ack`, `basic_nack` and `basic_reject` all begin with `_check_not_closed()`, so the guard rule above would make them raise during `RECOVERING` - and that would make the delivery-tag offset dead code in precisely the window it exists for. The offset's whole purpose is to handle a tag a worker was still holding when the socket dropped, and the guard fires before the arithmetic can run. Worse, the raise lands inside the user's `on_message_callback`, which runs under `_safe_dispatch`, so `ChannelRecovering` is logged and swallowed and the rest of the callback body is silently skipped. An exception nobody can catch is not fail-fast.

So the acknowledgement family is exempt, and the offset test is the only mechanism. This is right on its own terms and matches the reference clients: `AutorecoveringChannel.basicAck` in the Java client is a bare delegation with no state check at all, leaving `RecoveryAwareChannelN.basicAck` to drop a stale tag silently. Three reasons it is also right here. These calls are fire-and-forget, with no reply to fail. Any tag presented while `RECOVERING` is provably from a superseded generation, since no deliveries arrive on a channel that is down, so the offset test already classifies it correctly. And exempting them keeps manual-ack consumers working without change, which the guard would otherwise break for every one of them.

That corrects a claim made earlier in this document: at-least-once delivery is *not* "the single place where the no-application-code-changes goal does not hold" if acks raise during recovery, because then every manual-ack consumer needs an `except ChannelRecovering`. With the exemption, the earlier claim holds again.

### `basic_get` participates in the tag space too

The offset is applied and `_max_seen_delivery_tag` updated in `_make_delivery_callback`, which `basic_get` does not go through: it returns the raw `Basic.GetOk` verbatim, wire tag untouched. Left that way, a get tag is handed to the application un-offset and never counted, so after one recovery acking a retained get tag computes `real = tag - offset` and either lands positive - acknowledging a different, unprocessed message on the new generation, the silent loss the offset exists to prevent - or lands at or below zero, silently dropping a legitimate fresh get tag so its message is redelivered indefinitely. The under-count also shifts the whole application-facing space, so genuinely stale consume tags above the under-bumped offset test positive and transmit. `basic_get` therefore applies the offset and updates `_max_seen_delivery_tag` on the same path as a delivery.

**The thread doing the arithmetic is load-bearing, not incidental.** All three methods are fire-and-forget today: they do nothing on the caller's thread but the closed check, then defer to a closure that resolves `self._channel` late. The subtraction has to happen inside that closure, because only there is the offset guaranteed to reflect any reopen that has already run. Compute it on the caller's thread instead and this sequence loses a message silently: a consumer callback on a pool worker holds generation-1 tag 3; `_delivery_tag_offset` is still 0 because the reopen has not run; caller-thread arithmetic yields `real = 3` and schedules the ack; the loop thread then completes the reopen, bumping the offset and installing generation 2; the closure resolves the new channel and transmits 3 as valid, acknowledging a different, unprocessed message. That is precisely the silent loss the offset exists to prevent, reintroduced by doing the right arithmetic on the wrong thread. The worked example below only holds because the subtraction runs after the reopen.

**Order the zero check before the subtraction.** `basic_ack(0, multiple=True)` means "everything outstanding", and that carve-out must be tested on the value the application passed, not on `real`. With an offset of 3 and a stale generation-1 tag of 3, subtracting first yields `real == 0`, which matches the carve-out and requeue-nacks every outstanding delivery on the freshly recovered channel. Testing zero first but then transmitting `real` is equally wrong in the other direction: a negative value packed into `_PACK_LONGLONG = struct.Struct('>Q')` raises `struct.error` on the loop thread. The only correct form is: if `delivery_tag == 0 and multiple`, transmit a literal 0; otherwise compute `real`, drop when `real <= 0`, and transmit `real`.

### The confirm counter must be re-armed, and does not get an offset

`_next_publish_seq_no` lives on the `Channel` wrapper, which survives the swap, and `_arm_seq_no` guards itself with `if self._next_publish_seq_no is None` under the comment "Arm only once". Left as is, a reopened channel keeps counting from N while the broker's counter restarts at 1, and every subsequent `Basic.Ack` is misread. With RabbitMQ's normal `multiple=True` batching that is worse than a mismatch: an ack for tag 5 sweeps the application's pre-drop entries 1 through 5 as confirmed, silently marking as delivered messages that were lost to the drop. So the counter is re-armed wherever `Confirm.Select` is reissued, which is the shared reopen path and not `_recover_channel` alone. Assigning it to `_recover_channel` was the identical mistake this document already corrects for QoS and confirm mode: a connection-wide drop transitions only the connection's `_state` and never calls `_recover_channel`, so the primary case - an ordinary network drop - would come back with the counter still holding its pre-drop value. It belongs with the rest of the channel-session restoration.

**Re-arm it to 0, not 1.** `_arm_seq_no` sets `self._next_publish_seq_no = 0`; the counter is post-incremented on each publish and the public `next_publish_seq_no` property returns the stored value plus one. Java's `confirmSelect` setting its own counter to 1 is cited above as precedent for *resetting*, not for the value, because Java stores the next tag where pika stores the last one. Re-arming to 1 here would put every confirm off by one.

The existing "arm only once" reasoning is correct and does not conflict with this, but the distinction is narrow enough to state: it is about a `confirm_delivery` that timed out and was retried on the *same* channel, where the broker's counter genuinely did not reset and so neither may ours. A reopened channel is the opposite case - the broker's counter really does restart at 1 - so the guard needs to distinguish "arming again on the same channel session" from "arming a new one", rather than being unconditional.

All three clients reset rather than preserve, and two of them get it for free by construction, which is why pika has to do it explicitly. Java's `nextPublishSeqNo` lives on `ChannelN` and appears nowhere in `AutorecoveringChannel`, so the replacement channel starts at 0 and `recoverState()`'s `confirmSelect()` sets it to 1. .NET's `_nextPublishSeqNo` is likewise on the inner channel and is not carried over by `TakeOver`. `amqp091-go` does it explicitly: `confirms.reset()` sets `published = 0` and `expecting = 1`, closes the deferred confirmations so outstanding waiters are nacked, and clears the sequencer.

**Note the deliberate asymmetry with delivery tags.** Consumer delivery tags get an offset so the application sees one continuous space across recoveries; publish sequence numbers get no such treatment and simply restart. The two are not inconsistent, because the tags differ in who assigns them and who is holding them at the drop. A delivery tag is broker-assigned and the application may still be holding one when the socket dies, so it needs both a stable space and a way to detect a tag from a superseded generation. A publish sequence number is client-assigned, and after a drop the *outcome* of every outstanding publish is simply unknown - no numbering scheme can recover that information, so the only coherent behavior is to restart in step with the broker and tell the application, which is what the transition hook below is for. `next_publish_seq_no` is a public property and will visibly restart; every reference client behaves the same way.

### Publisher confirms hook

The `RECOVERING -> OPEN` transition is the natural place to signal "the confirm sequence has reset; treat outstanding publishes as unknown" - `findings.md` shows both reference clients reset delivery/confirm tags across recovery (unconfirmed set discarded in the Java client, pending `DeferredConfirmation`s nacked in `amqp091-go`). A confirm-tracking helper can subscribe via `add_state_change_listener` (or the `add_on_recovery_succeeded_callback` sugar) to fail and republish outstanding confirms at exactly that transition, giving the at-least-once pattern `findings.md` demonstrates a clean anchor instead of ad hoc detection. Consumers on the republish side must be idempotent - recovery can produce a genuine duplicate delivery when a message reached the broker but its confirmation was lost to the drop, the same as both reference clients.

### Channel-level recovery (broker-initiated single-channel close)

A broker soft-error (e.g. 406 `PRECONDITION_FAILED`, 404 `NOT_FOUND`) can close one channel while the connection stays healthy. The listener (`_register_recovery_close_listener`) is wired to `Connection._on_channel_closed_for_recovery` whenever `wrapper._recovery is not None`. **It is registered on the loop thread, not from `Channel.__init__`.** The adapter already establishes this invariant for the analogous hook and comments it: `Connection.channel()` registers `_on_broker_close` via `_schedule_unchecked`, "on the IOLoop thread, where the raw channel's callback stack is safe to mutate". Registering from `__init__` would mutate the same callback stack from a caller thread while the loop thread may be inside `process` or `cleanup` on it. The recovery listener registers exactly where `_on_broker_close` does, and for the same reason.

**It cannot be registered once and treated as permanent.** `pika.channel.Channel._transition_to_closed` fires the close callbacks and then, in a `finally`, calls `_cleanup()`, which calls `self.callbacks.cleanup(str(self.channel_number))` and removes every callback registered for that channel. The listener is therefore destroyed by the very close that fires it, so a channel that recovers once would never trigger a second channel-level pass. The registration has to be repeated against each new raw channel on every reopen. Java handles the same problem the same way: its listener lists live on the wrapper and `automaticallyRecover` calls `recoverShutdownListeners()`, `recoverReturnListeners()` and `recoverConfirmListeners()` to re-attach them to the new delegate. The rule is wider than user channel callbacks, and scoping it that narrowly leaves four registrations unrestored.

- `add_on_connection_blocked_callback` and `add_on_connection_unblocked_callback` register against the raw **connection**, not the raw channel: `getattr(self._connection, raw_method_name)(_wrapped)`. A redial builds a whole new `SelectConnection` with a fresh `CallbackManager`, so after one recovery a publisher that registered a `Connection.Blocked` handler - the documented way to react to a broker resource alarm - silently stops receiving them. `ChannelTopology` has no field for these and they are connection-scoped rather than channel-scoped, so they need their own record on the coordinator.
- `_on_broker_close` is registered on the raw channel by `Connection.channel()` and is adapter-internal rather than a user callback, so wording about user callbacks does not reach it. `callbacks.cleanup` destroys it like any other, and its own docstring explains what its absence costs: channels stay in `_channels` with their pool, worker thread and raw channel until the connection is torn down, which is the leak issue #1688 describes.
- `add_on_return_callback` and `add_on_cancel_callback` have no ledger field either - only `confirm_ack_nack_callback` is recorded - so mandatory-publish returns and server-initiated cancels stop being delivered after the first recovery.

So the ledger records every callback the adapter forwards downward, user-supplied or not, and reopen re-attaches all of them. The test for this is not "recovery works" but "each registration still fires after two consecutive recoveries", since a single recovery can pass on a listener that was re-attached by accident.

**That listener needs two filters, and neither is optional.** It fires for *every* channel close, and only a broker soft error should start a channel-level pass.

First, a client close must not trigger recovery. The existing `_on_broker_close` opens with `if isinstance(reason, ChannelClosedByClient): return` and the recovery listener needs the same test. Without it, `ch.close()` transitions the channel to `RECOVERING` and reopens it with all its consumers, resurrecting a channel the application deliberately closed; and a graceful `conn.close()`, which issues a per-channel close for each channel, resurrects all of them mid-teardown.

Second, a connection-wide drop must not start channel-level passes. `pika.connection.Connection._on_stream_terminated` runs `_on_close_meta(self._error)` for every channel *before* dispatching `ON_CONNECTION_CLOSED`, so at the moment each channel's listener fires, the connection's `_state` is still `OPEN` and each channel's own `_state` is still `OPEN` too. Both of the guards this section otherwise relies on evaluate False, `_recover_channel` starts on every channel, and every channel fires `on_recovery_started` - which contradicts the guarantee that a full reconnect fires only connection-level listeners, and breaks `TestFullReconnectDoesNotFireChannelRecoveryCallbacks`. The filter that works is a type test on the reason: a channel-level pass is warranted only when `isinstance(reason, AMQPChannelError)`, and a connection-wide drop delivers an `AMQPConnectionError` such as `StreamLostError`, so the two sets are disjoint. `self._connection.is_closed` is a workable alternative, since it is already True by the time `_on_close_meta` runs, but the type test says what is meant.

`_recover_channel(ch, reason)` mirrors `_recover_connection`'s shape but scoped to one channel and driven the same way - loop-scheduled steps, not a thread body, including the same retry loop with `config.next_interval(attempt)` backoff between reopen attempts and its own attempt counter: transition `ch._state` to `RECOVERING` (firing `ch`'s own `on_recovery_started`), reopen just this channel, call `_recover_topology([ch])`. On success, transition `ch._state` to `OPEN` (firing `ch.on_recovery_succeeded`); on exhaustion, transition to `CLOSED` (firing `ch.on_recovery_failed`) - **never** the connection-wide listeners, which stay reserved for `_recover_connection`, for the structural reason given under "Observability" above. Exhaustion does **not** tear down the whole connection - that one channel is simply left `CLOSED`. `_recover_topology([ch])` still recovers against the full connection-wide `coordinator.topology` (filtered to entries relevant to `ch`), not a store scoped to `ch` alone.

### Every pass owns its retry budget

`config.max_attempts` bounds a single recovery pass, and the counter belongs to that pass rather than to the coordinator. One shared counter would let unrelated passes corrupt each other's budget in three distinct ways. A channel that burned all five attempts on a permanent 406 would leave the counter exhausted, so the next real network drop would see the budget already spent and go straight to `CLOSED` without attempting one redial - converting a recoverable outage into a dead connection. Two channels recovering in interleaved chains would each increment the same slot and halve each other's budget. And a connection-level pass resetting the counter on success would rearm an in-flight channel pass, which would then retry indefinitely, defeating `max_attempts` entirely and producing exactly the unbounded episode chain the test plan sets out to prevent.

Both reference clients that expose a retry limit scope it to the pass: Java's `recoverConnection` declares `int attempts = 0` as a method local, and `amqp091-go`'s `reconnectChannel` counts with a loop variable, `for i := 0; i < ch.connection.MaxRetryCount(); i++`. Neither keeps a counter on a shared object. `config` is shared because it is immutable; the counter is not.

The same applies to the `skipped` and `error` payloads the recovery callbacks carry. Parking those on the coordinator alongside a shared counter would let a connection-level pass and a channel-level pass read each other's results, so each pass carries its own.

## Composing channel-level and connection-level recovery

Channel-level and connection-level recovery passes can interact in ways that need explicit handling:

- **Scenario A**: a channel-level recovery pass retriggering itself - a second broker-initiated close notification for the same channel arriving while its own `_recover_channel` pass is already in flight.
- **Scenario B**: a connection-level pass starting while a channel-level pass already owns a channel - the whole connection drops while one channel is mid-recovery from its own soft error.

Because everything relevant runs on one loop thread, neither scenario involves two callers actually executing at the same instant - only one callback body ever runs at a time - but callback chains can still interleave across their suspended points, so both still need guarding.

**A channel's effective state derives from the connection's state.** Rather than tracking "is this specific channel mid-recovery" as an independent boolean, a channel is `RECOVERING` if either its own `_state` says so, *or* the connection's `_state` is `RECOVERING`. Concretely, the guard a public channel method checks is `effective_state = LifecycleState.RECOVERING if self._wrapper._state is LifecycleState.RECOVERING else self._state`. Both operands are the one shared enum, so the comparison downstream is well-typed; an earlier draft wrote this as pseudocode naming a `ConnectionState`/`ChannelState` split that no longer exists, which would have reintroduced the always-False comparison that collapsing the enums removed. This answers Scenario B by construction: once the connection transitions to `RECOVERING`, every channel is observably `RECOVERING` too, immediately, with no separate flag to propagate and nothing to wait for.

**What single-threaded execution does not remove: staleness across a suspended callback chain.** `_recover_channel`'s own `_recover_topology([ch])` call is a chain of callbacks, each waiting on a broker reply - it is suspended, not actively running, between those replies. The connection can still drop for real while a channel-level pass is suspended mid-chain, because the drop is itself just another callback the loop will run next. The two are never *concurrent* (only one callback body ever executes at a time), but they can still **interleave**: the channel-level pass's next callback can fire after the connection-level transition has already happened. Every callback in `_recover_channel`'s chain checks `self._wrapper._state == LifecycleState.RECOVERING` (or a terminal state) before proceeding to its next step. Note the receiver: it is the adapter `Connection`, reached from a `Channel` as `self._wrapper`. Writing `self._connection._state` here would resolve at neither link - `Channel` holds no `_connection` attribute at all, and `_state` does not exist on `pika.connection.Connection`, whose field is the integer `connection_state`, and yields - clearing its own bookkeeping, logging at `info`, **not** firing `ch.on_recovery_failed` - if a connection-level pass has taken over. This is a hand-off, not a failure the app needs to hear about as one.

**Scenario A** reduces to a per-step check the same way: because only one callback body runs at a time, `_on_channel_closed_for_recovery` firing again for a channel that already has a `_recover_channel` chain in flight can simply check `ch._state == LifecycleState.RECOVERING` and return early - there is no window between "decide to start a pass" and "the pass's state change actually lands" for a second invocation to slip through, because both the decision and the state change happen in the same, uninterrupted callback.

**The redundant-reopen / orphaned-raw-channel risk is real regardless of thread model**: if a channel-level pass's replay completes successfully in the same window a connection-level pass's own `_reopen_channels_and_recover_topology()` reaches that channel, the connection-level pass still reopens and redeclares it again as part of its complete sweep - it does not try to detect "was this one already handled." `_reopen_channel(ch)` must explicitly close any existing, still-open `ch._channel` before installing its replacement, or that redundant reopen leaks a channel number that was never sent a `Channel.Close` but is no longer tracked client-side.

## Topology ledger

AMQP exchanges, queues, and bindings are scoped to the connection (vhost), not to whichever channel happened to declare them, and any channel can reference or delete an entity another channel created. A store that isolated each channel's entries from every other channel's would risk two concrete, broker-reproducible failure modes:

1. **Ordering failure.** Channel A declares exchange `X` and queue `Q`; channel B declares a binding from `Q` to `X`. If entries were recovered channel-by-channel and B happened to be processed before A, B's `queue_bind` would 404 against an exchange and queue that don't exist yet.
2. **Split-brain removal.** Channel A declares queue `X`; channel B later calls `queue_delete('X')`. If removal only searched the calling channel's own records, B's delete would find nothing, and `X` would get incorrectly redeclared on the next recovery as a queue the app had explicitly deleted.

We propose the same shape as `amqp091-go`'s `Connection.topologyConfiguration map[uint16]*TopologyConfiguration`: `coordinator.topology: dict[int, ChannelTopology]`, keyed by `channel_number`, on `RecoveryCoordinator` - `Channel` itself holds no topology state.

### The redial is asynchronous, and the outcome needs a generation-aware landing point

Step 3 above reads as a synchronous sequence, and it cannot be one. `SelectConnection.__init__` returns as soon as the connect has been *started*; the handshake resolves later, on the loop, into whichever callbacks were supplied. The adapter's existing pair, `_on_connection_open` and `_on_connection_open_error`, are one instance-level pair shared by every generation with no reference to any pass, and `_on_connection_open`'s entire body is `self._connected_event.set()` - which the redial path must not touch, since that event exists to hand the outcome of `__init__` back to the constructing thread. So if the redial reuses them, nothing increments the attempt counter, nothing reaches `_reopen_channels_and_recover_topology`, and nothing transitions to `OPEN`.

`_try_reconnect_once` therefore supplies its own handlers, bound to the pass rather than to the instance, and each one begins with the same generation filter the funnel uses:

```python
conn = SelectConnection(self._parameters, custom_ioloop=self._ioloop,
                        on_open_callback=partial(self._on_redial_open, pass_),
                        on_open_error_callback=partial(self._on_redial_error, pass_),
                        on_close_callback=self._on_connection_closed)
```

`_on_redial_open(pass_, conn)` ignores the callback unless `conn is self._connection`, then runs the reopen-and-replay sequence; `_on_redial_error(pass_, conn, error)` likewise, then increments `pass_.attempt` and returns to step 1. Neither touches `_connect_error` or `_connected_event`. The generation filter matters here for the same reason it does in the funnel: a redial that is superseded by `close()`, or by a later pass, must not resume against a connection that is no longer current.

Note also that the redial nests inside `AMQPConnectionWorkflow`'s own `connection_attempts`/`retry_delay` cycle, which `SelectConnection` runs by default. With `connection_attempts=5` and `max_attempts=5` a single drop makes up to twenty-five TCP attempts under two independent backoff policies. Either the redial constructs its inner connections with `connection_attempts=1` and owns retrying itself, or `RecoveryConfig` documents that the effective attempt count is the product. The former is clearer; whichever is chosen, the Non-goals note that multi-host failover is "initial-connect only" is not true of the redial path unless the redial explicitly opts out.

### A failed replay must not abandon a live connection

When replay aborts - `on_topology_entity_error` returning False, or a fatal error mid-chain - the pass increments its counter and returns to step 1, which eventually constructs a fresh `SelectConnection`. At that moment `self._connection` is a fully **open** inner connection, and nothing closes it. This document requires exactly this care one level down, twice: `_reopen_channel` must close a still-open previous raw channel rather than only overwriting the reference. The connection-level equivalent was missing.

Two consequences, beyond the leaked file descriptor. The abandoned connection keeps its heartbeat timers rescheduling on the shared loop, as described above, because nothing takes it through `_on_stream_terminated`. And it still holds every exclusive and auto-delete entity it declared, so replay on the *next* attempt draws 405 `RESOURCE_LOCKED` redeclaring the exclusive queue that the previous generation still owns - on that attempt and every remaining one, until the budget is spent. A design that retries five times would fail all five for a reason it created itself.

So step 3 closes the outgoing inner connection before constructing its replacement, and waits for that close to complete rather than assuming it is synchronous.

### How a wrapper's raw channel is reopened

This needs stating precisely, because the obvious call is the wrong one and an earlier draft of this document specified it. **Recovery must not call the adapter `Connection.channel()`.** Its signature is `channel(self, timeout=DEFAULT_RPC_TIMEOUT) -> Channel` - there is no `channel_number` parameter, only the base `pika.connection.Connection.channel` has one - and it fails three separate ways here. It is a blocking wrapper that schedules `_open` via `add_callback_threadsafe` and then waits on an event, so calling it from the loop thread is the self-deadlock this document elsewhere makes a hard correctness requirement to avoid. It constructs and appends a *brand new* `Channel` wrapper with its own work pool, so recovery would produce a second handle while the application still holds the first. And `channel()` is itself on the guarded list, so it would raise `ConnectionRecovering` during recovery.

What recovery uses instead is the **base** connection's channel method, on the loop thread, rebinding the existing wrapper rather than creating one:

```python
# on the loop thread, after the redial has produced an OPEN inner connection
raw = self._connection.channel(channel_number=n, on_open_callback=on_reopened)
# in on_reopened: ch._channel = raw, then re-register the close listeners
```

`pika.connection.Connection.channel` suits this exactly: it takes an explicit `channel_number`, is callback-based rather than blocking, and returns after sending `Channel.Open` rather than after the reply. It does raise `ConnectionWrongStateError` if the connection is not open, so the reopen must follow a successful redial, not run alongside it.

**Reusing number N races the base connection's cleanup, and the reopen has to wait for it.** `Connection._on_channel_cleanup` removes the entry by number, not by identity: `del self._channels[channel.channel_number]`. Reopening inserts under the same number via `self._channels[channel_number] = self._create_channel(...)`. So in the redundant-reopen window described below - where a channel already reopened on a live session gets reopened again - this order is possible: the old raw channel N is sent `Channel.Close`; `_channels[N]` is overwritten with the replacement; the old channel's `CloseOk` arrives; `_on_channel_cleanup(old)` deletes `_channels[N]` and evicts the *replacement*. The wrapper then holds a raw channel that still reports `is_open`, while every frame for that number takes `if value.channel_number not in self._channels:` and is discarded after a `LOGGER.critical` - deliveries, acks, and even the broker's own `Channel.Close`. A channel that reports itself fully recovered and receives nothing.

So `_reopen_channel(ch)` either waits for the old channel's cleanup to complete before reusing N, or accepts a fresh number and re-keys the ledger bucket. Waiting is preferable, since it keeps `TopologyRecoveryEntity.channel_number` stable, but it must be an explicit wait rather than an assumption that close is synchronous, because `Channel.close()` is not.

**The key only works if the number is preserved across the swap, so we preserve it.** A fresh inner connection allocates channel numbers from 1 (`Connection._next_channel_number` returns the lowest free number over an empty `_channels` dict), so if reopen let the number be reassigned, a wrapper that was channel 3 could come back as channel 2 and replay channel 2's bucket: the closed channel's queues, bindings and consumers, while its own bucket is never replayed and its consumer silently disappears. `_reopen_channels_and_recover_topology` therefore reopens each wrapper with its existing number, by the mechanism in "How a wrapper's raw channel is reopened" above - the base connection's `channel(channel_number=n, on_open_callback=...)`, not the adapter's.

The reference clients satisfy the same invariant two different ways, and the invariant is what matters: the ledger key must survive the swap. Java preserves the number explicitly, calling `connDelegate.createChannel(this.getChannelNumber())`. `amqp091-go` preserves it structurally, since its `Channel` struct persists across a reconnect and only the broker-side session is reopened, so the `uint16` key never changes. .NET does the opposite and lets the replacement take a fresh number, which is safe there because its recorded entities are associated with the channel *object* rather than with a number. Keying by wrapper identity instead would be equally correct and slightly more robust, but the number has to be tracked regardless because `TopologyRecoveryEntity.channel_number` reports it, so preserving it serves both purposes.

One caveat, unverified: `channel-max` is renegotiated on every connection, so a redial to a node proposing a smaller maximum could in principle leave a preserved number out of range. Java has the same exposure. The reopen path should fall back to a fresh number and re-key the bucket rather than failing, but we have not tested this.

`ChannelTopology` holds:

```python
@dataclass
class ExchangeRecord:
    name: str
    exchange_type: str
    durable: bool
    auto_delete: bool
    internal: bool
    arguments: dict[str, Any] | None

@dataclass
class QueueRecord:
    declared_name: str          # '' for a server-named queue
    actual_name: str            # what the broker assigned
    durable: bool
    exclusive: bool
    auto_delete: bool
    arguments: dict[str, Any] | None

@dataclass
class BindingRecord:
    queue: str
    exchange: str
    routing_key: str
    arguments: dict[str, Any] | None

@dataclass
class ExchangeBindingRecord:
    destination: str
    source: str
    routing_key: str
    arguments: dict[str, Any] | None

@dataclass
class ConsumerRecord:
    queue: str
    consumer_tag: str
    on_message_callback: Callable[..., None]
    auto_ack: bool
    exclusive: bool
    arguments: dict[str, Any] | None

@dataclass
class ChannelTopology:
    exchanges: dict[str, ExchangeRecord] = field(default_factory=dict)
    queues: dict[str, QueueRecord] = field(default_factory=dict)   # keyed by actual_name; see below
    bindings: list[BindingRecord] = field(default_factory=list)     # order-preserving, deduped on append
    exchange_bindings: list[ExchangeBindingRecord] = field(default_factory=list)
    consumers: dict[str, ConsumerRecord] = field(default_factory=dict)
    prefetch_consumer: tuple[int, int] | None = None   # (prefetch_size, prefetch_count) from a global_qos=False call
    prefetch_global: tuple[int, int] | None = None      # (prefetch_size, prefetch_count) from a global_qos=True call
    confirm_select: bool = False
    confirm_ack_nack_callback: Callable[[Any], None] | None = None
```

We propose the field name `exchange_type` (matching pika's own parameter name throughout `Channel`) rather than a generic `kind`.

**Container keys, and why the records are not hashable.** `exchanges` is keyed by exchange name. `queues` is keyed by `actual_name`, never by `declared_name`: a server-named queue records `declared_name=''`, so keying on that collides every server-named queue in a channel onto one entry. The rename rewrite after replay updates `actual_name` and re-keys the entry, which is why the key has to be the one that changes rather than the one that does not - the alternative, keying on `declared_name`, would leave the map stable but make the entry unidentifiable. `bindings` and `exchange_bindings` stay ordered lists rather than sets, because a default `@dataclass` sets `__hash__ = None` and the records are therefore unhashable; deduplication happens on append by field comparison. Ordering also matters for replay, since bindings must be reissued in a deterministic order for a failure to be reproducible.

**A note on the Python floor.** The type annotations throughout this document use PEP 604 unions and PEP 585 generics. `requires-python` is currently `>=3.7` and `main.yaml` runs a blocking legacy leg on 3.7, 3.8 and 3.9, where that syntax raises at import time - and `mypy.ini` pins `python_version = 3.10`, so `hatch run typecheck` cannot catch it. 25 modules in `pika/` already carry `from __future__ import annotations` for exactly this reason, and `pika/recovery.py` needs it too unless 2.0 raises the floor. Whether 2.0 raises the floor is not recorded in `README.md`'s constraints and should be; the future import is the safe choice either way, since it costs nothing if the floor moves.

**Recording:** `Channel.exchange_declare`, `queue_declare`, `queue_bind`, `exchange_bind`, `basic_consume`, `basic_qos`, `confirm_delivery` each call a coordinator method - `self._wrapper._recovery.record_exchange(self.channel_number, record)` and so on - guarded by `if self._wrapper._recovery is not None:` - the receiver is `self._wrapper`, since the adapter `Channel` has no `_connection` attribute and the raw `SelectConnection` has no `_recovery` - after the broker ack succeeds, under `_channel_waiters_lock` as described above.

**A passive declare records nothing.** `queue_declare(queue=q, passive=True)` and `exchange_declare(exchange=e, passive=True)` are existence checks, and their remaining arguments carry pika's defaults rather than the entity's real properties. Recording one would overwrite a correct record with `durable=False, exclusive=False, auto_delete=False`, and since durable entities survive a drop, replay would then issue `Queue.Declare(durable=False)` against a durable queue: 406 `PRECONDITION_FAILED`, which closes the channel mid-replay. This is not hypothetical for pika - a passive declare is the idiom this repository's own acceptance tests use to assert a queue exists, and two of the tests proposed below use it. All three reference clients exclude passive declares for this reason: Java's `queueDeclarePassive` and `exchangeDeclarePassive` pass straight through to the delegate without recording, .NET guards its record call with `if (false == passive)`, and `amqp091-go`'s `QueueDeclarePassive` has no record call. So the recording guard is `if self._wrapper._recovery is not None and not passive:`.

**Removal:** `exchange_delete`, `queue_delete`, `queue_unbind`, `exchange_unbind`, `basic_cancel` symmetrically call `remove_exchange(name)`, `remove_queue(name)`, etc. - no channel argument, scanning every bucket in `topology`. `remove_queue`/`remove_exchange` cascade: deleting a queue removes any binding referencing it from every bucket, returning the exchanges those bindings sourced from so an auto-delete exchange left with no remaining bindings can be forgotten too.

**`TopologyRecoveryMode.ONLY_TRANSIENT`** narrows what `_recover_topology` redeclares, unioning transient queue/exchange names across every channel's bucket before the phased pass begins: a queue is transient if `exclusive` or `auto_delete` is set, an exchange is transient if `auto_delete` is set, and a binding is kept if it references at least one transient entity. That binding rule is the one the enum docstring states and this paragraph previously omitted; they are one rule, stated here. Note its consequence, which is the same one the caveat below describes from the other direction: a binding kept because its queue is transient may reference an exchange this mode skipped, so if that exchange did not survive - a non-durable exchange after a broker restart, rather than after a network blip - the binding phase 404s on it. Consumers are never filtered by this mode, since a subscription is lost with the channel on every reconnect regardless of queue durability, and QoS and confirm mode are outside the mode's reach entirely because they are not topology - see "Channel-session state is restored before topology" below.

Note that "transient" here means **connection-scoped**, not non-durable. That is how `amqp091-go` uses the word for the same mode, and it is not how the AMQP specification uses it, where a non-durable queue is the transient one. The consequence is worth stating rather than leaving for a caller to discover: a non-durable queue that is neither exclusive nor auto-delete is skipped by this mode, and it does not survive a broker restart, so the unfiltered consumer phase can then 404 against it. That follows from what the mode is for - durable topology managed out of band - rather than being a defect, but a caller choosing the mode should know it.

### Channel-session state is restored before topology

`basic_qos` and `confirm_delivery` are not topology entities and have no place in the phased ordering. They are properties of the channel session, they are lost when the channel is reopened, and they must be back in force **before** the first `basic_consume` of the replay. All three reference clients do exactly this, and each restores them as part of reopening the channel rather than as part of replaying topology:

- Java's `AutorecoveringChannel.automaticallyRecover` calls `recoverState()`, which issues `basicQos` and `confirmSelect`, and `AutorecoveringConnection` runs `recoverChannels` before `recoverTopology`.
- .NET does the same in `AutorecoveringChannel`, restoring both prefetch values before it calls `RecoverConsumersAsync`.
- `amqp091-go`'s `openChannelSession` is documented as "resets client-side state, opens a fresh broker channel, and restores QoS/Confirm configuration", and `Reconnect` calls it before `RecoverTopology`.

The ordering is load-bearing rather than tidy. RabbitMQ applies a `global=false` prefetch to consumers created *after* the `basic.qos`, leaving existing ones unaffected, and the spec puts the start of message flow at `Consume-Ok`. A QoS issued after `basic.consume` therefore cannot bound a flow that has already started: the reopened channel would take delivery of the entire outage backlog with no prefetch limit, straight into the bounded consumer work queue, which is the `WorkQueueFullError` path. So channel-session state is restored by whatever reopens the channel, immediately after the reopen and before any replay, and `_recover_topology` never sees it. That means **both** paths: `_recover_channel` for a channel-level pass, and `_reopen_channels_and_recover_topology` for a connection-wide one. Assigning it only to `_recover_channel` would leave the primary case - an ordinary network drop, which transitions only the connection's `_state` and never calls `_recover_channel` - coming back with no prefetch and confirms off on the wire. The same applies to `_reopen_channel` when `_skip_or_abort` reopens a channel mid-replay. Putting it in the shared reopen helper rather than in each caller is what keeps the three paths from diverging.

Two prefetch values, not one. `basic_qos(prefetch_size, prefetch_count, global_qos)` sets two independent limits depending on `global_qos`, and a channel may have both in force. Java and .NET each track them separately (`prefetchCountConsumer` and `prefetchCountGlobal`) and replay both; `amqp091-go` keeps a single `QosConfig` that the next call overwrites, so a channel that set both loses one on recovery. We follow Java and .NET.

Consumer replay needs its own mechanism, and the obvious two candidates are both wrong. This document previously said to recover via `ch.basic_consume(queue, on_message_callback, consumer_tag=tag, ...)`, which would re-create an identical closure around the same Python callback object. That contradicts the hard requirement above: `Channel.basic_consume` is a blocking wrapper whose body is `self._blocking_rpc(...)`, so calling it from the loop thread deadlocks. The other candidate, calling `raw_channel.basic_consume(on_message_callback=<user callback>)`, does not deadlock but silently discards everything `_wrapped_callback` adds: the delivery lands inline on the loop thread instead of a pool worker, the user callback receives the raw channel as its first argument rather than the wrapper, `_safe_dispatch`'s exception containment is gone so a callback error tears down the connection, the bounded queue's `WorkQueueFullError` back-pressure is gone, and the delivery-tag offset is never applied.

Java and .NET both re-subscribe on the raw channel - `RecordedConsumer.recover()` calls `channel.getDelegate().basicConsume(..., this.consumer)`, and .NET's `RecoverConsumersAsync` passes the new inner `IChannel` - and that is safe for them for a structural reason pika does not share. Their per-delivery behavior lives *below* the recovery wrapper: `RecoveryAwareChannelN extends ChannelN` and `RecoveryAwareChannel : Channel` each override delivery handling to apply the tag offset, and the consumer dispatch thread pool is in the base channel too. Re-subscribing on the raw channel therefore loses nothing. In pika the equivalent behavior is entirely in the adapter `Channel`: `_safe_dispatch` and `_consumer_work_pool` do not appear in `pika/channel.py` at all.

So: extract the closure rather than duplicating or relocating it. `Channel` gains `_make_delivery_callback(on_message_callback)` returning exactly the callable `basic_consume` builds today, and both paths use it - `basic_consume` for the application's call, and `_recover_topology` for the replay, passing the result to `raw_channel.basic_consume` **together with the recorded `consumer_tag`**. The tag is not optional: `pika.channel.Channel.basic_consume` generates a fresh one when none is supplied, so a replayed consumer would return under a new tag while the application still holds the tag its original call returned and the ledger stays keyed by the old one. A later `basic_cancel(old_tag)` then finds it absent from `_consumers`, logs `basic_cancel - consumer not found` and returns without transmitting `Basic.Cancel` or registering the `CancelOk` callback, so the adapter's waiter is never set: the caller blocks for the full RPC timeout and raises `TimeoutError`, the consumer keeps delivering, and since the ledger record was never removed it is resurrected on the next recovery. Java's `RecordedConsumer.recover()` re-subscribes with the recorded tag for exactly this reason. One definition, no base-layer change, and the raw-channel path becomes legal. The duplication this avoids matters more than it looks: a per-delivery behavior defined twice would diverge on the next change to either copy, and the resulting bug is reachable only after a reconnect, which no test exercises unless it forces one.

Relocating the pool and dispatch guard into `pika.channel.Channel`, as Java and .NET have done, is the structurally cleaner end state and would make replay a plain raw-channel call with nothing to extract. It is deliberately out of scope here: it moves adapter machinery into the base layer, and it belongs with the 2.0 `_core` restructure rather than inside a recovery design. Worth revisiting there.

Server-generated queue names: `queue_declare('')` records `declared_name=''`, `actual_name=<broker-assigned>`; on recovery we redeclare with `declared_name=''` again, note the rename in `_recover_topology`'s local name-replacement map, and rewrite `actual_name` plus any `BindingRecord`/`ConsumerRecord` across every bucket still referencing the old name.

The receiver matters, and the wrapper property will not do. `ch.is_open`/`ch.is_closed` fold in the effective state, and during replay that state is `OPEN` on a connection-wide pass or `RECOVERING` on a channel-level one - never `CLOSED`. So a broker error that kills the raw channel mid-replay would leave the wrapper reporting not-closed, `_skip_or_abort` would decline to reopen, and every remaining entity would be issued against a dead raw channel where `_raise_if_not_open` raises on the loop thread, violating obligation 3. The raw channel is the authority on whether the session is usable; the wrapper property answers a different question.

### Skip-and-continue must reopen the channel

A broker-side protocol error during topology recovery (e.g. a 404 on a binding referencing a since-deleted queue) closes the entire channel, not just the offending entity. `_recover_topology`'s `_skip_or_abort` helper checks **`ch._channel.is_closed`** - the raw channel, deliberately not the wrapper property - after recording a skip and, if so, calls `_reopen_channel(ch)` before continuing with the channel's remaining entities - driven through the same callback-chained mechanism described above, not a blocking reopen call.

## Proposed file-by-file changes

- **`pika/recovery.py`** (new): `RecoveryConfig`, `TopologyRecoveryMode`, `RecoveryCoordinator` (holding `config` and `topology: dict[int, ChannelTopology]`, but no per-pass state such as an attempt counter, plus the `record_*`/`remove_*` methods described in "Topology ledger" above), `ChannelTopology`, the `*Record` dataclasses, `TopologyRecoveryEntity`.
- **`pika/exceptions.py`**: `ConnectionRecovering(ConnectionWrongStateError)`, `ChannelRecovering(ChannelWrongStateError)`.
- **`pika/adapters/thread_safe_connection.py`**:
  - `Connection.__init__` gains `recovery=`, `self._recovery`, `self._state: LifecycleState`, `self._ioloop` (constructed once, no longer tied to a single inner `SelectConnection`), `self._parameters`, `self._connect_timeout`.
  - a state test added in front of `_check_not_closed`'s existing closed-connection test, not replacing it; `state`/ `is_recovering` properties; `add_state_change_listener`, `add_on_close_callback`, `add_on_open_callback`, `add_on_recovery_*_callback` methods (the latter three implemented as listeners on `add_state_change_listener`, per the table above).
  - `is_open`/`is_closed` on both the adapter `Connection` and `Channel` changed from delegating to the inner `Connection`/`Channel` object to reading `self._state` directly (see "Observability" above).
  - Extended `_on_connection_closed`; `_recover_connection`, `_try_reconnect_once` (constructing new inner `SelectConnection` instances bound to the persistent `self._ioloop`), `_reopen_channels_and_recover_topology`, `_reopen_channel`, `_recover_topology` (driving the raw `Channel`'s non-blocking API directly - see "Topology replay must not use the blocking wrapper API" above), `_on_channel_closed_for_recovery`, `_recover_channel`.
  - `Channel` gains `self._closed`, `self._state: LifecycleState`, an effective-state check that also consults the connection's state (see "Composing channel-level and connection-level recovery" above), `_register_recovery_close_listener`, `add_state_change_listener`, `add_on_close_callback`, `add_on_open_callback`, `add_on_recovery_started_callback`, `add_on_recovery_succeeded_callback`, `add_on_recovery_failed_callback` (own listener list, populated only by `_recover_channel` - never by `_recover_connection`), plus recording/removal call sites in the declare/bind/consume/delete/unbind/ cancel methods that delegate to `self._wrapper._recovery.record_*`/ `remove_*`. It also gains `_delivery_tag_offset`, `_max_seen_delivery_tag`, and `_make_delivery_callback(on_message_callback)`, the last extracted from the closure currently built inline in `basic_consume` so that replay can reuse it rather than reimplement it.
  - Eleven existing method signatures (`basic_qos`, `basic_cancel`, `queue_declare`, `exchange_declare`, `queue_bind`, `queue_unbind`, `queue_delete`, `queue_purge`, `exchange_bind`, `exchange_unbind`, `exchange_delete`) need their declared return type corrected from `-> None` to `-> Any`. Verified by inspection: all eleven are declared `-> None` and return the value of `_blocking_rpc`, and `queue_purge`'s own docstring promises "The Queue.PurgeOk method frame".
- **`examples/thread_safe_recovery_example.py`** (new): a `Connection` with `recovery=RecoveryConfig()`, a background publisher thread that catches `ConnectionRecovering`/`ChannelRecovering` and retries after `on_recovery_succeeded` fires, a consumer registered via `basic_consume`, and state-change listeners logging transitions.
- **`tests/acceptance/thread_safe_recovery_test.py`** (new, requires RabbitMQ): acceptance tests, listed in the test plan below.
- **`tests/unit/recovery_tests.py`** (new, mock-based): unit tests covering config/coordinator/ledger logic and the guard behavior.
- **`tests/unit/thread_safe_connection_tests.py`**: existing bare `MagicMock()` fixtures used as `Channel` wrappers will need to explicitly set `wrapper._recovery = None` (a plain `MagicMock()` is truthy for any attribute access, so an un-set `_recovery` would look like an opted-in `RecoveryCoordinator`).

`pika/spec.py` remains untouched - no protocol/spec changes are needed, since recovery is pure client-side orchestration of existing AMQP methods.

## Proposed test plan

### Integration (`tests/acceptance/thread_safe_recovery_test.py`, real broker)

**A warning that governs several tests below: a passive declare cannot be used as the probe.** Tests that verify "the entity was redeclared" by issuing `queue_declare(passive=True)` or `exchange_declare(passive=True)` on the recovered channel are self-defeating. In the failing case such a test exists to catch - replay did not redeclare the entity - the probe draws a broker 404, delivered as `ChannelClosedByBroker`, which is a `ChannelClosed` and therefore an `AMQPChannelError`: precisely the trigger this design specifies for starting a channel-level recovery pass. So the probe closes the channel, `_recover_channel` reopens it and replays the ledger, and the entity the assertion said was missing now exists. Any recheck, or the retrying helper other tests already use, then reports success, making a broken replay indistinguishable from a working one. The run also produces an unplanned channel-level recovery episode whose absence two other tests assert.

Verify redeclaration without touching the channel under test instead: query the management HTTP API, or issue the passive declare on a **second, independent connection** configured without recovery, so a 404 closes that connection's channel and surfaces as a test failure rather than as a repair. Tests 3 and 6 below are written in terms of a passive declare and must use the separate-connection form.

We propose simulating drops with **`ForwardServer`** (an existing test helper that proxies TCP to the real broker in a subprocess), the same technique `tests/acceptance/thread_safe_connection_test.py` already uses. Queue/exchange names would be uuid-suffixed.

1. `TestPublishContinuityAcrossRecovery` - publish before a drop, confirm it landed (via passive declare), publish again after recovery succeeds (via a retrying helper), assert both messages are present. Durable, non-exclusive queue.
2. `TestConsumeContinuityAcrossRecovery` - **the core requirement**: register `basic_consume` once, drop the connection, publish more messages from a second, independent connection after recovery completes, assert they arrive at the *original* callback with zero additional app calls.
3. `TestExclusiveQueueRecovery` - an exclusive queue (deleted by the broker on disconnect) is transparently redeclared; verify from a second, recovery-free connection, never with a passive declare on the channel under test, per the warning above.
4. `TestDeletedQueueSkipAndContinue` - a binding to a queue declared on a separate, untracked connection is deleted while the primary connection is down, so binding recovery gets a real 404. Assert `on_topology_entity_error` fires with a `'binding'` entity whose `channel_number` matches, `on_recovery_succeeded`'s `skipped` list contains it, and everything else recovers fully.
5. `TestTopologyRecoveryDisabled` - `topology_recovery_mode=DISABLED`: reconnects but a previously-declared queue is not redeclared.
6. `TestOnlyTransientTopologyRecovery` - `ONLY_TRANSIENT`: an exclusive queue and its binding to a durable exchange are redeclared; a durable queue deleted out-of-band before the drop is not recreated. Both checks run from a second, recovery-free connection, per the warning above.
7. `TestRetryExhaustionRaisesCleanly` - `max_attempts=2` against a forwarder that's never restarted; assert `on_recovery_failed` fires, the original close callback fires, `connection.state == LifecycleState.CLOSED` (not a separate `FAILED` value), with no hang.
8. `TestExplicitCloseDoesNotTriggerRecovery` - `connection.close()` never fires `on_recovery_started`.
9. `TestDefaultBehaviorUnchangedWithoutRecoveryConfig` - regression guard: omitting `recovery=` reproduces today's exact wake-all/teardown behavior on a forced drop.
10. `TestOperationDuringRecoveryRaisesDedicatedException` - drop the connection, and while `state == LifecycleState.RECOVERING` (before the redial completes), issue a `basic_publish` and assert it raises `ConnectionRecovering` (or `ChannelRecovering`, for a channel-scoped call) synchronously rather than timing out. Repeat for a channel-scoped soft-error recovery episode.
11. `TestChannelLevelRecoveryWithoutFullConnectionReconnect` - redeclaring an existing exchange with mismatched durability triggers a 406 that closes only that channel; assert the channel's own `add_on_recovery_succeeded_callback` fires while the connection-wide ones never fire.
12. `TestMultiChannelTopologyRecoveryOrdering` - channel 1 declares a transient exchange and a server-named exclusive queue; channel 2 declares the binding and the consumer. After a drop and recovery, assert the full exchange -> binding -> queue -> consumer chain is functional on both channels regardless of processing order.
13. `TestCrossChannelDeletionRemovesStaleTopology` - channel A declares a queue; channel B deletes it. Drop and recover; assert the queue is *not* incorrectly redeclared from channel A's side of the store.
14. `TestChannelRecoveryDoesNotDuplicatePassOnPermanentConflict` - a permanent per-entity conflict, skipped via `should_skip` every attempt, produces exactly one active `_recover_channel` episode for that channel at a time, not an unbounded chain.
15. `TestChannelCloseAndOpenCallbacksFireOnRecovery` - register `add_on_close_callback`/`add_on_open_callback` on a channel, force a broker-initiated single-channel close, assert close fires with the broker's reason and open fires once usable again, with no connection-level `on_recovery_*` firing in between.
16. `TestFullReconnectDoesNotFireChannelRecoveryCallbacks` - force a full connection drop on a connection whose channels have `add_on_recovery_succeeded_callback` registered; assert the connection-wide callback fires exactly once while no per-channel `on_recovery_*` fires for any channel.
17. `TestConnectionCloseAndOpenCallbacksFireOnRecovery` - register `add_on_close_callback`/`add_on_open_callback` on the connection; assert `add_on_open_callback` fires on the `RECOVERING -> OPEN` transition, that is after topology replay rather than at redial success, and ordered ahead of `on_recovery_succeeded` within that transition; assert `add_on_close_callback` fires on an explicit `close()` even with no `RecoveryConfig` at all.
18. `TestConnectionRecoverySupersedesInFlightChannelRecovery` - trigger an isolated channel-level recovery, and while it's mid-backoff, force a full connection drop. Assert the channel-level pass's `on_recovery_failed` never fires (it yields), the connection-level pass's `on_recovery_succeeded` fires once, and the channel ends up open and recovered exactly once.
19. `TestSupersededChannelRecoverySucceedsAnyway` - the timing variant of test 18, where the channel-level pass's in-flight attempt completes right as the connection-level pass takes over: assert the channel ends up with exactly one live raw channel afterward.
20. `TestPersistentIOLoopSurvivesReconnect` - assert the same `self._ioloop`/thread identity is used before and after a forced drop and successful redial (e.g. by tagging the thread object and comparing identity, not just liveness), confirming the redial did not spin up a second thread.

We'd want these run multiple times in CI to check for flakiness before merging, given the timing-sensitive nature of drop simulation.

### Unit (`tests/unit/recovery_tests.py`, mocked)

- Config/entity/coordinator: backoff math, `should_skip` behavior, `RecoveryConfig.topology_recovery_mode` defaulting to `ALL`, coordinator defaults (`RecoveryCoordinator` has no `state` field; lifecycle state lives on the adapter object, per "Where state lives, and who owns the topology ledger" above).
- Topology store: record/remove semantics for every entity type (cross-channel removal, cascade removal, binding/exchange-binding dedup, server-generated-name rename propagation across buckets).
- State machine: every guarded public method raises `ConnectionRecovering`/`ChannelRecovering` (not a generic wrong-state error) when `state == RECOVERING`; the terminal rule (`CLOSING`/`CLOSED` -> only `CLOSED` may follow, even if a redial callback tries to set `OPEN` after `close()` ran); `state`/`is_recovering` properties reflect the current value under concurrent access from another thread.
- Callback-sugar correctness: each `add_on_*_callback` fires exactly on the transition table in "Observability" above, and not on any other transition; `add_state_change_listener` fires on every transition, including ones none of the named callbacks cover.
- Recovery-callback partition: `_recover_channel` fires only a channel's own listeners, never the coordinator/connection-wide ones, and vice versa for `_recover_connection` - asserted directly, so the split doesn't regress to a shared-list design.
- Composing channel/connection recovery: a channel's effective state reads `RECOVERING` while the *connection* is `RECOVERING`, even if the channel's own `_state` hasn't been individually transitioned; a `_recover_channel` callback chain that finds the connection has become `RECOVERING` mid-chain yields without firing `on_recovery_failed`; a second `_on_channel_closed_for_recovery` invocation for a channel whose `_state` is already `RECOVERING` returns immediately without starting a second chain.
- `_reopen_channel` closes a still-open previous raw channel before installing its replacement - exercised directly (call it twice on a channel that's still open in between).
- `_on_connection_closed` recovery triggers: fires for non-client closes, not for `ConnectionClosedByClient`; a second close event while already `RECOVERING` does not start a second recovery sequence or re-fire `on_recovery_started`.
- Close-during-recovery: `close()` sets the terminal state and is a no-op if called again; a pending reconnect timer either becomes a no-op or is cancelled outright, without ever blocking on a thread join beyond `self._ioloop_thread`, which `close()` already knows how to join today.
- Deadlock regression guard: `_recover_topology` never calls a `Channel` blocking wrapper method (`queue_declare`, `exchange_declare`, etc.) from the loop thread - asserted by patching those methods to raise if invoked from the recovery code path, so a future change that accidentally reintroduces a blocking call during replay fails a unit test instead of hanging an acceptance test.

### CI gates before merge

Standard project gates apply and should all be green before merge: `hatch run fmt-check`, `hatch run lint-check`, `hatch run docfmt-check`, `hatch run typecheck`, `hatch run unit`, and the acceptance suite against a real broker.

## Open questions

These questions are raised in `design-state-machine.md` and remain open here:

- **Exact adapter state set.** This proposal uses `{OPEN, RECOVERING, CLOSING, CLOSED}` for `LifecycleState`. Whether an `OPENING` value is also needed on the adapter (mirroring the base classes) for the initial-connect path, distinct from a post-drop `RECOVERING`, is open.
- **A separate "socket back, replay pending" signal.** `add_on_open_callback` fires on the `RECOVERING -> OPEN` transition, which is after topology replay, so nothing currently observes the moment the redial succeeded while replay is still running. Java exposes the equivalent as its own pair of recovery-listener callbacks rather than as an open notification. Whether pika should add one, and what it should be called, is open; overloading `add_on_open_callback` for it is not an option, for the reason given under the redial sequence.
- **Opt-in block-until-open.** Non-goals rules a blocking mode out of this proposal: a call during `RECOVERING` always fails fast. Whether to add an opt-in "block until open, with a timeout" mode later, for callers who would rather wait than handle the exception, is open. It would be purely additive, leaving fail-fast as the default.
- **Exception hierarchy.** `ConnectionRecovering` subclasses `ConnectionWrongStateError` and `ChannelRecovering` subclasses `ChannelWrongStateError`. That was originally justified as preserving existing `except` clauses, which is not true of this adapter: its guards re-raise the recorded close reason (typically `ConnectionClosedByBroker` or `StreamLostError`), `ConnectionWrongStateError` is raised at exactly one site, and `ChannelWrongStateError` is never raised at all. 2.0 permits breaking the API, so the choice is now free and should be made on merit. Recommendation: keep these bases. "You called at the wrong time" is what happened, both still land under `AMQPConnectionError`/`AMQPChannelError`, and the alternatives are worse - subclassing `ConnectionClosed`/`ChannelClosed` would say the handle is closed when it is not, and a standalone hierarchy under `AMQPError` would escape every broad handler that reasonably should catch it. Note this is a change of justification, not of code.

## Honest unknowns

- The claim that recovery can run entirely on the persistent loop with no extra thread rests on `SelectConnection`'s existing `custom_ioloop` support, confirmed to exist in `pika/adapters/select_connection.py`, but the full `_run_ioloop`/thread-lifecycle refactor in `Connection.__init__` needed to decouple the thread from the inner connection has not been prototyped end-to-end. A minimal prototype - persistent `self._ioloop`, one forced redial, one `RECOVERING`-gated `basic_publish` raising the new exception - would settle the remaining risk before the full phased build-out below.
- The callback-chained rewrite of `_recover_topology` (driving the raw `Channel`'s non-blocking API instead of the blocking wrapper methods) touches every entity type's declare/bind/consume call site, not just the phased-ordering logic that sits on top of it - a larger mechanical change than the topology ledger's data model alone suggests.

## Next steps

Pending sign-off on the direction above, implementation proceeds in phases, each closed out with its own unit and integration coverage:

1. **Core state machine** - `LifecycleState`, `ConnectionRecovering`/`ChannelRecovering`, `state`/`is_recovering` properties, `add_state_change_listener` and the `add_on_*_callback` sugar, guard integration into every existing public method's not-closed check. No topology, no reconnection yet - this phase makes "is it recovering" answerable and enforced even before recovery can succeed at anything.
   - Unit: guard raises the dedicated exception per state; terminal-rule enforcement; callback-sugar-fires-on-correct-transition tests.
   - Integration: none yet (no reconnection exists to exercise).
2. **Persistent IOLoop and connection-level redial** - decouple `self._ioloop`/thread from the inner `SelectConnection` in `__init__`; `_on_connection_closed` branching and the `_on_connection_open_error` suppression it needs; the teardown suppressions from "Teardown the drop path performs"; `_recover_connection`, `_try_reconnect_once` bound to `custom_ioloop=self._ioloop`; `close()` reworked to rely on the terminal rule rather than a condition variable.

   **Phases 2 and 3 ship before topology replay exists, and that is coherent rather than a gap.** Both call into replay - phase 2's `_try_reconnect_once` through `_reopen_channels_and_recover_topology`, phase 3's `_recover_channel` through `_recover_topology([ch])` - but `_recover_topology` itself is phase 4. Rather than stubbing it, phases 2 and 3 behave as `topology_recovery_mode=DISABLED`: reconnect the connection, reopen the channels, redeclare nothing. That is a supported configuration in its own right, so each phase is independently shippable and testable, and phase 4 adds the remaining modes rather than switching the feature on.
   - Unit: recovery-trigger conditions; close-during-recovery; persistent loop identity across a forced redial.
   - Integration: `TestRetryExhaustionRaisesCleanly`, `TestExplicitCloseDoesNotTriggerRecovery`, `TestDefaultBehaviorUnchangedWithoutRecoveryConfig`, `TestConnectionCloseAndOpenCallbacksFireOnRecovery`, `TestPersistentIOLoopSurvivesReconnect`, `TestOperationDuringRecoveryRaisesDedicatedException` (connection-level half).
3. **Channel-level recovery and state composition** (test 14 belongs in phase 4, not here: it is defined in terms of `should_skip`, which arrives with the ledger) - `_register_recovery_close_listener`, `_on_channel_closed_for_recovery`, `_recover_channel`, the effective-state derivation rule from "Composing channel-level and connection-level recovery." Both the Scenario A guard (a per-step state check) and the Scenario B guard (effective-state derivation) land in the same commit as `_recover_channel` itself: shipping the retry logic first and adding either guard later would leave a window where a permanent per-entity conflict spawns an unbounded chain of recovery episodes, or a channel-level and connection-level pass double-reopen the same channel.
   - Unit: effective-state derivation; per-step staleness checks; recovery-callback partition.
   - Integration: `TestChannelLevelRecoveryWithoutFullConnectionReconnect`, `TestChannelRecoveryDoesNotDuplicatePassOnPermanentConflict`, `TestChannelCloseAndOpenCallbacksFireOnRecovery`, `TestFullReconnectDoesNotFireChannelRecoveryCallbacks`, `TestConnectionRecoverySupersedesInFlightChannelRecovery`, `TestSupersededChannelRecoverySucceedsAnyway`, `TestOperationDuringRecoveryRaisesDedicatedException` (channel-level half).
4. **Topology ledger and callback-chained replay** - `coordinator.topology`, `ChannelTopology` and the `*Record` dataclasses, `record_*`/`remove_*` methods and call sites, `_recover_topology` rewritten against the raw `Channel`'s non-blocking API (see "Topology replay must not use the blocking wrapper API" above), server-generated- name rename handling, skip-and-continue reopen logic. **The topology store must be connection-wide from the first commit of this phase**, for the ordering/split-brain-removal reasons in "Topology ledger" above. **The callback-chained replay mechanism must also land complete in this phase**, not as a "blocking calls for now, convert later" intermediate step - a blocking call issued from the loop thread hangs the connection outright rather than degrading gracefully, so there is no safe partial version of this phase to ship.
   - Unit: `TopologyStoreTests`; the deadlock regression guard described above.
   - Integration: `TestPublishContinuityAcrossRecovery`, `TestConsumeContinuityAcrossRecovery`, `TestExclusiveQueueRecovery`, `TestDeletedQueueSkipAndContinue`, `TestTopologyRecoveryDisabled`, `TestOnlyTransientTopologyRecovery`, `TestMultiChannelTopologyRecoveryOrdering`, `TestCrossChannelDeletionRemovesStaleTopology`.
5. **Publisher confirms hook and the delivery-tag offset** - the offset was previously assigned to no phase and covered by no test, even though phase 2 already reopens channels and so already makes a stale ack reachable. It lands here with the confirm-counter re-arm, since the two are the same subject seen from opposite ends, and needs its own acceptance test: hold a delivery unacked across a forced drop, then ack it after recovery and assert the ack was dropped rather than applied to a different message. - the `RECOVERING -> OPEN` reset point described above, plus an example helper showing the fail-and-republish pattern from `findings.md`.
6. **Hardening pass** - run the full acceptance suite repeatedly to check for flakiness, run `fmt-check`/`lint-check`/`docfmt-check`/`typecheck` across all changed files.
7. **Example and docs** - `examples/thread_safe_recovery_example.py`, docstrings, changelog entry.

Each phase should be its own reviewable PR (or a small stack of PRs) rather than one large PR at the end, so reviewers can weigh in on the state machine and the persistent-loop refactor before the topology ledger is built on top of them.
