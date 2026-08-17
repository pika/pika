# Design: recovery as a first-class connection/channel state machine

Status: exploration. This is an alternative framing to `proposal-recovery.md`, not a replacement worked out to the same level of detail. It captures a direction to evaluate against that proposal, grounded in the measurements in `findings.md`.

Terminology note: pika now has two classes named `Connection` and two named `Channel`. "Base `Connection`/`Channel`" means `pika.connection.Connection` / `pika.channel.Channel` (one transport session; dies and stays dead). "Adapter `Connection`/`Channel`" means `pika.adapters.thread_safe_connection.Connection` / `.Channel` (the stable handle an application holds, which can swap its inner connection across reconnects). Unless qualified, "the connection" below means the adapter `Connection`.

## What this proposes, in one paragraph

Give the adapter `Connection` and `Channel` a single authoritative lifecycle state that includes a `RECOVERING` state. Every public operation guards on that state, and an operation attempted while `RECOVERING` raises a dedicated, catchable exception (`ConnectionRecovering` / `ChannelRecovering`, each subclassing the existing wrong-state error). Recovery is driven on a persistent event loop rather than a separate recovery thread. The state is observable (listeners) and reactive (the guard), so an application can both ask "am I recovering?" and catch the fact that it is.

## Where this differs from the current proposal

`proposal-recovery.md` makes two core choices this design reverses:

1. Recovery lives in a separate `RecoveryCoordinator` with its own `RecoveryState {IDLE, RECONNECTING, FAILED}`, deliberately kept apart from pika's real connection state, and no public operation reads it. A call during recovery "runs exactly as it would on a healthy connection." The consequence, which the proposal's Open Questions acknowledges, is that there is no synchronous way to ask whether recovery is in progress and nothing for an app to catch reactively.
2. Recovery runs on a new dedicated thread (`pika-recovery-{id}`), because the dying connection's IOLoop is being stopped.

`findings.md` is the empirical case against choice 1: both reference clients fail fast, and amqp091-go, whose docs the proposal cites, actually requires the app to gate on connection state and can cause a protocol violation otherwise. Choice 2 is the source of most of the proposal's concurrency machinery (the re-entrant `_recovering` flag, the `is_open`-first cross-level race guard, the condition-variable-vs-sleep backoff, the stale-connection early return); much of that is a tax on running recovery as a thread that mutates shared state the state machine does not know about.

## The concept we are borrowing

The RabbitMQ AMQP 1.0 Java client (`rabbitmq-amqp-java-client`) already implements this shape. Stripped of the 1.0-specific state contents, it is three ideas:

- One authoritative state: `ResourceBase` holds an `AtomicReference<State>` with `RECOVERING` as a first-class member alongside `OPEN`, `CLOSING`, `CLOSED`.
- Every operation guards on it. `AmqpPublisher.publish()`'s first line is `checkOpen()`, and `checkOpen()` throws a state-specific, catchable exception when not open (`AmqpResourceInvalidStateException`, naming the current state), so a publish during `RECOVERING` fails with a precise, catchable error.
- Transitions are driven on one loop, with the rule that once `CLOSING`/`CLOSED` is reached only `CLOSED` may follow, so a late recovery success cannot resurrect a closed resource. State changes are dispatched to listeners in order.

AMQP 0.9.1 has a different lifecycle than 1.0, so the state contents differ. What transfers is the concept: a single state machine that owns the lifecycle, includes recovery, and gates operations.

## What pika already has

pika is closer to this than the current proposal treats it as being.

- Base `Connection` already has a state machine (`CONNECTION_CLOSED/INIT/PROTOCOL/START/TUNE/OPEN/CLOSING`, set via `_set_connection_state`) and guards that already raise `ConnectionWrongStateError` when not open (for example `channel()` and `close()`).
- Base `Channel` has `CLOSED/OPENING/OPEN/CLOSING` and a single guard, `_raise_if_not_open`, that every public operation including `basic_publish` calls, already raising `ChannelWrongStateError` with a state-specific message.

So the mechanism this design needs (a state value, a guard that raises on not-open) already exists. What is missing is a `RECOVERING` state and a dedicated exception. Because the new exceptions would subclass the existing wrong-state errors, existing `except ConnectionWrongStateError` / `except ChannelWrongStateError` code keeps working, while recovery-aware code can catch the precise case.

## Proposed model for pika

### Where the state lives

Not on the base `Connection`: a base `Connection` is 1:1 with a transport session; it reaches `CLOSED` and never comes back, and recovery means constructing a new one. The authoritative "am I recovering" state must live on an object whose identity survives a reconnect. That object is the adapter `Connection`, which already wraps and can swap its inner connection. This matches amqp091-go (its `*Connection` handle is stable and swaps its inner transport) and the 1.0 client (`AmqpConnection` is stable and swaps its native connection). So: the adapter `Connection` and `Channel` gain a first-class lifecycle state; the base classes keep their existing per-session states unchanged.

### States

Add a `RECOVERING` state to the adapter `Connection`/`Channel` lifecycle, distinct from `OPEN`, `CLOSING`, and `CLOSED`. Adopt the 1.0 client's terminal rule: once `CLOSING`/`CLOSED`, only `CLOSED` may follow, so a recovery attempt that succeeds after the app has called `close()` cannot resurrect the handle. The exact set of states for 0.9.1 is an open question (see below), but at minimum recovery must be representable as its own state, not folded into "closed."

### The guard and the exceptions

An operation attempted while `RECOVERING` raises a dedicated exception:

- `ConnectionRecovering(ConnectionWrongStateError)`
- `ChannelRecovering(ChannelWrongStateError)`

Subclassing keeps backward compatibility (existing wrong-state handlers still catch it) while letting recovery-aware code do `except ChannelRecovering: wait_for_open(); republish()`. This is the direct answer to the goal: a publish attempted during recovery throws a dedicated exception that can be acted upon. Fail-fast (raise), not block, matches both reference clients and `findings.md`.

### Thread model: drive recovery on a persistent loop

The adapter runs a `SelectConnection` IOLoop on a background thread; today that loop is owned by the inner connection and stops when it dies, which is why the current proposal spawns a separate recovery thread. The cleaner path is to make the poller/IOLoop persistent and owned by the adapter `Connection`, decoupled from the inner connection instance, so a reconnect is "build a new inner connection bound to the same persistent loop" and the whole redial-plus-redeclare sequence is a series of loop-driven state transitions rather than a thread racing the handle. Recovery steps that touch the socket stay on the loop thread (as all protocol already does); backoff uses loop timers, not `time.sleep`. Note that the async adapters (asyncio, tornado, twisted) already own a persistent external loop, so this model is natural there with no extra thread at all; the thread-safe adapter is the one that needs the persistent-loop change to avoid a recovery thread.

### Observable and reactive

The state machine makes "is it recovering?" answerable two ways: a synchronous `state` / `is_recovering` property (closing the proposal's Open Question), and ordered state-change listeners (the equivalent of amqp091-go's `NotifyStateChange` and the 1.0 client's `StateListener`). Recovery-aware apps gate on `OPEN` via a listener; everything else can catch `*Recovering`.

### Publisher confirms hook

The `RECOVERING -> OPEN` transition is the single, natural place to signal "the confirm sequence has reset; treat outstanding publishes as unknown." That is exactly what the 1.0 client does (fail outstanding on recovery) and what the Java 0.9.1 client does implicitly (reset tags on recovery). A confirm-tracking helper can subscribe to that transition to fail and republish outstanding, giving the at-least-once pattern from `findings.md` a clean anchor instead of ad hoc detection.

## Scope decision to make first

Is the state model a base-level concept shared by all adapters, or thread-safe-adapter-only (the current proposal's scope)? Recommendation: share the state contract at the level where the stable handle lives, with per-adapter recovery drivers (an async loop for asyncio/tornado/twisted, the persistent thread-backed loop for the thread-safe adapter). The contract (states, guard, exceptions, listeners) can be common even though the driver differs. Deciding this early matters because it changes where the code lands.

## What this does not remove

A state machine makes the lifecycle explicit and the failures catchable. It does not remove the genuinely necessary parts of the current proposal: the connection-wide topology ledger, consumer re-subscription, server-named-queue rename handling, and at-least-once/idempotency. Those are still required. This design changes the contract around recovery, not the need to redeclare topology.

## Open questions

- The exact 0.9.1 state set for the adapter handles (do we mirror base `OPENING/OPEN/CLOSING/CLOSED` plus `RECOVERING`, or a smaller set?).
- Whether channel-level recovery (broker soft-error closing one channel while the connection stays up) is a `RECOVERING` state on just that `Channel`, and how it composes with a connection-level `RECOVERING`.
- Whether any operation should block-until-open with a timeout as an opt-in, or whether fail-fast is the only mode.
- Migration/compat: confirming that the new exceptions subclassing the existing wrong-state errors preserves current behavior for code that does not opt into recovery.

## Honest unknowns

- The claim that recovery can run entirely on a persistent loop with no extra thread is a structural reading of the adapter plus an inference from how the async adapters already work. It is not proven end-to-end in pika. A minimal prototype (add `RECOVERING` plus the exception plus a `state` property to the adapter `Connection`, gate `basic_publish`, and drive one reconnect on a persistent loop) would settle it, and it should not be asserted as fact until it runs.
- The persistent-loop refactor of the adapter's construction path is real work whose blast radius has not been assessed here.
