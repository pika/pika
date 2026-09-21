# Design: recovery as a first-class connection/channel state machine

Before reading: `README.md` in this directory records the pika 2.0.0 constraints that govern this design - one `Connection`/`Channel` type, all other adapters removed, and the API free to change. They override anything below that assumes otherwise.

Status: adopted, and kept as the derivation rather than as a specification. `proposal-recovery.md` now follows this framing and is the authoritative document; read it for the worked-out detail. This one records how the direction was arrived at, grounded in the measurements in `findings.md`, along with the precedent it borrows from and the questions it raised.

Terminology note: pika now has two classes named `Connection` and two named `Channel`. "Base `Connection`/`Channel`" means `pika.connection.Connection` / `pika.channel.Channel` (one transport session; dies and stays dead). "Adapter `Connection`/`Channel`" means `pika.adapters.thread_safe_connection.Connection` / `.Channel` (the stable handle an application holds, which can swap its inner connection across reconnects). Unless qualified, "the connection" below means the adapter `Connection`.

## Open questions

Three of these remain open in `proposal-recovery.md`, which reproduces them: the exact adapter state set, opt-in block-until-open, and the exception hierarchy. The proposal answers the fourth: channel-level recovery has its own sections there.

- The exact 0.9.1 state set for the adapter handles (do we mirror base `OPENING/OPEN/CLOSING/CLOSED` plus `RECOVERING`, or a smaller set?).
- Whether channel-level recovery (broker soft-error closing one channel while the connection stays up) is a `RECOVERING` state on just that `Channel`, and how it composes with a connection-level `RECOVERING`.
- Whether any operation should block-until-open with a timeout as an opt-in, or whether fail-fast is the only mode.
- What the new exceptions should subclass, now that the compatibility argument for the wrong-state bases has been retracted and 2.0 leaves the choice free.

## What this proposes, in one paragraph

Give the adapter `Connection` and `Channel` a single authoritative lifecycle state that includes a `RECOVERING` state. Every public operation guards on that state, and an operation attempted while `RECOVERING` raises a dedicated, catchable exception (`ConnectionRecovering` / `ChannelRecovering`, each subclassing the existing wrong-state error). Recovery is driven on a persistent event loop rather than a separate recovery thread. The state is observable (listeners) and reactive (the guard), so an application can both ask "am I recovering?" and catch the fact that it is.

## The two choices this reverses

`proposal-recovery.md` originally made two core choices that this framing reverses, and that it has since been rewritten to drop:

1. Recovery lived in a separate `RecoveryCoordinator` with its own `RecoveryState {IDLE, RECONNECTING, FAILED}`, deliberately kept apart from pika's real connection state, and no public operation read it. A call during recovery "ran exactly as it would on a healthy connection." The consequence, which that proposal's Open Questions acknowledged, was that there was no synchronous way to ask whether recovery was in progress and nothing for an app to catch reactively.
2. Recovery ran on a new dedicated thread (`pika-recovery-{id}`), because the dying connection's IOLoop was being stopped.

`findings.md` is the empirical case against choice 1: both reference clients fail fast, and amqp091-go, whose docs that framing cited, actually requires the app to gate on connection state and can cause a protocol violation otherwise. Choice 2 was the source of most of that proposal's concurrency machinery (a re-entrant `_recovering` flag, an `is_open`-first cross-level race guard, condition-variable-vs-sleep backoff, a stale-connection early return); much of it was a tax on running recovery as a thread that mutates shared state the state machine does not know about.

## The concept we are borrowing

The RabbitMQ AMQP 1.0 Java client (`rabbitmq-amqp-java-client`) already implements this shape. Stripped of the 1.0-specific state contents, it is three ideas:

- One authoritative state: `ResourceBase` holds an `AtomicReference<State>` with `RECOVERING` as a first-class member alongside `OPENING`, `OPEN`, `CLOSING`, `CLOSED`. Five members, not the four an earlier version of this line listed, and the omitted one was `OPENING` - which matters because whether the adapter needs an `OPENING` value is exactly the open question at the top of this document, and the precedent being cited for the rest of the enum had already answered it.
- Every operation guards on it. `AmqpPublisher.publish()`'s first line is `checkOpen()`, and `checkOpen()` throws a state-specific, catchable exception when not open (`AmqpResourceInvalidStateException`, naming the current state), so a publish during `RECOVERING` fails with a precise, catchable error.
- Transitions are driven on one loop, with the rule that once `CLOSING`/`CLOSED` is reached only `CLOSED` may follow, so a late recovery success cannot resurrect a closed resource. State changes are dispatched to listeners in order.

AMQP 0.9.1 has a different lifecycle than 1.0, so the state contents differ. What transfers is the concept: a single state machine that owns the lifecycle, includes recovery, and gates operations.

## What pika already has

pika is closer to this than the original proposal treated it as being.

- Base `Connection` already has a state machine (`CONNECTION_CLOSED/INIT/PROTOCOL/START/TUNE/OPEN/CLOSING`, set via `_set_connection_state`) and guards that already raise `ConnectionWrongStateError` when not open (for example `channel()` and `close()`).
- Base `Channel` has `CLOSED/OPENING/OPEN/CLOSING` and a single guard, `_raise_if_not_open`, that every public operation including `basic_publish` calls, already raising `ChannelWrongStateError` with a state-specific message.

So the mechanism this design needs (a state value, a guard that raises on not-open) already exists. What is missing is a `RECOVERING` state and a dedicated exception, which recovery-aware code can catch precisely. This document originally argued that subclassing the existing wrong-state errors would keep existing `except ConnectionWrongStateError` / `except ChannelWrongStateError` code working. That argument is wrong twice over and is retracted: the adapter does not raise those types in the first place, and 2.0 does not require preserving 1.x handlers anyway. See `proposal-recovery.md` under Open questions for the hierarchy choice as it now stands.

## Proposed model for pika

### Where the state lives

Not on the base `Connection`: a base `Connection` is 1:1 with a transport session; it reaches `CLOSED` and never comes back, and recovery means constructing a new one. The authoritative "am I recovering" state must live on an object whose identity survives a reconnect. That object is the adapter `Connection`, which already wraps and can swap its inner connection. This matches amqp091-go (its `*Connection` handle is stable and swaps its inner transport) and the 1.0 client (`AmqpConnection` is stable and swaps its native connection). So: the adapter `Connection` and `Channel` gain a first-class lifecycle state; the base classes keep their existing per-session states unchanged.

### States

Add a `RECOVERING` state to the adapter `Connection`/`Channel` lifecycle, distinct from `OPEN`, `CLOSING`, and `CLOSED`. Adopt the 1.0 client's terminal rule: once `CLOSING`/`CLOSED`, only `CLOSED` may follow, so a recovery attempt that succeeds after the app has called `close()` cannot resurrect the handle. The exact set of states for 0.9.1 is an open question (see "Open questions" above), but at minimum recovery must be representable as its own state, not folded into "closed."

### The guard and the exceptions

An operation attempted while `RECOVERING` raises a dedicated exception:

- `ConnectionRecovering(ConnectionWrongStateError)`
- `ChannelRecovering(ChannelWrongStateError)`

Subclassing lets recovery-aware code do `except ChannelRecovering: wait_for_open(); republish()`. It is not a compatibility measure: see the retraction above. This is the direct answer to the goal: a publish attempted during recovery throws a dedicated exception that can be acted upon. Fail-fast (raise), not block, matches both reference clients and `findings.md`.

### Thread model: drive recovery on a persistent loop

The adapter runs a `SelectConnection` IOLoop on a background thread; today that loop is owned by the inner connection and stops when it dies, which is why the original proposal spawned a separate recovery thread. The cleaner path is to make the poller/IOLoop persistent and owned by the adapter `Connection`, decoupled from the inner connection instance, so a reconnect is "build a new inner connection bound to the same persistent loop" and the whole redial-plus-redeclare sequence is a series of loop-driven state transitions rather than a thread racing the handle. Recovery steps that touch the socket stay on the loop thread (as all protocol already does); backoff uses loop timers, not `time.sleep`. In 2.0 this is not a choice between adapters, since only one remains: the persistent-loop change is the whole of the work rather than one adapter's share of it.

### Observable and reactive

The state machine makes "is it recovering?" answerable two ways: a synchronous `state` / `is_recovering` property, which `proposal-recovery.md` now specifies, and ordered state-change listeners (the equivalent of amqp091-go's `NotifyStateChange` and the 1.0 client's `StateListener`). Recovery-aware apps gate on `OPEN` via a listener; everything else can catch `*Recovering`.

### Publisher confirms hook

The `RECOVERING -> OPEN` transition is the single, natural place to signal "the confirm sequence has reset; treat outstanding publishes as unknown." That is exactly what the 1.0 client does (fail outstanding on recovery) and what the Java 0.9.1 client does implicitly (reset tags on recovery). A confirm-tracking helper can subscribe to that transition to fail and republish outstanding, giving the at-least-once pattern from `findings.md` a clean anchor instead of ad hoc detection.

## What this does not remove

A state machine makes the lifecycle explicit and the failures catchable. It does not remove the genuinely necessary parts of the proposal: the connection-wide topology ledger, consumer re-subscription, server-named-queue rename handling, and at-least-once/idempotency. Those are still required. This design changes the contract around recovery, not the need to redeclare topology.

## Honest unknowns

- The claim that recovery can run entirely on a persistent loop with no extra thread is a structural reading of the adapter. It was originally also an inference from how pika's async adapters worked, but those are removed in 2.0, so that support is gone and the claim now rests on the structural reading alone. It is not proven end-to-end in pika, and all three reference clients do the opposite - see "What the persistent loop costs" in `proposal-recovery.md`. A minimal prototype (add `RECOVERING` plus the exception plus a `state` property to the adapter `Connection`, gate `basic_publish`, and drive one reconnect on a persistent loop) would settle it, and it should not be asserted as fact until it runs.
- The persistent-loop refactor of the adapter's construction path is real work whose blast radius has not been assessed here.
