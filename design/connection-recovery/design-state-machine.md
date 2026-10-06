# Design: how the recovery framing was arrived at

Before reading: `README.md` in this directory records the pika 2.0.0 constraints that govern this design - one `Connection`/`Channel` type, all other adapters removed, and the API free to change. They override anything below that assumes otherwise.

**Nothing in this file is normative.** `proposal-recovery.md` is the specification, and it is the only place decisions and open questions are tracked. This file exists for one reason: to stop a future implementer re-proposing approaches that were already tried and rejected, and to record the prior art the framing came from. If a sentence here looks like it specifies behaviour, the proposal wins - and the contradiction is a defect worth reporting rather than a choice to make.

Terminology note: pika has two classes named `Connection` and two named `Channel`. "Base `Connection`/`Channel`" means `pika.connection.Connection` / `pika.channel.Channel` (one transport session; dies and stays dead). "Adapter `Connection`/`Channel`" means `pika.adapters.thread_safe_connection.Connection` / `.Channel` (the stable handle an application holds, which can swap its inner connection across reconnects). Unless qualified, "the connection" below means the adapter `Connection`.

## Approaches already tried and rejected

Both of these were proposed for pika's recovery design and rejected. They are recorded because each is the obvious design from a standing start, so an implementer who does not know they were tried will propose them again.

**1. Recovery state kept separate from connection state.** Recovery lived in a `RecoveryCoordinator` with its own `RecoveryState {IDLE, RECONNECTING, FAILED}`, deliberately apart from pika's real connection state, and no public operation read it - a call during recovery "ran exactly as it would on a healthy connection". The consequence is that there is no synchronous way to ask whether recovery is in progress and nothing for an application to catch. `findings.md` is the empirical case against it: the reference clients fail fast, and `amqp091-go`, whose documentation is sometimes cited in support of it, actually requires the application to gate on connection state and can cause a protocol violation otherwise. The replacement is a single authoritative lifecycle state with `RECOVERING` as a first-class member, which is what the proposal specifies.

**2. Recovery on its own dedicated thread.** A new thread named `pika-recovery-{id}` drove recovery, because the dying inner connection's IOLoop was being stopped and something had to outlive it. That single choice produces most of the concurrency machinery it needs: a re-entrant `_recovering` flag, an `is_open`-first cross-level race guard, condition-variable-versus-sleep backoff, and a stale-connection early return. Nearly all of it was a tax on running recovery as a thread mutating state the state machine did not know about. The replacement is a persistent loop owned by the adapter, which the proposal specifies and whose own costs it sets out under "What the persistent loop costs" - that section is the honest accounting, and it is not a free win.

Note what the second reversal does *not* claim. All three reference clients drive recovery on a thread that is not servicing anything else. pika's loop keeps serving live work throughout, which is a genuine divergence with a real price, and the proposal names three absolute obligations that follow from it. Rejecting the dedicated thread was a judgement that the price is worth paying, not a finding that the thread was unnecessary.

## What pika already had before this design

Worth knowing, because it means the mechanism is not new work.

- Base `Connection` already has a state machine, set through `_set_connection_state`, and guards that raise `ConnectionWrongStateError` when not open.
- Base `Channel` already has one too, with a single guard, `_raise_if_not_open`, that every public operation including `basic_publish` calls, raising `ChannelWrongStateError` with a state-specific message.

So "a state value, plus a guard that raises when not open" already exists twice over. What was missing is a `RECOVERING` member and a dedicated exception precise enough for recovery-aware code to catch. One caveat that is easy to miss and changes the conclusion: these are the *base* classes. The adapter raises neither wrong-state type, so subclassing them does not preserve any existing handler.

## The precedent this borrows from

The RabbitMQ AMQP 1.0 Java client implements this shape already. Stripped of the 1.0-specific state contents, it is three ideas:

- One authoritative state. `ResourceBase` holds an `AtomicReference<State>` whose members are `OPENING`, `OPEN`, `RECOVERING`, `CLOSING`, `CLOSED`. Five, and `OPENING` is among them - which is directly relevant to the proposal's open question about the adapter's state set, since the precedent cited for the rest of the enum has already answered it.
- Every operation guards on it. `AmqpPublisher.publish()`'s first statement is `checkOpen()`, which throws a state-specific catchable exception naming the current state, so a publish during recovery fails precisely rather than hanging or succeeding into a void.
- Transitions are driven on one loop, with the rule that once `CLOSING` or `CLOSED` is reached only `CLOSED` may follow, so a late recovery success cannot resurrect a closed resource. Listeners see transitions in order.

AMQP 0.9.1 has a different lifecycle, so the state contents differ. What transfers is the shape: one state machine that owns the lifecycle, includes recovery, and gates operations.

## What a state machine does not remove

Making the lifecycle explicit and the failures catchable does not reduce the rest of the work. The connection-wide topology ledger, consumer re-subscription, server-named-queue rename handling, and at-least-once handling are all still required, and they are the bulk of the proposal. This framing changes the contract around recovery, not the need to redeclare topology.

## The assumption this all rests on

That recovery can run with no extra thread is a structural reading of the adapter's code, not a demonstrated fact, and it is the load-bearing assumption of everything above. It was originally also an inference from pika's other async adapters, but those are removed in 2.0, so the reading stands alone. The proposal tracks it, along with the prototype that would settle it and the price the choice carries, under "Honest unknowns" and "What the persistent loop costs" - not repeated here, because one canonical location per fact is the rule this design area works under.
