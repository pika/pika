# Process

The phases are an order of work, not a second specification: what each delivers is defined in `concepts.md`, and each is done when its scenarios pass.

## Rules for every phase

- Before deciding any behaviour, read what the Java, AMQP 1.0 Java, .NET and Go clients do in that case. Where pika will differ, record the divergence and its reason in `decisions.md` before writing code.
- Write the phase's scenarios before its code. A scenario for new behaviour must fail before the code exists, for the reason it names; one that guards existing behaviour must pass before and after.
- Prove each scenario can fail. Once it passes, break the behaviour it guards in a throwaway checkout, with the change a plausible future edit would make, and confirm it fails.
- A guard lands with the mechanism it guards. Retry without its bound, or replay without its removal rules, never reaches `main`.
- No phase depends on what a later phase records.
- Every phase leaves `main` releasable. Recovery is reachable by applications only once its phase's scenarios pass, and becomes the default (D-1) only after phase 6.

## Phases

1. Fault-injection harness.
   - Toxiproxy in CI on Linux and Windows at a pinned version, the broker versions O-11 settles, and the test-timing hooks O-10 settles.
   - Helpers for each fault, and a recovery-free connection for checking the broker.
   - Proven against today's code: each fault produces the failure it should, and a scenario that ignores the fault fails.
2. States, fail-fast and generations, without reconnection.
   - The five states and their listeners, the errors (O-1), the terminal rule, and queued deliveries surviving a drop that recovery will follow.
   - Generations, and recording a requested close, ahead of any code that could act on a later generation.
   - Done: SC-13 for closing, SC-8, and SC-16 as a guard on existing behaviour.
3. Connection recovery, without topology.
   - The recovery driver, the delay policy and its defaults, the trigger rule, reopening channels and restoring their settings, delivery tags, confirms, transactions and blocked state, and forgetting a channel the application closes.
   - Until phase 4, recovery can be enabled only with topology recovery off, so no release implies that consumers survive a reconnect.
   - Before its scenarios are written: O-5, O-9, O-12, O-13, O-14, O-17, O-21, O-26 and O-27 are settled.
   - Done: SC-2, SC-3, SC-4, SC-9, SC-10, SC-11, SC-14 for `basic_get`, SC-19, SC-20, SC-21, SC-22 except during replay, SC-23, SC-24 for channels, SC-33, SC-34, SC-36 except the filter, SC-37, SC-38, SC-41 except during replay, and SC-35 over all of these.
4. The record and topology replay.
   - Recording and removal, replay order, server-named queues and their rename notification, the filter, skipped entities, retrying a resource the old connection holds, a drop during replay, a channel closed during replay, and `OPEN` only after replay.
   - Before its scenarios are written: O-2, O-15, O-16, O-18, O-19, O-20 and O-24 are settled.
   - Done: SC-1, SC-5, SC-6, SC-7, SC-12, SC-13, SC-14, SC-15, SC-17, SC-18, SC-22 during replay, SC-24 for consumers, SC-36 for the filter, SC-39, SC-40, SC-41 during replay, and SC-35 over all of these.
5. Channel and consumer recovery.
   - Broker-closed channels, broker-cancelled consumers with the capability check, stale tags of cancelled consumers, and the per-resource attempt limit.
   - Before its scenarios are written: O-6, O-22, O-23 and O-25 are settled.
   - Done: SC-25 to SC-32, and SC-35 over them.
6. Hardening and documentation.
   - Every scenario repeated in CI on each supported broker line until it meets the stability count O-11 sets.
   - User documentation, an example, and release notes that state D-21 and D-6's broker requirement.

O-7 and O-8 are possible follow-ons, outside these phases.

## Review

Each phase gets an adversarial review over its final code before it merges, including its scenarios: the review checks that each reaches the state it claims and fails when the behaviour breaks. A fix made after the review reopens it.
