# Invariants

What must never happen, whatever the implementation. Each names the scenarios in `success-criteria.md` that fail if it is broken.

- I-1 No wait can hang forever. Every wait in recovery and close is bounded by the caller's budget and by whether what it waits for can still happen, including `close()` while recovering. `close()` never waits for its own thread. SC-10, SC-22, SC-35.
- I-2 Application code cannot stall I/O. Listeners, the delay policy and the filter run with no recovery lock held, and an exception from any of them is contained and logged. A delay policy or filter that blocks delays recovery, and `close()` still ends it. The one permitted I/O stall is the existing consumer back-pressure, bounded by the work queue's put timeout. SC-36.
- I-3 Closing wins. Once a connection or channel is `CLOSING` or `CLOSED`, only `CLOSED` may follow. Once the application has asked to close, no recovery starts, and anything recovery opens afterwards is closed. SC-22, SC-23.
- I-4 One generation at a time. Work and waits bound to an ended generation of a connection or channel never act on a later one: a waiting call raises, and an acknowledgement is dropped. SC-33.
- I-5 Only what the broker accepted is recorded. SC-6.
- I-6 What the application removed stays removed: nothing it deleted, unbound, cancelled or closed is replayed, nor an auto-delete entity the broker removed because of that. SC-6, SC-24.
- I-7 No confirmation is lost silently. In confirm mode, every publish awaiting a confirmation or not yet written when its channel is lost gets an outcome-unknown notification, never a nack, queued before the channel reports `OPEN` and ahead of any new confirm. SC-3.
- I-8 Calls during recovery fail fast, except those `concepts.md` lists as accepted in any state. None blocks until recovery finishes and none is queued. SC-2, SC-24, SC-25.
- I-9 Every transition is reported once, in order, with its reason. SC-13.
- I-10 `OPEN` means usable. After connection recovery, nothing reports `OPEN` until replay has finished. After channel recovery, the channel reports `OPEN` only when its settings and every consumer not refused are restored and the entities that depended on it, other than any skipped (D-8), exist on the broker as recorded. SC-5, SC-13, SC-25.
- I-11 Channel and consumer recovery stop when the delay policy gives up. Connection recovery may retry forever, but `close()` always stops it. SC-10, SC-22, SC-27.
- I-12 No consumer tag is reused on a channel while active, by the application or by recovery. SC-32.
- I-13 A stale delivery tag is never applied to a different delivery. SC-14, SC-31, SC-33.
- I-14 Off means off. With recovery disabled, a lost connection goes to `CLOSED` and nothing is retried or replayed. SC-8.
- I-15 A drop is never lost. One recovery runs at a time. A drop before a recovered generation reports `OPEN` is another attempt of that recovery, and a drop after it starts a new one. SC-12, SC-34.
- I-16 Recovery does not leak. Repeated recoveries leave the number of threads and open file descriptors where it was before the first. SC-37.
