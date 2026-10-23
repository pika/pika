# Success criteria

Recovery is done when every scenario below passes against a real broker, run repeatedly. Each is a fault injected into a known state and the result the application must observe.

## Faults

| Fault | How | What the client sees |
| --- | --- | --- |
| F-close | `rabbitmqctl close_connection` | `connection.close` with 320 CONNECTION_FORCED |
| F-restart | Stop and start the broker | `connection.close` with 320, then refused connections until the broker is back |
| F-refuse | toxiproxy: disable the proxy | Open connections cut, new ones refused until it is re-enabled |
| F-stall | toxiproxy: `timeout` toxic | Bytes stop while the socket stays up, so only a missed heartbeat reveals it |
| F-reset | toxiproxy: `reset_peer` toxic | A TCP reset mid-stream |
| F-slow | toxiproxy: `latency` toxic on the broker-to-client direction | Replies arrive late, so a call can be caught waiting |

Toxiproxy runs at a pinned version (D-16). Stalls use the `timeout` toxic, because a zero-rate `bandwidth` toxic does not stall in toxiproxy 2.13.0 and later, as the .NET client records in `docs/internal/connection-shutdown-and-cancellation.md`, citing Shopify/toxiproxy#764. F-stall has been measured to keep the socket up on Linux only, so on Windows it is checked before it is relied on.

## Rules for every scenario

- Check the broker from outside the connection under test: the management API, polled until it settles, or a second connection with recovery off. A passive declare on a recovered channel cannot be the check, because if replay failed its 404 closes the channel under test.
- Assert that the state the scenario depends on was reached, for example that a confirmation was still outstanding when the fault landed. A scenario that can pass without reaching it tests nothing.
- Every scenario has a wall-clock bound and fails by timing out rather than hanging. Scenarios using F-stall set a heartbeat of a few seconds.
- Queues that must exist on 4.3 and later are durable or exclusive. Scenarios about exclusive consumers use classic queues, the only type that enforces them.
- Run each scenario repeatedly in CI, and on each broker line where it says the behaviour differs (O-11).

## Connection recovery

- SC-1 Consumers carry on. Consume from a queue, then inject each of F-close, F-restart, F-refuse, F-stall and F-reset in turn. At the first attempt after the broker is reachable again, the connection recovers, and messages published from another connection arrive at the original callback, once each, with no further application calls.
- SC-2 Calls during recovery fail fast. Hold the broker away with F-refuse. While `RECOVERING`, `basic_publish`, a declare, `basic_consume`, `basic_get`, `confirm_delivery` and `channel()` each raise the recovering error within 1 second, including a call made immediately after the drop is detected (O-10); a call already sent and waiting at that moment gets the close reason instead (SC-4). `basic_ack`, `basic_cancel` and `close()` do not raise, and registering a listener behaves as O-26 decides.
- SC-3 Lost confirmations are reported. Publish continuously in confirm mode to a durable queue and inject F-reset, asserting that at least one confirmation was outstanding and, with the O-10 hook, that one publish had not yet been written. Each publish without a confirmation, including one not yet written (O-12), gets an outcome-unknown notification and none is reported as a nack. Confirm frames received before the drop reach the callback before any of those notifications, which in turn arrive before any confirm from the recovered channel. With the confirm callback blocked, the channel reports `OPEN` while its outcome-unknown notifications have still not run. Sequence numbers after recovery start from 1. An application that republishes the unknown ones ends with every produced message in the queue, possibly duplicated, compared by draining it.
- SC-4 A waiting call gets the close reason. With F-slow holding a declare's reply, inject F-reset, which discards the delayed reply. The declare raises the close reason and is not sent again.
- SC-5 Topology is replayed. Across several channels, declare durable, exclusive and server-named queues, durable and auto-delete exchanges with arguments, queue bindings, an exchange-to-exchange binding, a binding on one channel to a queue declared on another, and consumers. Inject F-close. Afterwards every entity exists with its original arguments and flags, every binding routes and every consumer receives, and no state listener saw `OPEN` before the last of them was replayed. Repeat with a durable queue deleted from another connection while the client was away; it is restored.
- SC-6 Removed things stay removed. Before the fault: delete a queue from a channel other than the one that declared it, unbind a binding, cancel the last consumer of an auto-delete queue that has an auto-delete exchange bound to it, close the channel holding the last consumer of another auto-delete queue, make a passive declaration, and make a declaration the broker refuses. After recovery none of those comes back, and an entity declared alongside each, as a control, does.
- SC-7 Topology recovery can be narrowed. With topology recovery off, the connection recovers, nothing is redeclared and no consumer is re-subscribed. With a filter excluding one queue, that queue is not redeclared while its bindings and consumers are still offered to the filter on their own.
- SC-8 Off means off. With recovery disabled, F-close takes the connection and its channels `OPEN -> CLOSED` with the broker's reason, the constructor's `on_close_callback` fires once, and no reconnection is attempted.
- SC-9 Backoff is followed. Hold the broker away with F-refuse for several attempts. The connection waits before its first attempt, the intervals follow the default policy (D-2, O-13), and the policy sees each attempt's error, the first being the one that caused the drop.
- SC-10 Giving up is clean. A delay policy that gives up after two attempts takes the connection and its channels to `CLOSED` with the last attempt's error, listeners see `RECOVERING -> CLOSED`, and no call is left waiting.
- SC-11 Access errors on reconnect are retried. Set the vhost's connection limit to 0 and inject F-close: each attempt fails with 530, the policy sees it, and the connection recovers once the limit is lifted. Repeat with the user's password changed and then restored, which fails each attempt with 403. A policy that gives up on 530 takes the connection to `CLOSED`.
- SC-12 A drop during replay is another attempt. Inject F-reset when replay is part-way through, asserting that some entities had been redeclared and others not (O-10). Recovery continues as one episode: no second `OPEN -> RECOVERING`, the attempt number the policy sees carries on, and the connection ends `OPEN` with the full topology and nothing reported as skipped.
- SC-13 States are reported in order, once. With listeners on the connection and several channels, inject F-close: each reports `OPEN -> RECOVERING -> OPEN` exactly once, each transition carries a reason (O-14), and every channel's `OPEN` comes before the connection's. Closing reports `OPEN -> CLOSING -> CLOSED` for each.
- SC-14 Stale tags are dropped. Hold a delivery unacknowledged and inject F-close. After recovery, acknowledging it does not acknowledge any other message, the original message is redelivered, a tag delivered after recovery acknowledges normally, and every new tag is higher than every old one. A `multiple` acknowledgement up to an old tag is dropped, and one with tag 0 behaves as O-27 decides. The same holds for a `basic_get` delivery.
- SC-15 A resource still held by the old connection is retried. Make the client abandon the old connection while the broker still considers it alive (O-10). An exclusive queue's redeclare fails with 405 and an exclusive consumer's re-subscription on a classic queue fails with 403; each is retried through the policy, and both are restored once the broker releases them.
- SC-16 The first connect is not retried. A `Connection` whose first attempt is refused by every host raises from the constructor, and no reconnection follows.
- SC-17 A server-named queue keeps working. After recovery the queue has a new name, its bindings and consumers use it, and the rename notification gave the old and new names before the consumers were re-subscribed.
- SC-18 One failing entity does not stop replay. While the client is away, delete a recorded queue from another connection and redeclare it with different arguments. Replay skips it, replays everything after it, reaches `OPEN`, and reports that queue as skipped.
- SC-19 pika's own closes are final. Overflow a consumer's work queue so pika closes the connection. It goes to `CLOSED` and no recovery starts.
- SC-20 Transactions are restored. On a transactional channel, hold a commit's reply with F-slow and inject F-reset. The commit raises the close reason, and after recovery the channel is transactional again.
- SC-21 Blocked becomes unblocked. Trigger a resource alarm so the connection is blocked, and inject F-close while the alarm is still set and before any unblocked notification. Clear the alarm. On recovery the unblocked callback fires.

## Closing

- SC-22 Closing wins at every step. Call `close()` during the delay, during a connection attempt (O-10) and during replay. Each reaches `CLOSED` within the close timeout, the management API shows no connection from this client, and the client stays `CLOSED` when the step it interrupted completes.
- SC-23 A close is never lost (O-10). Call `close()` from a state listener running during recovery, from the delay policy, and from a consumer callback. Separately, inject F-reset during the application's own close handshake. In each case the connection ends `CLOSED`, `close()` returns within its timeout, and no recovery starts afterwards.
- SC-24 Removal during recovery is honoured. While `RECOVERING`, close one channel and cancel a consumer on another. After recovery neither the channel nor the consumer is replayed.

## Channel and consumer recovery

- SC-25 A broker-closed channel recovers on its own (O-10). On a channel with prefetch, confirm mode, and a consumer on an exclusive auto-delete queue with a binding, acknowledge a tag above any delivered so far; pika sends it, and the broker closes the channel with 406. The channel's first reopen is immediate. While it is `RECOVERING` a publish on it raises the recovering error. It reports `OPEN` only after its prefetch, confirm mode and consumer are restored and the queue and its binding exist again, and the connection and every other channel stay `OPEN` throughout.
- SC-26 A close that answers a call is not recovered. A passive declare of a missing queue raises the 404 to its caller, and the channel goes `OPEN -> CLOSED` with no `RECOVERING`.
- SC-27 Channel and consumer recovery are bounded. With the default policy, start channel recovery as in SC-25 on a channel whose consumer's classic queue has an exclusive consumer held by another connection, so each re-subscription is refused: after 5 attempts the consumer is given up through the cancel callback, and the channel ends `OPEN` without it. If the other connection releases the queue after 3 attempts, the consumer is restored, and a later refusal starts a new count of 5. A policy that gives up before a channel's first reopen takes the channel to `CLOSED` with the error that closed it. The connection and other channels are unaffected throughout.
- SC-28 A consumer timeout recovers wherever the broker allows it. Consume from a durable quorum queue declared with a short `x-consumer-timeout` and leave a delivery unacknowledged. On 4.1 and 4.2 the broker closes the channel and channel recovery restores the consumer. On 4.3.7 and later, with the default `consumer_timeout_response`, the broker cancels the consumer and it is re-subscribed. In both cases the message is delivered again.
- SC-29 A cancel without the capability is reported. Against 4.3.0 to 4.3.6, let a consumer time out as in SC-28; against 4.1 or 4.2, delete a consumed queue from another connection. The cancel callback fires and no re-subscription is attempted.
- SC-30 A cancel for a deleted queue gives the consumer up. Against 4.3.7 or later, with two consumers on one channel, delete one consumer's queue from another connection. Its re-subscription fails with 404 once, the cancel callback reports it as gone, the queue is not redeclared, and the channel goes `RECOVERING -> OPEN` with the other consumer restored.
- SC-31 Tags of a cancelled consumer are stale. Against 4.3.7 or later, let a consumer on a quorum queue time out as in SC-28 while holding a delivery, then acknowledge that delivery's tag after re-subscription. The acknowledgement is dropped, the channel never goes `RECOVERING`, and the redelivered message stays unacknowledged.
- SC-32 Tags are not reused. After recovery, `basic_consume` with the tag of a re-subscribed consumer raises before anything is sent, and the broker never sees the tag twice. A consumer the application registers with a recorded tag while that consumer awaits re-subscription is the one that remains.

## Generations, hangs and leaks

- SC-33 Old work stays in its generation (O-10). Fill a consumer's work queue so deliveries are queued, inject F-close, and let their callbacks acknowledge them after recovery: no acknowledgement reaches the new channel, which never goes `RECOVERING` again. A blocking call scheduled just before the drop raises the close reason rather than running on the new generation.
- SC-34 A drop is never lost (O-10). Inject F-reset immediately after the connection reports `OPEN` from a recovery. A second recovery starts and completes.
- SC-35 Nothing hangs. Every scenario above completes within its bound when run repeatedly, and `close()` called at random points during recovery always returns within its timeout.
- SC-36 Misbehaving application code does not stop the connection. A state listener that raises, a listener that blocks for longer than a recovery attempt, and a filter that raises leave recovery running and I/O working, with the failure logged. A delay policy that blocks delays the next attempt while I/O and `close()` keep working. A delay policy that calls `basic_publish` gets the recovering error. A delay policy that raises has the outcome O-5 decides.
- SC-37 Recovery does not leak. After 50 recoveries in a row, the process has the same number of threads and open file descriptors as before the first.

## Hosts, replay and abort

- SC-38 Recovery reaches another host. Give the connection two hosts through two toxiproxy proxies in front of one broker. While it is connected through one, disable that proxy: the connection recovers through the other. Across repeated recoveries, the order in which the hosts are tried varies.
- SC-39 A channel closed during replay is recovered within it. With the O-10 hook, have the broker close one channel while replay is part-way through. That channel's part of the replay is redone, and nothing reports `OPEN` until it is back.
- SC-40 A broker-generated consumer tag survives. A consumer registered without a tag, so the broker named it, is re-subscribed after F-close under the same tag, and the application's `basic_cancel` with that tag works afterwards.
- SC-41 `abort()` behaves as `close()`. Repeat SC-22 with `abort()`; the outcomes are the same.
