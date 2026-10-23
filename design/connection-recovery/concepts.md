# Concepts

The behaviour, described once. Reasons and the reference-client comparison for each choice are in `decisions.md`; the guarantees are in `invariants.md`.

## States

A `Connection` and each of its channels has its own state.

| State | Meaning to a caller |
| --- | --- |
| `OPENING` | Being opened for the first time. The constructor and `channel()` return only once open, so an application does not see this state. |
| `OPEN` | Usable. `is_open` is true only here. |
| `RECOVERING` | Lost and being restored. Calls fail fast (see "Calls during recovery"). |
| `CLOSING` | The application asked to close it. |
| `CLOSED` | Finished. `is_closed` is true only here, and later calls raise the recorded reason. |

Transitions:

- `OPENING -> OPEN`, or `OPENING -> CLOSED` if the first open fails.
- `OPEN -> RECOVERING` when recovery starts, `RECOVERING -> OPEN` when it succeeds, and `RECOVERING -> CLOSED` when the delay policy gives up or pika closes the new connection itself (D-3). A channel also goes `RECOVERING -> CLOSED` when its connection does.
- `OPEN -> CLOSED` when the connection is lost with recovery off or for a reason recovery does not cover, when a channel is closed in reply to a call, and when a channel's connection reaches `CLOSED`.
- `OPEN -> CLOSING -> CLOSED` and `RECOVERING -> CLOSING -> CLOSED` when the application closes it.

Closing is final (I-3).

When the connection is lost and recovery starts, every open channel goes `RECOVERING` with it. After replay, each channel reports `OPEN` and then the connection does, so a listener on the connection's `OPEN` finds its channels usable (D-7). A channel the broker closes with no call waiting recovers on its own while the connection stays `OPEN` (D-5).

Each connection and channel has state listeners. They receive every transition once, in order (I-9), on the connection's event worker rather than the I/O thread. The constructor's `on_close_callback` fires only on reaching `CLOSED`, never on entering `RECOVERING`, and `abort()` behaves as `close()`.

## Events and responses

| Event | Response |
| --- | --- |
| A socket error or a missed heartbeat | Connection recovery |
| The broker closes the connection, with any reply code | Connection recovery |
| pika closes the connection itself: a full consumer work queue, a protocol or frame error, or the I/O loop stopping on an unhandled exception | `CLOSED`, no recovery (D-3) |
| The application calls `close()` | `CLOSING -> CLOSED`, no recovery |
| The first connection attempt fails | The constructor raises (D-9) |
| The broker closes a channel in reply to a call the application is waiting on | The caller gets the error; the channel goes `CLOSED` (D-5) |
| The broker closes a channel with no call waiting, such as a consumer timeout or an acknowledgement of an unknown tag | Channel recovery (D-5) |
| The broker cancels a consumer and advertises `accept_consumer_cancel_ok` | The consumer is re-subscribed (D-6) |
| The broker cancels a consumer and does not advertise it | The cancel callback is told and the consumer is forgotten (D-6) |

A silent network failure is detected by heartbeats (D-21).

## What is recorded

The record holds:

- exchanges, queues, queue bindings and exchange-to-exchange bindings, with their arguments and flags, declared through any channel of the connection;
- consumers, with queue, tag, callback, arguments and exclusivity, per channel;
- each channel's settings: prefetch with both its per-consumer and channel-wide values, confirm mode, transaction mode (D-19), and the cancel, return and confirm callbacks registered on it;
- the connection's blocked and unblocked callbacks. A connection blocked when it dropped reports unblocked once recovered (D-20).

Only what the broker accepted is recorded, so a passive or refused declaration is never replayed (D-15, I-5).

What the application removes is forgotten (I-6): a delete, an unbind, a client-side cancel, or closing a channel, which forgets that channel's consumers and settings as if each consumer had been cancelled. Entities declared on a closed channel stay recorded, because they belong to the connection. An auto-delete queue is forgotten when the application cancels, or closes the channel of, the last consumer this connection has on it, together with its bindings and any auto-delete exchange left with no binding as its source. Consumers on other connections are invisible to pika, so the broker may keep a queue this rule forgets.

A loss is not a removal. When the connection is lost, the broker deletes its exclusive queues and any auto-delete queue whose consumers went with it. When a channel is lost, the broker deletes any auto-delete queue whose last consumer was on that channel. The record keeps all of them, so recovery recreates them.

## Replay

Connection recovery reopens each channel with its original number, restores its settings, and then replays the record across all channels so that nothing is declared before what it depends on. With topology recovery off, only the channels and their settings are restored, and consumers are not re-subscribed (D-8).

- A server-named queue is redeclared and gets a new name. The record carries the new name into the bindings and consumers that used the old one, and the application is told both names before those consumers are re-subscribed (D-26).
- A consumer is re-subscribed with its recorded tag, including one the broker generated, and the record follows a different tag if the broker returns one (D-17).
- A step refused because the old connection still holds a resource, 405 for an exclusive queue or 403 for an exclusive consumer on a classic queue, is retried through the delay policy (D-24). What the policy is told about such a step, and what follows when it gives up, is open (O-18).
- A step that fails for any other reason, such as 406 because a queue's arguments were changed outside pika, is skipped, and replay continues with the next entity. The skipped entities are reported with `RECOVERING -> OPEN` (D-8). Because the refusal closes the channel, what is redone on it, and whether the skipped entity stays recorded, are open (O-19).
- A channel the broker closes during replay for a reason other than a refused step, such as a consumer timeout, is reopened and its part of the replay redone before anything reports `OPEN` (D-23).

Channel recovery reopens one channel, restores its settings and consumers, and redeclares what depended on that channel: the auto-delete queues its consumers held, with their bindings and any auto-delete exchange that went with them (I-10). A consumer refused during channel recovery is handled as below. With topology recovery off it restores only the channel's settings; how topology recovery off applies to broker cancels is open (O-20).

## How recovery runs

Each connection to the broker is a generation, and so is each reopen of a channel. Work and waits belong to the generation current when they were scheduled, and never act on a later one (I-4).

One recovery driver per connection runs every connection, channel and consumer recovery, one at a time (D-10). A connection recovery takes over any channel or consumer recovery in progress, since those channels go `RECOVERING` with the connection. How the driver orders several channel and consumer recoveries, and their delays, is open (O-17).

- Connection recovery begins once the old connection has stopped. Each attempt tries the connection's hosts in a newly shuffled order (D-25) and, once connected, reopens the channels and replays the record before reporting `OPEN`.
- The policy is asked before every attempt. The connection waits the delay it returns, including before the first attempt; a channel's first reopen ignores the delay but honours giving up (D-2).
- Recovery's own calls are allowed while `RECOVERING`; a call from application code is not, whichever thread makes it.
- A drop before the recovered connection reports `OPEN` is another attempt of the same recovery, and its count carries on (I-15).
- A call made after the drop sees `RECOVERING`, never a wait on the old connection.
- A requested close always wins over recovery, including a drop during the application's own close handshake (I-3).
- Application code, whether listeners, the delay policy or the filter, runs as I-2 requires.

Whether deliveries queued for a consumer's callback when the connection drops are still dispatched is open (O-24); acknowledgements they make refer to the old generation and are dropped (I-13).

## Calls during recovery

While a connection or channel is `RECOVERING`, a call that uses it raises the recovering error at once (I-8). That covers publishing, declaring, binding, `basic_consume`, `basic_get`, prefetch, confirm and transaction calls, and `channel()`. These are accepted in any state:

- acknowledgements, nacks and rejects, which are dropped, because the broker has already requeued what they refer to;
- `basic_cancel`, which removes the consumer from the record so it is not re-subscribed;
- `close()` and `abort()`;
- registering callbacks and listeners (O-26).

A call already waiting for the broker when the connection drops raises the close reason and is not retried (D-14). `basic_consume` with a tag already active on the channel raises before anything is sent (D-18).

## Closing during recovery

`close()` called while `RECOVERING` stops recovery at whatever step it has reached: it ends the delay early, abandons a connection attempt and closes the connection if it opens, and stops replay. The connection and its channels go `CLOSING -> CLOSED`. Acknowledgements made by deliveries dispatched during that close cannot reach the broker and are dropped (O-24). `close()` waits for recovery to stop as I-1 bounds it.

Closing a channel while it is `RECOVERING` abandons its reopen, closes it at the broker if recovery had already reopened it, and forgets its consumers and settings.

## Delivery tags

Each channel keeps one increasing sequence of delivery tags across recoveries, for consumer deliveries and `basic_get` alike (D-13). A tag from an earlier generation of the channel is stale, and acknowledging it is a no-op, never an acknowledgement of whatever delivery now has that number (I-13). A single acknowledgement of a tag from a consumer the broker cancelled while the channel stayed open is dropped too, because the broker has requeued those deliveries, or deleted them with their queue. A `multiple` acknowledgement up to a current tag acknowledges the current generation's deliveries up to it; one up to a stale tag is dropped; what one with tag 0 does is open (O-27). How a `multiple` acknowledgement that reaches past a cancelled consumer's tags is handled is open (O-23).

## Publisher confirms

When a channel in confirm mode is lost, every publish still awaiting a confirmation is reported through a separate outcome-unknown notification that names its sequence numbers (D-4, O-21). The broker may or may not have the message. A publish accepted by `basic_publish` but not yet written when the channel was lost is reported as outcome unknown too (D-28, O-12). The confirm callback keeps receiving the broker's `Basic.Ack` and `Basic.Nack` frames.

Their ordering against confirms and against `OPEN` is I-7, and `OPEN` does not wait for them to run (D-4). The recovered channel's sequence numbers start from 1, as the broker's do, so a sequence number is not unique across recovery: an application that tracks pending publishes clears them on the outcome-unknown notification or on the channel's `RECOVERING` transition.

Publishes on a channel without confirm mode carry no outcome (D-22).

## Policies an application controls

- Recovery on or off, on by default (D-1). Off means a lost connection goes to `CLOSED` (I-14).
- Topology recovery on or off, and a filter consulted for each exchange, queue, binding and consumer independently (D-8).
- The delay policy (D-2). Before each attempt it is told the attempt number, the last error, and which connection, channel or consumer is recovering; for the first attempt, the last error is the one that caused the drop. It returns a delay or gives up. The default waits 1 second, doubling to 30, with jitter (O-13); it retries a connection forever and gives up on a channel or consumer after 5 attempts. The count starts again after each successful recovery (D-2); a failure that recurs right after each reopen is open (O-22). Giving up takes a connection or channel to `CLOSED` with the last error, and reports a consumer through the cancel callback.

## Consumers cancelled by the broker

When the broker cancels a consumer and advertises `accept_consumer_cancel_ok`, pika replies with `basic.cancel-ok` and then re-subscribes the consumer on the same channel (D-6). A refused re-subscription closes the channel, which goes through channel recovery and reports `OPEN` without that consumer:

- a 404 means the queue is gone, so the consumer is given up at once and reported through the cancel callback, and the queue is not redeclared;
- any other refusal counts as one of the consumer's attempts, and the consumer is re-subscribed again later on its own count, until the policy gives up on it.

What a given-up consumer, and a queue that got a 404, leave in the record is open (O-25).

## Broker versions

Behaviour that differs by version:

- 4.1 and 4.2 time out a consumer by closing its channel with 406, which channel recovery handles. The timeout applies to classic and quorum queues and is set through the `x-consumer-timeout` queue argument or a policy.
- 4.3 and later time out consumers on quorum queues only. They put the unacknowledged messages back in the queue and cancel the consumer; whether it is re-subscribed depends on the capability (D-6). A broker configured with `consumer_timeout_response = close_channel`, available from 4.3.7, closes the channel instead, as 4.1 and 4.2 do.
- 4.3 and later refuse a queue that is neither durable nor exclusive, auto-delete queues included.
