# Findings: what clients do when you publish during recovery

This document records what we measured about how existing AMQP 0.9.1 clients behave when an application publishes while the client is recovering a dropped connection. It is the evidence for D-11 and R-1 in `decisions.md`.

The runnable harnesses that produced these results are at https://github.com/lukebakken/amqp091-misc (a `java/` harness against the RabbitMQ Java client and a `go/` harness against amqp091-go). Both publish continuously while a TCP outage is injected with toxiproxy (disable the proxy in front of the broker, wait, re-enable), and both verify reliable delivery end-to-end by draining the queue and comparing to what was produced.

## The core result

A publish issued while the client is recovering does not behave "exactly as it would on a healthy connection" in either client. It fails fast. Nothing is buffered, queued, or transparently replayed.

| Behavior | RabbitMQ Java client | amqp091-go |
|---|---|---|
| Publish while the connection is down | fails fast with `AlreadyClosedException` | fails fast with `ErrClosed` (reply code 504, "channel/connection is not open") |
| Publish caught mid-write at the drop instant | raw `SocketException: Broken pipe` | raw `net.OpError: broken pipe` |
| Publisher confirms across recovery | sequence number resets, unconfirmed set discarded | pending `DeferredConfirmation`s nacked (`Wait()` returns false), delivery tags reset |
| Buffering or queueing of publishes during recovery | none | none |

## Points that matter for a recovery API

1. In amqp091-go a recovery-nack is indistinguishable from a real broker nack: `DeferredConfirmation.Wait()` returns false in both cases. The application cannot tell "the broker rejected this message" from "the connection dropped and the outcome is unknown" from that signal alone.

2. amqp091-go has a reconnect-handshake hazard a naive publisher can hit. During recovery the channel's internal closed flag is cleared before the `channel.open` handshake finishes, so a publish issued blindly in that window can interleave a frame with the handshake and trigger a protocol violation. The client documents this and expects the application to gate publishing on the channel being open (via its state-change notifications). This is the case where "a call while reconnecting runs as it would on a healthy connection" is not just untrue but actively unsafe. It is source-confirmed; the window is narrow and we did not reproduce it directly (the Java client has an analogous, also-unreproduced window where a publish can hit a not-yet-redeclared exchange after the channel reopens).

3. Reliable delivery is achievable but is at-least-once, and it requires the application (or a helper) to track confirmations, gate on connection state, and republish. Our harnesses do this and deliver every produced message across an 8s outage with zero loss. We also observed the duplicate case in practice: a message that reached the broker but whose confirmation was nacked by recovery gets republished, so consumers must be idempotent. There is no exactly-once here for free.

## Why this bears on the design

Treating recovery as "purely additive" - a call made while reconnecting running "exactly as it would on a healthy connection", with no code path reading recovery state - is the design these measurements rule out. In pika it would mean a publish during recovery landing as either a `TimeoutError` (work scheduled onto a stopped IOLoop) or a generic "channel is closed", with no synchronous way to ask "is this connection recovering?" and nothing an application could catch reactively.

The measured behavior of both reference clients argues the other way: they fail fast, and amqp091-go, whose docs that framing cited for "runs as on a healthy connection", actually requires the application to gate on connection state and can cause a protocol violation if it does not. That is why pika makes recovery an observable state and fails calls made during it with a catchable error (D-11).
