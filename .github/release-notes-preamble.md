[GitHub Milestone](https://github.com/pika/pika/milestone/28?closed=1)

## Upgrading to 1.5.1

Five changes are worth reading before you upgrade.

### `pika.Connection` publishers now wait for the socket

In 1.5.0, `basic_publish` on a `pika.Connection` channel queued without limit, so a publisher could run arbitrarily far ahead of the socket, and a `close()` whose timeout expired silently dropped the publishes still queued. In 1.5.1, `basic_publish` waits while 256 KiB of accepted publishes have not yet reached the transport, or while the transport is not taking more, as the RabbitMQ Java and .NET clients make a publisher wait, so a `close()` has much less left to send (#1859).

A publisher that outruns the network or the broker now blocks in `basic_publish` rather than growing the backlog. The wait has no time limit: during a broker resource alarm it lasts until the alarm clears, unless `blocked_connection_timeout` is set. Publishing from a consumer or confirm callback still works. While `close()` is in progress, `basic_publish` can raise `ConnectionWrongStateError`; the `Connection.close` documentation says when. After the close, it raises the connection's close reason, as other methods do. Publishes a close still drops are logged in a WARNING.

### A frame larger than the negotiated `frame_max` closes the connection

In 1.5.0, pika accepted an inbound frame up to `max_inbound_message_body_size` (64 MiB by default) whatever `frame_max` the connection negotiated. In 1.5.1, a method, body or heartbeat frame larger than the negotiated `frame_max`, or larger than `FRAME_MIN_SIZE` (8 KiB) before `Connection.Tune` arrives, closes the connection with `FrameTooLargeError`; the RabbitMQ Java and Go clients also follow the negotiated `frame_max`. A content header frame, which carries a message's properties, keeps the 1.5.0 limit, because RabbitMQ sends one as large as the properties whatever its `frame_max`.

### Received headers may nest at most 32 levels deep

In 1.5.0, pika decoded received field tables and arrays to any depth: message headers nested a few hundred levels deep raised `RecursionError` and closed the connection, and anything shallower was delivered. In 1.5.1, a table or array nested more than 32 levels deep is refused, the limit the RabbitMQ Java client uses. A delivery to an active consumer whose headers exceed it is logged and, unless its consumer uses `auto_ack`, rejected without requeueing, so it goes to the queue's dead-letter exchange if one is configured and is dropped otherwise; the connection stays open. A `basic_get` reply or returned message nested that deeply closes the connection, as a malformed frame does.

### Connection parameters that carry TLS options can be deep-copied

`copy.deepcopy` of a parameters object holding `SSLOptions`, including `URLParameters('amqps://...')`, raised `TypeError: cannot pickle 'SSLContext' object`, because `ssl.SSLContext` cannot be copied. Now `copy.deepcopy` of an `SSLOptions` returns the same instance, so parameters with TLS options copy cleanly and the copy shares the original's `SSLOptions` (#1845).

### A closed channel's number can be reused from its close callback

On an asynchronous connection, `connection.channel(channel_number=n)` called from channel `n`'s on-close callback, including `TwistedChannel.on_closed` and an errback of a pending call the close failed, raised `ValueError` in 1.5.0, so the reopen had to be scheduled for later. It now opens the new channel while the connection remains open. The number of a channel that is still opening, open or closing still raises `ValueError` (#1841).

The same notes are in the changelog, which also carries the full version history: https://pika.github.io/pika/latest/changelog/
