[GitHub Milestone](https://github.com/pika/pika/milestone/28?closed=1)

## Upgrading to 1.5.1

Three changes are worth reading before you upgrade.

### `pika.Connection` publishers now wait for the socket

In 1.5.0, `basic_publish` on a `pika.Connection` channel queued without limit, so a publisher could run arbitrarily far ahead of the socket, and a `close()` whose timeout expired silently dropped the publishes still queued. In 1.5.1, `basic_publish` waits while 256 KiB of accepted publishes are still unsent, as the RabbitMQ Java and .NET clients make a publisher wait, so a `close()` has at most that much left to send (#1859).

A publisher that outruns the network or the broker now blocks in `basic_publish` rather than growing the backlog. The wait has no time limit: during a broker resource alarm it lasts until the alarm clears, unless `blocked_connection_timeout` is set. Publishing from a consumer or confirm callback still works. Once `close()` has started, `basic_publish` raises `ConnectionWrongStateError`, and publishes a close still drops are logged in a WARNING saying how many.

### Connection parameters that carry TLS options can be deep-copied

`copy.deepcopy` of a parameters object holding `SSLOptions`, including `URLParameters('amqps://...')`, raised `TypeError: cannot pickle 'SSLContext' object`, because `ssl.SSLContext` cannot be copied. Now `copy.deepcopy` of an `SSLOptions` returns the same instance, so parameters with TLS options copy cleanly and the copy shares the original's `SSLOptions` (#1845).

### A closed channel's number can be reused from its close callback

On an asynchronous connection, `connection.channel(channel_number=n)` called from channel `n`'s on-close callback, including `TwistedChannel.on_closed` and an errback of a pending call the close failed, raised `ValueError` in 1.5.0, so the reopen had to be scheduled for later. It now opens the new channel while the connection remains open. The number of a channel that is still opening, open or closing still raises `ValueError` (#1841).

The same notes are in the changelog, which also carries the full version history: https://pika.github.io/pika/latest/changelog/
