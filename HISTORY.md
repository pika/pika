# Changelog

Notes for each release are also published on the [GitHub releases page](https://github.com/pika/pika/releases), generated from the pull requests and issues the release contains.

## Upgrading to 1.5.1

One change is worth reading before you upgrade.

### `pika.Connection` publishers now wait for the socket

In 1.5.0, `basic_publish` on a `pika.Connection` channel queued without limit, so a publisher could run arbitrarily far ahead of the socket, and a `close()` whose timeout expired silently dropped the publishes still queued. In 1.5.1, `basic_publish` waits while 64 KiB of accepted publishes are still unsent, as the RabbitMQ Java and .NET clients make a publisher wait, so a `close()` has at most that much left to send (#1859).

A publisher that outruns the network or the broker now blocks in `basic_publish` rather than growing the backlog. The wait has no time limit: during a broker resource alarm it lasts until the alarm clears, unless `blocked_connection_timeout` is set. Publishing from a consumer or confirm callback still works. Once `close()` has started, `basic_publish` raises `ConnectionWrongStateError`, and publishes a close still drops are logged in a WARNING saying how many.

## Upgrading to 1.5.0

Twelve changes are worth reading before you upgrade. One adds `pika.Connection`, three can turn a passing build red, two change what a working application observes when it cancels a consumer or closes a channel, one lets RabbitMQ close a channel sooner after a consumer timeout, one refuses messages larger than 64 MiB, one changes what the Twisted adapter reports when a connection closes, one closes the connection when a consumer or reply callback raises `BlockingIOError`, one changes the Python types that field-table numbers decode to, and one makes URL `ssl_options` verify the broker.

### `pika.Connection` is new, and is the connection to use

1.5.0 introduces `pika.Connection` and its `Channel`, a connection that runs its own IOLoop on a background thread and offers a thread-safe blocking API that does not stall heartbeats while your code processes a message. There is nothing to migrate *from*: these classes are new in this release, and `pika.Connection` is the documented import.

`from pika.adapters.thread_safe_connection import Connection` also works, though the module moves in 2.0 (see #1681). `Channel` is not exported from `pika`, because channels come from `Connection.channel()`.

If you are starting new code, use `pika.Connection`. If you have existing code on one of the adapters below, it keeps working in 1.5.0 and the deprecation notice tells you where it is going.

### The other adapters now emit a `DeprecationWarning`, which may fail your tests

`BlockingConnection`, `TornadoConnection`, `TwistedProtocolConnection` and `GeventConnection` each warn from their constructor that they will be removed in Pika 2.0. They all still work.

The upgrade risk is that **a `DeprecationWarning` raised from a constructor is a hard error for anyone running with warnings-as-errors**, which is a common test configuration. It fires at construction, before any I/O, so it does not depend on reaching a broker:

```
E       DeprecationWarning: BlockingConnection is deprecated and will be removed in Pika 2.0. ...
pika/adapters/blocking_connection.py:367: DeprecationWarning
```

The same happens under `python -W error` and `warnings.simplefilter('error')`. `BlockingConnection` is pika's most widely used entry point, so this is both likely and likely to be misread as pika being broken rather than as an intentional notice. Outside a test runner it is usually invisible, because Python's default filters hide `DeprecationWarning` unless it is triggered from `__main__`; pytest is what turns it back on.

Either filter it or migrate. The warning uses `stacklevel=2`, so it is reported against the line that constructs the connection rather than against pika, and a filter scoped to the pika module does not match. Match on the message instead:

```toml
# pyproject.toml, for pytest
filterwarnings = [
    "ignore:.*will be removed in Pika 2\\.0:DeprecationWarning",
]
```

```bash
python -W 'ignore:BlockingConnection is deprecated' your_app.py
```

The two forms are not interchangeable. In `filterwarnings` the message is a regular expression, while `-W` on the command line, for `python` and for `pytest` alike, matches it as a literal prefix, so the `pyproject.toml` pattern given as a `-W` argument matches nothing and the warning still fails the run.

### Cancelling a consumer no longer rejects the messages still arriving for it

A `Basic.Cancel` does not stop the broker immediately: the specification allows it to keep dispatching until it sends `Basic.CancelOk`, and pika used to reject every delivery that arrived in that window, or discard it outright for an `auto_ack` consumer. Discarding loses the message, because an `auto_ack` delivery has no copy left at the broker, and rejecting is a decision pika was making on your behalf that no other client makes. Measured on RabbitMQ 4.3.6, closing a channel with an `auto_ack` consumer half a second after its first delivery, 400 messages, `work_queue_maxsize=50` and a 50 ms callback. At 8 KiB bodies with `close(timeout=10)`: 339 of 400 lost before this change, 179 to 189 across five runs after it. `close(timeout=None)` loses none after the change, at either body size, and lost the same 339 before it, because that loss was the cancel window rather than the budget. At 64 bytes the deliveries still queued when a bounded close returns fit in the work queue, so the worker finishes them and nothing is lost; at 8 KiB they do not fit and the excess is discarded when the pool shuts. Quote the whole configuration with any figure from this area, because the message count, body size, queue size, callback cost and timeout all move it. `utils/measure_close_loss.py` in the repository reproduces each of them.

Those deliveries are now passed to your callback instead, which is what the Java client does, as long as the channel is still open.

One thing follows for existing code: a consumer callback can now run for a message that arrives after you called `basic_cancel`, so a callback that assumed cancellation was immediate may see further deliveries. `BlockingChannel.basic_cancel` still rejects them for an `auto_ack=False` consumer, so they return to the queue at once as before, and its return value is unchanged; the channel no longer does that rejecting, the wrapper does. The generator-style `BlockingChannel.cancel` discards them instead, which #1767 covers.

### `channel.close()` on an asynchronous adapter still discards those deliveries

A channel that is closing discards them, because a callback could not acknowledge one anyway and the attempt would abort the connection; the Java client discards them at the same point. `Channel.close` cancels each consumer without waiting for the broker to confirm, so this is what a bare `channel.close()` does on `SelectConnection`, `AsyncioConnection`, `TornadoConnection` and `GeventConnection`. A discarded delivery is redelivered to the next consumer with `auto_ack=False` and lost with `auto_ack=True`. Pika now logs a warning naming the count as the channel closes.

`pika.Connection` and `BlockingConnection` are unaffected and keep every delivery across a close, since both cancel their consumers and wait for the broker to confirm before closing anything. To get the same guarantee from an asynchronous adapter, cancel each consumer with a callback and close the channel from the last `Basic.CancelOk`; the [channel documentation](https://pika.github.io/pika/latest/modules/channel/#closing-a-channel-with-active-consumers) has the pattern. Pika 2.0 removes these adapters, which leaves `Connection` and its `Channel` as the only close path and so retires the gap rather than fixing it.

### A consumer RabbitMQ cancels for a timeout is released at once, so its channel can close sooner

On RabbitMQ 4.3 and later, a consumer on a quorum queue whose delivery exceeds the [consumer timeout](https://www.rabbitmq.com/docs/consumers#acknowledgement-timeout) is cancelled with `Basic.Cancel` instead of having its channel closed. pika now confirms that cancellation with `Basic.CancelOk`, as the Java, .NET and Go clients do, so the broker releases the consumer at once instead of after its own 10 second wait.

If the consumer still holds another unacknowledged delivery, the broker closes the channel with 406 `PRECONDITION_FAILED` when that delivery times out as well. With 1.4.x that happened only when the second delivery timed out more than 10 seconds after the first; it now happens whenever it times out after the cancellation. Code that handles the cancellation should also expect the channel to close, or acknowledge within the timeout, raise `consumer-timeout`, or lower the prefetch. The broker side is discussed in [rabbitmq/rabbitmq-server#18207](https://github.com/rabbitmq/rabbitmq-server/discussions/18207).

### Messages larger than 64 MiB now close the connection

pika now refuses an inbound message whose body is larger than `max_inbound_message_body_size`, 64 MiB by default, and a frame larger than that limit or 128 KiB, whichever is greater. Either closes the connection with `MessageBodyTooLargeError` or `FrameTooLargeError`, both subclasses of `AMQPConnectionError`. The Java and .NET clients use the same 64 MiB default. RabbitMQ refuses messages above its own `max_message_size`, 16 MiB by default, so the limit is reached only when the broker is configured to allow larger messages.

If you consume larger messages, raise the limit with `ConnectionParameters(max_inbound_message_body_size=...)` or `?max_inbound_message_body_size=` in a URL, up to 512 MiB, RabbitMQ's own maximum. A refused message delivered without `auto_ack` is requeued, so a consumer that reconnects is closed again: raise the limit, or dead-letter or remove the message on the broker. With `auto_ack` the broker has already discarded it. Returned messages and `basic_get` replies are checked too, so a publisher can be closed by its own oversized mandatory message coming back.

### `TwistedProtocolConnection` reports why it closed, not `ConnectionDone`

When pika closes a Twisted connection, the reason now reaches the `closed` deferred, the `on_close_callback`, and pending channel and queue deferreds, as on the other adapters, instead of `twisted.internet.error.ConnectionDone`: `ConnectionClosedByClient` after `close()`, which is also what `yield connection.close()` returns; `ConnectionClosedByBroker` when the broker closes it; `AMQPHeartbeatTimeout`; `ConnectionBlockedTimeout`; and the size errors above. Code that traps or tests for `ConnectionDone` must handle these instead. A peer that drops the connection without pika closing it still reports `ConnectionDone`.

### A `BlockingIOError` from a consumer or reply callback now closes the connection

On a connection without TLS, `SelectConnection`, `AsyncioConnection`, `TornadoConnection` and `GeventConnection` used to treat a `BlockingIOError`, or any `OSError` with a would-block errno, raised by a callback pika runs for a received frame (a consumer, RPC-reply or close callback) as the socket having no more data: pika logged it at ERROR with a traceback, kept the connection open, and left the frames after it unprocessed until the next read. It now closes the connection with `StreamLostError`, as any other exception from those callbacks does. A callback that does its own non-blocking I/O should catch the error itself.

### `pika.compat` is gone

The module is removed. It held Python 2 compatibility shims and platform checks, was never documented as public API, and nothing in pika imports it any more. It had no leading underscore, though, so it has been importable for years and an upgrade turns `import pika.compat` into `ModuleNotFoundError`.

Twelve of its names moved to `pika._utils`: `ON_LINUX`, `ON_OSX`, `ON_WINDOWS`, `LINUX_VERSION`, `SOL_TCP`, `HAVE_SIGNAL`, `RE_NUM`, `time_now()`, `as_bytes()`, `to_digit()`, `get_linux_version()` and `nonblocking_socketpair()`. That module is private, so it is not a migration target; copy what you need instead. Four that are gone were Python 2 artefacts, and the standard library covers each: `byte()` (`bytes((n,))`), `SOCKET_ERROR` (`OSError`), `AbstractBase` (`abc.ABC`), `str_or_bytes` (`(str, bytes)`). `pika.spec.str`, which was `str = bytes`, goes with them. `long` is gone too; in a field table, `pika.data.LongLong` replaces it, as the next section describes.

### 64-bit field-table integers keep their type, and floats decode as `float`

An `l` or `L` field-table value now decodes as `pika.data.LongLong`, an `int` subclass that is always sent as `l`, so a header passed on keeps its 64-bit type, as it did with 1.4.x's `long`. Wrap an `int` in `LongLong` to send it as `l`, which is what a Java consumer reads as `Long`; a plain `int` is still sent as `I` when it fits a signed 32 bits. An `f` or `d` value now decodes as a `float`, where 1.4.x truncated it to an integer.

### URL `ssl_options` now always verify the broker

A URL whose `ssl_options` set `protocol` to anything other than `PROTOCOL_TLS_CLIENT` used to build a context that accepted any certificate for any host, because `ssl.SSLContext` starts at `CERT_NONE` for those protocols. Every URL `ssl_options` context now requires a certificate that chains to the named CA, or to the system CAs when none is named, and checks the host name, as `ssl.create_default_context()` does. A connection that only worked because nothing was checked now fails at the TLS handshake. No URL key turns verification off; to connect without it, build an `ssl.SSLContext` yourself and pass it in `pika.SSLOptions`.

### `pika.spec` is now annotated, which may fail your type checker

Pika ships a `py.typed` marker, so a type checker reads pika's own annotations. `pika.spec` is generated code and was previously excluded from pika's `mypy` configuration, which left every field of a decoded frame typed as `Any`. `Any` silences all checking, so a downstream `mypy` run that was clean can go red on upgrade without any change on your end. The most common line in every pika tutorial is affected:

```
error: Argument "delivery_tag" to "basic_ack" of "BlockingChannel" has incompatible type "int | None"; expected "int"  [arg-type]
error: Argument "routing_key" to "basic_publish" of "BlockingChannel" has incompatible type "str | bytes | None"; expected "str"  [arg-type]
```

Two properties of the generated code account for it. Every generated constructor defaults its arguments to `None`, so fields are optional even though a frame off the wire always carries them. And AMQP `shortstr` has no declared encoding, so pika falls back to raw `bytes` on invalid UTF-8, making `routing_key`, `exchange` and `consumer_tag` all `str | bytes | None`.

The errors are accurate and the annotations are an improvement, so nothing here should be reverted. The FAQ covers the narrowing idioms: https://pika.github.io/pika/latest/faq/

`pyright` users see little change, since it reported most of these already.

## [1.5.0](https://github.com/pika/pika/tree/1.5.0) (2026-11-17)

[Full Changelog](https://github.com/pika/pika/compare/1.4.0...1.5.0)

**Implemented enhancements:**

- Raise a `TypeError` naming a wrong-typed body or short string [\#1838](https://github.com/pika/pika/pull/1838) ([lukebakken](https://github.com/lukebakken))
- Passing the wrong type to `basic_publish` raises a confusing error [\#1836](https://github.com/pika/pika/issues/1836)
- Run every example in CI against a real broker [\#1794](https://github.com/pika/pika/pull/1794) ([alonfaraj](https://github.com/alonfaraj))
- Make the shutdown drain budget settable per connection [\#1762](https://github.com/pika/pika/issues/1762)
- Gate the no-hard-wrap rule for `.md` files [\#1757](https://github.com/pika/pika/issues/1757)
- Add Python 3.15 official support [\#1741](https://github.com/pika/pika/pull/1741) ([alonfaraj](https://github.com/alonfaraj))
- Adopt writing guidance for agents, and trim AGENTS.md [\#1739](https://github.com/pika/pika/pull/1739) ([lukebakken](https://github.com/lukebakken))
- Run the examples in CI, so they cannot rot silently again [\#1735](https://github.com/pika/pika/issues/1735)
- Build the changelog entry from the milestone, not by crawling history [\#1732](https://github.com/pika/pika/pull/1732) ([lukebakken](https://github.com/lukebakken))
- Replace `github_changelog_generator` with a milestone-driven generator [\#1731](https://github.com/pika/pika/issues/1731)
- Trigger the release from a pushed tag, not a push to `main` [\#1729](https://github.com/pika/pika/pull/1729) ([lukebakken](https://github.com/lukebakken))
- Trigger the release from a pushed tag, not a push to main [\#1728](https://github.com/pika/pika/issues/1728)
- Reply with `Basic.CancelOk` to brokers that accept it [\#1722](https://github.com/pika/pika/pull/1722) ([suchitd](https://github.com/suchitd))
- Redirect the retired ReadTheDocs site to GitHub Pages [\#1720](https://github.com/pika/pika/pull/1720) ([lukebakken](https://github.com/lukebakken))
- Check `pika/spec.py` against `codegen.py` output in CI [\#1695](https://github.com/pika/pika/pull/1695) ([alonfaraj](https://github.com/alonfaraj))
- suggestion: Enforce spec regen in CI [\#1677](https://github.com/pika/pika/issues/1677)
- Build the release dist once and add a TestPyPI test mode [\#1656](https://github.com/pika/pika/pull/1656) ([lukebakken](https://github.com/lukebakken))
- Bound work queues in `ThreadSafe*` adapters. [\#1651](https://github.com/pika/pika/pull/1651) ([jholaj](https://github.com/jholaj))
- Add ThreadSafe examples for publisher confirms and long-running publisher [\#1637](https://github.com/pika/pika/pull/1637) ([alonfaraj](https://github.com/alonfaraj))
- Include hostname and port in connection error reporting [\#1635](https://github.com/pika/pika/pull/1635) ([lukebakken](https://github.com/lukebakken))
- Include py.typed marker in distribution [\#1632](https://github.com/pika/pika/pull/1632) ([alonfaraj](https://github.com/alonfaraj))
- Include `py.typed` file in distribution [\#1631](https://github.com/pika/pika/issues/1631)
- `Channel.confirm_delivery`: append ack/nack callback instead replace [\#1630](https://github.com/pika/pika/pull/1630) ([alonfaraj](https://github.com/alonfaraj))
- feat: Callback removal API [\#1628](https://github.com/pika/pika/pull/1628) ([alonfaraj](https://github.com/alonfaraj))
- Rename ThreadSafeConnection to Connection and ThreadSafeChannel to Channel [\#1617](https://github.com/pika/pika/issues/1617)
- Deprecate Tornado, Twisted, Blocking, and Gevent connection adapters [\#1615](https://github.com/pika/pika/pull/1615) ([alonfaraj](https://github.com/alonfaraj))
- Deprecate BlockingConnection, TornadoConnection, TwistedProtocolConnection, and GeventConnection in 1.5.0 [\#1608](https://github.com/pika/pika/issues/1608)
- Add ThreadSafe* examples for publisher confirms and long-running publisher [\#1607](https://github.com/pika/pika/issues/1607)
- Add TLS test runs to Linux and macOS CI [\#1605](https://github.com/pika/pika/issues/1605)
- `Channel.confirm_delivery` appends callbacks instead of replacing on repeated calls [\#1602](https://github.com/pika/pika/issues/1602)
- Add public callback removal API to `Channel` and `Connection` [\#1601](https://github.com/pika/pika/issues/1601)
- Bound work queues in `ThreadSafe*` adapters [\#1600](https://github.com/pika/pika/issues/1600)
- Add ThreadSafeConnection: fix \_tx\_buffers race and thread-safety bugs [\#1582](https://github.com/pika/pika/pull/1582) ([Dheeraj4103](https://github.com/Dheeraj4103))
- Update example code to work with RabbitMQ 4.3 [\#1580](https://github.com/pika/pika/issues/1580)
- Remove pylint suppression comments and .pylintrc [\#1570](https://github.com/pika/pika/issues/1570)
- Review and update GitHub issue labels [\#1567](https://github.com/pika/pika/issues/1567)
- Remove all type: ignore comments from the codebase [\#1565](https://github.com/pika/pika/issues/1565)
- Remove Python 2 compatibility code and modernize for Python 3 [\#1559](https://github.com/pika/pika/issues/1559)

**Fixed bugs:**

- Copy connection parameters without mutating the caller's object [\#1844](https://github.com/pika/pika/pull/1844) ([anish-yen](https://github.com/anish-yen))
- Connections built concurrently from one parameters object can leave its `ssl_options` as `None` [\#1842](https://github.com/pika/pika/issues/1842)
- Connection.channel\(\) accepts a channel\_number that is in use or out of range [\#1839](https://github.com/pika/pika/issues/1839)
- Reject non-finite floats when encoding field tables [\#1835](https://github.com/pika/pika/pull/1835) ([alonfaraj](https://github.com/alonfaraj))
- A NaN or inf float in a header makes RabbitMQ close the whole connection [\#1834](https://github.com/pika/pika/issues/1834)
- Log each transport failure once, at the layer that aborts [\#1833](https://github.com/pika/pika/pull/1833) ([lukebakken](https://github.com/lukebakken))
- Verify the broker for every URL `ssl_options` protocol [\#1832](https://github.com/pika/pika/pull/1832) ([lukebakken](https://github.com/lukebakken))
- Raise encoding errors from `pika.Connection` publishes at the call site [\#1831](https://github.com/pika/pika/pull/1831) ([lukebakken](https://github.com/lukebakken))
- URL `ssl_options` with a CA and a non-client `protocol` does not verify the broker [\#1828](https://github.com/pika/pika/issues/1828)
- `pika.Connection.basic_publish` silently drops a message whose properties cannot be encoded [\#1827](https://github.com/pika/pika/issues/1827)
- Build the IOLoop interrupt pair without a listener on POSIX [\#1826](https://github.com/pika/pika/pull/1826) ([lukebakken](https://github.com/lukebakken))
- Load the default CAs when URL `ssl_options` names none [\#1825](https://github.com/pika/pika/pull/1825) ([lukebakken](https://github.com/lukebakken))
- Abort the connection when a callback raises `BlockingIOError` [\#1824](https://github.com/pika/pika/pull/1824) ([lukebakken](https://github.com/lukebakken))
- Add `pika.data.LongLong` to send integers as 64-bit `l` values [\#1823](https://github.com/pika/pika/pull/1823) ([lukebakken](https://github.com/lukebakken))
- Transport failures log the same traceback two or more times [\#1822](https://github.com/pika/pika/issues/1822)
- `pika.data.LongLong`: restore 64-bit `l` field-table integers lost in 1.5.0 [\#1821](https://github.com/pika/pika/issues/1821)
- `nonblocking_socketpair` accepts any peer on its loopback listener [\#1820](https://github.com/pika/pika/issues/1820)
- A `BlockingIOError` raised from a callback is handled as a would-block, and the connection stays open [\#1819](https://github.com/pika/pika/issues/1819)
- `URLParameters` raises `TypeError` when `ssl_options` has no CA, including the documented example [\#1818](https://github.com/pika/pika/issues/1818)
- Start a final release's GitHub notes at the previous final release [\#1809](https://github.com/pika/pika/pull/1809) ([lukebakken](https://github.com/lukebakken))
- Retitle the 1.4.3 changelog entry as 1.4.4 [\#1808](https://github.com/pika/pika/pull/1808) ([lukebakken](https://github.com/lukebakken))
- Ask the broker to cancel a gone or cancelling tag [\#1805](https://github.com/pika/pika/pull/1805) ([lukebakken](https://github.com/lukebakken))
- `pika.Connection` `basic_cancel` of a gone or unknown tag waits out its timeout [\#1804](https://github.com/pika/pika/issues/1804)
- Complete each `basic_cancel` callback only on its own `CancelOk` [\#1803](https://github.com/pika/pika/pull/1803) ([alonfaraj](https://github.com/alonfaraj))
- `basic_cancel` completes on another consumer's `Basic.CancelOk` [\#1802](https://github.com/pika/pika/issues/1802)
- Ignore SIGINT in the runner tests only on the main thread [\#1801](https://github.com/pika/pika/pull/1801) ([lukebakken](https://github.com/lukebakken))
- Document the connection parameter properties once, on `Parameters` [\#1797](https://github.com/pika/pika/pull/1797) ([lukebakken](https://github.com/lukebakken))
- Kill a stuck `ForwardServer` subprocess with `Process.kill()` [\#1796](https://github.com/pika/pika/pull/1796) ([lukebakken](https://github.com/lukebakken))
- `ConnectionParameters` docs show each property as its constructor assignment, without its docstring [\#1795](https://github.com/pika/pika/issues/1795)
- `ForwardServer.stop()` raises `TypeError` when it falls back to killing the subprocess [\#1790](https://github.com/pika/pika/issues/1790)
- docfmt and docfmt-check do not exclude the generated `pika/spec.py` [\#1789](https://github.com/pika/pika/issues/1789)
- List tags through the `git` helper when no root is given [\#1787](https://github.com/pika/pika/pull/1787) ([lukebakken](https://github.com/lukebakken))
- tag\_release tests read the repository's real tags, so 1.5.0rc1 broke them locally [\#1786](https://github.com/pika/pika/issues/1786)
- Declare `test_exchange` durable in the publishing comparison [\#1785](https://github.com/pika/pika/pull/1785) ([lukebakken](https://github.com/lukebakken))
- Make `--use-tls` reach the xdist workers that run the tests [\#1784](https://github.com/pika/pika/pull/1784) ([lukebakken](https://github.com/lukebakken))
- The examples declare test\_exchange with conflicting durable settings [\#1783](https://github.com/pika/pika/issues/1783)
- The tls=true CI legs have run the plaintext suite since pytest-xdist was enabled [\#1782](https://github.com/pika/pika/issues/1782)
- Decode AMQP decimals without applying the caller's context [\#1780](https://github.com/pika/pika/pull/1780) ([JackyHsu-X](https://github.com/JackyHsu-X))
- Annotate the return values of the blocking RPC wrappers [\#1772](https://github.com/pika/pika/pull/1772) ([lukebakken](https://github.com/lukebakken))
- Drain the callback pools before the connection closes [\#1768](https://github.com/pika/pika/pull/1768) ([lukebakken](https://github.com/lukebakken))
- BlockingConnection leaves cancelled-consumer deliveries unacknowledged on an open channel [\#1767](https://github.com/pika/pika/issues/1767)
- Identify a pool worker by a stamp, not by the channel list [\#1764](https://github.com/pika/pika/pull/1764) ([lukebakken](https://github.com/lukebakken))
- Delivery callbacks run after `close()` tears the connection down, duplicating work [\#1763](https://github.com/pika/pika/issues/1763)
- Three ways `pika.Connection` can hang or leak at shutdown [\#1761](https://github.com/pika/pika/issues/1761)
- Contain any exception a `_BoundedWorkPool` work item raises [\#1756](https://github.com/pika/pika/pull/1756) ([lukebakken](https://github.com/lukebakken))
- Render every documentation cross-reference, and check it [\#1753](https://github.com/pika/pika/pull/1753) ([lukebakken](https://github.com/lukebakken))
- `_BoundedWorkPool` loses its worker when a dispatched callback raises [\#1751](https://github.com/pika/pika/issues/1751)
- Sphinx inline roles reach the published docs as literal text [\#1750](https://github.com/pika/pika/issues/1750)
- Validate user callbacks before wrapping channel operations [\#1749](https://github.com/pika/pika/pull/1749) ([ac1982](https://github.com/ac1982))
- Honor connection close deadlines from callback pool workers [\#1748](https://github.com/pika/pika/pull/1748) ([Grivn](https://github.com/Grivn))
- Accept an AMQP URL with a username but no password [\#1747](https://github.com/pika/pika/pull/1747) ([eeshsaxena](https://github.com/eeshsaxena))
- Report a crashed IOLoop through `is_open` and `is_closed` [\#1745](https://github.com/pika/pika/pull/1745) ([lukebakken](https://github.com/lukebakken))
- `Connection.is_open` reports True after the IOLoop thread crashes [\#1743](https://github.com/pika/pika/issues/1743)
- `Channel.confirm_delivery` accepts a non-callable and fails once per confirm [\#1742](https://github.com/pika/pika/issues/1742)
- Stop the preview leg reporting an unexpanded check name [\#1726](https://github.com/pika/pika/pull/1726) ([lukebakken](https://github.com/lukebakken))
- Give the big-message acceptance test its own timeout budget [\#1725](https://github.com/pika/pika/pull/1725) ([lukebakken](https://github.com/lukebakken))
- Let a pre-release be promoted to its final version [\#1723](https://github.com/pika/pika/pull/1723) ([lukebakken](https://github.com/lukebakken))
- release.yaml cannot promote a pre-release to its final version [\#1721](https://github.com/pika/pika/issues/1721)
- Reject out-of-range integers and pre-epoch timestamps on encode [\#1718](https://github.com/pika/pika/pull/1718) ([eeshsaxena](https://github.com/eeshsaxena))
- Reject bool for port and socket/stack timeouts [\#1716](https://github.com/pika/pika/pull/1716) ([Pitchfork-and-Torch](https://github.com/Pitchfork-and-Torch))
- Client properties advertise the old pika.rtfd.org docs URL [\#1713](https://github.com/pika/pika/issues/1713)
- Force-close from a pool-worker callback ignores close\(timeout\) on an unresponsive broker [\#1706](https://github.com/pika/pika/issues/1706)
- Encode Python `float` values as the AMQP `d` field type [\#1705](https://github.com/pika/pika/pull/1705) ([dylanpulver](https://github.com/dylanpulver))
- Reject decimal.Decimal values with no AMQP wire representation [\#1704](https://github.com/pika/pika/pull/1704) ([afonsojanu](https://github.com/afonsojanu))
- Stop `StreamLostError` from hiding the original traceback [\#1703](https://github.com/pika/pika/pull/1703) ([jholaj](https://github.com/jholaj))
- Stop an out-of-range `timestamp` header from closing the connection [\#1699](https://github.com/pika/pika/pull/1699) ([alonfaraj](https://github.com/alonfaraj))
- Fix `Connection.close()` self-deadlock from a consumer callback [\#1697](https://github.com/pika/pika/pull/1697) ([zanarellidev](https://github.com/zanarellidev))
- Fix: Stop `ThreadSafeChannel` confirm tags drifting from the broker [\#1694](https://github.com/pika/pika/pull/1694) ([alonfaraj](https://github.com/alonfaraj))
- Make `CallbackManager.remove` honor its remove-all docstring [\#1693](https://github.com/pika/pika/pull/1693) ([lukebakken](https://github.com/lukebakken))
- Bound how long a channel retains nowait-cancelled consumer tags [\#1692](https://github.com/pika/pika/pull/1692) ([alonfaraj](https://github.com/alonfaraj))
- `Channel._cancelled` grows without bound on nowait `basic_cancel` [\#1691](https://github.com/pika/pika/issues/1691)
- Contain exceptions from the basic\_publish on\_publish callback [\#1690](https://github.com/pika/pika/pull/1690) ([aryansk](https://github.com/aryansk))
- Drop each closed channel from the connection's tracking list [\#1689](https://github.com/pika/pika/pull/1689) ([alonfaraj](https://github.com/alonfaraj))
- `Connection._channels` is append-only, so every closed channel is retained forever [\#1688](https://github.com/pika/pika/issues/1688)
- An exception from `basic_publish(on_publish=...)` kills the whole connection [\#1687](https://github.com/pika/pika/issues/1687)
- `Connection.close()` called from a consumer callback self-deadlocks [\#1686](https://github.com/pika/pika/issues/1686)
- Fix: gh-1684 \(Release each blocking RPC's channel callbacks\) [\#1685](https://github.com/pika/pika/pull/1685) ([alonfaraj](https://github.com/alonfaraj))
- Every blocking RPC on a `ThreadSafeChannel` leaks a channel close callback [\#1684](https://github.com/pika/pika/issues/1684)
- `ThreadSafeChannel` publisher-confirm tags drift out of sync with the broker [\#1683](https://github.com/pika/pika/issues/1683)
- Fix: gh-1679 \(Reject work scheduled on a closed ThreadSafeConnection\) [\#1680](https://github.com/pika/pika/pull/1680) ([alonfaraj](https://github.com/alonfaraj))
- `ThreadSafeConnection.add_callback_threadsafe` doesn't raise error when connection closed [\#1679](https://github.com/pika/pika/issues/1679)
- fix: Stop the getaddrinfo acceptance tests from depending on live DNS [\#1667](https://github.com/pika/pika/pull/1667) ([alonfaraj](https://github.com/alonfaraj))
- Flaky getaddrinfo acceptance tests depend on live DNS [\#1661](https://github.com/pika/pika/issues/1661)
- Stop mutating global asyncio event loop policy on import [\#1641](https://github.com/pika/pika/pull/1641) ([lukebakken](https://github.com/lukebakken))
- Fix `DeliveryMode` to inherit from `IntEnum` [\#1604](https://github.com/pika/pika/pull/1604) ([lukebakken](https://github.com/lukebakken))
- `DeliveryMode` enum is not an integer; `struct.pack` rejects it on Python 3.12+ [\#1603](https://github.com/pika/pika/issues/1603)
- fix: handle channel closure in BlockingConnection.process\_data\_events [\#1586](https://github.com/pika/pika/pull/1586) ([dadodimauro](https://github.com/dadodimauro))
- fix: examples with rabbitmq 4.3 need durable queues [\#1585](https://github.com/pika/pika/pull/1585) ([dadodimauro](https://github.com/dadodimauro))
- BlockingConnection.process\_data\_events does not account for channel closure [\#1472](https://github.com/pika/pika/issues/1472)
- Remove the NACK during the stop\_consuming  [\#1389](https://github.com/pika/pika/issues/1389)
- AsyncioConnection with a custom\_ioloop raises exception [\#1296](https://github.com/pika/pika/issues/1296)
- Duplicate callback found for "0:Connection.Unblocked" [\#1052](https://github.com/pika/pika/issues/1052)
- Table encoder and decoder mixes signed and unsigned integers [\#601](https://github.com/pika/pika/issues/601)
- Decoded floats and doubles get cast to long ints [\#599](https://github.com/pika/pika/issues/599)

**Documentation:**

- Add the consumer-timeout channel close to the 1.5.0 upgrade notes [\#1815](https://github.com/pika/pika/pull/1815) ([lukebakken](https://github.com/lukebakken))
- Say the two warning-filter forms are not interchangeable [\#1788](https://github.com/pika/pika/pull/1788) ([lukebakken](https://github.com/lukebakken))
- Make `RELEASE.md` the procedure, and move the reasoning out [\#1778](https://github.com/pika/pika/pull/1778) ([lukebakken](https://github.com/lukebakken))
- Smoke-test the connection 1.5.0 documents, and note `pika.compat` [\#1776](https://github.com/pika/pika/pull/1776) ([lukebakken](https://github.com/lukebakken))
- Document what happens when a consumer callback raises [\#1774](https://github.com/pika/pika/pull/1774) ([lukebakken](https://github.com/lukebakken))
- Add a script that reproduces the close-loss figures [\#1771](https://github.com/pika/pika/pull/1771) ([lukebakken](https://github.com/lukebakken))
- Document that no pika connection survives os.fork\(\) [\#1766](https://github.com/pika/pika/pull/1766) ([lukebakken](https://github.com/lukebakken))
- Narrow the empty-intensifier rule, and clear the real cases [\#1760](https://github.com/pika/pika/pull/1760) ([lukebakken](https://github.com/lukebakken))
- Narrow the empty-intensifier rule, then clear the real cases [\#1759](https://github.com/pika/pika/issues/1759)
- Unwrap the hard-wrapped markdown, and broaden the rule [\#1758](https://github.com/pika/pika/pull/1758) ([lukebakken](https://github.com/lukebakken))
- Fix the defects the 1.5.0 documentation review found [\#1744](https://github.com/pika/pika/pull/1744) ([lukebakken](https://github.com/lukebakken))
- `Channel.confirm_delivery` has required argument `ack_nack_callback` [\#1737](https://github.com/pika/pika/issues/1737)
- Remove the duplicated TLS example and rework the TLS consumer [\#1734](https://github.com/pika/pika/pull/1734) ([lukebakken](https://github.com/lukebakken))
- Final documentation review before 1.5.0 [\#1730](https://github.com/pika/pika/issues/1730)
- Add the 1.5.0 upgrade notes, in both places a reader looks [\#1727](https://github.com/pika/pika/pull/1727) ([lukebakken](https://github.com/lukebakken))
- Rename ThreadSafeConnection/Channel to Connection/Channel [\#1682](https://github.com/pika/pika/pull/1682) ([lukebakken](https://github.com/lukebakken))
- Pre-1.5.0 miscellaneous items [\#1675](https://github.com/pika/pika/issues/1675)
- README: drop unsupported `maxAge` parameter from downloads badge [\#1668](https://github.com/pika/pika/pull/1668) ([alonfaraj](https://github.com/alonfaraj))
- Correct stale file names and CI details in `AGENTS.md` [\#1665](https://github.com/pika/pika/pull/1665) ([alonfaraj](https://github.com/alonfaraj))
- Clarify connection\_is\_idle single-interval threshold is by design [\#1648](https://github.com/pika/pika/pull/1648) ([lukebakken](https://github.com/lukebakken))
- Deploy versioned docs to GitHub Pages via mike [\#1614](https://github.com/pika/pika/pull/1614) ([alonfaraj](https://github.com/alonfaraj))
- Deploy MkDocs documentation to GitHub Pages [\#1612](https://github.com/pika/pika/issues/1612)
- feat\(docs\): add DeliveryMode enum documentation [\#1592](https://github.com/pika/pika/pull/1592) ([alonfaraj](https://github.com/alonfaraj))
- Migrate Docs to MkDocs [\#1576](https://github.com/pika/pika/pull/1576) ([alonfaraj](https://github.com/alonfaraj))
- Migrate documentation to MkDocs + Material + GitHub Pages [\#1571](https://github.com/pika/pika/issues/1571)
- Review and update all inline code documentation and comments [\#1569](https://github.com/pika/pika/issues/1569)
- Expand docs and examples with regard to thread-safety [\#1426](https://github.com/pika/pika/issues/1426)

**Closed issues:**

- Enable mypy's warn\_return\_any, so a false return annotation cannot pass [\#1773](https://github.com/pika/pika/issues/1773)
- Consolidate the three divergent \_make\_connection harnesses in the thread-safe unit tests [\#1765](https://github.com/pika/pika/issues/1765)
- Drop the versionless `License ::` classifier now that `license` is a valid SPDX expression [\#1700](https://github.com/pika/pika/issues/1700)
- An out-of-range field-table timestamp tears down the connection [\#1698](https://github.com/pika/pika/issues/1698)
- `ruff`: select vs extend-select approach [\#1663](https://github.com/pika/pika/issues/1663)
- suggestion: add docformatter to enforce PEP 257 docstring style [\#1623](https://github.com/pika/pika/issues/1623)
- Configure readthedocs.io redirects to new GitHub Pages docs site [\#1620](https://github.com/pika/pika/issues/1620)
- Remove remaining 21 `type: ignore` comments [\#1618](https://github.com/pika/pika/issues/1618)
- Add codecov.yml to fix premature coverage reporting [\#1611](https://github.com/pika/pika/issues/1611)
- DeliveryMode is not documented [\#1406](https://github.com/pika/pika/issues/1406)
- SelectConnection catches all errors and hides details.  [\#1390](https://github.com/pika/pika/issues/1390)

**Merged pull requests:**

- Reject a channel number that is in use or out of range [\#1840](https://github.com/pika/pika/pull/1840) ([alonfaraj](https://github.com/alonfaraj))
- Remove dead code [\#1817](https://github.com/pika/pika/pull/1817) ([alonfaraj](https://github.com/alonfaraj))
- pika 1.5.0rc4 [\#1813](https://github.com/pika/pika/pull/1813) ([lukebakken](https://github.com/lukebakken))
- Remove EINTR retry code that PEP 475 made unreachable [\#1812](https://github.com/pika/pika/pull/1812) ([alonfaraj](https://github.com/alonfaraj))
- Share one `_remaining` deadline helper in `thread_safe_connection` [\#1811](https://github.com/pika/pika/pull/1811) ([alonfaraj](https://github.com/alonfaraj))
- Define `TRY_IO_AGAIN_SOCK_ERROR_CODES` once in `pika._utils` [\#1810](https://github.com/pika/pika/pull/1810) ([alonfaraj](https://github.com/alonfaraj))
- pika 1.5.0rc3 [\#1807](https://github.com/pika/pika/pull/1807) ([lukebakken](https://github.com/lukebakken))
- build\(deps\): bump the github-actions group with 5 updates [\#1806](https://github.com/pika/pika/pull/1806) ([dependabot[bot]](https://github.com/apps/dependabot))
- pika 1.5.0rc2 [\#1800](https://github.com/pika/pika/pull/1800) ([lukebakken](https://github.com/lukebakken))
- Gate the no-hard-wrap rule with mdformat [\#1799](https://github.com/pika/pika/pull/1799) ([alonfaraj](https://github.com/alonfaraj))
- Exclude only `pika/spec.py` from the docformatter recipes [\#1793](https://github.com/pika/pika/pull/1793) ([alonfaraj](https://github.com/alonfaraj))
- build\(deps\): bump the github-actions group with 3 updates [\#1781](https://github.com/pika/pika/pull/1781) ([dependabot[bot]](https://github.com/apps/dependabot))
- pika 1.5.0rc1 [\#1779](https://github.com/pika/pika/pull/1779) ([lukebakken](https://github.com/lukebakken))
- Enable mypy's `warn_return_any` and make the returns true [\#1777](https://github.com/pika/pika/pull/1777) ([alonfaraj](https://github.com/alonfaraj))
- Share one connection harness between the five test classes [\#1775](https://github.com/pika/pika/pull/1775) ([lukebakken](https://github.com/lukebakken))
- build\(deps\): bump the github-actions group with 5 updates [\#1746](https://github.com/pika/pika/pull/1746) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump the github-actions group with 4 updates [\#1733](https://github.com/pika/pika/pull/1733) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump the github-actions group with 4 updates [\#1715](https://github.com/pika/pika/pull/1715) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump the github-actions group with 3 updates [\#1710](https://github.com/pika/pika/pull/1710) ([dependabot[bot]](https://github.com/apps/dependabot))
- CI: run macOS on one Python version in a minimal non-blocking leg [\#1709](https://github.com/pika/pika/pull/1709) ([lukebakken](https://github.com/lukebakken))
- CI: Test against pre-release Python 3.15 in a non-blocking leg [\#1707](https://github.com/pika/pika/pull/1707) ([alonfaraj](https://github.com/alonfaraj))
- build\(deps\): bump the github-actions group with 3 updates [\#1702](https://github.com/pika/pika/pull/1702) ([dependabot[bot]](https://github.com/apps/dependabot))
- Resolve "Drop the versionless License :: classifier now that license is a valid SPDX expression [\#1701](https://github.com/pika/pika/pull/1701) ([btschwertfeger](https://github.com/btschwertfeger))
- build\(deps\): bump the github-actions group with 3 updates [\#1696](https://github.com/pika/pika/pull/1696) ([dependabot[bot]](https://github.com/apps/dependabot))
- Perf: codec hot paths [\#1676](https://github.com/pika/pika/pull/1676) ([alonfaraj](https://github.com/alonfaraj))
- Type-annotate the generated `spec.py` and drop its mypy exemption [\#1674](https://github.com/pika/pika/pull/1674) ([alonfaraj](https://github.com/alonfaraj))
- Drop the six tractable Twisted `type: ignore` comments [\#1672](https://github.com/pika/pika/pull/1672) ([lukebakken](https://github.com/lukebakken))
- Make `AbstractSelectorIOLoop` a structural `Protocol` [\#1671](https://github.com/pika/pika/pull/1671) ([lukebakken](https://github.com/lukebakken))
- Remove six `type: ignore` comments from three adapters [\#1670](https://github.com/pika/pika/pull/1670) ([lukebakken](https://github.com/lukebakken))
- ruff: extend the default rule set and fix the fallout [\#1666](https://github.com/pika/pika/pull/1666) ([alonfaraj](https://github.com/alonfaraj))
- Upload coverage to Codecov once after all tests finish [\#1664](https://github.com/pika/pika/pull/1664) ([alonfaraj](https://github.com/alonfaraj))
- Add Ruff quote style \(`Q`\) [\#1662](https://github.com/pika/pika/pull/1662) ([alonfaraj](https://github.com/alonfaraj))
- Fix ruff 0.16.0 `RUF036` and `ISC004` violations [\#1659](https://github.com/pika/pika/pull/1659) ([lukebakken](https://github.com/lukebakken))
- Bump the github-actions group across 1 directory with 6 updates [\#1657](https://github.com/pika/pika/pull/1657) ([dependabot[bot]](https://github.com/apps/dependabot))
- Optimization: Speed up field-table codec [\#1655](https://github.com/pika/pika/pull/1655) ([alonfaraj](https://github.com/alonfaraj))
- Optimization: Eliminate quadratic copying in inbound frame buffer [\#1653](https://github.com/pika/pika/pull/1653) ([alonfaraj](https://github.com/alonfaraj))
- Fix: pyright errors [\#1652](https://github.com/pika/pika/pull/1652) ([alonfaraj](https://github.com/alonfaraj))
- Fix: address pyright issues [\#1649](https://github.com/pika/pika/pull/1649) ([alonfaraj](https://github.com/alonfaraj))
- Remove dead capabilities-stripping code from Connection.\_set\_server\_information [\#1647](https://github.com/pika/pika/pull/1647) ([alonfaraj](https://github.com/alonfaraj))
- Chore: guard type-only imports and tighten mypy [\#1646](https://github.com/pika/pika/pull/1646) ([alonfaraj](https://github.com/alonfaraj))
- Bump the github-actions group with 3 updates [\#1644](https://github.com/pika/pika/pull/1644) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `FA` ruff rule and apply fixes [\#1643](https://github.com/pika/pika/pull/1643) ([alonfaraj](https://github.com/alonfaraj))
- Bump the github-actions group with 4 updates [\#1640](https://github.com/pika/pika/pull/1640) ([dependabot[bot]](https://github.com/apps/dependabot))
- Chore: general typing fixes [\#1638](https://github.com/pika/pika/pull/1638) ([alonfaraj](https://github.com/alonfaraj))
- Bump actions/cache from 5 to 6 in the github-actions group [\#1636](https://github.com/pika/pika/pull/1636) ([dependabot[bot]](https://github.com/apps/dependabot))
- Chore: expand ruff rules [\#1633](https://github.com/pika/pika/pull/1633) ([alonfaraj](https://github.com/alonfaraj))
- Chore: re-apply redundant `rtype` removal lost in \#1625 merge [\#1629](https://github.com/pika/pika/pull/1629) ([alonfaraj](https://github.com/alonfaraj))
- Bump actions/checkout from 6 to 7 in the github-actions group [\#1627](https://github.com/pika/pika/pull/1627) ([dependabot[bot]](https://github.com/apps/dependabot))
- Chore: remove redundant `rtype` [\#1626](https://github.com/pika/pika/pull/1626) ([alonfaraj](https://github.com/alonfaraj))
- Add docformatter to enforce PEP 257 docstring style [\#1625](https://github.com/pika/pika/pull/1625) ([alonfaraj](https://github.com/alonfaraj))
- Bump codecov/codecov-action from 6 to 7 in the github-actions group [\#1622](https://github.com/pika/pika/pull/1622) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix: Wide typehints and docstrings [\#1621](https://github.com/pika/pika/pull/1621) ([alonfaraj](https://github.com/alonfaraj))
- Remove type: ignore comments from the codebase [\#1619](https://github.com/pika/pika/pull/1619) ([lukebakken](https://github.com/lukebakken))
- Fix flaky test\_concurrent\_publishes\_all\_scheduled [\#1616](https://github.com/pika/pika/pull/1616) ([alonfaraj](https://github.com/alonfaraj))
- Add codecov.yml to fix premature coverage reporting [\#1613](https://github.com/pika/pika/pull/1613) ([alonfaraj](https://github.com/alonfaraj))
- Run tests in parallel via pytest-xdist by default [\#1610](https://github.com/pika/pika/pull/1610) ([alonfaraj](https://github.com/alonfaraj))
- feat\(CI\): Add TLS to Linux and macOS [\#1609](https://github.com/pika/pika/pull/1609) ([alonfaraj](https://github.com/alonfaraj))
- Reorganize CI under .ci/, drop testdata/versions.json, always-latest [\#1606](https://github.com/pika/pika/pull/1606) ([lukebakken](https://github.com/lukebakken))
- chore: upgrade RabbitMQ to 4.3.1 and pin Linux CI Docker image [\#1599](https://github.com/pika/pika/pull/1599) ([alonfaraj](https://github.com/alonfaraj))
- feat\(CI\): improved version release flow [\#1598](https://github.com/pika/pika/pull/1598) ([alonfaraj](https://github.com/alonfaraj))
- tests: fix invisible tests and improve coverage [\#1595](https://github.com/pika/pika/pull/1595) ([alonfaraj](https://github.com/alonfaraj))
- fix: codecov coverage upload [\#1594](https://github.com/pika/pika/pull/1594) ([alonfaraj](https://github.com/alonfaraj))
- tests: Expand unit tests for `DeliveryMode` and enhance `ExchangeType` and `exceptions` [\#1593](https://github.com/pika/pika/pull/1593) ([alonfaraj](https://github.com/alonfaraj))
- Expands Ruff lint coverage [\#1591](https://github.com/pika/pika/pull/1591) ([alonfaraj](https://github.com/alonfaraj))
- Enable expanded Ruff rules [\#1590](https://github.com/pika/pika/pull/1590) ([alonfaraj](https://github.com/alonfaraj))
- Fix mypy error caused by twisted [\#1589](https://github.com/pika/pika/pull/1589) ([alonfaraj](https://github.com/alonfaraj))
- Update PULL\_REQUEST\_TEMPLATE.md for clarity and structure [\#1588](https://github.com/pika/pika/pull/1588) ([alonfaraj](https://github.com/alonfaraj))
- CI: Add macOS [\#1587](https://github.com/pika/pika/pull/1587) ([alonfaraj](https://github.com/alonfaraj))
- feat: remove pylint suppression comments and .pylintrc  [\#1584](https://github.com/pika/pika/pull/1584) ([dadodimauro](https://github.com/dadodimauro))
- feat: Inline comments fixes [\#1583](https://github.com/pika/pika/pull/1583) ([dadodimauro](https://github.com/dadodimauro))
- Remove Python 2 compatibility code and modernize for Python 3 [\#1581](https://github.com/pika/pika/pull/1581) ([alonfaraj](https://github.com/alonfaraj))

## [1.4.4](https://github.com/pika/pika/tree/1.4.4) (2026-08-06)

[Full Changelog](https://github.com/pika/pika/compare/1.4.2...1.4.4)

**Merged pull requests:**

- Revert "Fix callLater typing by passing keyword arguments" [\#1678](https://github.com/pika/pika/pull/1678) ([jeremycline](https://github.com/jeremycline))

## [1.4.2](https://github.com/pika/pika/tree/1.4.2) (2026-07-23)

[Full Changelog](https://github.com/pika/pika/compare/1.4.1...1.4.2)

**Closed issues:**

- Importing pika.adapters can break asyncio subprocesses on Windows [\#1639](https://github.com/pika/pika/issues/1639)

**Merged pull requests:**

- Stop mutating global asyncio event loop policy on import [\#1642](https://github.com/pika/pika/pull/1642) ([lukebakken](https://github.com/lukebakken))

## [1.4.1](https://github.com/pika/pika/tree/1.4.1) (2026-05-22)

**Merged pull requests:**

- Fix Channel.close() for channels with multiple consumers [\#1596](https://github.com/pika/pika/pull/1596) ([gbenson](https://github.com/gbenson))

## [1.4.0](https://github.com/pika/pika/tree/1.4.0) (2026-05-06)

[Full Changelog](https://github.com/pika/pika/compare/1.4.0b0...1.4.0)

**Implemented enhancements:**

- Enforce yapf/google formatting in CI [\#1558](https://github.com/pika/pika/issues/1558)
- Add Hatch dev environment and scripts [\#1579](https://github.com/pika/pika/pull/1579) ([lukebakken](https://github.com/lukebakken))
- Drop more Python 2 compatibility code [\#1561](https://github.com/pika/pika/pull/1561) ([lukebakken](https://github.com/lukebakken))

**Closed issues:**

- Add Hatch scripts to standardize developer commands [\#1578](https://github.com/pika/pika/issues/1578)
- Fix outdated and broken documentation across the project [\#1568](https://github.com/pika/pika/issues/1568)
- Update Codecov default branch and badge [\#1563](https://github.com/pika/pika/issues/1563)
- GitHub actions workflows and test code need updates for RabbitMQ 4.3 [\#1547](https://github.com/pika/pika/issues/1547)
- datetime.datetime.utcfromtimestamp\(\) is deprecated [\#1539](https://github.com/pika/pika/issues/1539)
- URLParameters这个类有bug [\#1533](https://github.com/pika/pika/issues/1533)
- Custom transport [\#1532](https://github.com/pika/pika/issues/1532)
- x-delay value is being returned in the header as a UINT64 and not a SINT16 [\#1531](https://github.com/pika/pika/issues/1531)
- Pika should advertise the `exchange_exchange_bindings` client capability [\#1530](https://github.com/pika/pika/issues/1530)
- Missing type annotations [\#1523](https://github.com/pika/pika/issues/1523)
- There is no info about return type of queue\_declare\(\) method of pika.channel [\#1522](https://github.com/pika/pika/issues/1522)
- Getting the user who sent the message [\#1510](https://github.com/pika/pika/issues/1510)
- Where is examples/consume\_recover\_retry.py ? [\#1499](https://github.com/pika/pika/issues/1499)
- Type Hint Issue with arguments parameter in queue\_declare method of BlockingChannel Class - \(expected "DeclareOk | None"  \[arg-type\]\) [\#1482](https://github.com/pika/pika/issues/1482)
- queue\_declare does not receive the callback at random times [\#1480](https://github.com/pika/pika/issues/1480)
- There is no current event loop in thread [\#1479](https://github.com/pika/pika/issues/1479)
- Cannot find reference 'exceptions' in '\_\_init\_\_.pyi'  [\#1473](https://github.com/pika/pika/issues/1473)
- Convert to `pytest` [\#1469](https://github.com/pika/pika/issues/1469)
- Add a CI lint check using `ruff` and fix all findings [\#1371](https://github.com/pika/pika/issues/1371)
- Add support for proxy configuration \(Socks5\) [\#1359](https://github.com/pika/pika/issues/1359)
- BlockingIOError: \[WinError 10035\] A non-blocking socket operation could not be completed immediately [\#1314](https://github.com/pika/pika/issues/1314)

**Merged pull requests:**

- Update outdated documentation across the pika project [\#1577](https://github.com/pika/pika/pull/1577) ([suchitd](https://github.com/suchitd))
- Fix TypeError in select\_connection [\#1575](https://github.com/pika/pika/pull/1575) ([suchitd](https://github.com/suchitd))
- Support Python 3.7+ in CI and fix `typing_extensions` import [\#1574](https://github.com/pika/pika/pull/1574) ([lukebakken](https://github.com/lukebakken))
- Add `yapf` formatter enforcement [\#1573](https://github.com/pika/pika/pull/1573) ([alonfaraj](https://github.com/alonfaraj))
- Replace `PIKA_TEST_TLS` env with pytest flag [\#1572](https://github.com/pika/pika/pull/1572) ([alonfaraj](https://github.com/alonfaraj))
- Fix field table type decoding to match RabbitMQ wire format [\#1566](https://github.com/pika/pika/pull/1566) ([lukebakken](https://github.com/lukebakken))
- Legacy file fixes [\#1562](https://github.com/pika/pika/pull/1562) ([alonfaraj](https://github.com/alonfaraj))
- Add AGENTS.md with AI agent guidelines [\#1560](https://github.com/pika/pika/pull/1560) ([lukebakken](https://github.com/lukebakken))
- Bump the github-actions group with 2 updates [\#1557](https://github.com/pika/pika/pull/1557) ([dependabot[bot]](https://github.com/apps/dependabot))
- Align Python versions metadata and docs  [\#1556](https://github.com/pika/pika/pull/1556) ([alonfaraj](https://github.com/alonfaraj))
- Docs: Add return value documentation for Channel methods \(\#1522\) [\#1555](https://github.com/pika/pika/pull/1555) ([suchitd](https://github.com/suchitd))
- Fix `BlockingIOError` from `_read_interrupt` on Windows \(GH-1314\) [\#1554](https://github.com/pika/pika/pull/1554) ([lukebakken](https://github.com/lukebakken))
- Remove dead Code Climate config files [\#1553](https://github.com/pika/pika/pull/1553) ([lukebakken](https://github.com/lukebakken))
- Bump the github-actions group with 4 updates [\#1552](https://github.com/pika/pika/pull/1552) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `ruff` lint check to CI \(GH-1371\) [\#1551](https://github.com/pika/pika/pull/1551) ([lukebakken](https://github.com/lukebakken))
- Migrate to pytest [\#1550](https://github.com/pika/pika/pull/1550) ([alonfaraj](https://github.com/alonfaraj))
- Add python 3.14 to CI and drop 3.9 [\#1549](https://github.com/pika/pika/pull/1549) ([alonfaraj](https://github.com/alonfaraj))
- Implement tests compatibility patch for RabbitMQ 4.3  [\#1548](https://github.com/pika/pika/pull/1548) ([alonfaraj](https://github.com/alonfaraj))
- Jpaluska/add threadsafe publisher example [\#1546](https://github.com/pika/pika/pull/1546) ([jpaluska](https://github.com/jpaluska))
- fix: replace 3 bare except clauses with except Exception [\#1545](https://github.com/pika/pika/pull/1545) ([haosenwang1018](https://github.com/haosenwang1018))
- Fix asyncio.get\_event\_loop\(\) deprecation in \_AsyncioIOServicesAdapter [\#1544](https://github.com/pika/pika/pull/1544) ([geomarceau](https://github.com/geomarceau))
- Fix Issue \# 1530: advertise exchange\_exchange\_bindings  [\#1540](https://github.com/pika/pika/pull/1540) ([kenliao94](https://github.com/kenliao94))
- Add type annotations [\#1529](https://github.com/pika/pika/pull/1529) ([dadodimauro](https://github.com/dadodimauro))
- Fix tests with Python 3.14, except RuntimeError from asyncio.get\_event\_loop [\#1524](https://github.com/pika/pika/pull/1524) ([hroncok](https://github.com/hroncok))
- Update versions [\#1514](https://github.com/pika/pika/pull/1514) ([lukebakken](https://github.com/lukebakken))
- Add description for the example blocking\_consume\_recover\_multiple\_hosts [\#1512](https://github.com/pika/pika/pull/1512) ([federicsp](https://github.com/federicsp))
- Fix `basic_consumer_threaded` example [\#1507](https://github.com/pika/pika/pull/1507) ([leogallot](https://github.com/leogallot))
- Update ConnectionParameters doc [\#1494](https://github.com/pika/pika/pull/1494) ([ryoung9550](https://github.com/ryoung9550))
- removal of pika.compat.dictkeys\(\) [\#1492](https://github.com/pika/pika/pull/1492) ([a-detiste](https://github.com/a-detiste))
- specify your own consumer\_tag when using consume\(\) method of BlockingChannel [\#1485](https://github.com/pika/pika/pull/1485) ([ivan-eguidazu](https://github.com/ivan-eguidazu))
- Convert utcnow\(\) and utcfromtimestamp\(\) [\#1484](https://github.com/pika/pika/pull/1484) ([mh21](https://github.com/mh21))
- Add all public APIs to `__all__` [\#1476](https://github.com/pika/pika/pull/1476) ([augustelalande](https://github.com/augustelalande))
- More logical validation of long RpcParameters [\#46](https://github.com/pika/pika/pull/46) ([kmpm](https://github.com/kmpm))
- Overrode \_adapter\_disconnect\(\) in BlockingConnection & fixed connection.py typos. [\#45](https://github.com/pika/pika/pull/45) ([williamsjj](https://github.com/williamsjj))
- Changed timeout for poll to be in milliseconds [\#42](https://github.com/pika/pika/pull/42) ([samuel](https://github.com/samuel))
- BlockingConnection refactored to allow multiple basic\_consumes. [\#38](https://github.com/pika/pika/pull/38) ([williamsjj](https://github.com/williamsjj))
- Encode bools as 't' type. [\#29](https://github.com/pika/pika/pull/29) ([majek](https://github.com/majek))
- Fixed various issues with table encoding/decoding [\#27](https://github.com/pika/pika/pull/27) ([majek](https://github.com/majek))

## [1.4.0b0](https://github.com/pika/pika/tree/1.4.0b0) (2024-07-22)

[Full Changelog](https://github.com/pika/pika/compare/1.3.2...1.4.0b0)

**Closed issues:**

- SkyPVP [\#1475](https://github.com/pika/pika/issues/1475)
- Add Python `3.12` to GitHub Actions [\#1470](https://github.com/pika/pika/issues/1470)
- Error with log files rotation [\#1468](https://github.com/pika/pika/issues/1468)
- Liquid bandage  [\#1455](https://github.com/pika/pika/issues/1455)
- ERR NOAUTH Authentication required. [\#1446](https://github.com/pika/pika/issues/1446)
- isssue with steam and updates [\#1420](https://github.com/pika/pika/issues/1420)

**Merged pull requests:**

- List Python 3.12 as supported [\#1474](https://github.com/pika/pika/pull/1474) ([lukebakken](https://github.com/lukebakken))
- Use sys.platform instead of platform.system\(\) [\#1467](https://github.com/pika/pika/pull/1467) ([cvijdea-bd](https://github.com/cvijdea-bd))
- Shrink the compatibility module and remove Python \<3.7 support [\#1441](https://github.com/pika/pika/pull/1441) ([alfawal](https://github.com/alfawal))
- Misc Windows CI updates [\#1438](https://github.com/pika/pika/pull/1438) ([lukebakken](https://github.com/lukebakken))
- Add examples of using client\_properties to URLParameters and ConnectionParameters [\#1436](https://github.com/pika/pika/pull/1436) ([johanrhodin](https://github.com/johanrhodin))
- example: Remove unnecessary guard [\#1435](https://github.com/pika/pika/pull/1435) ([jpmckinney](https://github.com/jpmckinney))
- Feature \#1363 -- Bump examples python version to 3 [\#1416](https://github.com/pika/pika/pull/1416) ([alfawal](https://github.com/alfawal))
- Update example to use python3 syntax [\#1408](https://github.com/pika/pika/pull/1408) ([AleksaC](https://github.com/AleksaC))
- Add more connections to the adapters [\#1405](https://github.com/pika/pika/pull/1405) ([dgoeries](https://github.com/dgoeries))
- fix typo [\#1404](https://github.com/pika/pika/pull/1404) ([khadkarajesh](https://github.com/khadkarajesh))
- Documentation improvements [\#1403](https://github.com/pika/pika/pull/1403) ([hcmturner](https://github.com/hcmturner))
- Add Python 3.11 to various places [\#1399](https://github.com/pika/pika/pull/1399) ([lukebakken](https://github.com/lukebakken))
- Fix typo in example docstring [\#1396](https://github.com/pika/pika/pull/1396) ([kinoute](https://github.com/kinoute))
- Fixing pika v1.0.0 breaking change (order of parameters) for the `cha… [\#1393](https://github.com/pika/pika/pull/1393) ([mprinc](https://github.com/mprinc))
- feat: add python3.10 support and make code python3.7+ [\#1351](https://github.com/pika/pika/pull/1351) ([vinitkumar](https://github.com/vinitkumar))

## [1.3.2](https://github.com/pika/pika/tree/1.3.2) (2023-05-05)

[Full Changelog](https://github.com/pika/pika/compare/1.3.1...1.3.2)

**Closed issues:**

- GeventConnection may now work on Windows [\#1407](https://github.com/pika/pika/issues/1407)
- ExchangeType should be from str type [\#1409](https://github.com/pika/pika/issues/1409)

**Merged pull requests:**

- Attempt to re-enable GeventConnection on Windows [\#1417](https://github.com/pika/pika/pull/1417) ([lukebakken](https://github.com/lukebakken))
- fix typo [\#1414](https://github.com/pika/pika/pull/1414) ([uwe](https://github.com/uwe))
- Fix exchange type enum type [\#1410](https://github.com/pika/pika/pull/1410) ([liortct](https://github.com/liortct))

## [1.3.1](https://github.com/pika/pika/tree/1.3.1) (2022-10-18)

[Full Changelog](https://github.com/pika/pika/compare/1.3.0...1.3.1)

**Closed issues:**

- Open to funding? [\#1391](https://github.com/pika/pika/issues/1391)
- pika迁移数据到redis [\#1378](https://github.com/pika/pika/issues/1378)
- Improve release process a bit [\#1375](https://github.com/pika/pika/issues/1375)
- Update fallback SSL/TLS protocol version [\#1385](https://github.com/pika/pika/issues/1385)

**Merged pull requests:**

- Fix a very minor typo in a source code comment [\#1388](https://github.com/pika/pika/pull/1388) ([martinky24](https://github.com/martinky24))
- Update connection module to fix strange character [\#1387](https://github.com/pika/pika/pull/1387) ([Zerpet](https://github.com/Zerpet))
- Long running publisher example [\#1384](https://github.com/pika/pika/pull/1384) ([karls](https://github.com/karls))
- Update CHANGELOG.md process for releases. [\#1392](https://github.com/pika/pika/pull/1392) ([lukebakken](https://github.com/lukebakken))
- Update connection module to use TLS v1.2 as fallback [\#1386](https://github.com/pika/pika/pull/1386) ([Zerpet](https://github.com/Zerpet))
- Add async callback examples [\#1383](https://github.com/pika/pika/pull/1383) ([lukebakken](https://github.com/lukebakken))
- Add requries-python to pyproject.toml [\#1376](https://github.com/pika/pika/pull/1376) ([gopackgo90](https://github.com/gopackgo90))

## Version History

`pika` uses \[Semantic Versioning\](<https://semver.org>)

## 1.3.0 2022-06-29

[GitHub milestone](https://github.com/pika/pika/milestone/19?closed=1)

## 1.2.1 2022-04-27

[GitHub milestone](https://github.com/pika/pika/milestone/18?closed=1)

## 1.2.0 2021-02-04

[GitHub milestone](https://github.com/pika/pika/milestone/17?closed=1)

## 1.1.0 2019-07-16

[GitHub milestone](https://github.com/pika/pika/milestone/16?closed=1)

## 1.0.1 2019-04-12

[GitHub milestone](https://github.com/pika/pika/milestone/15?closed=1)

-   API docstring updates
-   Twisted adapter: Add basic_consume Deferred to the call list
    ([PR](https://github.com/pika/pika/pull/1202))

## 1.0.0 2019-03-26

[GitHub milestone](https://github.com/pika/pika/milestone/8?closed=1)

-   `AsyncioConnection`, `TornadoConnection` and
    `TwistedProtocolConnection` are no longer auto-imported
    ([PR](https://github.com/pika/pika/pull/1129))
-   `BlockingConnection.consume` now returns `(None, None, None)` when
    inactivity timeout is reached
    ([PR](https://github.com/pika/pika/pull/901))
-   Python 3.7 support
    ([Issue](https://github.com/pika/pika/issues/1107))
-   `all_channels` parameter of the `Channel.basic_qos` method renamed
    to `global_qos`
-   `global_` parameter of the `Basic.Qos` spec class renamed to
    `global_qos`
-   **NOTE:** `heartbeat_interval` is removed, use `heartbeat` instead.
-   **NOTE:** The `backpressure_detection` option of
    `ConnectionParameters` and `URLParameters`
    property is REMOVED in favor of `Connection.Blocked` and
    `Connection.Unblocked`. See
    `Connection.add_on_connection_blocked_callback`.
-   **NOTE:** The legacy `basic_publish` method is removed, and
    `publish` renamed to `basic_publish`
-   **NOTE**: The signature of the following methods has changed from
    Pika 0.13.0. In general, the callback parameter that indicates
    completion of the method has been moved to the end of the parameter
    list to be consistent with other parts of Pika\'s API and with other
    libraries in general.

**IMPORTANT**: The signature of the following methods has changed from
Pika 0.13.0. In general, the callback parameter that indicates
completion of the method has been moved to the end of the parameter list
to be consistent with other parts of Pika\'s API and with other
libraries in general.

-   `basic_cancel`
-   `basic_consume`
-   `basic_get`
-   `basic_qos`
-   `basic_recover`
-   `confirm_delivery`
-   `exchange_bind`
-   `exchange_declare`
-   `exchange_delete`
-   `exchange_unbind`
-   `flow`
-   `queue_bind`
-   `queue_declare`
-   `queue_delete`
-   `queue_purge`
-   `queue_unbind`

**IMPORTANT**: When specifying TLS / SSL options, the `SSLOptions` class
must be used, and a `dict` is no longer supported.

## 0.13.1 2019-02-04

[GitHub milestone](https://github.com/pika/pika/milestone/14)

## 0.13.0 2019-01-17

[GitHub milestone](https://github.com/pika/pika/milestone/13)

## 0.12.0 2018-06-19

[GitHub milestone](https://github.com/pika/pika/milestone/12)

This is an interim release prior to version `1.0.0`. It
includes the following backported pull requests and commits from the
`master` branch:

-   [PR #901](https://github.com/pika/pika/pull/901)
-   [PR #908](https://github.com/pika/pika/pull/908)
-   [PR #910](https://github.com/pika/pika/pull/910)
-   [PR #918](https://github.com/pika/pika/pull/918)
-   [PR #920](https://github.com/pika/pika/pull/920)
-   [PR #924](https://github.com/pika/pika/pull/924)
-   [PR #937](https://github.com/pika/pika/pull/937)
-   [PR #938](https://github.com/pika/pika/pull/938)
-   [PR #933](https://github.com/pika/pika/pull/933)
-   [PR #940](https://github.com/pika/pika/pull/940)
-   [PR #932](https://github.com/pika/pika/pull/932)
-   [PR #928](https://github.com/pika/pika/pull/928)
-   [PR #934](https://github.com/pika/pika/pull/934)
-   [PR #915](https://github.com/pika/pika/pull/915)
-   [PR #946](https://github.com/pika/pika/pull/946)
-   [PR #947](https://github.com/pika/pika/pull/947)
-   [PR #952](https://github.com/pika/pika/pull/952)
-   [PR #956](https://github.com/pika/pika/pull/956)
-   [PR #966](https://github.com/pika/pika/pull/966)
-   [PR #975](https://github.com/pika/pika/pull/975)
-   [PR #978](https://github.com/pika/pika/pull/978)
-   [PR #981](https://github.com/pika/pika/pull/981)
-   [PR #994](https://github.com/pika/pika/pull/994)
-   [PR #1007](https://github.com/pika/pika/pull/1007)
-   [PR #1045](https://github.com/pika/pika/pull/1045) (manually
    backported)
-   [PR #1011](https://github.com/pika/pika/pull/1011)

Commits:

Travis CI fail fast - 3f0e739

New features:

`BlockingConnection.consume` now returns `(None, None, None)` when
inactivity timeout is reached
([PR](https://github.com/pika/pika/pull/901))

`BlockingConnection` now supports the `add_callback_threadsafe` method
which allows a function to be executed correctly on the IO loop thread.
The main use-case for this is as follows:

-   Application sets up a thread for `BlockingConnection` and calls
    `basic_consume` on it
-   When a message is received, work is done on another thread
-   When the work is done, the worker uses
    `connection.add_callback_threadsafe` to call the `basic_ack` method
    on the channel instance.

Please see `examples/basic_consumer_threaded.py` for an example. As
always, `SelectConnection` and a fully async consumer/publisher is the
preferred method of using Pika.

Heartbeats are now sent at an interval equal to 1/2 of the negotiated
idle connection timeout. RabbitMQ\'s default timeout value is 60
seconds, so heartbeats will be sent at a 30 second interval. In
addition, Pika\'s check for an idle connection will be done at an
interval equal to the timeout value plus 5 seconds to allow for delays.
This results in an interval of 65 seconds by default.

## 0.11.2 2017-11-30

[GitHub milestone](https://github.com/pika/pika/milestone/11)

[0.11.2](https://github.com/pika/pika/compare/0.11.1...0.11.2)

-   Remove `+` character from platform releases string
    ([PR](https://github.com/pika/pika/pull/895))

## 0.11.1 2017-11-27

[GitHub milestone](https://github.com/pika/pika/milestone/10)

[0.11.1](https://github.com/pika/pika/compare/0.11.0...0.11.1)

-   Fix `BlockingConnection` to ensure event loop exits
    ([PR](https://github.com/pika/pika/pull/887))
-   Heartbeat timeouts will use the client value if specified
    ([PR](https://github.com/pika/pika/pull/874))
-   Allow setting some common TCP options
    ([PR](https://github.com/pika/pika/pull/880))
-   Errors when decoding Unicode are ignored
    ([PR](https://github.com/pika/pika/pull/890))
-   Fix large number encoding
    ([PR](https://github.com/pika/pika/pull/888))

## 0.11.0 2017-07-29

[GitHub milestone](https://github.com/pika/pika/milestone/9)

[0.11.0](https://github.com/pika/pika/compare/0.10.0...0.11.0)

> -   Simplify Travis CI configuration for OS X.
> -   Add `asyncio` connection adapter for Python 3.4 and
>     newer.
> -   Connection failures that occur after the socket is opened and
>     before the AMQP connection is ready to go are now reported by
>     calling the connection error callback. Previously these were not
>     consistently reported.
> -   In BaseConnection.close, call \_handle_ioloop_stop only if the
>     connection is already closed to allow the asynchronous close
>     operation to complete gracefully.
> -   Pass error information from failed socket connection to user
>     callbacks on_open_error_callback and on_close_callback with
>     result_code=-1.
> -   ValueError is raised when a completion callback is passed to an
>     asynchronous (nowait) Channel operation. It\'s an application
>     error to pass a non-None completion callback with an asynchronous
>     request, because this callback can never be serviced in the
>     asynchronous scenario.
> -   `Channel.basic_reject` fixed to allow
>     `delivery_tag` to be of type `long` as
>     well as `int`. (by quantum5)
> -   Implemented support for blocked connection timeouts in
>     `pika.connection.Connection`. This feature is
>     available to all pika adapters. See
>     `pika.connection.ConnectionParameters` docstring to
>     learn more about `blocked_connection_timeout`
>     configuration.
> -   Deprecated the `heartbeat_interval` arg in
>     `pika.ConnectionParameters` in favor of the
>     `heartbeat` arg for consistency with the other
>     connection parameters classes
>     `pika.connection.Parameters` and
>     `pika.URLParameters`.
> -   When the `port` arg is not set explicitly in
>     `ConnectionParameters` constructor, but the
>     `ssl` arg is set explicitly, then set the port value
>     to to the default AMQP SSL port if SSL is enabled, otherwise to
>     the default AMQP plaintext port.
> -   `URLParameters` will raise ValueError if a non-empty
>     URL scheme other than {amqp \| amqps \| http \| https} is
>     specified.
> -   `InvalidMinimumFrameSize` and
>     `InvalidMaximumFrameSize` exceptions are deprecated.
>     pika.connection.Parameters.frame_max property setter now raises
>     the standard `ValueError` exception when the value is
>     out of bounds.
> -   Removed deprecated parameter `type` in
>     `Channel.exchange_declare` and
>     `BlockingChannel.exchange_declare` in favor of the
>     `exchange_type` arg that doesn\'t overshadow the
>     builtin `type` keyword.
> -   Channel.close() on OPENING channel transitions it to CLOSING
>     instead of raising ChannelClosed.
> -   Channel.close() on CLOSING channel raises
>     `ChannelAlreadyClosing`; used to raise
>     `ChannelClosed`.
> -   Connection.channel() raises `ConnectionClosed` if
>     connection is not in OPEN state.
> -   When performing graceful close on a channel and
>     `Channel.Close` from broker arrives while waiting for
>     CloseOk, don\'t release the channel number until CloseOk arrives
>     to avoid race condition that may lead to a new channel receiving
>     the CloseOk that was destined for the closing channel.
> -   The `backpressure_detection` option of
>     `ConnectionParameters` and `URLParameters`
>     property is DEPRECATED in favor of
>     `Connection.Blocked` and
>     `Connection.Unblocked`. See
>     `Connection.add_on_connection_blocked_callback`.

## 0.10.0 2015-09-02

[0.10.0](https://github.com/pika/pika/compare/0.9.14...0.10.0)

> -   a9bf96d - LibevConnection: Fixed dict chgd size during iteration
>     (Michael Laing)
> -   388c55d - SelectConnection: Fixed KeyError exceptions in IOLoop
>     timeout executions (Shinji Suzuki)
> -   4780de3 - BlockingConnection: Add support to make
>     BlockingConnection a Context Manager (@reddec)

## 0.10.0b2 2015-07-15

> -   f72b58f - Fixed failure to purge \_ConsumerCancellationEvt from
>     BlockingChannel.\_pending_events during basic_cancel. (Vitaly
>     Kruglikov)

## 0.10.0b1 2015-07-10

High-level summary of notable changes:

-   Change to 3-Clause BSD License
-   Python 3.x support
-   Over 150 commits from 19 contributors
-   Refactoring of SelectConnection ioloop
-   This major release contains certain non-backward-compatible API
    changes as well as significant performance improvements in the
    `BlockingConnection` adapter.
-   Non-backward-compatible changes in
    `Channel.add_on_return_callback` callback\'s signature.
-   The `AsyncoreConnection` adapter was retired

**Details**

Python 3.x: this release introduces python 3.x support. Tested on Python
3.3 and 3.4.

\`AsyncoreConnection\`: Retired this legacy adapter to reduce
maintenance burden; the recommended replacement is the
`SelectConnection` adapter.

\`SelectConnection\`: ioloop was refactored for compatibility with other
ioloops.

\`Channel.add_on_return_callback\`: The callback is now passed the
individual parameters channel, method, properties, and body instead of a
tuple of those values for congruence with other similar callbacks.

\`BlockingConnection\`: This adapter underwent a makeover under the hood
and gained significant performance improvements as well as enhanced
timer resolution. It is now implemented as a client of the
`SelectConnection` adapter.

Below is an overview of the `BlockingConnection` and
`BlockingChannel` API changes:

> -   Recursion: the new implementation eliminates callback recursion
>     that sometimes blew out the stack in the legacy implementation
>     (e.g., publish -\> consumer_callback -\> publish -\>
>     consumer_callback, etc.). While
>     `BlockingConnection.process_data_events` and
>     `BlockingConnection.sleep` may still be called from
>     the scope of the blocking adapter\'s callbacks in order to process
>     pending I/O, additional callbacks will be suppressed whenever
>     `BlockingConnection.process_data_events` and
>     `BlockingConnection.sleep` are nested in any
>     combination; in that case, the callback information will be
>     bufferred and dispatched once nesting unwinds and control returns
>     to the level-zero dispatcher.
>
> -   \`BlockingConnection.connect\`: this method was removed in favor
>     of the constructor as the only way to establish connections; this
>     reduces maintenance burden, while improving reliability of the
>     adapter.
>
> -   \`BlockingConnection.process_data_events\`: added the optional
>     parameter `time_limit`.
>
> -   \`BlockingConnection.add_on_close_callback\`: removed; legacy
>     raised `NotImplementedError`.
>
> -   \`BlockingConnection.add_on_open_callback\`: removed; legacy
>     raised `NotImplementedError`.
>
> -   \`BlockingConnection.add_on_open_error_callback\`: removed; legacy
>     raised `NotImplementedError`.
>
> -   \`BlockingConnection.add_backpressure_callback\`: not supported
>
> -   \`BlockingConnection.set_backpressure_multiplier\`: not supported
>
> -   \`BlockingChannel.add_on_flow_callback\`: not supported; per
>     docstring in channel.py: \"Note that newer versions of RabbitMQ
>     will not issue this but instead use TCP backpressure\".
>
> -   \`BlockingChannel.flow\`: not supported
>
> -   \`BlockingChannel.force_data_events\`: removed as it is no longer
>     necessary following redesign of the adapter.
>
> -   Removed the `nowait` parameter from
>     `BlockingChannel` methods, forcing
>     `nowait=False` (former API default) in the
>     implementation; this is more suitable for the blocking nature of
>     the adapter and its error-reporting strategy; this concerns the
>     following methods: `basic_cancel`,
>     `confirm_delivery`, `exchange_bind`,
>     `exchange_declare`, `exchange_delete`,
>     `exchange_unbind`, `queue_bind`,
>     `queue_declare`, `queue_delete`, and
>     `queue_purge`.
>
> -   \`BlockingChannel.basic_cancel\`: returns a sequence instead of
>     None; for a `no_ack=True` consumer,
>     `basic_cancel` returns a sequence of pending messages
>     that arrived before broker confirmed the cancellation.
>
> -   \`BlockingChannel.consume\`: added new optional kwargs
>     `arguments` and `inactivity_timeout`.
>     Also, raises ValueError if the consumer creation parameters don\'t
>     match those used to create the existing queue consumer generator,
>     if any; this happens when you break out of the consume loop, then
>     call `BlockingChannel.consume` again with different
>     consumer-creation args without first cancelling the previous queue
>     consumer generator via `BlockingChannel.cancel`. The
>     legacy implementation would silently resume consuming from the
>     existing queue consumer generator even if the subsequent
>     `BlockingChannel.consume` was invoked with a different
>     queue name, etc.
>
> -   \`BlockingChannel.cancel\`: returns 0; the legacy implementation
>     tried to return the number of requeued messages, but this number
>     was not accurate as it didn\'t include the messages returned by
>     the Channel class; this count is not generally useful, so
>     returning 0 is a reasonable replacement.
>
> -   \`BlockingChannel.open\`: removed in favor of having a single
>     mechanism for creating a channel
>     (`BlockingConnection.channel`); this reduces
>     maintenance burden, while improving reliability of the adapter.
>
> -   \`BlockingChannel.confirm_delivery\`: raises UnroutableError when
>     unroutable messages that were sent prior to this call are returned
>     before we receive Confirm.Select-ok.
>
> -   `BlockingChannel.basic_publish`: always returns True when delivery
>     confirmation is not enabled (publisher-acks = off); the legacy
>     implementation returned a bool in this case if
>     `mandatory=True` to indicate whether the message was
>     delivered; however, this was non-deterministic, because
>     Basic.Return is asynchronous and there is no way to know how long
>     to wait for it or its absence. The legacy implementation returned
>     None when publishing with publisher-acks = off and
>     `mandatory=False`. The new implementation always
>     returns True when publishing while publisher-acks = off.
>
> -   
>
>     \`BlockingChannel.publish\`: a new alternate method (vs. `basic_publish`) for
>
>     :   publishing a message with more detailed error reporting via
>         UnroutableError and NackError exceptions.
>
> -   \`BlockingChannel.start_consuming\`: raises
>     pika.exceptions.RecursionError if called from the scope of a
>     `BlockingConnection` or `BlockingChannel`
>     callback.
>
> -   \`BlockingChannel.get_waiting_message_count\`: new method; returns
>     the number of messages that may be retrieved from the current
>     queue consumer generator via `BasicChannel.consume`
>     without blocking.

**Commits**

> -   5aaa753 - Fixed SSL import and removed no_ack=True in favor of
>     explicit AMQP message handling based on deferreds (skftn)
> -   7f222c2 - Add checkignore for codeclimate (Gavin M. Roy)
> -   4dec370 - Implemented BlockingChannel.flow; Implemented
>     BlockingConnection.add_on_connection_blocked_callback; Implemented
>     BlockingConnection.add_on_connection_unblocked_callback. (Vitaly
>     Kruglikov)
> -   4804200 - Implemented blocking adapter acceptance test for
>     exchange-to-exchange binding. Added rudimentary validation of
>     BasicProperties passthru in blocking adapter publish tests.
>     Updated CHANGELOG. (Vitaly Kruglikov)
> -   4ec07fd - Fixed sending of data in TwistedProtocolConnection
>     (Vitaly Kruglikov)
> -   a747fb3 - Remove my copyright from forward_server.py test utility.
>     (Vitaly Kruglikov)
> -   94246d2 - Return True from basic_publish when pubacks is off.
>     Implemented more blocking adapter accceptance tests. (Vitaly
>     Kruglikov)
> -   3ce013d - PIKA-609 Wait for broker to dispatch all messages to
>     client before cancelling consumer in
>     TestBasicCancelWithNonAckableConsumer and
>     TestBasicCancelWithAckableConsumer (Vitaly Kruglikov)
> -   293f778 - Created CHANGELOG entry for release 0.10.0. Fixed up
>     callback documentation for basic_get, basic_consume, and
>     add_on_return_callback. (Vitaly Kruglikov)
> -   16d360a - Removed the legacy AsyncoreConnection adapter in favor
>     of the recommended SelectConnection adapter. (Vitaly Kruglikov)
> -   240a82c - Defer creation of poller\'s event loop interrupt socket
>     pair until start is called, because some SelectConnection users
>     (e.g., BlockingConnection adapter) don\'t use the event loop, and
>     these sockets would just get reported as resource leaks. (Vitaly
>     Kruglikov)
> -   aed5cae - Added EINTR loops in select_connection pollers.
>     Addressed some pylint findings, including an error or two. Wrap
>     socket.send and socket.recv calls in EINTR loops Use the correct
>     exception for socket.error and select.error and get errno
>     depending on python version. (Vitaly Kruglikov)
> -   498f1be - Allow passing exchange, queue and routing_key as text,
>     handle short strings as text in python3 (saarni)
> -   9f7f243 - Restored basic_consume, basic_cancel, and
>     add_on_cancel_callback (Vitaly Kruglikov)
> -   18c9909 - Reintroduced BlockingConnection.process_data_events.
>     (Vitaly Kruglikov)
> -   4b25cb6 - Fixed BlockingConnection/BlockingChannel acceptance and
>     unit tests (Vitaly Kruglikov)
> -   bfa932f - Facilitate proper connection state after
>     BasicConnection.\_adapter_disconnect (Vitaly Kruglikov)
> -   9a09268 - Fixed BlockingConnection test that was failing with
>     ConnectionClosed error. (Vitaly Kruglikov)
> -   5a36934 - Copied synchronous_connection.py from pika-synchronous
>     branch Fixed pylint findings Integrated SynchronousConnection with
>     the new ioloop in SelectConnection Defined dedicated message
>     classes PolledMessage and ConsumerMessage and moved from
>     BlockingChannel to module-global scope. Got rid of nowait args
>     from BlockingChannel public API methods Signal unroutable messages
>     via UnroutableError exception. Signal Nack\'ed messages via
>     NackError exception. These expose more information about the
>     failure than legacy basic_publich API. Removed set_timeout and
>     backpressure callback methods Restored legacy
>     `is_open`, etc. property names (Vitaly Kruglikov)
> -   6226dc0 - Remove deprecated \--use-mirrors (Gavin M. Roy)
> -   1a7112f - Raise ConnectionClosed when sending a frame with no
>     connection (#439) (Gavin M. Roy)
> -   9040a14 - Make delivery_tag non-optional (#498) (Gavin M. Roy)
> -   86aabc2 - Bump version (Gavin M. Roy)
> -   562075a - Update a few testing things (Gavin M. Roy)
> -   4954d38 - use unicode_type in blocking_connection.py (Antti
>     Haapala)
> -   133d6bc - Let Travis install ordereddict for Python 2.6, and ttest
>     3.3, 3.4 too. (Antti Haapala)
> -   0d2287d - Pika Python 3 support (Antti Haapala)
> -   3125c79 - SSLWantRead is not supported before python 2.7.9 and 3.3
>     (Will)
> -   9a9c46c - Fixed TestDisconnectDuringConnectionStart: it turns out
>     that depending on callback order, it might get either
>     ProbableAuthenticationError or ProbableAccessDeniedError. (Vitaly
>     Kruglikov)
> -   cd8c9b0 - A fix the write starvation problem that we see with
>     tornado and pika (Will)
> -   8654fbc - SelectConnection - make interrupt socketpair
>     non-blocking (Will)
> -   4f3666d - Added copyright in forward_server.py and fixed NameError
>     bug (Vitaly Kruglikov)
> -   f8ebbbc - ignore docs (Gavin M. Roy)
> -   a344f78 - Updated codeclimate config (Gavin M. Roy)
> -   373c970 - Try and fix pathing issues in codeclimate (Gavin M. Roy)
> -   228340d - Ignore codegen (Gavin M. Roy)
> -   4db0740 - Add a codeclimate config (Gavin M. Roy)
> -   7e989f9 - Slight code re-org, usage comment and better naming of
>     test file. (Will)
> -   287be36 - Set up \_kqueue member of KQueuePoller before calling
>     super constructor to avoid exception due to missing \_kqueue
>     member. Call `self._map_event(event)` instead of
>     `self._map_event(event.filter)`, because
>     `KQueuePoller._map_event()` assumes it\'s getting an
>     event, not an event filter. (Vitaly Kruglikov)
> -   62810fb - Fix issue #412: reset BlockingConnection.\_read_poller
>     in BlockingConnection.\_adapter_disconnect() to guard against
>     accidental access to old file descriptor. (Vitaly Kruglikov)
> -   03400ce - Rationalise adapter acceptance tests (Will)
> -   9414153 - Fix bug selecting non epoll poller (Will)
> -   4f063df - Use user heartbeat setting if server proposes none (Pau
>     Gargallo)
> -   9d04d6e - Deactivate heartbeats when heartbeat_interval is 0 (Pau
>     Gargallo)
> -   a52a608 - Bug fix and review comments. (Will)
> -   e3ebb6f - Fix incorrect x-expires argument in acceptance tests
>     (Will)
> -   294904e - Get BlockingConnection into consistent state upon loss
>     of TCP/IP connection with broker and implement acceptance tests
>     for those cases. (Vitaly Kruglikov)
> -   7f91a68 - Make SelectConnection behave like an ioloop (Will)
> -   dc9db2b - Perhaps 5 seconds is too agressive for travis (Gavin M.
>     Roy)
> -   c23e532 - Lower the stuck test timeout (Gavin M. Roy)
> -   1053ebc - Late night bug (Gavin M. Roy)
> -   cd6c1bf - More BaseConnection.\_handle_error cleanup (Gavin M.
>     Roy)
> -   a0ff21c - Fix the test to work with Python 2.6 (Gavin M. Roy)
> -   748e8aa - Remove pypy for now (Gavin M. Roy)
> -   1c921c1 - Socket close/shutdown cleanup (Gavin M. Roy)
> -   5289125 - Formatting update from PR (Gavin M. Roy)
> -   d235989 - Be more specific when calling getaddrinfo (Gavin M. Roy)
> -   b5d1b31 - Reflect the method name change in pika.callback
>     (Gavin M. Roy)
> -   df7d3b7 - Cleanup BlockingConnection in a few places (Gavin M.
>     Roy)
> -   cd99e1c - Rename method due to use in BlockingConnection (Gavin M.
>     Roy)
> -   7e0d1b3 - Use google style with yapf instead of pep8 (Gavin M.
>     Roy)
> -   7dc9bab - Refactor socket writing to not use sendall #481
>     (Gavin M. Roy)
> -   4838789 - Dont log the fd #521 (Gavin M. Roy)
> -   765107d - Add Connection.Blocked callback registration methods
>     #476 (Gavin M. Roy)
> -   c15b5c1 - Fix \_blocking typo pointed out in #513 (Gavin M. Roy)
> -   759ac2c - yapf of codegen (Gavin M. Roy)
> -   9dadd77 - yapf cleanup of codegen and spec (Gavin M. Roy)
> -   ddba7ce - Do not reject consumers with no_ack=True #486 #530
>     (Gavin M. Roy)
> -   4528a1a - yapf reformatting of tests (Gavin M. Roy)
> -   e7b6d73 - Remove catching AttributError (#531) (Gavin M. Roy)
> -   41ea5ea - Update README badges \[skip ci\] (Gavin M. Roy)
> -   6af987b - Add note on contributing (Gavin M. Roy)
> -   161fc0d - yapf formatting cleanup (Gavin M. Roy)
> -   edcb619 - Add PYPY to travis testing (Gavin M. Roy)
> -   2225771 - Change the coverage badge (Gavin M. Roy)
> -   8f7d451 - Move to codecov from coveralls (Gavin M. Roy)
> -   b80407e - Add confirm_delivery to example (Andrew Smith)
> -   6637212 - Update base_connection.py (bstemshorn)
> -   1583537 - #544 get_waiting_message_count() (markcf)
> -   0c9be99 - Fix #535: pass expected reply_code and reply_text from
>     method frame to Connection.\_on_disconnect from
>     Connection.\_on_connection_closed (Vitaly Kruglikov)
> -   d11e73f - Propagate ConnectionClosed exception out of
>     BlockingChannel.\_send_method() and log ConnectionClosed in
>     BlockingConnection.\_on_connection_closed() (Vitaly Kruglikov)
> -   63d2951 - Fix #541 - make sure connection state is properly reset
>     when BlockingConnection.\_check_state_on_disconnect raises
>     ConnectionClosed. This supplements the previously-merged PR #450
>     by getting the connection into consistent state. (Vitaly
>     Kruglikov)
> -   71bc0eb - Remove unused self.fd attribute from BaseConnection
>     (Vitaly Kruglikov)
> -   8c08f93 - PIKA-532 Removed unnecessary params (Vitaly Kruglikov)
> -   6052ecf - PIKA-532 Fix bug in BlockingConnection.\_handle_timeout
>     that was preventing \_on_connection_closed from being called when
>     not closing. (Vitaly Kruglikov)
> -   562aa15 - pika: callback: Display exception message when callback
>     fails. (Stuart Longland)
> -   452995c - Typo fix in connection.py (Andrew)
> -   361c0ad - Added some missing yields (Robert Weidlich)
> -   0ab5a60 - Added complete example for python twisted service
>     (Robert Weidlich)
> -   4429110 - Add deployment and webhooks (Gavin M. Roy)
> -   7e50302 - Fix has_content style in codegen (Andrew Grigorev)
> -   28c2214 - Fix the trove categorization (Gavin M. Roy)
> -   de8b545 - Ensure frames can not be interspersed on send (Gavin M.
>     Roy)
> -   8fe6bdd - Fix heartbeat behaviour after connection failure.
>     (Kyösti Herrala)
> -   c123472 - Updating BlockingChannel.basic_get doc (it does not
>     receive a callback like the rest of the adapters) (Roberto
>     Decurnex)
> -   b5f52fb - Fix number of arguments passed to \_on_return callback
>     (Axel Eirola)
> -   765139e - Lower default TIMEOUT to 0.01 (bra-fsn)
> -   6cc22a5 - Fix confirmation on reconnects (bra-fsn)
> -   f4faf0a - asynchronous publisher and subscriber examples
>     refactored to follow the StepDown rule (Riccardo Cirimelli)

## 0.9.14 - 2014-07-11

[0.9.14](https://github.com/pika/pika/compare/0.9.13...0.9.14)

> -   57fe43e - fix test to generate a correct range of random ints (ml)
> -   0d68dee - fix async watcher for libev_connection (ml)
> -   01710ad - Use default username and password if not specified in
>     URLParameters (Sean Dwyer)
> -   fae328e - documentation typo (Jeff Fein-Worton)
> -   afbc9e0 - libev_connection: reset_io_watcher (ml)
> -   24332a2 - Fix the manifest (Gavin M. Roy)
> -   acdfdef - Remove useless test (Gavin M. Roy)
> -   7918e1a - Skip libev tests if pyev is not installed or if they are
>     being run in pypy (Gavin M. Roy)
> -   bb583bf - Remove the deprecated test (Gavin M. Roy)
> -   aecf3f2 - Don\'t reject a message if the channel is not open
>     (Gavin M. Roy)
> -   e37f336 - Remove UTF-8 decoding in spec (Gavin M. Roy)
> -   ddc35a9 - Update the unittest to reflect removal of force binary
>     (Gavin M. Roy)
> -   fea2476 - PEP8 cleanup (Gavin M. Roy)
> -   9b97956 - Remove force_binary (Gavin M. Roy)
> -   a42dd90 - Whitespace required (Gavin M. Roy)
> -   85867ea - Update the content_frame_dispatcher tests to reflect
>     removal of auto-cast utf-8 (Gavin M. Roy)
> -   5a4bd5d - Remove unicode casting (Gavin M. Roy)
> -   efea53d - Remove force binary and unicode casting (Gavin M. Roy)
> -   e918d15 - Add methods to remove deprecation warnings from asyncore
>     (Gavin M. Roy)
> -   117f62d - Add a coveragerc to ignore the auto generated pika.spec
>     (Gavin M. Roy)
> -   52f4485 - Remove pypy tests from travis for now (Gavin M. Roy)
> -   c3aa958 - Update README.rst (Gavin M. Roy)
> -   3e2319f - Delete README.md (Gavin M. Roy)
> -   c12b0f1 - Move to RST (Gavin M. Roy)
> -   704f5be - Badging updates (Gavin M. Roy)
> -   7ae33ca - Update for coverage info (Gavin M. Roy)
> -   ae7ca86 - add libev_adapter_tests.py; modify .travis.yml to
>     install libev and pyev (ml)
> -   f86aba5 - libev_connection: add \*\*kwargs to \_handle_event;
>     suppress default_ioloop reuse warning (ml)
> -   603f1cf - async_test_base: add necessary args to \_on_cconn_closed
>     (ml)
> -   3422007 - add libev_adapter_tests.py (ml)
> -   6cbab0c - removed relative imports and importing urlparse from
>     urllib.parse for py3+ (a-tal)
> -   f808464 - libev_connection: add async watcher; add optional
>     parameters to add_timeout (ml)
> -   c041c80 - Remove ev all together for now (Gavin M. Roy)
> -   9408388 - Update the test descriptions and timeout (Gavin M. Roy)
> -   1b552e0 - Increase timeout (Gavin M. Roy)
> -   69a1f46 - Remove the pyev requirement for 2.6 testing (Gavin M.
>     Roy)
> -   fe062d2 - Update package name (Gavin M. Roy)
> -   611ad0e - Distribute the LICENSE and README.md (#350) (Gavin M.
>     Roy)
> -   df5e1d8 - Ensure that the entire frame is written using
>     socket.sendall (#349) (Gavin M. Roy)
> -   69ec8cf - Move the libev install to before_install (Gavin M. Roy)
> -   a75f693 - Update test structure (Gavin M. Roy)
> -   636b424 - Update things to ignore (Gavin M. Roy)
> -   b538c68 - Add tox, nose.cfg, update testing config (Gavin M. Roy)
> -   a0e7063 - add some tests to increase coverage of pika.connection
>     (Charles Law)
> -   c76d9eb - Address issue #459 (Gavin M. Roy)
> -   86ad2db - Raise exception if positional arg for parameters isn\'t
>     an instance of Parameters (Gavin M. Roy)
> -   14d08e1 - Fix for python 2.6 (Gavin M. Roy)
> -   bd388a3 - Use the first unused channel number addressing #404,
>     #460 (Gavin M. Roy)
> -   e7676e6 - removing a debug that was left in last commit (James
>     Mutton)
> -   6c93b38 - Fixing connection-closed behavior to detect on attempt
>     to publish (James Mutton)
> -   c3f0356 - Initialize bytes_written in \_handle_write() (Jonathan
>     Kirsch)
> -   4510e95 - Fix \_handle_write() may not send full frame (Jonathan
>     Kirsch)
> -   12b793f - fixed Tornado Consumer example to successfully reconnect
>     (Yang Yang)
> -   f074444 - remove forgotten import of ordereddict (Pedro Abranches)
> -   1ba0aea - fix last merge (Pedro Abranches)
> -   10490a6 - change timeouts structure to list to maintain scheduling
>     order (Pedro Abranches)
> -   7958394 - save timeouts in ordered dict instead of dict (Pedro
>     Abranches)
> -   d2746bf - URLParameters and ConnectionParameters accept unicode
>     strings (Allard Hoeve)
> -   596d145 - previous fix for AttributeError made parent and child
>     class methods identical, remove duplication (James Mutton)
> -   42940dd - UrlParameters Docs: fixed amqps scheme examples
>     (Riccardo Cirimelli)
> -   43904ff - Dont test this in PyPy due to sort order issue (Gavin M.
>     Roy)
> -   d7d293e - Don\'t leave \_\_repr\_\_ sorting up to chance (Gavin M.
>     Roy)
> -   848c594 - Add integration test to travis and fix invocation
>     (Gavin M. Roy)
> -   2678275 - Add pypy to travis tests (Gavin M. Roy)
> -   1877f3d - Also addresses issue #419 (Gavin M. Roy)
> -   470c245 - Address issue #419 (Gavin M. Roy)
> -   ca3cb59 - Address issue #432 (Gavin M. Roy)
> -   a3ff6f2 - Default frame max should be AMQP FRAME_MAX (Gavin M.
>     Roy)
> -   ff3d5cb - Remove max consumer tag test due to change in code.
>     (Gavin M. Roy)
> -   6045dda - Catch KeyError (#437) to ensure that an exception is not
>     raised in a race condition (Gavin M. Roy)
> -   0b4d53a - Address issue #441 (Gavin M. Roy)
> -   180e7c4 - Update license and related files (Gavin M. Roy)
> -   256ed3d - Added Jython support. (Erik Olof Gunnar Andersson)
> -   f73c141 - experimental work around for recursion issue. (Erik Olof
>     Gunnar Andersson)
> -   a623f69 - Prevent #436 by iterating the keys and not the dict
>     (Gavin M. Roy)
> -   755fcae - Add support for authentication_failure_close,
>     connection.blocked (Gavin M. Roy)
> -   c121243 - merge upstream master (Michael Laing)
> -   a08dc0d - add arg to channel.basic_consume (Pedro Abranches)
> -   10b136d - Documentation fix (Anton Ryzhov)
> -   9313307 - Fixed minor markup errors. (Jorge Puente Sarrín)
> -   fb3e3cf - Fix the spelling of UnsupportedAMQPFieldException
>     (Garrett Cooper)
> -   03d5da3 - connection.py: Propagate the force_channel keyword
>     parameter to methods involved in channel creation (Michael Laing)
> -   7bbcff5 - Documentation fix for basic_publish (JuhaS)
> -   01dcea7 - Expose no_ack and exclusive to BlockingChannel.consume
>     (Jeff Tang)
> -   d39b6aa - Fix BlockingChannel.basic_consume does not block on
>     non-empty queues (Juhyeong Park)
> -   6e1d295 - fix for issue 391 and issue 307 (Qi Fan)
> -   d9ffce9 - Update parameters.rst (cacovsky)
> -   6afa41e - Add additional badges (Gavin M. Roy)
> -   a255925 - Fix return value on dns resolution issue (Laurent
>     Eschenauer)
> -   3f7466c - libev_connection: tweak docs (Michael Laing)
> -   0aaed93 - libev_connection: Fix varable naming (Michael Laing)
> -   0562d08 - libev_connection: Fix globals warning (Michael Laing)
> -   22ada59 - libev_connection: use globals to track sigint and
>     sigterm watchers as they are created globally within libev
>     (Michael Laing)
> -   2649b31 - Move badge \[skip ci\] (Gavin M. Roy)
> -   f70eea1 - Remove pypy and installation attempt of pyev (Gavin M.
>     Roy)
> -   f32e522 - Conditionally skip external connection adapters if lib
>     is not installed (Gavin M. Roy)
> -   cce97c5 - Only install pyev on python 2.7 (Gavin M. Roy)
> -   ff84462 - Add travis ci support (Gavin M. Roy)
> -   cf971da - lib_evconnection: improve signal handling; add callback
>     (Michael Laing)
> -   9adb269 - bugfix in returning a list in Py3k (Alex Chandel)
> -   c41d5b9 - update exception syntax for Py3k (Alex Chandel)
> -   c8506f1 - fix \_adapter_connect (Michael Laing)
> -   67cb660 - Add LibevConnection to README (Michael Laing)
> -   1f9e72b - Propagate low-level connection errors to the
>     AMQPConnectionError. (Bjorn Sandberg)
> -   e1da447 - Avoid race condition in \_on_getok on successive
>     basic_get() when clearing out callbacks (Jeff)
> -   7a09979 - Add support for upcoming Connection.Blocked/Unblocked
>     (Gavin M. Roy)
> -   53cce88 - TwistedChannel correctly handles multi-argument
>     deferreds. (eivanov)
> -   66f8ace - Use uuid when creating unique consumer tag (Perttu
>     Ranta-aho)
> -   4ee2738 - Limit the growth of Channel.\_cancelled, use deque
>     instead of list. (Perttu Ranta-aho)
> -   0369aed - fix adapter references and tweak docs (Michael Laing)
> -   1738c23 - retry select.select() on EINTR (Cenk Alti)
> -   1e55357 - libev_connection: reset internal state on reconnect
>     (Michael Laing)
> -   708559e - libev adapter (Michael Laing)
> -   a6b7c8b - Prioritize EPollPoller and KQueuePoller over PollPoller
>     and SelectPoller (Anton Ryzhov)
> -   53400d3 - Handle socket errors in PollPoller and EPollPoller
>     Correctly check \'select.poll\' availability (Anton Ryzhov)
> -   a6dc969 - Use dict.keys & items instead of iterkeys & iteritems
>     (Alex Chandel)
> -   5c1b0d0 - Use print function syntax, in examples (Alex Chandel)
> -   ac9f87a - Fixed a typo in the name of the Asyncore Connection
>     adapter (Guruprasad)
> -   dfbba50 - Fixed bug mentioned in Issue #357 (Erik Andersson)
> -   c906a2d - Drop additional flags when getting info for the
>     hostnames, log errors (#352) (Gavin M. Roy)
> -   baf23dd - retry poll() on EINTR (Cenk Alti)
> -   7cd8762 - Address ticket #352 catching an error when
>     socket.getprotobyname fails (Gavin M. Roy)
> -   6c3ec75 - Prep for 0.9.14 (Gavin M. Roy)
> -   dae7a99 - Bump to 0.9.14p0 (Gavin M. Roy)
> -   620edc7 - Use default port and virtual host if omitted in
>     URLParameters (Issue #342) (Gavin M. Roy)
> -   42a8787 - Move the exception handling inside the while loop
>     (Gavin M. Roy)
> -   10e0264 - Fix connection back pressure detection issue #347
>     (Gavin M. Roy)
> -   0bfd670 - Fixed mistake in commit 3a19d65. (Erik Andersson)
> -   da04bc0 - Fixed Unknown state on disconnect error message
>     generated when closing connections. (Erik Andersson)
> -   3a19d65 - Alternative solution to fix #345. (Erik Andersson)
> -   abf9fa8 - switch to sendall to send entire frame (Dustin Koupal)
> -   9ce8ce4 - Fixed the async publisher example to work with
>     reconnections (Raphaël De Giusti)
> -   511028a - Fix typo in TwistedChannel docstring (cacovsky)
> -   8b69e5a - calls self.\_adapter_disconnect() instead of
>     self.disconnect() which doesn\'t actually exist #294 (Mark
>     Unsworth)
> -   06a5cf8 - add NullHandler to prevent logging warnings (Cenk Alti)
> -   f404a9a - Fix #337 cannot start ioloop after stop (Ralf Nyren)

## 0.9.13 - 2013-05-15

[0.9.13](https://github.com/pika/pika/compare/0.9.12...0.9.13)

**Major Changes**

-   IPv6 Support with thanks to Alessandro Tagliapietra for initial
    prototype
-   Officially remove support for \<= Python 2.5 even though it was
    broken already
-   Drop pika.simplebuffer.SimpleBuffer in favor of the Python stdlib
    collections.deque object
-   New default object for receiving content is a \"bytes\" object which
    is a str wrapper in Python 2, but paves way for Python 3 support
-   New \"Raw\" mode for frame decoding content frames (#334) addresses
    issues #331, #229 added by Garth Williamson
-   Connection and Disconnection logic refactored, allowing for cleaner
    separation of protocol logic and socket handling logic as well as
    connection state management
-   New \"on_open_error_callback\" argument in creating connection
    objects and new Connection.add_on_open_error_callback method
-   New Connection.connect method to cleanly allow for reconnection code
-   Support for all AMQP field types, using protocol specified
    signed/unsigned unpacking

**Backwards Incompatible Changes**

-   Method signature for creating connection objects has new argument
    \"on_open_error_callback\" which is positionally before
    \"on_close_callback\"
-   Internal callback variable names in connection.Connection have been
    renamed and constants used. If you relied on any of these callbacks
    outside of their internal use, make sure to check out the new
    constants.
-   Connection.\_connect method, which was an internal only method is
    now deprecated and will raise a DeprecationWarning. If you relied on
    this method, your code needs to change.
-   pika.simplebuffer has been removed

**Bugfixes**

-   BlockingConnection consumer generator does not free buffer when
    exited (#328)
-   Unicode body payloads in the blocking adapter raises exception
    (#333)
-   Support \"b\" short-short-int AMQP data type (#318)
-   Docstring type fix in adapters/select_connection (#316) fix by
    Rikard Hultén
-   IPv6 not supported (#309)
-   Stop the HeartbeatChecker when connection is closed (#307)
-   Unittest fix for SelectConnection (#336) fix by Erik Andersson
-   Handle condition where no connection or socket exists but
    SelectConnection needs a timeout for retrying a connection (#322)
-   TwistedAdapter lagging behind BaseConnection changes (#321) fix by
    Jan Urbański

**Other**

-   Refactored documentation
-   Added Twisted Adapter example (#314) by nolinksoft

## 0.9.12 - 2013-03-18

[0.9.12](https://github.com/pika/pika/compare/0.9.11...0.9.12)

**Bugfixes**

-   New timeout id hashing was not unique

## 0.9.11 - 2013-03-17

[0.9.11](https://github.com/pika/pika/compare/0.9.10...0.9.11)

**Bugfixes**

-   Address inconsistent channel close callback documentation and add
    the signature change to the TwistedChannel class (#305)
-   Address a missed timeout related internal data structure name change
    introduced in the SelectConnection 0.9.10 release. Update all
    connection adapters to use same signature and docstring (#306).

## 0.9.10 - 2013-03-16

[0.9.10](https://github.com/pika/pika/compare/0.9.9...0.9.10)

**Bugfixes**

-   Fix timeout in twisted adapter (Submitted by cellscape)
-   Fix blocking_connection poll timer resolution to milliseconds
    (Submitted by cellscape)
-   Fix channel.\_on_close() without a method frame (Submitted by
    Richard Boulton)
-   Addressed exception on close (Issue #279 - fix by patcpsc)
-   \'messages\' not initialized in BlockingConnection.cancel() (Issue
    #289 - fix by Mik Kocikowski)
-   Make queue_unbind behave like queue_bind (Issue #277)
-   Address closing behavioral issues for connections and channels
    (Issue #275)
-   Pass a Method frame to Channel.\_on_close in
    Connection.\_on_disconnect (Submitted by Jan Urbański)
-   Fix channel closed callback signature in the Twisted adapter
    (Submitted by Jan Urbański)
-   Don\'t stop the IOLoop on connection close for in the Twisted
    adapter (Submitted by Jan Urbański)
-   Update the asynchronous examples to fix reconnecting and have it
    work
-   Warn if the socket was closed such as if RabbitMQ dies without a
    Close frame
-   Fix URLParameters ssl_options (Issue #296)
-   Add state to BlockingConnection addressing (Issue #301)
-   Encode unicode body content prior to publishing (Issue #282)
-   Fix an issue with unicode keys in BasicProperties headers key (Issue
    #280)
-   Change how timeout ids are generated (Issue #254)
-   Address post close state issues in Channel (Issue #302)

\*\* Behavior changes \*\*

-   Change core connection communication behavior to prefer outbound
    writes over reads, addressing a recursion issue
-   Update connection on close callbacks, changing callback method
    signature
-   Update channel on close callbacks, changing callback method
    signature
-   Give more info in the ChannelClosed exception
-   Change the constructor signature for BlockingConnection, block
    open/close callbacks
-   Disable the use of add_on_open_callback/add_on_close_callback
    methods in BlockingConnection

## 0.9.9 - 2013-01-29

[0.9.9](https://github.com/pika/pika/compare/0.9.8...0.9.9)

**Bugfixes**

-   Only remove the tornado_connection.TornadoConnection file descriptor
    from the IOLoop if it\'s still open (Issue #221)
-   Allow messages with no body (Issue #227)
-   Allow for empty routing keys (Issue #224)
-   Don\'t raise an exception when trying to send a frame to a closed
    connection (Issue #229)
-   Only send a Connection.CloseOk if the connection is still open.
    (Issue #236 - Fix by noleaf)
-   Fix timeout threshold in blocking connection - (Issue #232 - Fix by
    Adam Flynn)
-   Fix closing connection while a channel is still open (Issue #230 -
    Fix by Adam Flynn)
-   Fixed misleading warning and exception messages in BaseConnection
    (Issue #237 - Fix by Tristan Penman)
-   Pluralised and altered the wording of the AMQPConnectionError
    exception (Issue #237 - Fix by Tristan Penman)
-   Fixed \_adapter_disconnect in TornadoConnection class (Issue #237 -
    Fix by Tristan Penman)
-   Fixing hang when closing connection without any channel in
    BlockingConnection (Issue #244 - Fix by Ales Teska)
-   Remove the process_timeouts() call in SelectConnection (Issue #239)
-   Change the string validation to basestring for host connection
    parameters (Issue #231)
-   Add a poller to the BlockingConnection to address latency issues
    introduced in Pika 0.9.8 (Issue #242)
-   reply_code and reply_text is not set in ChannelException (Issue
    #250)
-   Add the missing constraint parameter for Channel.\_on_return
    callback processing (Issue #257 - Fix by patcpsc)
-   Channel callbacks not being removed from callback manager when
    channel is closed or deleted (Issue #261)

## 0.9.8 - 2012-11-18

[0.9.8](https://github.com/pika/pika/compare/0.9.7...0.9.8)

**Bugfixes**

-   Channel.queue_declare/BlockingChannel.queue_declare not setting up
    callbacks property for empty queue name (Issue #218)
-   Channel.queue_bind/BlockingChannel.queue_bind not allowing empty
    routing key
-   Connection.\_on_connection_closed calling wrong method in Channel
    (Issue #219)
-   Fix tx_commit and tx_rollback bugs in BlockingChannel (Issue #217)

## 0.9.7 - 2012-11-11

[0.9.7](https://github.com/pika/pika/compare/0.9.6...0.9.7)

**New features**

-   generator based consumer in BlockingChannel (See
    `examples/blocking_consumer_generator`{.interpreted-text role="doc"}
    for example)

**Changes**

-   BlockingChannel.\_send_method will only wait if explicitly told to

**Bugfixes**

-   Added the exchange \"type\" parameter back but issue a
    DeprecationWarning
-   Dont require a queue name in Channel.queue_declare()
-   Fixed KeyError when processing timeouts (Issue \# 215 - Fix by
    Raphael De Giusti)
-   Don\'t try and close channels when the connection is closed (Issue
    #216 - Fix by Charles Law)
-   Dont raise UnexpectedFrame exceptions, log them instead
-   Handle multiple synchronous RPC calls made without waiting for the
    call result (Issues #192, #204, #211)
-   Typo in docs (Issue #207 Fix by Luca Wehrstedt)
-   Only sleep on connection failure when retry attempts are \> 0 (Issue
    #200)
-   Bypass \_rpc method and just send frames for Basic.Ack, Basic.Nack,
    Basic.Reject (Issue #205)

## 0.9.6 - 2012-10-29

[0.9.6](https://github.com/pika/pika/compare/0.9.5...0.9.6)

**New features**

-   URLParameters
-   BlockingChannel.start_consuming() and
    BlockingChannel.stop_consuming()
-   Delivery Confirmations
-   Improved unittests

**Major bugfix areas**

-   Connection handling
-   Blocking functionality in the BlockingConnection
-   SSL
-   UTF-8 Handling

**Removals**

-   pika.reconnection_strategies
-   pika.channel.ChannelTransport
-   pika.log
-   pika.template
-   examples directory

## 0.9.5 - 2011-03-29

[0.9.5](https://github.com/pika/pika/compare/0.9.4...0.9.5)

**Changelog**

-   Scope changes with adapter IOLoops and CallbackManager allowing for
    cleaner, multi-threaded operation
-   Add support for Confirm.Select with
    channel.Channel.confirm_delivery()
-   Add examples of delivery confirmation to examples
    (demo_send_confirmed.py)
-   Update uses of log.warn with warning.warn for TCP Back-pressure
    alerting
-   License boilerplate updated to simplify license text in source files
-   Increment the timeout in select_connection.SelectPoller reducing CPU
    utilization
-   Bug fix in Heartbeat frame delivery addressing issue #35
-   Remove abuse of pika.log.method_call through a majority of the code
-   Rename of key modules: table to data, frames to frame
-   Cleanup of frame module and related classes
-   Restructure of tests and test runner
-   Update functional tests to respect RABBITMQ_HOST, RABBITMQ_PORT
    environment variables
-   Bug fixes to reconnection_strategies module
-   Fix the scale of timeout for PollPoller to be specified in
    milliseconds
-   Remove mutable default arguments in RPC calls
-   Add data type validation to RPC calls
-   Move optional credentials erasing out of connection.Connection into
    credentials module
-   Add support to allow for additional external credential types
-   Add a NullHandler to prevent the \'No handlers could be found for
    logger \"pika\"\' error message when not using pika.log in a client
    app at all.
-   Clean up all examples to make them easier to read and use
-   Move documentation into its own repository
    <https://github.com/pika/documentation>
-   channel.py
    -   Move channel.MAX_CHANNELS constant from connection.CHANNEL_MAX
    -   Add default value of None to ChannelTransport.rpc
    -   Validate callback and acceptable replies parameters in
        ChannelTransport.RPC
    -   Remove unused connection attribute from Channel
-   connection.py
    -   Remove unused import of struct
    -   Remove direct import of pika.credentials.PlainCredentials
        -   Change to import pika.credentials
    -   Move CHANNEL_MAX to channel.MAX_CHANNELS
    -   Change ConnectionParameters initialization parameter heartbeat
        to boolean
    -   Validate all inbound parameter types in ConnectionParameters
    -   Remove the Connection.\_erase_credentials stub method in favor
        of letting the Credentials object deal with that itself.
    -   Warn if the credentials object intends on erasing the
        credentials and a reconnection strategy other than
        NullReconnectionStrategy is specified.
    -   Change the default types for callback and acceptable_replies in
        Connection.\_rpc
    -   Validate the callback and acceptable_replies data types in
        Connection.\_rpc
-   adapters.blocking_connection.BlockingConnection
    -   Addition of \_adapter_disconnect to
        blocking_connection.BlockingConnection
    -   Add timeout methods to BlockingConnection addressing issue #41
    -   BlockingConnection didn\'t allow you register more than one
        consumer callback because basic_consume was overridden to block
        immediately. New behavior allows you to do so.
    -   Removed overriding of base basic_consume and basic_cancel
        methods. Now uses underlying Channel versions of those methods.
    -   Added start_consuming() method to BlockingChannel to start the
        consumption loop.
    -   Updated stop_consuming() to iterate through all the registered
        consumers in self.\_consumers and issue a basic_cancel.
