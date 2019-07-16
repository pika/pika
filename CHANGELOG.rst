Version History
===============

1.1.0 2019-07-16
----------------

`GitHub milestone <https://github.com/pika/pika/milestone/16?closed=1>`_

1.0.1 2019-04-12
----------------

`GitHub milestone <https://github.com/pika/pika/milestone/15?closed=1>`_

- API docstring updates
- Twisted adapter: Add basic_consume Deferred to the call list (`PR <https://github.com/pika/pika/pull/1202>`_)

1.0.0 2019-03-26
----------------

`GitHub milestone <https://github.com/pika/pika/milestone/8?closed=1>`_

- ``AsyncioConnection``, ``TornadoConnection`` and ``TwistedProtocolConnection`` are no longer auto-imported (`PR <https://github.com/pika/pika/pull/1129>`_)
- ``BlockingConnection.consume`` now returns ``(None, None, None)`` when inactivity timeout is reached (`PR <https://github.com/pika/pika/pull/899>`_)
- Python 3.7 support (`Issue <https://github.com/pika/pika/issues/1107>`_)
- ``all_channels`` parameter of the ``Channel.basic_qos`` method renamed to ``global_qos``
- ``global_`` parameter of the ``Basic.Qos`` spec class renamed to ``global_qos``
- **NOTE:** ``heartbeat_interval`` is removed, use ``heartbeat`` instead.
- **NOTE:** The `backpressure_detection` option of `ConnectionParameters` and `URLParameters` property is REMOVED in favor of `Connection.Blocked` and `Connection.Unblocked`. See `Connection.add_on_connection_blocked_callback`.
- **NOTE:** The legacy ``basic_publish`` method is removed, and ``publish`` renamed to ``basic_publish``
- **NOTE**: The signature of the following methods has changed from Pika 0.13.0. In general, the callback parameter that indicates completion of the method has been moved to the end of the parameter list to be consistent with other parts of Pika's API and with other libraries in general.

**IMPORTANT**: The signature of the following methods has changed from Pika 0.13.0. In general, the callback parameter that indicates completion of the method has been moved to the end of the parameter list to be consistent with other parts of Pika's API and with other libraries in general.

- ``basic_cancel``
- ``basic_consume``
- ``basic_get``
- ``basic_qos``
- ``basic_recover``
- ``confirm_delivery``
- ``exchange_bind``
- ``exchange_declare``
- ``exchange_delete``
- ``exchange_unbind``
- ``flow``
- ``queue_bind``
- ``queue_declare``
- ``queue_delete``
- ``queue_purge``
- ``queue_unbind``

**IMPORTANT**: When specifying TLS / SSL options, the ``SSLOptions`` class must be used, and a ``dict`` is no longer supported.

0.13.1 2019-02-04
-----------------

`GitHub milestone <https://github.com/pika/pika/milestone/14>`_

0.13.0 2019-01-17
-----------------

`GitHub milestone <https://github.com/pika/pika/milestone/13>`_

0.12.0 2018-06-19
-----------------

`GitHub milestone <https://github.com/pika/pika/milestone/12>`_

This is an interim release prior to version `1.0.0`. It includes the following backported pull requests and commits from the `master` branch:

- `PR #908 <https://github.com/pika/pika/pull/908>`_
- `PR #910 <https://github.com/pika/pika/pull/910>`_
- `PR #918 <https://github.com/pika/pika/pull/918>`_
- `PR #920 <https://github.com/pika/pika/pull/920>`_
- `PR #924 <https://github.com/pika/pika/pull/924>`_
- `PR #937 <https://github.com/pika/pika/pull/937>`_
- `PR #938 <https://github.com/pika/pika/pull/938>`_
- `PR #933 <https://github.com/pika/pika/pull/933>`_
- `PR #940 <https://github.com/pika/pika/pull/940>`_
- `PR #932 <https://github.com/pika/pika/pull/932>`_
- `PR #928 <https://github.com/pika/pika/pull/928>`_
- `PR #934 <https://github.com/pika/pika/pull/934>`_
- `PR #915 <https://github.com/pika/pika/pull/915>`_
- `PR #946 <https://github.com/pika/pika/pull/946>`_
- `PR #947 <https://github.com/pika/pika/pull/947>`_
- `PR #952 <https://github.com/pika/pika/pull/952>`_
- `PR #956 <https://github.com/pika/pika/pull/956>`_
- `PR #966 <https://github.com/pika/pika/pull/966>`_
- `PR #975 <https://github.com/pika/pika/pull/975>`_
- `PR #978 <https://github.com/pika/pika/pull/978>`_
- `PR #981 <https://github.com/pika/pika/pull/981>`_
- `PR #994 <https://github.com/pika/pika/pull/994>`_
- `PR #1007 <https://github.com/pika/pika/pull/1007>`_
- `PR #1045 <https://github.com/pika/pika/pull/1045>`_ (manually backported)
- `PR #1011 <https://github.com/pika/pika/pull/1011>`_

Commits:

Travis CI fail fast - 3f0e739

New features:

`BlockingConnection` now supports the `add_callback_threadsafe` method which allows a function to be executed correctly on the IO loop thread. The main use-case for this is as follows:

- Application sets up a thread for `BlockingConnection` and calls `basic_consume` on it
- When a message is received, work is done on another thread
- When the work is done, the worker uses `connection.add_callback_threadsafe` to call the `basic_ack` method on the channel instance.

Please see `examples/basic_consumer_threaded.py` for an example. As always, `SelectConnection` and a fully async consumer/publisher is the preferred method of using Pika.

Heartbeats are now sent at an interval equal to 1/2 of the negotiated idle connection timeout. RabbitMQ's default timeout value is 60 seconds, so heartbeats will be sent at a 30 second interval. In addition, Pika's check for an idle connection will be done at an interval equal to the timeout value plus 5 seconds to allow for delays. This results in an interval of 65 seconds by default.

0.11.2 2017-11-30
-----------------

`GitHub milestone <https://github.com/pika/pika/milestone/11>`_

`0.11.2 <https://github.com/pika/pika/compare/0.11.1...0.11.2>`_

- Remove `+` character from platform releases string (`PR <https://github.com/pika/pika/pull/895>`_)

0.11.1 2017-11-27
-----------------

`GitHub milestone <https://github.com/pika/pika/milestone/10>`_

`0.11.1 <https://github.com/pika/pika/compare/0.11.0...0.11.1>`_

- Fix `BlockingConnection` to ensure event loop exits (`PR <https://github.com/pika/pika/pull/887>`_)
- Heartbeat timeouts will use the client value if specified (`PR <https://github.com/pika/pika/pull/874>`_)
- Allow setting some common TCP options (`PR <https://github.com/pika/pika/pull/880>`_)
- Errors when decoding Unicode are ignored (`PR <https://github.com/pika/pika/pull/890>`_)
- Fix large number encoding (`PR <https://github.com/pika/pika/pull/888>`_)

0.11.0 2017-07-29
-----------------

`GitHub milestone <https://github.com/pika/pika/milestone/9>`_

`0.11.0 <https://github.com/pika/pika/compare/0.10.0...0.11.0>`_

 - Simplify Travis CI configuration for OS X.
 - Add `asyncio` connection adapter for Python 3.4 and newer.
 - Connection failures that occur after the socket is opened and before the
   AMQP connection is ready to go are now reported by calling the connection
   error callback.  Previously these were not consistently reported.
 - In BaseConnection.close, call _handle_ioloop_stop only if the connection is
   already closed to allow the asynchronous close operation to complete
   gracefully.
 - Pass error information from failed socket connection to user callbacks
   on_open_error_callback and on_close_callback with result_code=-1.
 - ValueError is raised when a completion callback is passed to an asynchronous
   (nowait) Channel operation. It's an application error to pass a non-None
   completion callback with an asynchronous request, because this callback can
   never be serviced in the asynchronous scenario.
 - `Channel.basic_reject` fixed to allow `delivery_tag` to be of type `long`
   as well as `int`. (by quantum5)
 - Implemented support for blocked connection timeouts in
   `pika.connection.Connection`. This feature is available to all pika adapters.
   See `pika.connection.ConnectionParameters` docstring to learn more about
   `blocked_connection_timeout` configuration.
 - Deprecated the `heartbeat_interval` arg in `pika.ConnectionParameters` in
   favor of the `heartbeat` arg for consistency with the other connection
   parameters classes `pika.connection.Parameters` and `pika.URLParameters`.
 - When the `port` arg is not set explicitly in `ConnectionParameters`
   constructor, but the `ssl` arg is set explicitly, then set the port value to
   to the default AMQP SSL port if SSL is enabled, otherwise to the default
   AMQP plaintext port.
 - `URLParameters` will raise ValueError if a non-empty URL scheme other than
   {amqp | amqps | http | https} is specified.
 - `InvalidMinimumFrameSize` and `InvalidMaximumFrameSize` exceptions are
   deprecated. pika.connection.Parameters.frame_max property setter now raises
   the standard `ValueError` exception when the value is out of bounds.
 - Removed deprecated parameter `type` in `Channel.exchange_declare` and
   `BlockingChannel.exchange_declare` in favor of the `exchange_type` arg that
   doesn't overshadow the builtin `type` keyword.
 - Channel.close() on OPENING channel transitions it to CLOSING instead of
   raising ChannelClosed.
 - Channel.close() on CLOSING channel raises `ChannelAlreadyClosing`; used to
   raise `ChannelClosed`.
 - Connection.channel() raises `ConnectionClosed` if connection is not in OPEN
   state.
 - When performing graceful close on a channel and `Channel.Close` from broker
   arrives while waiting for CloseOk, don't release the channel number until
   CloseOk arrives to avoid race condition that may lead to a new channel
   receiving the CloseOk that was destined for the closing channel.
 - The `backpressure_detection` option of `ConnectionParameters` and
   `URLParameters` property is DEPRECATED in favor of `Connection.Blocked` and
   `Connection.Unblocked`. See `Connection.add_on_connection_blocked_callback`.

0.10.0 2015-09-02
-----------------

`0.10.0 <https://github.com/pika/pika/compare/0.9.14...0.10.0>`_

 - a9bf96d - LibevConnection: Fixed dict chgd size during iteration (Michael Laing)
 - 388c55d - SelectConnection: Fixed KeyError exceptions in IOLoop timeout executions (Shinji Suzuki)
 - 4780de3 - BlockingConnection: Add support to make BlockingConnection a Context Manager (@reddec)

0.10.0b2 2015-07-15
-------------------

 - f72b58f - Fixed failure to purge _ConsumerCancellationEvt from BlockingChannel._pending_events during basic_cancel. (Vitaly Kruglikov)

0.10.0b1 2015-07-10
-------------------

High-level summary of notable changes:

- Change to 3-Clause BSD License
- Python 3.x support
- Over 150 commits from 19 contributors
- Refactoring of SelectConnection ioloop
- This major release contains certain non-backward-compatible API changes as
  well as significant performance improvements in the `BlockingConnection`
  adapter.
- Non-backward-compatible changes in `Channel.add_on_return_callback` callback's
  signature.
- The `AsyncoreConnection` adapter was retired

**Details**

Python 3.x: this release introduces python 3.x support. Tested on Python 3.3
and 3.4.

`AsyncoreConnection`: Retired this legacy adapter to reduce maintenance burden;
the recommended replacement is the `SelectConnection` adapter.

`SelectConnection`: ioloop was refactored for compatibility with other ioloops.

`Channel.add_on_return_callback`: The callback is now passed the individual
parameters channel, method, properties, and body instead of a tuple of those
values for congruence with other similar callbacks.

`BlockingConnection`: This adapter underwent a makeover under the hood and
gained significant performance improvements as well as enhanced timer
resolution. It is now implemented as a client of the `SelectConnection` adapter.

Below is an overview of the `BlockingConnection` and `BlockingChannel` API
changes:

  - Recursion: the new implementation eliminates callback recursion that
    sometimes blew out the stack in the legacy implementation (e.g.,
    publish -> consumer_callback -> publish -> consumer_callback, etc.). While
    `BlockingConnection.process_data_events` and `BlockingConnection.sleep` may
    still be called from the scope of the blocking adapter's callbacks in order
    to process pending I/O, additional callbacks will be suppressed whenever
    `BlockingConnection.process_data_events` and `BlockingConnection.sleep` are
    nested in any combination; in that case, the callback information will be
    bufferred and dispatched once nesting unwinds and control returns to the
    level-zero dispatcher.
  - `BlockingConnection.connect`: this method was removed in favor of the
    constructor as the only way to establish connections; this reduces
    maintenance burden, while improving reliability of the adapter.
  - `BlockingConnection.process_data_events`: added the optional parameter
    `time_limit`.
  - `BlockingConnection.add_on_close_callback`: removed; legacy raised
    `NotImplementedError`.
  - `BlockingConnection.add_on_open_callback`: removed; legacy raised
    `NotImplementedError`.
  - `BlockingConnection.add_on_open_error_callback`: removed; legacy raised
    `NotImplementedError`.
  - `BlockingConnection.add_backpressure_callback`: not supported
  - `BlockingConnection.set_backpressure_multiplier`: not supported
  - `BlockingChannel.add_on_flow_callback`: not supported; per docstring in
    channel.py: "Note that newer versions of RabbitMQ will not issue this but
    instead use TCP backpressure".
  - `BlockingChannel.flow`: not supported
  - `BlockingChannel.force_data_events`: removed as it is no longer necessary
    following redesign of the adapter.
  - Removed the `nowait` parameter from `BlockingChannel` methods, forcing
    `nowait=False` (former API default) in the implementation; this is more
    suitable for the blocking nature of the adapter and its error-reporting
    strategy; this concerns the following methods: `basic_cancel`,
    `confirm_delivery`, `exchange_bind`, `exchange_declare`, `exchange_delete`,
    `exchange_unbind`, `queue_bind`, `queue_declare`, `queue_delete`, and
    `queue_purge`.
  - `BlockingChannel.basic_cancel`: returns a sequence instead of None; for a
    `no_ack=True` consumer, `basic_cancel` returns a sequence of pending
    messages that arrived before broker confirmed the cancellation.
  - `BlockingChannel.consume`: added new optional kwargs `arguments` and
    `inactivity_timeout`. Also, raises ValueError if the consumer creation
    parameters don't match those used to create the existing queue consumer
    generator, if any; this happens when you break out of the consume loop, then
    call `BlockingChannel.consume` again with different consumer-creation args
    without first cancelling the previous queue consumer generator via
    `BlockingChannel.cancel`. The legacy implementation would silently resume
    consuming from the existing queue consumer generator even if the subsequent
    `BlockingChannel.consume` was invoked with a different queue name, etc.
  - `BlockingChannel.cancel`: returns 0; the legacy implementation tried to
    return the number of requeued messages, but this number was not accurate
    as it didn't include the messages returned by the Channel class; this count
    is not generally useful, so returning 0 is a reasonable replacement.
  - `BlockingChannel.open`: removed in favor of having a single mechanism for
    creating a channel (`BlockingConnection.channel`); this reduces maintenance
    burden, while improving reliability of the adapter.
  - `BlockingChannel.confirm_delivery`: raises UnroutableError when unroutable
    messages that were sent prior to this call are returned before we receive
    Confirm.Select-ok.
  - `BlockingChannel.basic_publish: always returns True when delivery
    confirmation is not enabled (publisher-acks = off); the legacy implementation
    returned a bool in this case if `mandatory=True` to indicate whether the
    message was delivered; however, this was non-deterministic, because
    Basic.Return is asynchronous and there is no way to know how long to wait
    for it or its absence. The legacy implementation returned None when
    publishing with publisher-acks = off and `mandatory=False`. The new
    implementation always returns True when publishing while
    publisher-acks = off.
  - `BlockingChannel.publish`: a new alternate method (vs. `basic_publish`) for
     publishing a message with more detailed error reporting via UnroutableError
     and NackError exceptions.
  - `BlockingChannel.start_consuming`: raises pika.exceptions.RecursionError if
    called from the scope of a `BlockingConnection` or `BlockingChannel`
    callback.
  - `BlockingChannel.get_waiting_message_count`: new method; returns the number
    of messages that may be retrieved from the current queue consumer generator
    via `BasicChannel.consume` without blocking.

**Commits**

 - 5aaa753 - Fixed SSL import and removed no_ack=True in favor of explicit AMQP message handling based on deferreds (skftn)
 - 7f222c2 - Add checkignore for codeclimate (Gavin M. Roy)
 - 4dec370 - Implemented BlockingChannel.flow; Implemented BlockingConnection.add_on_connection_blocked_callback; Implemented BlockingConnection.add_on_connection_unblocked_callback. (Vitaly Kruglikov)
 - 4804200 - Implemented blocking adapter acceptance test for exchange-to-exchange binding. Added rudimentary validation of BasicProperties passthru in blocking adapter publish tests. Updated CHANGELOG. (Vitaly Kruglikov)
 - 4ec07fd - Fixed sending of data in TwistedProtocolConnection (Vitaly Kruglikov)
 - a747fb3 - Remove my copyright from forward_server.py test utility. (Vitaly Kruglikov)
 - 94246d2 - Return True from basic_publish when pubacks is off. Implemented more blocking adapter accceptance tests. (Vitaly Kruglikov)
 - 3ce013d - PIKA-609 Wait for broker to dispatch all messages to client before cancelling consumer in TestBasicCancelWithNonAckableConsumer and TestBasicCancelWithAckableConsumer (Vitaly Kruglikov)
 - 293f778 - Created CHANGELOG entry for release 0.10.0. Fixed up callback documentation for basic_get, basic_consume, and add_on_return_callback. (Vitaly Kruglikov)
 - 16d360a - Removed the legacy AsyncoreConnection adapter in favor of the recommended SelectConnection adapter. (Vitaly Kruglikov)
 - 240a82c - Defer creation of poller's event loop interrupt socket pair until start is called, because some SelectConnection users (e.g., BlockingConnection adapter) don't use the event loop, and these sockets would just get reported as resource leaks. (Vitaly Kruglikov)
 - aed5cae - Added EINTR loops in select_connection pollers. Addressed some pylint findings, including an error or two. Wrap socket.send and socket.recv calls in EINTR loops Use the correct exception for socket.error and select.error and get errno depending on python version. (Vitaly Kruglikov)
 - 498f1be - Allow passing exchange, queue and routing_key as text, handle short strings as text in python3 (saarni)
 - 9f7f243 - Restored basic_consume, basic_cancel, and add_on_cancel_callback (Vitaly Kruglikov)
 - 18c9909 - Reintroduced BlockingConnection.process_data_events. (Vitaly Kruglikov)
 - 4b25cb6 - Fixed BlockingConnection/BlockingChannel acceptance and unit tests (Vitaly Kruglikov)
 - bfa932f - Facilitate proper connection state after BasicConnection._adapter_disconnect (Vitaly Kruglikov)
 - 9a09268 - Fixed BlockingConnection test that was failing with ConnectionClosed error. (Vitaly Kruglikov)
 - 5a36934 - Copied synchronous_connection.py from pika-synchronous branch Fixed pylint findings Integrated SynchronousConnection with the new ioloop in SelectConnection Defined dedicated message classes PolledMessage and ConsumerMessage and moved from BlockingChannel to module-global scope. Got rid of nowait args from BlockingChannel public API methods Signal unroutable messages via UnroutableError exception. Signal Nack'ed messages via NackError exception. These expose more information about the failure than legacy basic_publich API. Removed set_timeout and backpressure callback methods Restored legacy `is_open`, etc. property names (Vitaly Kruglikov)
 - 6226dc0 - Remove deprecated --use-mirrors (Gavin M. Roy)
 - 1a7112f - Raise ConnectionClosed when sending a frame with no connection (#439) (Gavin M. Roy)
 - 9040a14 - Make delivery_tag non-optional (#498) (Gavin M. Roy)
 - 86aabc2 - Bump version (Gavin M. Roy)
 - 562075a - Update a few testing things (Gavin M. Roy)
 - 4954d38 - use unicode_type in blocking_connection.py (Antti Haapala)
 - 133d6bc - Let Travis install ordereddict for Python 2.6, and ttest 3.3, 3.4 too. (Antti Haapala)
 - 0d2287d - Pika Python 3 support (Antti Haapala)
 - 3125c79 - SSLWantRead is not supported before python 2.7.9 and 3.3 (Will)
 - 9a9c46c - Fixed TestDisconnectDuringConnectionStart: it turns out that depending on callback order, it might get either ProbableAuthenticationError or ProbableAccessDeniedError. (Vitaly Kruglikov)
 - cd8c9b0 - A fix the write starvation problem that we see with tornado and pika (Will)
 - 8654fbc - SelectConnection - make interrupt socketpair non-blocking (Will)
 - 4f3666d - Added copyright in forward_server.py and fixed NameError bug (Vitaly Kruglikov)
 - f8ebbbc - ignore docs (Gavin M. Roy)
 - a344f78 - Updated codeclimate config (Gavin M. Roy)
 - 373c970 - Try and fix pathing issues in codeclimate (Gavin M. Roy)
 - 228340d - Ignore codegen (Gavin M. Roy)
 - 4db0740 - Add a codeclimate config (Gavin M. Roy)
 - 7e989f9 - Slight code re-org, usage comment and better naming of test file. (Will)
 - 287be36 - Set up _kqueue member of KQueuePoller before calling super constructor to avoid exception due to missing _kqueue member. Call `self._map_event(event)` instead of `self._map_event(event.filter)`, because `KQueuePoller._map_event()` assumes it's getting an event, not an event filter. (Vitaly Kruglikov)
 - 62810fb - Fix issue #412: reset BlockingConnection._read_poller in BlockingConnection._adapter_disconnect() to guard against accidental access to old file descriptor. (Vitaly Kruglikov)
 - 03400ce - Rationalise adapter acceptance tests (Will)
 - 9414153 - Fix bug selecting non epoll poller (Will)
 - 4f063df - Use user heartbeat setting if server proposes none (Pau Gargallo)
 - 9d04d6e - Deactivate heartbeats when heartbeat_interval is 0 (Pau Gargallo)
 - a52a608 - Bug fix and review comments. (Will)
 - e3ebb6f - Fix incorrect x-expires argument in acceptance tests (Will)
 - 294904e - Get BlockingConnection into consistent state upon loss of TCP/IP connection with broker and implement acceptance tests for those cases. (Vitaly Kruglikov)
 - 7f91a68 - Make SelectConnection behave like an ioloop (Will)
 - dc9db2b - Perhaps 5 seconds is too agressive for travis (Gavin M. Roy)
 - c23e532 - Lower the stuck test timeout (Gavin M. Roy)
 - 1053ebc - Late night bug (Gavin M. Roy)
 - cd6c1bf - More BaseConnection._handle_error cleanup (Gavin M. Roy)
 - a0ff21c - Fix the test to work with Python 2.6 (Gavin M. Roy)
 - 748e8aa - Remove pypy for now (Gavin M. Roy)
 - 1c921c1 - Socket close/shutdown cleanup (Gavin M. Roy)
 - 5289125 - Formatting update from PR (Gavin M. Roy)
 - d235989 - Be more specific when calling getaddrinfo (Gavin M. Roy)
 - b5d1b31 - Reflect the method name change in pika.callback (Gavin M. Roy)
 - df7d3b7 - Cleanup BlockingConnection in a few places (Gavin M. Roy)
 - cd99e1c - Rename method due to use in BlockingConnection (Gavin M. Roy)
 - 7e0d1b3 - Use google style with yapf instead of pep8 (Gavin M. Roy)
 - 7dc9bab - Refactor socket writing to not use sendall #481 (Gavin M. Roy)
 - 4838789 - Dont log the fd #521 (Gavin M. Roy)
 - 765107d - Add Connection.Blocked callback registration methods #476 (Gavin M. Roy)
 - c15b5c1 - Fix _blocking typo pointed out in #513 (Gavin M. Roy)
 - 759ac2c - yapf of codegen (Gavin M. Roy)
 - 9dadd77 - yapf cleanup of codegen and spec (Gavin M. Roy)
 - ddba7ce - Do not reject consumers with no_ack=True #486 #530 (Gavin M. Roy)
 - 4528a1a - yapf reformatting of tests (Gavin M. Roy)
 - e7b6d73 - Remove catching AttributError (#531) (Gavin M. Roy)
 - 41ea5ea - Update README badges [skip ci] (Gavin M. Roy)
 - 6af987b - Add note on contributing (Gavin M. Roy)
 - 161fc0d - yapf formatting cleanup (Gavin M. Roy)
 - edcb619 - Add PYPY to travis testing (Gavin M. Roy)
 - 2225771 - Change the coverage badge (Gavin M. Roy)
 - 8f7d451 - Move to codecov from coveralls (Gavin M. Roy)
 - b80407e - Add confirm_delivery to example (Andrew Smith)
 - 6637212 - Update base_connection.py (bstemshorn)
 - 1583537 - #544 get_waiting_message_count() (markcf)
 - 0c9be99 - Fix #535: pass expected reply_code and reply_text from method frame to Connection._on_disconnect from Connection._on_connection_closed (Vitaly Kruglikov)
 - d11e73f - Propagate ConnectionClosed exception out of BlockingChannel._send_method() and log ConnectionClosed in BlockingConnection._on_connection_closed() (Vitaly Kruglikov)
 - 63d2951 - Fix #541 - make sure connection state is properly reset when BlockingConnection._check_state_on_disconnect raises ConnectionClosed. This supplements the previously-merged PR #450 by getting the connection into consistent state. (Vitaly Kruglikov)
 - 71bc0eb - Remove unused self.fd attribute from BaseConnection (Vitaly Kruglikov)
 - 8c08f93 - PIKA-532 Removed unnecessary params (Vitaly Kruglikov)
 - 6052ecf - PIKA-532 Fix bug in BlockingConnection._handle_timeout that was preventing _on_connection_closed from being called when not closing. (Vitaly Kruglikov)
 - 562aa15 - pika: callback: Display exception message when callback fails. (Stuart Longland)
 - 452995c - Typo fix in connection.py (Andrew)
 - 361c0ad - Added some missing yields (Robert Weidlich)
 - 0ab5a60 - Added complete example for python twisted service (Robert Weidlich)
 - 4429110 - Add deployment and webhooks (Gavin M. Roy)
 - 7e50302 - Fix has_content style in codegen (Andrew Grigorev)
 - 28c2214 - Fix the trove categorization (Gavin M. Roy)
 - de8b545 - Ensure frames can not be interspersed on send (Gavin M. Roy)
 - 8fe6bdd - Fix heartbeat behaviour after connection failure. (Kyösti Herrala)
 - c123472 - Updating BlockingChannel.basic_get doc (it does not receive a callback like the rest of the adapters) (Roberto Decurnex)
 - b5f52fb - Fix number of arguments passed to _on_return callback (Axel Eirola)
 - 765139e - Lower default TIMEOUT to 0.01 (bra-fsn)
 - 6cc22a5 - Fix confirmation on reconnects (bra-fsn)
 - f4faf0a - asynchronous publisher and subscriber examples refactored to follow the StepDown rule (Riccardo Cirimelli)

0.9.14 - 2014-07-11
-------------------

`0.9.14 <https://github.com/pika/pika/compare/0.9.13...0.9.14>`_

 - 57fe43e - fix test to generate a correct range of random ints (ml)
 - 0d68dee - fix async watcher for libev_connection (ml)
 - 01710ad - Use default username and password if not specified in URLParameters (Sean Dwyer)
 - fae328e - documentation typo (Jeff Fein-Worton)
 - afbc9e0 - libev_connection: reset_io_watcher (ml)
 - 24332a2 - Fix the manifest (Gavin M. Roy)
 - acdfdef - Remove useless test (Gavin M. Roy)
 - 7918e1a - Skip libev tests if pyev is not installed or if they are being run in pypy (Gavin M. Roy)
 - bb583bf - Remove the deprecated test (Gavin M. Roy)
 - aecf3f2 - Don't reject a message if the channel is not open (Gavin M. Roy)
 - e37f336 - Remove UTF-8 decoding in spec (Gavin M. Roy)
 - ddc35a9 - Update the unittest to reflect removal of force binary (Gavin M. Roy)
 - fea2476 - PEP8 cleanup (Gavin M. Roy)
 - 9b97956 - Remove force_binary (Gavin M. Roy)
 - a42dd90 - Whitespace required (Gavin M. Roy)
 - 85867ea - Update the content_frame_dispatcher tests to reflect removal of auto-cast utf-8 (Gavin M. Roy)
 - 5a4bd5d - Remove unicode casting (Gavin M. Roy)
 - efea53d - Remove force binary and unicode casting (Gavin M. Roy)
 - e918d15 - Add methods to remove deprecation warnings from asyncore (Gavin M. Roy)
 - 117f62d - Add a coveragerc to ignore the auto generated pika.spec (Gavin M. Roy)
 - 52f4485 - Remove pypy tests from travis for now (Gavin M. Roy)
 - c3aa958 - Update README.rst (Gavin M. Roy)
 - 3e2319f - Delete README.md (Gavin M. Roy)
 - c12b0f1 - Move to RST (Gavin M. Roy)
 - 704f5be - Badging updates (Gavin M. Roy)
 - 7ae33ca - Update for coverage info (Gavin M. Roy)
 - ae7ca86 - add libev_adapter_tests.py; modify .travis.yml to install libev and pyev (ml)
 - f86aba5 - libev_connection: add **kwargs to _handle_event; suppress default_ioloop reuse warning (ml)
 - 603f1cf - async_test_base: add necessary args to _on_cconn_closed (ml)
 - 3422007 - add libev_adapter_tests.py (ml)
 - 6cbab0c - removed relative imports and importing urlparse from urllib.parse for py3+ (a-tal)
 - f808464 - libev_connection: add async watcher; add optional parameters to add_timeout (ml)
 - c041c80 - Remove ev all together for now (Gavin M. Roy)
 - 9408388 - Update the test descriptions and timeout (Gavin M. Roy)
 - 1b552e0 - Increase timeout (Gavin M. Roy)
 - 69a1f46 - Remove the pyev requirement for 2.6 testing (Gavin M. Roy)
 - fe062d2 - Update package name (Gavin M. Roy)
 - 611ad0e - Distribute the LICENSE and README.md (#350) (Gavin M. Roy)
 - df5e1d8 - Ensure that the entire frame is written using socket.sendall (#349) (Gavin M. Roy)
 - 69ec8cf - Move the libev install to before_install (Gavin M. Roy)
 - a75f693 - Update test structure (Gavin M. Roy)
 - 636b424 - Update things to ignore (Gavin M. Roy)
 - b538c68 - Add tox, nose.cfg, update testing config (Gavin M. Roy)
 - a0e7063 - add some tests to increase coverage of pika.connection (Charles Law)
 - c76d9eb - Address issue #459 (Gavin M. Roy)
 - 86ad2db - Raise exception if positional arg for parameters isn't an instance of Parameters (Gavin M. Roy)
 - 14d08e1 - Fix for python 2.6 (Gavin M. Roy)
 - bd388a3 - Use the first unused channel number addressing #404, #460 (Gavin M. Roy)
 - e7676e6 - removing a debug that was left in last commit (James Mutton)
 - 6c93b38 - Fixing connection-closed behavior to detect on attempt to publish (James Mutton)
 - c3f0356 - Initialize bytes_written in _handle_write() (Jonathan Kirsch)
 - 4510e95 - Fix _handle_write() may not send full frame (Jonathan Kirsch)
 - 12b793f - fixed Tornado Consumer example to successfully reconnect (Yang Yang)
 - f074444 - remove forgotten import of ordereddict (Pedro Abranches)
 - 1ba0aea - fix last merge (Pedro Abranches)
 - 10490a6 - change timeouts structure to list to maintain scheduling order (Pedro Abranches)
 - 7958394 - save timeouts in ordered dict instead of dict (Pedro Abranches)
 - d2746bf - URLParameters and ConnectionParameters accept unicode strings (Allard Hoeve)
 - 596d145 - previous fix for AttributeError made parent and child class methods identical, remove duplication (James Mutton)
 - 42940dd - UrlParameters Docs: fixed amqps scheme examples (Riccardo Cirimelli)
 - 43904ff - Dont test this in PyPy due to sort order issue (Gavin M. Roy)
 - d7d293e - Don't leave __repr__ sorting up to chance (Gavin M. Roy)
 - 848c594 - Add integration test to travis and fix invocation (Gavin M. Roy)
 - 2678275 - Add pypy to travis tests (Gavin M. Roy)
 - 1877f3d - Also addresses issue #419 (Gavin M. Roy)
 - 470c245 - Address issue #419 (Gavin M. Roy)
 - ca3cb59 - Address issue #432 (Gavin M. Roy)
 - a3ff6f2 - Default frame max should be AMQP FRAME_MAX (Gavin M. Roy)
 - ff3d5cb - Remove max consumer tag test due to change in code. (Gavin M. Roy)
 - 6045dda - Catch KeyError (#437) to ensure that an exception is not raised in a race condition (Gavin M. Roy)
 - 0b4d53a - Address issue #441 (Gavin M. Roy)
 - 180e7c4 - Update license and related files (Gavin M. Roy)
 - 256ed3d - Added Jython support. (Erik Olof Gunnar Andersson)
 - f73c141 - experimental work around for recursion issue. (Erik Olof Gunnar Andersson)
 - a623f69 - Prevent #436 by iterating the keys and not the dict (Gavin M. Roy)
 - 755fcae - Add support for authentication_failure_close, connection.blocked (Gavin M. Roy)
 - c121243 - merge upstream master (Michael Laing)
 - a08dc0d - add  arg to channel.basic_consume (Pedro Abranches)
 - 10b136d - Documentation fix (Anton Ryzhov)
 - 9313307 - Fixed minor markup errors. (Jorge Puente Sarrín)
 - fb3e3cf - Fix the spelling of UnsupportedAMQPFieldException (Garrett Cooper)
 - 03d5da3 - connection.py: Propagate the force_channel keyword parameter to methods involved in channel creation (Michael Laing)
 - 7bbcff5 - Documentation fix for basic_publish (JuhaS)
 - 01dcea7 - Expose no_ack and exclusive to BlockingChannel.consume (Jeff Tang)
 - d39b6aa - Fix BlockingChannel.basic_consume does not block on non-empty queues (Juhyeong Park)
 - 6e1d295 - fix for issue 391 and issue 307 (Qi Fan)
 - d9ffce9 - Update parameters.rst (cacovsky)
 - 6afa41e - Add additional badges (Gavin M. Roy)
 - a255925 - Fix return value on dns resolution issue (Laurent Eschenauer)
 - 3f7466c - libev_connection: tweak docs (Michael Laing)
 - 0aaed93 - libev_connection: Fix varable naming (Michael Laing)
 - 0562d08 - libev_connection: Fix globals warning (Michael Laing)
 - 22ada59 - libev_connection: use globals to track sigint and sigterm watchers as they are created globally within libev (Michael Laing)
 - 2649b31 - Move badge [skip ci] (Gavin M. Roy)
 - f70eea1 - Remove pypy and installation attempt of pyev (Gavin M. Roy)
 - f32e522 - Conditionally skip external connection adapters if lib is not installed (Gavin M. Roy)
 - cce97c5 - Only install pyev on python 2.7 (Gavin M. Roy)
 - ff84462 - Add travis ci support (Gavin M. Roy)
 - cf971da - lib_evconnection: improve signal handling; add callback (Michael Laing)
 - 9adb269 - bugfix in returning a list in Py3k (Alex Chandel)
 - c41d5b9 - update exception syntax for Py3k (Alex Chandel)
 - c8506f1 - fix _adapter_connect (Michael Laing)
 - 67cb660 - Add LibevConnection to README (Michael Laing)
 - 1f9e72b - Propagate low-level connection errors to the AMQPConnectionError. (Bjorn Sandberg)
 - e1da447 - Avoid race condition in _on_getok on successive basic_get() when clearing out callbacks (Jeff)
 - 7a09979 - Add support for upcoming Connection.Blocked/Unblocked (Gavin M. Roy)
 - 53cce88 - TwistedChannel correctly handles multi-argument deferreds. (eivanov)
 - 66f8ace - Use uuid when creating unique consumer tag (Perttu Ranta-aho)
 - 4ee2738 - Limit the growth of Channel._cancelled, use deque instead of list. (Perttu Ranta-aho)
 - 0369aed - fix adapter references and tweak docs (Michael Laing)
 - 1738c23 - retry select.select() on EINTR (Cenk Alti)
 - 1e55357 - libev_connection: reset internal state on reconnect (Michael Laing)
 - 708559e - libev adapter (Michael Laing)
 - a6b7c8b - Prioritize EPollPoller and KQueuePoller over PollPoller and SelectPoller (Anton Ryzhov)
 - 53400d3 - Handle socket errors in PollPoller and EPollPoller Correctly check 'select.poll' availability (Anton Ryzhov)
 - a6dc969 - Use dict.keys & items instead of iterkeys & iteritems (Alex Chandel)
 - 5c1b0d0 - Use print function syntax, in examples (Alex Chandel)
 - ac9f87a - Fixed a typo in the name of the Asyncore Connection adapter (Guruprasad)
 - dfbba50 - Fixed bug mentioned in Issue #357 (Erik Andersson)
 - c906a2d - Drop additional flags when getting info for the hostnames, log errors (#352) (Gavin M. Roy)
 - baf23dd - retry poll() on EINTR (Cenk Alti)
 - 7cd8762 - Address ticket #352 catching an error when socket.getprotobyname fails (Gavin M. Roy)
 - 6c3ec75 - Prep for 0.9.14 (Gavin M. Roy)
 - dae7a99 - Bump to 0.9.14p0 (Gavin M. Roy)
 - 620edc7 - Use default port and virtual host if omitted in URLParameters (Issue #342) (Gavin M. Roy)
 - 42a8787 - Move the exception handling inside the while loop (Gavin M. Roy)
 - 10e0264 - Fix connection back pressure detection issue #347 (Gavin M. Roy)
 - 0bfd670 - Fixed mistake in commit 3a19d65. (Erik Andersson)
 - da04bc0 - Fixed Unknown state on disconnect error message generated when closing  connections. (Erik Andersson)
 - 3a19d65 - Alternative solution to fix #345. (Erik Andersson)
 - abf9fa8 - switch to sendall to send entire frame (Dustin Koupal)
 - 9ce8ce4 - Fixed the async publisher example to work with reconnections (Raphaël De Giusti)
 - 511028a - Fix typo in TwistedChannel docstring (cacovsky)
 - 8b69e5a - calls self._adapter_disconnect() instead of self.disconnect() which doesn't actually exist #294 (Mark Unsworth)
 - 06a5cf8 - add NullHandler to prevent logging warnings (Cenk Alti)
 - f404a9a - Fix #337 cannot start ioloop after stop (Ralf Nyren)

0.9.13 - 2013-05-15
-------------------

`0.9.13 <https://github.com/pika/pika/compare/0.9.12...0.9.13>`_

**Major Changes**

- IPv6 Support with thanks to Alessandro Tagliapietra for initial prototype
- Officially remove support for <= Python 2.5 even though it was broken already
- Drop pika.simplebuffer.SimpleBuffer in favor of the Python stdlib collections.deque object
- New default object for receiving content is a "bytes" object which is a str wrapper in Python 2, but paves way for Python 3 support
- New "Raw" mode for frame decoding content frames (#334) addresses issues #331, #229 added by Garth Williamson
- Connection and Disconnection logic refactored, allowing for cleaner separation of protocol logic and socket handling logic as well as connection state management
- New "on_open_error_callback" argument in creating connection objects and new Connection.add_on_open_error_callback method
- New Connection.connect method to cleanly allow for reconnection code
- Support for all AMQP field types, using protocol specified signed/unsigned unpacking

**Backwards Incompatible Changes**

- Method signature for creating connection objects has new argument "on_open_error_callback" which is positionally before "on_close_callback"
- Internal callback variable names in connection.Connection have been renamed and constants used. If you relied on any of these callbacks outside of their internal use, make sure to check out the new constants.
- Connection._connect method, which was an internal only method is now deprecated and will raise a DeprecationWarning. If you relied on this method, your code needs to change.
- pika.simplebuffer has been removed

**Bugfixes**

- BlockingConnection consumer generator does not free buffer when exited (#328)
- Unicode body payloads in the blocking adapter raises exception (#333)
- Support "b" short-short-int AMQP data type (#318)
- Docstring type fix in adapters/select_connection (#316) fix by Rikard Hultén
- IPv6 not supported (#309)
- Stop the HeartbeatChecker when connection is closed (#307)
- Unittest fix for SelectConnection (#336) fix by Erik Andersson
- Handle condition where no connection or socket exists but SelectConnection needs a timeout for retrying a connection (#322)
- TwistedAdapter lagging behind BaseConnection changes (#321) fix by Jan Urbański

**Other**

- Refactored documentation
- Added Twisted Adapter example (#314) by nolinksoft

0.9.12 - 2013-03-18
-------------------

`0.9.12 <https://github.com/pika/pika/compare/0.9.11...0.9.12>`_

**Bugfixes**

- New timeout id hashing was not unique

0.9.11 - 2013-03-17
-------------------

`0.9.11 <https://github.com/pika/pika/compare/0.9.10...0.9.11>`_

**Bugfixes**

- Address inconsistent channel close callback documentation and add the signature
  change to the TwistedChannel class (#305)
- Address a missed timeout related internal data structure name change
  introduced in the SelectConnection 0.9.10 release. Update all connection
  adapters to use same signature and docstring (#306).

0.9.10 - 2013-03-16
-------------------

`0.9.10 <https://github.com/pika/pika/compare/0.9.9...0.9.10>`_

**Bugfixes**

- Fix timeout in twisted adapter (Submitted by cellscape)
- Fix blocking_connection poll timer resolution to milliseconds (Submitted by cellscape)
- Fix channel._on_close() without a method frame (Submitted by Richard Boulton)
- Addressed exception on close (Issue #279 - fix by patcpsc)
- 'messages' not initialized in BlockingConnection.cancel() (Issue #289 - fix by Mik Kocikowski)
- Make queue_unbind behave like queue_bind (Issue #277)
- Address closing behavioral issues for connections and channels (Issue #275)
- Pass a Method frame to Channel._on_close in Connection._on_disconnect (Submitted by Jan Urbański)
- Fix channel closed callback signature in the Twisted adapter (Submitted by Jan Urbański)
- Don't stop the IOLoop on connection close for in the Twisted adapter (Submitted by Jan Urbański)
- Update the asynchronous examples to fix reconnecting and have it work
- Warn if the socket was closed such as if RabbitMQ dies without a Close frame
- Fix URLParameters ssl_options (Issue #296)
- Add state to BlockingConnection addressing (Issue #301)
- Encode unicode body content prior to publishing (Issue #282)
- Fix an issue with unicode keys in BasicProperties headers key (Issue #280)
- Change how timeout ids are generated (Issue #254)
- Address post close state issues in Channel (Issue #302)

** Behavior changes **

- Change core connection communication behavior to prefer outbound writes over reads, addressing a recursion issue
- Update connection on close callbacks, changing callback method signature
- Update channel on close callbacks, changing callback method signature
- Give more info in the ChannelClosed exception
- Change the constructor signature for BlockingConnection, block open/close callbacks
- Disable the use of add_on_open_callback/add_on_close_callback methods in BlockingConnection


0.9.9 - 2013-01-29
------------------

`0.9.9 <https://github.com/pika/pika/compare/0.9.8...0.9.9>`_

**Bugfixes**

- Only remove the tornado_connection.TornadoConnection file descriptor from the IOLoop if it's still open (Issue #221)
- Allow messages with no body (Issue #227)
- Allow for empty routing keys (Issue #224)
- Don't raise an exception when trying to send a frame to a closed connection (Issue #229)
- Only send a Connection.CloseOk if the connection is still open. (Issue #236 - Fix by noleaf)
- Fix timeout threshold in blocking connection - (Issue #232 - Fix by Adam Flynn)
- Fix closing connection while a channel is still open (Issue #230 - Fix by Adam Flynn)
- Fixed misleading warning and exception messages in BaseConnection (Issue #237 - Fix by Tristan Penman)
- Pluralised and altered the wording of the AMQPConnectionError exception (Issue #237 - Fix by Tristan Penman)
- Fixed _adapter_disconnect in TornadoConnection class (Issue #237 - Fix by Tristan Penman)
- Fixing hang when closing connection without any channel in BlockingConnection (Issue #244 - Fix by Ales Teska)
- Remove the process_timeouts() call in SelectConnection (Issue #239)
- Change the string validation to basestring for host connection parameters (Issue #231)
- Add a poller to the BlockingConnection to address latency issues introduced in Pika 0.9.8 (Issue #242)
- reply_code and reply_text is not set in ChannelException (Issue #250)
- Add the missing constraint parameter for Channel._on_return callback processing (Issue #257 - Fix by patcpsc)
- Channel callbacks not being removed from callback manager when channel is closed or deleted (Issue #261)

0.9.8 - 2012-11-18
------------------

`0.9.8 <https://github.com/pika/pika/compare/0.9.7...0.9.8>`_

**Bugfixes**

- Channel.queue_declare/BlockingChannel.queue_declare not setting up callbacks property for empty queue name (Issue #218)
- Channel.queue_bind/BlockingChannel.queue_bind not allowing empty routing key
- Connection._on_connection_closed calling wrong method in Channel (Issue #219)
- Fix tx_commit and tx_rollback bugs in BlockingChannel (Issue #217)

0.9.7 - 2012-11-11
------------------

`0.9.7 <https://github.com/pika/pika/compare/0.9.6...0.9.7>`_

**New features**

- generator based consumer in BlockingChannel (See :doc:`examples/blocking_consumer_generator` for example)

**Changes**

- BlockingChannel._send_method will only wait if explicitly told to

**Bugfixes**

- Added the exchange "type" parameter back but issue a DeprecationWarning
- Dont require a queue name in Channel.queue_declare()
- Fixed KeyError when processing timeouts (Issue # 215 - Fix by Raphael De Giusti)
- Don't try and close channels when the connection is closed (Issue #216 - Fix by Charles Law)
- Dont raise UnexpectedFrame exceptions, log them instead
- Handle multiple synchronous RPC calls made without waiting for the call result (Issues #192, #204, #211)
- Typo in docs (Issue #207 Fix by Luca Wehrstedt)
- Only sleep on connection failure when retry attempts are > 0 (Issue #200)
- Bypass _rpc method and just send frames for Basic.Ack, Basic.Nack, Basic.Reject (Issue #205)

0.9.6 - 2012-10-29
------------------

`0.9.6 <https://github.com/pika/pika/compare/0.9.5...0.9.6>`_

**New features**

- URLParameters
- BlockingChannel.start_consuming() and BlockingChannel.stop_consuming()
- Delivery Confirmations
- Improved unittests

**Major bugfix areas**

- Connection handling
- Blocking functionality in the BlockingConnection
- SSL
- UTF-8 Handling

**Removals**

- pika.reconnection_strategies
- pika.channel.ChannelTransport
- pika.log
- pika.template
- examples directory

0.9.5 - 2011-03-29
------------------

`0.9.5 <https://github.com/pika/pika/compare/0.9.4...0.9.5>`_

**Changelog**

- Scope changes with adapter IOLoops and CallbackManager allowing for cleaner, multi-threaded operation
- Add support for Confirm.Select with channel.Channel.confirm_delivery()
- Add examples of delivery confirmation to examples (demo_send_confirmed.py)
- Update uses of log.warn with warning.warn for TCP Back-pressure alerting
- License boilerplate updated to simplify license text in source files
- Increment the timeout in select_connection.SelectPoller reducing CPU utilization
- Bug fix in Heartbeat frame delivery addressing issue #35
- Remove abuse of pika.log.method_call through a majority of the code
- Rename of key modules: table to data, frames to frame
- Cleanup of frame module and related classes
- Restructure of tests and test runner
- Update functional tests to respect RABBITMQ_HOST, RABBITMQ_PORT environment variables
- Bug fixes to reconnection_strategies module
- Fix the scale of timeout for PollPoller to be specified in milliseconds
- Remove mutable default arguments in RPC calls
- Add data type validation to RPC calls
- Move optional credentials erasing out of connection.Connection into credentials module
- Add support to allow for additional external credential types
- Add a NullHandler to prevent the 'No handlers could be found for logger "pika"' error message when not using pika.log in a client app at all.
- Clean up all examples to make them easier to read and use
- Move documentation into its own repository https://github.com/pika/documentation

- channel.py

  - Move channel.MAX_CHANNELS constant from connection.CHANNEL_MAX
  - Add default value of None to ChannelTransport.rpc
  - Validate callback and acceptable replies parameters in ChannelTransport.RPC
  - Remove unused connection attribute from Channel

- connection.py

  - Remove unused import of struct
  - Remove direct import of pika.credentials.PlainCredentials
    - Change to import pika.credentials
  - Move CHANNEL_MAX to channel.MAX_CHANNELS
  - Change ConnectionParameters initialization parameter heartbeat to boolean
  - Validate all inbound parameter types in ConnectionParameters
  - Remove the Connection._erase_credentials stub method in favor of letting the Credentials object deal with  that itself.
  - Warn if the credentials object intends on erasing the credentials and a reconnection strategy other than NullReconnectionStrategy is specified.
  - Change the default types for callback and acceptable_replies in Connection._rpc
  - Validate the callback and acceptable_replies data types in Connection._rpc

- adapters.blocking_connection.BlockingConnection

  - Addition of _adapter_disconnect to blocking_connection.BlockingConnection
  - Add timeout methods to BlockingConnection addressing issue #41
  - BlockingConnection didn't allow you register more than one consumer callback because basic_consume was overridden to block immediately. New behavior allows you to do so.
  - Removed overriding of base basic_consume and basic_cancel methods. Now uses underlying Channel versions of those methods.
  - Added start_consuming() method to BlockingChannel to start the consumption loop.
  - Updated stop_consuming() to iterate through all the registered consumers in self._consumers and issue a basic_cancel.
