0.10.0dev0 2015-07-10
---------------------

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
- The `AsynchoreConnection` adapter was retired

**Details**

Python 3.x: this release introduces python 3.x support. Tested on Python 3.3
and 3.4.

`AsynchoreConnection`: Retired this legacy adapter to reduce maintenance burden;
the recommended replacement is the `SelectConnection` adapter.

`SelectConnection`: ioloop was refactored for compatibility with other ioloops.

`Channel.add_on_return_callback`: The callback is now passed the individual
parameters channel, method, properties, and body instead of a tuple of those
values for congruence with other similar callbacks.

`BlockingConnection`: This adapter underwent a makeover under the hood and
gained significant performance improvements as well as ehnanced timer
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


0.9.13 - 2013-05-15
-------------------
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

**Bugfixes**

- New timeout id hashing was not unique

0.9.11 - 2013-03-17
-------------------

**Bugfixes**

- Address inconsistent channel close callback documentation and add the signature
  change to the TwistedChannel class (#305)
- Address a missed timeout related internal data structure name change
  introduced in the SelectConnection 0.9.10 release. Update all connection
  adapters to use same signature and docstring (#306).

0.9.10 - 2013-03-16
-------------------

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

**Bugfixes**

- Channel.queue_declare/BlockingChannel.queue_declare not setting up callbacks property for empty queue name (Issue #218)
- Channel.queue_bind/BlockingChannel.queue_bind not allowing empty routing key
- Connection._on_connection_closed calling wrong method in Channel (Issue #219)
- Fix tx_commit and tx_rollback bugs in BlockingChannel (Issue #217)

0.9.7 - 2012-11-11
------------------

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
