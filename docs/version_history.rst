Version History
===============

0.9.9 - Unreleased
------------------

**Bugfixes**

- Only remove the tornado_connection.TornadoConnection file descriptor from the IOLoop if it's still open (Issue #221)
- Allow messages with no body (Issue #227)
- Allow for empty routing keys (Issue #224)
- Don't raise an exception when trying to send a frame to a closed connection (Issue #229)
- Only send a Connection.CloseOk if the connection is still open. (Issue #236 - Fix by "noleaf")
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
