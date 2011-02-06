# Pika, an AMQP 0-9-1 client library for Python

## Introduction

Pika is a pure-Python implementation of the AMQP 0-9-1 protocol that tries
to stay fairly independent of the underlying network support library.

 * Since threads aren't appropriate to every situation, it doesn't
   require threads. It takes care not to forbid them, either. The same
   goes for greenlets, callbacks, continuations and generators.

 * People may be using direct sockets, `asyncore`, Twisted, plain old
   `select()`, or any of the wide variety of ways of getting network
   events to and from a python application. Pika tries to stay
   compatible with all of these, and to make adapting it to a new
   environment as simple as possible.

## Documenation

Pika's documentation is now at http://tonyg.github.com/pika

## Pika provides the following adapters

 * SelectConnection   - fast asynchronous adapter
 * AsyncoreConnection - based off the standard Python library asyncore
 * TornadoConnection  - adapter for use with the Tornado IO Loop http://tornadoweb.org
 * BlockingConnection - enables blocking, synchronous operation on top of library for simple uses

Support for Twisted and other IO frameworks are on the horizon.

## Major Changes to Pika since 0.5.2

 * Pika has been restructured and made to be fully asynchronous at its core
   and now supports AMQP 0-9-1 only (RabbitMQ 2.0+)
 * There have been many method definition changes
	 * Asynchronous AMQP commands now take a callback parameter for notification of
       completion
 * AMQP commands that are specified as synchronous buffer other calls on the same
   channel, sending them when the synchronous commands send their response frame
 * SelectConnection is now the recommended connection adapter and shows better
   performance than the AsyncoreConnection. SelectConnection implements select,
   poll, epoll and kqueue for event handling.
 * Client channel flow control has been removed, see the section of the document
   below for information on this
 * TornadoConnection adds a connection adapter for the Tornado IOLoop
 * Support for additional AMQP data types has been added
 * More extensive unit and functional tests added

### Internal Changes since 0.5.2

 * Low level debug logging demonstrates client behavior and can be toggled via
   pika.log.DEBUG boolean value.
 * Classes now implement new style classes
 * Major restructuring of existing modules and classes
 * Universal callback mechanism added, removing events and other callback
   methods
 * Added BaseConenction which extends Connection and builds in default behaviors
   for asynchronous connection adapters.
 * Abstracted content frame handling to its own class, channel.ContentHandler
 * ConnectionState class moved from codec to connection
 * Frame class definitions moved from codec to frames
 * Reconnection strategies moved into own module
 * HeartbeatChecker moved to own module
 * PlainCredentials moved to credentials module for extensibility
 * AsyncoreConnection rewritten to align with BaseConnection
 * PEP8ification and use more pythonic idioms for areas as appropriate

## Licensing

Pika is licensed under the MPL, and may also be used under the terms
of the GPL. The full license text is included with the source code for
the package. If you have any questions regarding licensing, please
contact us at <info@rabbitmq.com>.

## Channel Flow Control

RabbitMQ 2.0+ has removed the Channel.Flow system for notifying clients that
they need to wait until they are notified before they can call Basic.Deliver
again. In addition, prior to AMQP 1-0, there is no defined behavior for
clients turning on flow control on the broker. As such all Channel.Flow related
functionality has been removed. In its place, we attempt to detect when
the RabbitMQ server is using TCP backpressure to throttle a client who
is delivering messages too quickly.

## TCP Backpressure from RabbitMQ

In the place of Channel.Flow being delivered to clients, RabbitMQ uses
TCP backpressure to throttle client connections. This manifests in the
client with socket timeouts and slow delivery. To address this issue
Pika has a Connection.add_backpressure_callback() which will notify clients
who register with it that there outbound delivery queue is backing up.
The current methodology is to count the number of bytes and frames sent
to create an average frame size and then look for 10x the average frame size
in the outbound buffer after we call Connection.flush_outbound(). To
adjust the threshold for the multiplier, call Channel.set_backpressure_multiplier
with the desired value.
