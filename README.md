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

## Pika provides the following adapters

 * SelectConnection   - fast asynchronous adapter
 * AsyncoreConnection - based off the standard Python library asyncore
 * TornadoConnection  - adapter for use with the Tornado IO Loop http://tornadoweb.org
 * BlockingConnection - enables blocking, synchronous operation on top of library
                        for simple uses

Support for Twist and other IO frameworks are on the horizon.

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

## Installation via `pip` (and, optionally, `virtualenv`)

You can install this package directly from github using `pip`:

    pip install -e git://github.com/tonyg/pika.git#egg=pika

If you are using `virtualenv` for context-specific Python module
installations,

    pip -E my_virtual_env install -e git://github.com/tonyg/pika.git#egg=pika

or

    virtualenv my_virtual_env
    cd my_virtual_env
    . bin/activate
    ## Now you're already in the right virtual environment, so the next
    ## command automatically installs pika to the correct context
    pip install -e git://github.com/tonyg/pika.git#egg=pika

## Licensing

Pika is licensed under the MPL, and may also be used under the terms
of the GPL. The full license text is included with the source code for
the package. If you have any questions regarding licensing, please
contact us at <info@rabbitmq.com>.

## Asynchronous programming style

This style of programming is the only technique suitable for
programming large or complex event-driven systems in python. It makes
all control flow explicit using continuation-passing style, in a
manner reminiscent of Javascript or Twisted network programming. Once
you get your head around the unusual presentation of the code,
reasoning about control becomes much easier than in the synchronous
style.

    import pika

    # Variables to hold our connection and channel
    connection = None
    channel = None

    # Called when our connection to RabbitMQ is closed
    def on_closed(frame):
        global connection

        # connection.ioloop is blocking, this will stop and exit the app
        connection.ioloop.stop()

    # Called when we have connected to RabbitMQ
    def on_connected(connection):
        global channel

        # Create a channel on our connection passing the on_channel_open callback
        connection.channel(on_channel_open)

    # Called after line #110 is finished, when our channel is open
    def on_channel_open(channel_):
        global channel

        # Our usable channel has been passed to us, assign it for future use
        channel = channel_

        # Declare a queue
        channel.queue_declare(queue="test", durable=True,
                              exclusive=False, auto_delete=False,
                              callback=on_queue_declared)

    # Called when line #119 is finished, our queue is declared.
    def on_queue_declared(frame):
        global channel

        # Send a message
        channel.basic_publish(exchange='',
                              routing_key="test",
                              body="Hello World!",
                              properties=pika.BasicProperties(
                                content_type="text/plain",
                                delivery_mode=1))

        # Add a callback so we can stop the ioloop
        connection.add_on_close_callback(on_closed)

        # Close our connection
        connection.close()

    # Create our connection parameters and connect to RabbitMQ
    parameters = pika.ConnectionParameters('localhost')
    connection = pika.SelectConnection(parameters, on_connected)

    # Start our IO/Event loop
    connection.ioloop.start()

The asynchronous programming style can be used in both multi-threaded and
single-threaded environments. The same care must be taken when
programming in a multi-threaded environment using an asynchronous
style as is taken when using a synchronous style.

## Synchronous programming style, no concurrency

This style of programming is especially appropriate for small scripts,
short-lived programs, or other simple tasks. Code is easy to read and
somewhat easy to reason about.

    import pika

    # Create our connection parameters and connect to RabbitMQ
    parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(parameters)

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test", durable=True,
                      exclusive=False, auto_delete=False)

    # Construct a message and send it
    channel.basic_publish(exchange='',
                      routing_key="test",
                      body="Hello World!",
                      properties=pika.BasicProperties(
                          content_type="text/plain",
                          delivery_mode=1))

## Synchronous programming style, with concurrency

This style of programming can be used when small scripts grow bigger
(as they always seem to do). Code is still easy to read, but reasoning
about it becomes more difficult, and care must be taken when sharing
Pika resources among multiple threads of control. Beyond a certain
point, the complexity of the approach will outweigh the benefits, and
rewriting for the asynchronous style will look increasingly
worthwhile.

The main consideration when throwing threading into the mix is
locking. Each connection, and all AMQP channels carried by it, must be
guarded by a connection-specific mutex, if sharing Pika resources
between threads is desired.

The recommended alternative is sidestepping the locking complexity
completely by making sure that a connection and its channels is never
shared between threads: that each thread owns its own AMQP connection.

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
