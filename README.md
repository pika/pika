# Pika, an AMQP 0-8/0-9-1 client library for Python

Pika is a pure-Python implementation of the AMQP 0-8 protocol (with an
0-9-1 implementation on a separate git branch, for now) that tries to
stay fairly independent of the underlying network support library. It
also tries to stay neutral as to programming style, supporting (where
possible) both synchronous and asynchronous approaches.

 * Since threads aren't appropriate to every situation, it doesn't
   require threads. It takes care not to forbid them, either. The same
   goes for greenlets, callbacks, continuations and generators.

 * People may be using direct sockets, `asyncore`, Twisted, plain old
   `select()`, or any of the wide variety of ways of getting network
   events to and from a python application. Pika tries to stay
   compatible with all of these, and to make adapting it to a new
   environment as simple as possible.

Pika provides adapters for

 * asyncore (part of the Python standard library)
 * direct blocking socket I/O

Support for Twisted and `select()` (as distinct from `asyncore`) is on
the horizon.

## Roadmap

 * Support continuation-passing-style, for asynchronous programming
   (and, eventually, Twisted support)

 * Fix up the slightly odd `ChannelHandler.inbound` queue: it plainly
   wants to be something else (after all, when do we get a command we
   don't understand? and why aren't we complaining with the
   appropriate AMQP exception when we do?)

 * Track `Channel.Flow` state for each channel. Emit events when it
   changes.

 * Complain when publishing to a flow-limited channel, either by a
   thrown exception or a block-until-flow-control-goes-away option.

## Synchronous programming style, no concurrency

This style of programming is especially appropriate for small scripts,
short-lived programs, or other simple tasks. Code is easy to read and
somewhat easy to reason about.

    import pika
    import asyncore
    conn = pika.AsyncoreConnection(pika.ConnectionParameters('localhost')
    ch = conn.channel()
    ch.exchange_declare(exchange="test_x", type="fanout", durable=False)
    ch.queue_declare(queue="test_q", durable=True, exclusive=False, auto_delete=False)
    ch.queue_bind(queue="test_q", exchange="test_x", routing_key="")
    ch.basic_publish(exchange="test_x", routing_key="", body="Hello World!")
    conn.close()
    asyncore.loop()

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

## Asynchronous programming style

This style of programming is the only technique suitable for
programming large or complex event-driven systems in python. It makes
all control flow explicit using continuation-passing style, in a
manner reminiscent of Javascript or Twisted network programming. Once
you get your head around the unusual presentation of the code,
reasoning about control becomes much easier than in the synchronous
style.

    (example TBD)

The asynchronous programming style can be used in both multi- and
single-threaded environments. The same care must be taken when
programming in a multi-threaded environment using an asynchronous
style as is taken when using a synchronous style.
