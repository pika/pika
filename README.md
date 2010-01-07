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

### Dealing with Channel.Flow flow control

Occasionally the server will decide it needs publishing clients to be
quiet for a while so it can let messages drain. When it does so, it
sends out a `Channel.Flow` command to connected clients, which are
then expected to handle it and stop publishing messages until told (by
another `Channel.Flow`) that they're allowed to resume.

By default, Pika will honour `Channel.Flow` requests by setting an
internal flag and throwing a `ContentTransmissionForbidden` exception
if an application tries to publish a message when flow-control is in
effect. An application has two approaches available for coping with
this situation: it may supply `True` to the optional keyword argument
`block_on_flow_control` to the `Channel.basic_publish` method, and/or
it may register for notifications of flow-control state changes using
the `Channel.addFlowChangeHandler` method.

Use `block_on_flow_control` carefully: it enters a nested event loop
if it needs to wait for flow-control to stop, so your entire
application must be accordingly reentrant. Here's an example of a
flow-control-blocking publish call:

    ch.basic_publish(exchange="test_x", routing_key="", body="Hello World!",
                     block_on_flow_control=True)

Here's an example flow-control state change handler:

    def my_flow_handler(the_channel, transmission_permitted):
      if transmission_permitted:
        print 'Transmission is now permitted on channel', the_channel
      else:
        print 'Transmission is temporarily NOT permitted on channel', the_channel
    ch.addFlowChangeHandler(my_flow_handler)

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
