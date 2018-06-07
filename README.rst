Pika
====
Pika is a RabbitMQ (AMQP-0-9-1) client library for Python.

|Version| |Python versions| |Status| |Coverage| |License| |Docs|

Introduction
-------------
Pika is a pure-Python implementation of the AMQP 0-9-1 protocol including RabbitMQ's
extensions.

- Python 2.7 and 3.4+ are supported.

- Since threads aren't appropriate to every situation, it doesn't
  require threads. Pika core takes care not to forbid them, either. The same
  goes for greenlets, callbacks, continuations, and generators. An instance of
  Pika's built-in connection adapters is not thread-safe, however.

- People may be using direct sockets, plain old `select()`,
  or any of the wide variety of ways of getting network events to and from a
  python application. Pika tries to stay compatible with all of these, and to
  make adapting it to a new environment as simple as possible.

Documentation
-------------
Pika's documentation can be found at `https://pika.readthedocs.io <https://pika.readthedocs.io>`_

Example
-------
Here is the most simple example of use, sending a message with the BlockingConnection adapter:

.. code :: python

    import pika
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.basic_publish(exchange='example',
                          routing_key='test',
                          body=b'Test Message')
    connection.close()

And an example of writing a blocking consumer:

.. code :: python

    import pika
    connection = pika.BlockingConnection()
    channel = connection.channel()

    for method_frame, properties, body in channel.consume('test'):

        # Display the message parts and ack the message
        print(method_frame, properties, body)
        channel.basic_ack(method_frame.delivery_tag)

        # Escape out of the loop after 10 messages
        if method_frame.delivery_tag == 10:
            break

    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    print('Requeued %i messages' % requeued_messages)
    connection.close()

Pika provides the following adapters
------------------------------------

- AsyncioConnection  - adapter for the Python3 AsyncIO event loop
- BlockingConnection - enables blocking, synchronous operation on top of library for simple usage
- SelectConnection   - fast asynchronous adapter without 3rd-party dependencies
- TornadoConnection  - adapter for use with the Tornado IO Loop http://tornadoweb.org
- TwistedProtocolConnection  - adapter for use with the Twisted asynchronous package http://twistedmatrix.com/

Multiple Connection Parameters
------------------------------
You can also pass multiple connection parameter instances for
fault-tolerance as in the code snippet below (host names are just examples, of
course). To enable retries, set `connection_attempts` and `retry_delay` as
needed in the last `pika.ConnectionParameters` element of the sequence. Retries
occur after connection attempts using all of the given connection parameters
fail.

.. code :: python

    import pika
    configs = (
        pika.ConnectionParameters(host='rabbitmq.zone1.yourdomain.com'),
        pika.ConnectionParameters(host='rabbitmq.zone2.yourdomain.com',
                                  connection_attempts=5, retry_delay=1))
    connection = pika.BlockingConnection(configs)

With non-blocking adapters, such as `SelectConnection` and `AsyncioConnection`,
you can request a connection using multiple connection parameter instances via
the connection adapter's `create_connection()` class method.

Requesting message ACKs from another thread
-------------------------------------------
The single-threaded usage constraint of an individual Pika connection adapter
instance may result in a dropped AMQP/stream connection due to AMQP heartbeat
timeout in consumers that take a long time to process an incoming message. A
common solution is to delegate processing of the incoming messages to another
thread, while the connection adapter's thread continues to service its ioloop's
message pump, permitting AMQP heartbeats and other I/O to be serviced in a
timely fashion.

Messages processed in another thread may not be ACK'ed directly from that thread,
since all accesses to the connection adapter instance must be from a single
thread - the thread that is running the adapter's ioloop. However, this may be
accomplished by requesting a callback to be executed in the adapter's ioloop
thread. For example, the callback function's implementation might look like this:

.. code :: python

    def ack_message(channel, delivery_tag):
        """Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass

The code running in the other thread may request the `ack_message()` function
to be executed in the connection adapter's ioloop thread using an
adapter-specific mechanism:

- :py:class:`pika.BlockingConnection` abstracts its ioloop from the application
  and thus exposes :py:meth:`pika.BlockingConnection.add_callback_threadsafe()`.
  Refer to this method's docstring for additional information. For example:

  .. code :: python

      connection.add_callback_threadsafe(functools.partial(ack_message, channel, delivery_tag))

- When using a non-blocking connection adapter, such as
  :py:class:`pika.AsyncioConnection` or :py:class:`pika.SelectConnection`, you
  use the underlying asynchronous framework's native API for requesting an
  ioloop-bound callback from another thread. For example, `SelectConnection`'s
  `IOLoop` provides `add_callback_threadsafe()`, `Tornado`'s `IOLoop` has
  `add_callback()`, while `asyncio`'s event loop exposes `call_soon_threadsafe()`.

This threadsafe callback request mechanism may also be used to delegate
publishing of messages, etc., from a background thread to the connection adapter's
thread.

Connection recovery
-------------------

Some RabbitMQ clients (Bunny, Java, .NET, Objective-C/Swift) provide a way to automatically recover connection, its channels
and topology (e.g. queues, bindings and consumers) after a network failure.
Others require connection recovery to be performed by the application code and strive to make
it a straightforward process. Pika falls into the second category.

Pika supports multiple connection adapters. They take different approaches
to connection recovery.

For BlockingConnection adapter exception handling can be used to check for
connection errors. Here's a very basic example:

.. code :: python

    import pika
    while(True):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.basic_consume('queue-name', on_message_callback)
            channel.start_consuming()
        # Do not recover if connection was closed by broker
        except pika.exceptions.ConnectionClosedByBroker:
            break
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError:
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            continue

This example can be found in `examples/consume_recover.py`.

Generic operation retry libraries such as `retry <https://github.com/invl/retry>`_
can be used:

.. code :: python

    from retry import retry
    @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
    def consume():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_consume('queue-name', on_message_callback)
        try:
            channel.start_consuming()
        # Do not recover connections closed by server
        except pika.exceptions.ConnectionClosedByBroker:
            pass
    consume()

Decorators make it possible to configure some additional recovery behaviours,
like delays between retries and limiting the number of retries.

The above example can be found in `examples/consume_recover_retry.py`.

For asynchronous adapters, use `on_close_callback` to react to connection failure events.
This callback can be used to clean up and recover the connection.

An example of recovery using `on_close_callback` can be found
in `examples/asynchronous_consumer_example.py`

Contributing
------------
To contribute to pika, please make sure that any new features or changes
to existing functionality **include test coverage**.

*Pull requests that add or change code without adequate test coverage will be rejected.*

Additionally, please format your code using `yapf <http://pypi.python.org/pypi/yapf>`_
with ``google`` style prior to issuing your pull request.

Extending to support additional I/O frameworks
----------------------------------------------
New non-blocking adapters may be implemented in either of the following ways:

- By subclassing :py:class:`pika.adapters.base_connection.BaseConnection` and
  implementing its abstract method(s) and passing BaseConnection's constructor
  an implementation of
  :py.class:`pika.adapters.utils.nbio_interface.AbstractIOServices`.
  `BaseConnection` implements `pika.connection.connection.Connection`'s pure
  virtual methods, including internally-initiated connection logic. For
  examples, refer to the implementations of
  :py:class:`pika.AsyncioConnection` and :py:class:`pika.TornadoConnection`.
- By subclassing :py:class:`pika.connection.connection.Connection` and
  implementing its abstract method(s). This approach facilitates implementation
  of of custom connection-establishment and transport mechanisms. For an example,
  refer to the implementation of
  :py:class:`pika.adapters.twisted_connection.TwistedProtocolConnection`.

.. |Version| image:: https://img.shields.io/pypi/v/pika.svg?
   :target: http://badge.fury.io/py/pika

.. |Python versions| image:: https://img.shields.io/pypi/pyversions/pika.svg
    :target: https://pypi.python.org/pypi/pika

.. |Status| image:: https://img.shields.io/travis/pika/pika.svg?
   :target: https://travis-ci.org/pika/pika

.. |Coverage| image:: https://img.shields.io/codecov/c/github/pika/pika.svg?
   :target: https://codecov.io/github/pika/pika?branch=master

.. |License| image:: https://img.shields.io/pypi/l/pika.svg?
   :target: https://pika.readthedocs.io

.. |Docs| image:: https://readthedocs.org/projects/pika/badge/?version=stable
   :target: https://pika.readthedocs.io
   :alt: Documentation Status
