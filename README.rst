Pika
====
Pika is a RabbitMQ (AMQP 0-9-1) client library for Python.

|Version| |Python versions| |Actions Status| |Coverage| |License| |Docs|

Introduction
------------
Pika is a pure-Python implementation of the AMQP 0-9-1 protocol including
RabbitMQ's extensions.

- Supports Python 3.4+ (`1.1.0` was the last version to support 2.7)
- Since threads aren't appropriate to every situation, it doesn't require
  threads. Pika core takes care not to forbid them, either. The same goes for
  greenlets, callbacks, continuations, and generators. An instance of Pika's
  built-in connection adapters isn't thread-safe, however.
- People may be using direct sockets, plain old ``select()``, or any of the
  wide variety of ways of getting network events to and from a Python
  application. Pika tries to stay compatible with all of these, and to make
  adapting it to a new environment as simple as possible.

Documentation
-------------
Pika's documentation can be found at https://pika.readthedocs.io.

Example
-------
Here is the most simple example of use, sending a message with the
``pika.BlockingConnection`` adapter:

.. code :: python

    import pika

    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.basic_publish(exchange='test', routing_key='test',
                          body=b'Test message.')
    connection.close()

And an example of writing a blocking consumer:

.. code :: python

    import pika

    connection = pika.BlockingConnection()
    channel = connection.channel()

    for method_frame, properties, body in channel.consume('test'):
        # Display the message parts and acknowledge the message
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

- ``pika.adapters.asyncio_connection.AsyncioConnection`` - asynchronous adapter
  for Python 3 `AsyncIO <https://docs.python.org/3/library/asyncio.html>`_'s
  I/O loop.
- ``pika.BlockingConnection`` - synchronous adapter on top of library for
  simple usage.
- ``pika.SelectConnection`` - asynchronous adapter without third-party
  dependencies.
- ``pika.adapters.gevent_connection.GeventConnection`` - asynchronous adapter
  for use with `Gevent <http://www.gevent.org>`_'s I/O loop.
- ``pika.adapters.tornado_connection.TornadoConnection`` - asynchronous adapter
  for use with `Tornado <http://tornadoweb.org>`_'s I/O loop.
- ``pika.adapters.twisted_connection.TwistedProtocolConnection`` - asynchronous
  adapter for use with `Twisted <http://twistedmatrix.com>`_'s I/O loop.

Multiple connection parameters
------------------------------
You can also pass multiple ``pika.ConnectionParameters`` instances for
fault-tolerance as in the code snippet below (host names are just examples, of
course). To enable retries, set ``connection_attempts`` and ``retry_delay`` as
needed in the last ``pika.ConnectionParameters`` element of the sequence.
Retries occur after connection attempts using all of the given connection
parameters fail.

.. code :: python

    import pika

    parameters = (
        pika.ConnectionParameters(host='rabbitmq.zone1.yourdomain.com'),
        pika.ConnectionParameters(host='rabbitmq.zone2.yourdomain.com',
                                  connection_attempts=5, retry_delay=1))
    connection = pika.BlockingConnection(parameters)

With non-blocking adapters, such as ``pika.SelectConnection`` and
``pika.adapters.asyncio_connection.AsyncioConnection``, you can request a
connection using multiple connection parameter instances via the connection
adapter's ``create_connection()`` class method.

Requesting message acknowledgements from another thread
-------------------------------------------------------
The single-threaded usage constraint of an individual Pika connection adapter
instance may result in a dropped AMQP/stream connection due to AMQP heartbeat
timeout in consumers that take a long time to process an incoming message. A
common solution is to delegate processing of the incoming messages to another
thread, while the connection adapter's thread continues to service its I/O
loop's message pump, permitting AMQP heartbeats and other I/O to be serviced in
a timely fashion.

Messages processed in another thread may not be acknowledged directly from that
thread, since all accesses to the connection adapter instance must be from a
single thread, which is the thread running the adapter's I/O loop. This is
accomplished by requesting a callback to be executed in the adapter's
I/O loop thread. For example, the callback function's implementation might look
like this:

.. code :: python

    def ack_message(channel, delivery_tag):
        """Note that `channel` must be the same Pika channel instance via which
        the message being acknowledged was retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't acknowledge this message;
            # log and/or do something that makes sense for your app in this case.
            pass

The code running in the other thread may request the ``ack_message()`` function
to be executed in the connection adapter's I/O loop thread using an
adapter-specific mechanism:

- ``pika.BlockingConnection`` abstracts its I/O loop from the application and
  thus exposes ``pika.BlockingConnection.add_callback_threadsafe()``. Refer to
  this method's docstring for additional information. For example:

  .. code :: python

      connection.add_callback_threadsafe(functools.partial(ack_message, channel, delivery_tag))

- When using a non-blocking connection adapter, such as
  ``pika.adapters.asyncio_connection.AsyncioConnection`` or
  ``pika.SelectConnection``, you use the underlying asynchronous framework's
  native API for requesting an I/O loop-bound callback from another thread. For
  example, ``pika.SelectConnection``'s I/O loop provides
  ``add_callback_threadsafe()``,
  ``pika.adapters.tornado_connection.TornadoConnection``'s I/O loop has
  ``add_callback()``, while
  ``pika.adapters.asyncio_connection.AsyncioConnection``'s I/O loop exposes
  ``call_soon_threadsafe()``.

This threadsafe callback request mechanism may also be used to delegate
publishing of messages, etc., from a background thread to the connection
adapter's thread.

Connection recovery
-------------------

Some RabbitMQ clients (Bunny, Java, .NET, Objective-C, Swift) provide a way to
automatically recover connection, its channels and topology (e.g. queues,
bindings and consumers) after a network failure. Others require connection
recovery to be performed by the application code and strive to make it a
straightforward process. Pika falls into the second category.

Pika supports multiple connection adapters. They take different approaches to
connection recovery.

For ``pika.BlockingConnection`` adapter exception handling can be used to check
for connection errors. Here is a very basic example:

.. code :: python

    import pika

    while True:
        try:
            connection = pika.BlockingConnection()
            channel = connection.channel()
            channel.basic_consume('test', on_message_callback)
            channel.start_consuming()
        # Don't recover if connection was closed by broker
        except pika.exceptions.ConnectionClosedByBroker:
            break
        # Don't recover on channel errors
        except pika.exceptions.AMQPChannelError:
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            continue

This example can be found in `examples/consume_recover.py`.

Generic operation retry libraries such as
`retry <https://github.com/invl/retry>`_ can be used. Decorators make it
possible to configure some additional recovery behaviours, like delays between
retries and limiting the number of retries:

.. code :: python

    from retry import retry


    @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
    def consume():
        connection = pika.BlockingConnection()
        channel = connection.channel()
        channel.basic_consume('test', on_message_callback)

        try:
            channel.start_consuming()
        # Don't recover connections closed by server
        except pika.exceptions.ConnectionClosedByBroker:
            pass


    consume()

This example can be found in `examples/consume_recover_retry.py`.

For asynchronous adapters, use ``on_close_callback`` to react to connection
failure events. This callback can be used to clean up and recover the
connection.

An example of recovery using ``on_close_callback`` can be found in
`examples/asynchronous_consumer_example.py`.

Contributing
------------
To contribute to Pika, please make sure that any new features or changes to
existing functionality **include test coverage**.

*Pull requests that add or change code without adequate test coverage will be
rejected.*

Additionally, please format your code using
`Yapf <http://pypi.python.org/pypi/yapf>`_ with ``google`` style prior to
issuing your pull request. *Note: only format those lines that you have changed
in your pull request. If you format an entire file and change code outside of
the scope of your PR, it will likely be rejected.*

Extending to support additional I/O frameworks
----------------------------------------------
New non-blocking adapters may be implemented in either of the following ways:

- By subclassing ``pika.BaseConnection``, implementing its abstract method and
  passing its constructor an implementation of
  ``pika.adapters.utils.nbio_interface.AbstractIOServices``.
  ``pika.BaseConnection`` implements ``pika.connection.Connection``'s abstract
  methods, including internally-initiated connection logic. For examples, refer
  to the implementations of
  ``pika.adapters.asyncio_connection.AsyncioConnection``,
  ``pika.adapters.gevent_connection.GeventConnection`` and
  ``pika.adapters.tornado_connection.TornadoConnection``.
- By subclassing ``pika.connection.Connection`` and implementing its abstract
  methods. This approach facilitates implementation of custom
  connection-establishment and transport mechanisms. For an example, refer to
  the implementation of
  ``pika.adapters.twisted_connection.TwistedProtocolConnection``.

.. |Version| image:: https://img.shields.io/pypi/v/pika.svg?
   :target: http://badge.fury.io/py/pika

.. |Python versions| image:: https://img.shields.io/pypi/pyversions/pika.svg
    :target: https://pypi.python.org/pypi/pika

.. |Actions Status| image:: https://github.com/pika/pika/actions/workflows/main.yaml/badge.svg
   :target: https://github.com/pika/pika/actions/workflows/main.yaml

.. |Coverage| image:: https://img.shields.io/codecov/c/github/pika/pika.svg?
   :target: https://codecov.io/github/pika/pika?branch=main

.. |License| image:: https://img.shields.io/pypi/l/pika.svg?
   :target: https://pika.readthedocs.io

.. |Docs| image:: https://readthedocs.org/projects/pika/badge/?version=stable
   :target: https://pika.readthedocs.io
   :alt: Documentation Status
