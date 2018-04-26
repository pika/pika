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
  require threads. It takes care not to forbid them, either. The same
  goes for greenlets, callbacks, continuations and generators. It is
  not necessarily thread-safe however, and your mileage will vary.

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

With non-blocking adapters, you can request a connection using multiple
connection parameter instances via the connection adapter's
`create_connection()` class method.

Pika provides the following adapters
------------------------------------

- AsyncioConnection  - adapter for the Python3 AsyncIO event loop
- BlockingConnection - enables blocking, synchronous operation on top of library for simple usage
- SelectConnection   - fast asynchronous adapter without 3rd-party dependencies
- TornadoConnection  - adapter for use with the Tornado IO Loop http://tornadoweb.org
- TwistedConnection  - adapter for use with the Twisted asynchronous package http://twistedmatrix.com/

Contributing
------------
To contribute to pika, please make sure that any new features or changes
to existing functionality **include test coverage**.

*Pull requests that add or change code without coverage will be rejected.*

Additionally, please format your code using `yapf <http://pypi.python.org/pypi/yapf>`_
with ``google`` style prior to issuing your pull request.

Extending to support additional I/O frameworks
----------------------------------------------
New non-blocking adapters may be implemented in either of the following ways:
- By subclassing :py:class:`pika.adapters.base_connection.BaseConnection` and
  implementing its abstract method(s) and passing BaseConnection's constructor
  an implementation of
  :py.class:`pika.adapters.utils.nbio_interface.AbstractIOServices`. For
  examples, refer to the implementations of
  :py:class:`pika.AsyncioConnection` and :py:class:`pika.TornadoConnection`.
- By subclassing :py:class:`pika.connection.connection.Connection` and
  implementing its abstract method(s). For an example, refer to the
  implementation of
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
