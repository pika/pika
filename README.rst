Pika
====
Pika is a RabbitMQ (AMQP-0-9-1) client library for Python.

|Version| |Downloads| |Status| |Coverage| |License| |Docs|

Introduction
-------------
Pika is a pure-Python implementation of the AMQP 0-9-1 protocol including RabbitMQ's
extensions.

- Python 2.6+ and 3.3+ are supported.

- Since threads aren't appropriate to every situation, it doesn't
  require threads. It takes care not to forbid them, either. The same
  goes for greenlets, callbacks, continuations and generators. It is
  not necessarily thread-safe however, and your mileage will vary.

- People may be using direct sockets, plain old `select()`,
  or any of the wide variety of ways of getting network events to and from a
  python application. Pika tries to stay compatible with all of these, and to
  make adapting it to a new environment as simple as possible.

This is a Pika fork allowing you to tune the pika client properties. 
Waiting pull request (https://github.com/pika/pika/pull/571) to be merged or 
getting similar functionality on pika master before removing this fork. 

Documentation
-------------
Pika's documentation can be found at `https://pika.readthedocs.org <https://pika.readthedocs.org>`_

Example
-------
Here is the most simple example of use, sending a message with the BlockingConnection adapter:

.. code :: python

    import pika
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.basic_publish(exchange='example',
                          routing_key='test',
                          body='Test Message')
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

And an example of tuning your client properties through Pika 

.. code :: python

     import pika

    client_properties = {
        'product': 'my product',
        'information': 'my information',
        'my_other_props': 'my other property',
    }

    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters("localhost", 5672, '/',
                                           credentials=credentials, client_props=client_properties)
    connection = pika.BlockingConnection(parameters)

Pika provides the following adapters
------------------------------------

- BlockingConnection - enables blocking, synchronous operation on top of library for simple uses
- LibevConnection    - adapter for use with the libev event loop http://libev.schmorp.de
- SelectConnection   - fast asynchronous adapter
- TornadoConnection  - adapter for use with the Tornado IO Loop http://tornadoweb.org
- TwistedConnection  - adapter for use with the Twisted asynchronous package http://twistedmatrix.com/

Contributing
------------
To contribute to pika, please make sure that any new features or changes
to existing functionality **include test coverage**.

*Pull requests that add or change code without coverage will most likely be rejected.*

Additionally, please format your code using `yapf <http://pypi.python.org/pypi/yapf>`_
with ``google`` style prior to issuing your pull request.

.. |Version| image:: https://img.shields.io/pypi/v/pika.svg?
   :target: http://badge.fury.io/py/pika

.. |Status| image:: https://img.shields.io/travis/pika/pika.svg?
   :target: https://travis-ci.org/pika/pika

.. |Coverage| image:: https://img.shields.io/codecov/c/github/pika/pika.svg?
   :target: https://codecov.io/github/pika/pika?branch=master

.. |Downloads| image:: https://img.shields.io/pypi/dm/pika.svg?
   :target: https://pypi.python.org/pypi/pika

.. |License| image:: https://img.shields.io/pypi/l/pika.svg?
   :target: https://pika.readthedocs.org

.. |Docs| image:: https://readthedocs.org/projects/pika/badge/?version=stable
   :target: https://pika.readthedocs.org
   :alt: Documentation Status
