.. Pika documentation master file, created by
   sphinx-quickstart on Tue Feb  1 18:03:40 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction to Pika
====================

Pika is a pure-Python implementation of the AMQP 0-9-1 protocol that tries to stay fairly independent of the underlying network support library.

This documentation is a combination of both user documentation and module development documentation. Modules and classes called out in the Using Pika section below will cover a majority of what users who are implementation pika in their applications will need. The Pika Core Objects section below lists all of the modules that are internal to Pika.

If you have not developed with Pika or RabbitMQ before, the examples page (tbd) is a good resource to get started.

Using Pika
----------
Pika 0.9a is a major refactor of the Pika library implementing true asynchronous behavior at its core. It is important to understand asynchronous programming concepts to properly use Pika in your application. If you have previously used :py:meth:`~channel.Channel.basic_consume` in an application with Pika v0.5.2 or earlier, you have already implemented continuation-passing style asynchronous code with Pika.

Adapters
^^^^^^^^
.. automodule:: adapters.base_connection
   :members:

For a basic overview off all the adapters and changes related to connecting to RabbitMQ, see the :doc:`/adapters` page.

Channel
^^^^^^^
The core communication with Pika is coordinated via the channel.Channel class. Pika invokes this for you when you call the :py:meth:`~connection.Connection.channel` method.

Pika Core Objects
^^^^^^^^^^^^^^^^^
.. toctree::
   :maxdepth: 2

   adapters
   callback
   channel
   connection
   credentials
   exceptions
   frames
   heartbeat
   log
   object
   reconnection_strategies
   simplebuffer
   spec
   table

Pika Authors
^^^^^^^^^^^^
- `Ask Solem Hoel <http://github.com/ask>`_
- `Gavin M. Roy <http://github.com/gmr>`_
- `Marek Majkowski <http://github.com/majek>`_
- `Tony Garnock-Jones <http://github.com/tonyg>`_

Indices and tables
^^^^^^^^^^^^^^^^^^

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
