.. Pika documentation master file, created by
   sphinx-quickstart on Tue Feb  1 18:03:40 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction to Pika
====================

Pika is a pure-Python implementation of the AMQP 0-9-1 protocol that tries to stay fairly independent of the underlying network support library.

This documentation is a combination of both user documentation and module development documentation. Modules and classes called out in the Using Pika section below will cover a majority of what users who are implementation pika in their applications will need. The Pika Core Objects section below lists all of the modules that are internal to Pika.

If you have not developed with Pika or RabbitMQ before, the :doc:`connecting` documentation is a good place to get started.

If you have developed with Pika prior to version |version|, it is recommended that you also read the :doc:`connecting` documentation to see the differences between |version| and previous versions. In addition if you have used client flow control in the past, you will want to read the section on :ref:`intro_to_backpressure`.

Installing Pika
---------------

Pika is available for download via PyPI and may be installed using easy_install or pip::

    pip install pika

or::

    easy_install pika

You may also download the source for this version as a zip or tar.gz with the following URLs:

- https://github.com/tonyg/pika/tarball/v0.9a
- https://github.com/tonyg/pika/zipball/v0.9a

If you'd like to install from the development (master) branch, you can download an egg file directly from GitHub with pip::

    pip install -e git://github.com/tonyg/pika.git#egg=pika

Using Pika
----------
Pika 0.9a is a major refactor of the Pika library implementing true asynchronous behavior at its core. It is important to understand asynchronous programming concepts to properly use Pika in your application. If you have previously used :py:meth:`~channel.Channel.basic_consume` in an application with Pika v0.5.2 or earlier, you have already implemented continuation-passing style asynchronous code with Pika.

.. toctree::
   :maxdepth: 2

   connecting
   communicating
   examples

Pika Core Modules and Classes
-----------------------------
.. NOTE::
   The following documentation is for Pika development and is not intended to be end-user documentation.

.. toctree::
   :maxdepth: 3

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
------------
- `Ask Solem Hoel <http://github.com/ask>`_
- `Gavin M. Roy <http://github.com/gmr>`_
- `Marek Majkowski <http://github.com/majek>`_
- `Tony Garnock-Jones <http://github.com/tonyg>`_

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
