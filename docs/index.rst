Introduction to Pika
====================

Pika is a pure-Python implementation of the AMQP 0-9-1 protocol that tries to stay fairly independent of the underlying network support library.

This documentation is a combination of both user documentation and module development documentation. Modules and classes called out in the Using Pika section below will cover a majority of what users who are implementation pika in their applications will need. The Pika Core Objects section below lists all of the modules that are internal to Pika.

If you have not developed with Pika or RabbitMQ before, the :doc:`connecting` documentation is a good place to get started.

Installing Pika
---------------

Pika is available for download via PyPI and may be installed using easy_install or pip::

    pip install pika

or::

    easy_install pika

To install from source, run "python setup.py install" in the root source directory.

Using Pika
----------
Pika supports two modes of development, synchronous using the BlockingConnection adapter and asynchronous using one of the AsyncoreConnection, SelectConnection and TwistedConnection adapters.

.. toctree::
   :glob:
   :maxdepth: 2

   connecting
   examples/index
   faq

0.9.6 Release Notes
-------------------

- New features

  - URLParameters
  - BlockingChannel.start_consuming() and BlockingChannel.stop_consuming()
  - Delivery Confirmations
  - Improved unittests

- Major bugfix areas

  - Connection handling
  - Blocking functionality in the BlockingConnection
  - SSL
  - UTF-8 Handling

- Removals

  - pika.reconnection_strategies
  - pika.channel.ChannelTransport
  - pika.log
  - pika.template
  - examples directory

- Over 700 commits from 37 contributors

Pika Core Modules and Classes
-----------------------------
.. NOTE::
   The following documentation is for Pika development and is not intended to be end-user documentation.

.. toctree::
   :maxdepth: 2

   adapters
   amqp_object
   callback
   channel
   connection
   credentials
   data
   exceptions
   frame
   heartbeat
   simplebuffer
   spec
   utils

Authors
-------
- `Tony Garnock-Jones <http://github.com/tonyg>`_
- `Gavin M. Roy <http://github.com/gmr>`_

Contributors
------------
- Alexey Myasnikov
- Anton V. Yanchenko
- Ask Solem
- Asko Soukka
- Brian K. Jones
- Charles Law
- David Strauss
- Fredrik Svensson
- George y
- Hunter Morris
- Jacek 'Forger' Całusiński
- Jan Urbański
- Jason J. W. Williams
- Jonty Wareing
- Josh Braegger
- Josh Hansen
- Jozef Van Eenbergen
- Kamil Kisiel
- Kane
- Kyösti Herrala
- Lars van de Kerkhof
- Marek Majkowski
- Michael Kenney
- Michael Laing
- Milan Skuhra
- Njal Karevoll
- Olivier Le Thanh Duong
- Pankrat
- Pavlobaron
- Peter Magnusson
- Raphaël De Giusti
- Roey Berman
- Samuel Stauffer
- Sigurd Høgsbro

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
