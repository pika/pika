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
Pika supports two modes of development, synchronous using the BlockingConnection adapter and asynchronous using one of the AsyncoreConnection, SelectConnection and TornadoConnection adapters.

.. toctree::
   :glob:
   :maxdepth: 2

   connecting
   channel_use
   examples/index
   faq

0.9.7 Release Nodes
-------------------

- New featurs
 - generator based consumer in BlockingChannel

- Bugfixs
 - Added the exchange "type" parameter back but issue a DeprecationWarning
 - Dont require a queue name in Channel.queue_declare()
 - Fixed KeyError when processing timeouts (Issue # 215 - Fix by Raphael De Giusti)
 - Don't try and close channels when the connection is closed (Issue #216 - Fix by Charles Law)
 - Dont raise UnexpectedFrame exceptions, log them instead
 - Handle multiple synchronous RPC calls made without waiting for the call result (Issues #192, #204, #211)
 - Typo in docs (Issue #207 Fix by Luca Wehrstedt)
 - Only sleep on connection failure when retry attempts are > 0 (Issue #200)
 - Bypass _rpc method and just send frames for Basic.Ack, Basic.Nack, Basic.Reject (Issue #205)

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
