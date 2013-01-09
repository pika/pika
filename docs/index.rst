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
   blocking_channel_use
   examples/index
   faq
   version_history

0.9.9 - Unreleased
------------------

**Bugfixes**

- Only remove the tornado_connection.TornadoConnection file descriptor from the IOLoop if it's still open (Issue #221)
- Allow messages with no body (Issue #227)
- Allow for empty routing keys (Issue #224)
- Don't raise an exception when trying to send a frame to a closed connection (Issue #229)
- Only send a Connection.CloseOk if the connection is still open. (Issue #236 - Fix by "noleaf")
- Fix timeout threshold in blocking connection - (Issue #232 - Fix by Adam Flynn)
- Fix closing connection while a channel is still open (Issue #230 - Fix by Adam Flynn)
- Fixed misleading warning and exception messages in BaseConnection (Issue #237 - Fix by Tristan Penman)
- Pluralised and altered the wording of the AMQPConnectionError exception (Issue #237 - Fix by Tristan Penman)
- Fixed _adapter_disconnect in TornadoConnection class (Issue #237 - Fix by Tristan Penman)
- Fixing hang when closing connection without any channel in BlockingConnection (Issue #244 - Fix by Ales Teska)
- Remove the process_timeouts() call in SelectConnection (Issue #239)
- Change the string validation to basestring for host connection parameters (Issue #231)
- Add a poller to the BlockingConnection to address latency issues introduced in Pika 0.9.8 (Issue #242)
- reply_code and reply_text is not set in ChannelException (Issue #250)

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
- Adam Flynn
- Ales Teska
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
- nonleaf
- Olivier Le Thanh Duong
- Pankrat
- Pavlobaron
- Peter Magnusson
- Raphaël De Giusti
- Roey Berman
- Samuel Stauffer
- Sigurd Høgsbro
- Tristan Penman

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
