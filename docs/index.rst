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

0.9.13 - 2013-04-??
-------------------
**Major Changes**

- Connection and Disconnection logic refactored, allowing for cleaner separation of protocol logic and socket handling logic as well as connection state management
- IPv6 Support with thanks to Alessandro Tagliapietra for initial prototype
- New "on_open_error_callback" argument in creating connection objects and new Connection.add_on_open_error_callback method
- Support for all AMQP field types, using protocol specified signed/unsigned unpacking
- New Connection.connect method to cleanly allow for reconnection code

**Backwards Incompatible Changes**

- Method signature for creating connection objects has new argument "on_open_error_callback" which is positionally before "on_close_callback"
- Internal callback variable names in connection.Connection have been renamed and constants used. If you relied on any of these callbacks outside of their internal use, make sure to check out the new constants.
- Connection._connect method, which was an internal only method is now deprecated and will raise a DeprecationWarning. If you relied on this method, your code needs to change.

**Bugfixes**

- Support "b" short-short-int AMQP data type (#318)
- Docstring type fix in adapters/select_connection (#316) fix by Rikard Hultén
- IPv6 not supported (#309)
- Stop the HeartbeatChecker when connection is closed (#307)

**Other**

- Added Twisted Adapter example (#314)


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
