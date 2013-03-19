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

0.9.12 - 2013-03-18
-------------------

**Bugfixes**

- New timeout id hashing was not unique

0.9.11 - 2013-03-17
-------------------

**Bugfixes**

- Address inconsistent channel close callback documentation and add the signature
  change to the TwistedChannel class (#305)
- Address a missed timeout related internal data structure name change
  introduced in the SelectConnection 0.9.10 release. Update all connection
  adapters to use same signature and docstring (#306).

0.9.10 - 2013-03-16
-------------------

**Bugfixes**

- Fix timeout in twisted adapter (Submitted by cellscape)
- Fix blocking_connection poll timer resolution to milliseconds (Submitted by cellscape)
- Fix channel._on_close() without a method frame (Submitted by Richard Boulton)
- Addressed exception on close (Issue #279 - fix by patcpsc)
- 'messages' not initialized in BlockingConnection.cancel() (Issue #289 - fix by Mik Kocikowski)
- Make queue_unbind behave like queue_bind (Issue #277)
- Address closing behavioral issues for connections and channels (Issues #275,
- Pass a Method frame to Channel._on_close in Connection._on_disconnect (Submitted by wulczer)
- Fix channel closed callback signature in the Twisted adapter (Submitted by wulczer)
- Don't stop the IOLoop on connection close for in the Twisted adapter (Submitted by wulczer)
- Update the asynchronous examples to fix reconnecting and have it work
- Warn if the socket was closed such as if RabbitMQ dies without a Close frame
- Fix URLParameters ssl_options (Issue #296)
- Add state to BlockingConnection addressing (Issue #301)
- Encode unicode body content prior to publishing (Issue #282)
- Fix an issue with unicode keys in BasicProperties headers key (Issue #280)
- Change how timeout ids are generated (Issue #254)
- Address post close state issues in Channel (Issue #302)

** Behavioral changes **

- Change core connection communication behavior to prefer outbound writes over reads, addressing a recursion issue
- Update connection on close callbacks, changing callback method signature
- Update channel on close callbacks, changing callback method signature
- Give more info in the ChannelClosed exception
- Change the constructor signature for BlockingConnection, block open/close callbacks
- Disable the use of add_on_open_callback/add_on_close_callback methods in BlockingConnection


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
