Introduction to Pika
====================
Pika is a pure-Python implementation of the AMQP 0-9-1 protocol that tries to stay fairly independent of the underlying network support library. Currently pika only supports Python 2.6 and 2.7. Work to support 3.3+ is underway.

If you have not developed with Pika or RabbitMQ before, the :doc:`intro` documentation is a good place to get started.

Installing Pika
---------------
Pika is available for download via PyPI and may be installed using easy_install or pip::

    pip install pika

or::

    easy_install pika

To install from source, run "python setup.py install" in the root source directory.

Using Pika
----------
.. toctree::
   :glob:
   :maxdepth: 1

   intro
   modules/index
   examples
   faq
   contributors
   version_history

0.9.13 - 2013-05-15
-------------------
**Major Changes**

- IPv6 Support with thanks to Alessandro Tagliapietra for initial prototype
- Officially remove support for <= Python 2.5 even though it was broken already
- Drop pika.simplebuffer.SimpleBuffer in favor of the Python stdlib collections.deque object
- New default object for receiving content is a "bytes" object which is a str wrapper in Python 2, but paves way for Python 3 support
- New "Raw" mode for frame decoding content frames (#334) addresses issues #331, #229 added by Garth Williamson
- Connection and Disconnection logic refactored, allowing for cleaner separation of protocol logic and socket handling logic as well as connection state management
- New "on_open_error_callback" argument in creating connection objects and new Connection.add_on_open_error_callback method
- New Connection.connect method to cleanly allow for reconnection code
- Support for all AMQP field types, using protocol specified signed/unsigned unpacking

**Backwards Incompatible Changes**

- Method signature for creating connection objects has new argument "on_open_error_callback" which is positionally before "on_close_callback"
- Internal callback variable names in connection.Connection have been renamed and constants used. If you relied on any of these callbacks outside of their internal use, make sure to check out the new constants.
- Connection._connect method, which was an internal only method is now deprecated and will raise a DeprecationWarning. If you relied on this method, your code needs to change.
- pika.simplebuffer has been removed

**Bugfixes**

- BlockingConnection consumer generator does not free buffer when exited (#328)
- Unicode body payloads in the blocking adapter raises exception (#333)
- Support "b" short-short-int AMQP data type (#318)
- Docstring type fix in adapters/select_connection (#316) fix by Rikard Hultén
- IPv6 not supported (#309)
- Stop the HeartbeatChecker when connection is closed (#307)
- Unittest fix for SelectConnection (#336) fix by Erik Andersson
- Handle condition where no connection or socket exists but SelectConnection needs a timeout for retrying a connection (#322)
- TwistedAdapter lagging behind BaseConnection changes (#321) fix by Jan Urbański

**Other**

- Refactored documentation
- Added Twisted Adapter example (#314) by nolinksoft

Authors
-------
- `Tony Garnock-Jones <http://github.com/tonyg>`_
- `Gavin M. Roy <http://github.com/gmr>`_

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
