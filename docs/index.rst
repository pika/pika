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

0.9.14 - 2013-06-*
-------------------

**Bugfixes**

- Major issue with socket buffer refactor in 0.9.13 (#328) fixes by cooper6581 and Erik Andersson
- Fix a bug in SelectConnection that did not allow for a IOLoop to be restarted (#337) fix by Ralf Nyrén
- Fix an issue in BlockingConnection disconnections (#340) fix by Mark Unsworth



**Other**

- Add NullHandler to prevent logging warnings when not configured (#339) by Cenk Altı
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
