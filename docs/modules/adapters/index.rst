Connection Adapters
===================
Pika uses connection adapters to provide a flexible method for adapting pika's
core communication to different IOLoop implementations. In addition to asynchronous adapters, there is the :class:`BlockingConnection <pika.adapters.blocking_connection.BlockingConnection>` adapter that provides a more idiomatic procedural approach to using Pika.

Adapters
--------
.. toctree::
   :glob:
   :maxdepth: 1

   blocking
   select
   tornado
   twisted
