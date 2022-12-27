Asyncio Connection Adapter
==========================
.. automodule:: pika.adapters.asyncio_connection

Be sure to check out the :doc:`asynchronous examples </examples>` including the asyncio specific :doc:`consumer </examples/asyncio_consumer>` example.

Interacting with Pika from another thread
-----------------------------------------
``pika.adapters.asyncio_connection.AsyncioConnection``'s I/O loop exposes ``call_soon_threadsafe()`` to allow interacting with Pika from another thread.

Class Reference
----------------

.. autoclass:: pika.adapters.asyncio_connection.AsyncioConnection
  :members:
  :inherited-members:
