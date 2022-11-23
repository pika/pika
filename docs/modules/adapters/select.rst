Select Connection Adapter
==========================
Asynchronous adapter without third-party dependencies.


Interacting with Pika from another thread
-----------------------------------------
 ``pika.SelectConnection``'s I/O loop provides ``add_callback_threadsafe()`` to allow interacting with Pika from another thread.

.. automodule:: pika.adapters.select_connection



.. autoclass:: pika.adapters.select_connection.SelectConnection
  :members:
  :inherited-members:
