Blocking Connection Adapter
.. automodule:: pika.adapters.blocking_connection

Be sure to check out examples in :doc:`/examples`.

Interacting with Pika from another thread
-----------------------------------------

``pika.BlockingConnection`` abstracts its I/O loop from the application and thus exposes ``pika.BlockingConnection.add_callback_threadsafe()``  to allow interacting with Pika from another thread.

.. autoclass:: pika.adapters.blocking_connection.BlockingConnection
  :members:
  :inherited-members:

.. autoclass:: pika.adapters.blocking_connection.BlockingChannel
  :members:
  :inherited-members:
