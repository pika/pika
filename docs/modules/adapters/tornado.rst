Tornado Connection Adapter
==========================
.. automodule:: pika.adapters.tornado_connection

Be sure to check out the :doc:`asynchronous examples </examples>` including the Tornado specific :doc:`consumer </examples/tornado_consumer>` example.

Interacting with Pika from another thread
-----------------------------------------
```pika.adapters.tornado_connection.TornadoConnection``'s I/O loop provides
  ``add_callback()`` to allow interacting with Pika from another thread.


.. autoclass:: pika.adapters.tornado_connection.TornadoConnection
  :members:
  :inherited-members:
