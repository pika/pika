adapters
========

.. NOTE::
    The following class level documentation is not intended for use by those using Pika in their applications. This documentation is for those who are extending Pika or otherwise working on the driver itself. For an overview of how to use adapters, please reference the :doc:`connecting` documentation.

base_connection
---------------
.. automodule:: adapters.base_connection

BaseConnection
^^^^^^^^^^^^^^
.. autoclass:: BaseConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

select_connection
-----------------
.. automodule:: adapters.select_connection

SelectConnection
^^^^^^^^^^^^^^^^
.. autoclass:: SelectConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

IOLoop
^^^^^^
.. autoclass:: IOLoop
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :inherited-members:

SelectPoller
^^^^^^^^^^^^
.. autoclass:: SelectPoller
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :inherited-members:

KQueuePoller
^^^^^^^^^^^^
.. autoclass:: KQueuePoller
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :inherited-members:

PollPoller
^^^^^^^^^^
.. autoclass:: PollPoller
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :inherited-members:

EPollPoller
^^^^^^^^^^^
.. autoclass:: EPollPoller
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :inherited-members:

asyncore_connection
-------------------
.. automodule:: adapters.asyncore_connection

AsyncoreConnection
^^^^^^^^^^^^^^^^^^
.. autoclass:: AsyncoreConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

AsyncoreDispatcher
^^^^^^^^^^^^^^^^^^
.. autoclass:: AsyncoreDispatcher
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

tornado_connection
-------------------
.. automodule:: adapters.tornado_connection

TornadoConnection
^^^^^^^^^^^^^^^^^
.. autoclass:: TornadoConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

blocking_connection
-------------------
.. automodule:: adapters.blocking_connection

BlockingConnection
^^^^^^^^^^^^^^^^^^
.. autoclass:: BlockingConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

BlockingChannel
^^^^^^^^^^^^^^^
.. autoclass:: BlockingChannel
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

BlockingChannelTransport
^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: BlockingChannelTransport
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:
