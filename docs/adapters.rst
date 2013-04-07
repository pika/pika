adapters
========

.. NOTE::
    The following class level documentation is not intended for use by those using Pika in their applications. This documentation is for those who are extending Pika or otherwise working on the driver itself. For an overview of how to use adapters, please reference the :doc:`connecting` documentation.

base_connection
---------------
.. automodule:: pika.adapters.base_connection

BaseConnection
^^^^^^^^^^^^^^
.. autoclass:: BaseConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:


asyncore_connection
-------------------
.. automodule:: pika.adapters.asyncore_connection

AsyncoreConnection
^^^^^^^^^^^^^^^^^^
.. autoclass:: AsyncoreConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

PikaDispatcher
^^^^^^^^^^^^^^
.. autoclass:: PikaDispatcher
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:


blocking_connection
-------------------
.. automodule:: pika.adapters.blocking_connection

BlockingConnection
^^^^^^^^^^^^^^^^^^
.. autoclass:: BlockingConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:


select_connection
-----------------
.. automodule:: pika.adapters.select_connection

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


tornado_connection
-------------------
.. automodule:: pika.adapters.tornado_connection

TornadoConnection
^^^^^^^^^^^^^^^^^
.. autoclass:: TornadoConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:


twisted_connection
-------------------
.. automodule:: pika.adapters.twisted_connection

TwistedConnection
^^^^^^^^^^^^^^^^^
.. autoclass:: TwistedConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:

.. autoclass:: TwistedProtocolConnection
   :members:
   :member-order: bysource
   :private-members: True
   :undoc-members:
   :noindex:
