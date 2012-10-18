frame
=====
The frame module contains the object structure for mapping AMQP Classes and Methods on to Python objects. In addition frame contains the Dispatcher class which steps through the synchronous receipt order of frames received for Basic.Deliver and Basic.GetOk.

.. NOTE::
    This class level documentation is not intended for use by those using Pika in their applications. This documentation is for those who are extending Pika or otherwise working on the driver itself.

.. automodule:: pika.frame

Frame
-----
.. autoclass:: Frame
   :members:
   :inherited-members:
   :member-order: bysource
   :private-members: True

Method
------
.. autoclass:: Method
   :members:
   :inherited-members:
   :member-order: bysource

Header
------
.. autoclass:: Header
   :members:
   :inherited-members:
   :member-order: bysource

Body
----
.. autoclass:: Body
   :members:
   :inherited-members:
   :member-order: bysource

ProtocolHeader
--------------
.. autoclass:: ProtocolHeader
   :members:
   :inherited-members:
   :member-order: bysource

Frame Decoding
--------------

.. autofunction:: decode_frame(data_in)
