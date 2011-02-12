log
===
pika.log is a wrapper which provides a wrapper for the `python logging <http://docs.python.org/library/logging.html>`_ module. Pika registeres itself with the logging.getLogger() method using 'pika' as the value.

Debugging Decorator
-------------------
.. py:decorator:: log.method_call

For debugging, a log.method_call decorator is available for use that will automatically log the call of that function and all of the parameters passed to it.
In addition, there is a color logging.Formatter which makes reading debug output a little easier.

Using pika.log
--------------

To turn Pika debug logging on::

     import pika.log
     pika.log.setup(pika.log.DEBUG, color=True)

Once this is done you will receive all low level information about what Pika is doing including the frames it is sending and receiving from RabbitMQ.

Module Documentation
--------------------
.. automodule:: log
   :members:
   :member-order: bysource
