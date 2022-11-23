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
   asyncio
   tornado
   twisted
   gevent

Requesting message acknowledgements from another thread
-------------------------------------------------------
The single-threaded usage constraint of an individual Pika connection adapter
instance may result in a dropped AMQP/stream connection due to AMQP heartbeat
timeout in consumers that take a long time to process an incoming message. A
common solution is to delegate processing of the incoming messages to another
thread, while the connection adapter's thread continues to service its I/O
loop's message pump, permitting AMQP heartbeats and other I/O to be serviced in
a timely fashion.

Messages processed in another thread may not be acknowledged directly from that
thread, since all accesses to the connection adapter instance must be from a
single thread, which is the thread running the adapter's I/O loop. This is
accomplished by requesting a callback to be executed in the adapter's
I/O loop thread. For example, the callback function's implementation might look
like this:

.. code :: python

    def ack_message(channel, delivery_tag):
        """Note that `channel` must be the same Pika channel instance via which
        the message being acknowledged was retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't acknowledge this message;
            # log and/or do something that makes sense for your app in this case.
            pass

The code running in the other thread may request the ``ack_message()`` function
to be executed in the connection adapter's I/O loop thread using an
adapter-specific mechanism:

- ``pika.BlockingConnection`` abstracts its I/O loop from the application and
  thus exposes ``pika.BlockingConnection.add_callback_threadsafe()``. Refer to
  this method's docstring for additional information. For example:

  .. code :: python

      connection.add_callback_threadsafe(functools.partial(ack_message, channel, delivery_tag))

Please see the documentation of other adapters for their specific methods.

This threadsafe callback request mechanism may also be used to delegate
publishing of messages, etc., from a background thread to the connection
adapter's thread.
