Introduction to Pika
====================

IO and Event Looping
--------------------
As AMQP is a two-way RPC protocol where the client can send requests to the server and the server can send requests to a client, Pika implements or extends IO loops in each of its asynchronous connection adapters. These IO loops are blocking methods which loop and listen for events. Each asynchronous adapter follows the same standard for invoking the IO loop. The IO loop is created when the connection adapter is created. To start an IO loop for any given adapter, call the ``connection.ioloop.start()`` method.

If you are using an external IO loop such as Tornado's :class:`~tornado.ioloop.IOLoop` you invoke it normally and then add a pika adapter to it.

Example::

    import pika

    def on_open(connection):
        # Invoked when the connection is open
        pass

    # Create our connection object, passing in the on_open method
    connection = pika.SelectConnection(on_open_callback=on_open)

    try:
        # Loop so we can communicate with RabbitMQ
        connection.ioloop.start()
    except KeyboardInterrupt:
        # Gracefully close the connection
        connection.close()
        # Loop until we're fully closed, will stop on its own
        connection.ioloop.start()

.. _intro_to_cps:

Continuation-Passing Style
--------------------------

Interfacing with Pika asynchronously is done by passing in callback methods you would like to have invoked when a certain event is completed. For example, if you are going to declare a queue, you pass in a method that will be called when the RabbitMQ server returns a `Queue.DeclareOk <http://www.rabbitmq.com/amqp-0-9-1-quickref.html#queue.declare>`_ response.

In our example below we use the following four easy steps:

#. We start by creating our connection object, then starting our event loop.
#. When we are connected, the *on_connected* method is called. In that method we create a channel.
#. When the channel is created, the *on_channel_open* method is called. In that method we declare a queue.
#. When the queue is declared successfully, *on_queue_declared* is called. In that method we call :py:meth:`channel.basic_consume <channel.Channel.basic_consume>` telling it to call the handle_delivery for each message RabbitMQ delivers to us.
#. When RabbitMQ has a message to send us, it calls the handle_delivery method passing the AMQP Method frame, Header frame, and Body.

.. NOTE::
    Step #1 is on line #28 and Step #2 is on line #6. This is so that Python knows about the functions we'll call in Steps #2 through #5.

.. _cps_example:

Example::

    import pika

    # Create a global channel variable to hold our channel object in
    channel = None

    # Step #2
    def on_connected(connection):
        """Called when we are fully connected to RabbitMQ"""
        # Open a channel
        connection.channel(on_channel_open)

    # Step #3
    def on_channel_open(new_channel):
        """Called when our channel has opened"""
        global channel
        channel = new_channel
        channel.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False, callback=on_queue_declared)

    # Step #4
    def on_queue_declared(frame):
        """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
        channel.basic_consume(handle_delivery, queue='test')

    # Step #5
    def handle_delivery(channel, method, header, body):
        """Called when we receive a message from RabbitMQ"""
        print(body)

    # Step #1: Connect to RabbitMQ using the default parameters
    parameters = pika.ConnectionParameters()
    connection = pika.SelectConnection(parameters, on_connected)

    try:
        # Loop so we can communicate with RabbitMQ
        connection.ioloop.start()
    except KeyboardInterrupt:
        # Gracefully close the connection
        connection.close()
        # Loop until we're fully closed, will stop on its own
        connection.ioloop.start()

Credentials
-----------
The :mod:`pika.credentials` module provides the mechanism by which you pass the username and password to the :py:class:`ConnectionParameters <pika.connection.ConnectionParameters>` class when it is created.

Example::

    import pika
    credentials = pika.PlainCredentials('username', 'password')
    parameters = pika.ConnectionParameters(credentials=credentials)

.. _connection_parameters:

Connection Parameters
---------------------
There are two types of connection parameter classes in Pika to allow you to pass the connection information into a connection adapter, :class:`ConnectionParameters <pika.connection.ConnectionParameters>` and :class:`URLParameters <pika.connection.URLParameters>`. Both classes share the same default connection values.


.. _intro_to_backpressure:

TCP Backpressure
----------------

As of RabbitMQ 2.0, client side `Channel.Flow <http://www.rabbitmq.com/amqp-0-9-1-quickref.html#channel.flow>`_ has been removed [#f1]_. Instead, the RabbitMQ broker uses TCP Backpressure to slow your client if it is delivering messages too fast. If you pass in backpressure_detection into your connection parameters, Pika attempts to help you handle this situation by providing a mechanism by which you may be notified if Pika has noticed too many frames have yet to be delivered. By registering a callback function with the :py:meth:`add_backpressure_callback <pika.connection.Connection.add_backpressure_callback>` method of any connection adapter, your function will be called when Pika sees that a backlog of 10 times the average frame size you have been sending has been exceeded. You may tweak the notification multiplier value by calling the :py:meth:`set_backpressure_multiplier <pika.connection.Connection.set_backpressure_multiplier>` method passing any integer value.

Example::

    import pika

    parameters = pika.URLParameters('amqp://guest:guest@rabbit-server1:5672/%2F?backpressure_detection=t')

.. rubric:: Footnotes

.. [#f1] "more effective flow control mechanism that does not require cooperation from clients and reacts quickly to prevent the broker from exhausing memory - see http://www.rabbitmq.com/extensions.html#memsup" from http://lists.rabbitmq.com/pipermail/rabbitmq-announce/attachments/20100825/2c672695/attachment.txt
