Connecting to RabbitMQ
======================
Pika provides multiple adapters to connect to RabbitMQ allowing for different ways of providing socket communication depending on what is appropriate for your application.

- :ref:`adapters_select_connection_SelectConnection`: A native event based connection adapter that implements select, kqueue, poll and epoll.
- :ref:`adapters_asyncore_connection_AsyncoreConnection`: Legacy adapter kept for convenience of previous Pika users.
- :ref:`adapters_tornado_connection_TornadoConnection`: Connection adapter for use with the Tornado IO Loop.
- :ref:`adapters_blocking_connection_BlockingConnection`: Enables blocking, synchronous operation on top of library for simple uses.

.. _intro_to_ioloop:

IO and Event Looping
--------------------

Due to the need to check for and send content on a consistent basis, Pika now implements or extends IOLoops in each of its asynchronous connection adapters. These IOLoops are blocking methods which loop and listen for events. Each asynchronous adapters follows the same standard for invoking the IOLoop. The IOLoop is created when the connection adapter is created. To start it simply call the connection.ioloop.start() method.

If you are using an external IOLoop such as Tornado's IOLoop, you may invoke that as you normally would and then add the adapter to it.

Example::

    import pika

    def on_open(connection):
        # Invoked when the connection is open
        pass

    # Create our connection object, passing in the on_open method
    connection = pika.SelectConnection(on_open)

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

Interfacing with Pika asynchronously is done by passing in callback methods you would like to have invoked when a certain event has completed. For example, if you are going to declare a queue, you pass in a method that will be called when the RabbitMQ server returns a `Queue.DeclareOk <http://www.rabbitmq.com/amqp-0-9-1-quickref.html#queue.declare>`_ response.

In our example below we use the following four easy steps:

#. We start by creating our connection object, then starting our event loop.
#. When we are connected, the *on_connected* method is called. In that method we create a channel.
#. When the channel is created, the *on_channel_open* method is called. In that method we declare a queue.
#. When the queue is declared successfully, *on_queue_declared* is called. In that method we call :py:meth:`channel.basic_consume <channel.Channel.basic_consume>` telling it to call the handle_delivery for each message RabbitMQ delivers to us.
#. When RabbitMQ has a message to send us, it call the handle_delivery method passing the AMQP Method frame, Header frame and Body.

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
        print body

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

The credentials module provides the mechanism by which you pass the username
and password to the :py:meth:`connection.ConnectionParameters` class when it
is created.

.. autoclass:: pika.credentials.PlainCredentials
   :members:
   :inherited-members:
   :member-order: bysource

Example::

    import pika
    credentials = pika.PlainCredentials('username', 'password')
    parameters = pika.ConnectionParameters(credentials=credentials)

.. _connection_parameters:

Connection Parameters
---------------------

There are two types of connection parameter classes in Pika to allow you to pass the connection information into a connection adapter, ConnectionParameters and URLParameters. Both classes share the same default connection values.

Default Parameter Values
^^^^^^^^^^^^^^^^^^^^^^^^
The connection parameters classes extend pika.connection.Parameters to create a consistent definition of default values and internal attributes.

.. autodata:: pika.connection.Parameters

ConnectionParameters
^^^^^^^^^^^^^^^^^^^^
The ConnectionParameters class allows you to specify the options needed when creating the object.

.. autoclass:: pika.connection.ConnectionParameters
   :members:
   :inherited-members:
   :member-order: bysource

Example::

    import pika

    # Set the connection parameters to connect to rabbit-server1 on port 5672
    # on the / virtual host using the username "guest" and password "guest"
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('rabbit-server1',
                                           5672
                                           '/',
                                           credentials)


URLParameters
^^^^^^^^^^^^^

The URLParameters class allows you to pass in an AMQP URL when creating the object and supports the host, port, virtual host, ssl, username and password in the base URL and other options are passed in via query parameters.

.. autoclass:: pika.connection.URLParameters
   :members:
   :inherited-members:
   :member-order: bysource

Example::

    import pika

    # Set the connection parameters to connect to rabbit-server1 on port 5672
    # on the / virtual host using the username "guest" and password "guest"
    parameters = pika.URLParameters('amqp://guest:guest@rabbit-server1:5672/%2F')


.. _intro_to_backpressure:

TCP Backpressure
----------------

As of RabbitMQ 2.0, client side `Channel.Flow <http://www.rabbitmq.com/amqp-0-9-1-quickref.html#channel.flow>`_ has been removed [#f1]_. Instead, the RabbitMQ broker uses TCP Backpressure to slow your client if it is delivering messages too fast. If you pass in backpressure_detection into your connection parameters, Pika attempts to help you handle this situation by providing a mechanism by which you may be notified if Pika has noticed too many frames have yet to be delivered. By registering a callback function with the :py:meth:`add_backpressure_callback <pika.connection.Connection.add_backpressure_callback>` method of any connection adapter, your function will be called when Pika sees that a backlog of 10 times the average frame size you have been sending has been exceeded. You may tweak the notification multiplier value by calling the :py:meth:`set_backpressure_multiplier <pika.connection.Connection.set_backpressure_multiplier>` method passing any integer value.

Example::

    import pika

    parameters = pika.URLParameters('amqp://guest:guest@rabbit-server1:5672/%2F?backpressure_detection=t')

Available Adapters
------------------

The following connection adapters are available for connecting with RabbitMQ:

.. _adapters_asyncore_connection_AsyncoreConnection:

AsyncoreConnection
^^^^^^^^^^^^^^^^^^
.. NOTE::
    Use It is recommended that you use SelectConnection and its method signatures are the same as AsyncoreConnection.

The AsyncoreConnection class is provided for legacy support and quicker porting from applications that used Pika version 0.5.2 and prior.

.. autoclass:: pika.adapters.asyncore_connection.AsyncoreConnection
   :members:
   :inherited-members:
   :member-order: bysource

.. _adapters_blocking_connection_BlockingConnection:

BlockingConnection
^^^^^^^^^^^^^^^^^^
The BlockingConnection creates a layer on top of Pika's asynchronous core providng methods that will block until their expected response has returned.
Due to the asynchronous nature of the Basic.Deliver and Basic.Return calls from RabbitMQ to your application, you are still required to implement
continuation-passing style asynchronous methods if you'd like to receive messages from RabbitMQ using basic_consume or if you want to be notified of
a delivery failure when using basic_publish.

Basic.Get is a blocking call which will either return the Method Frame, Header Frame and Body of a message, or it will return a Basic.GetEmpty frame as the Method Frame.

For more information on using the BlockingConnection, see :py:meth:`BlockingChannel <pika.adapters.blocking_connection.BlockingChannel>`

Publishing Example::

    import pika

    # Open a connection to RabbitMQ on localhost using all default parameters
    connection = pika.BlockingConnection()

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False)

    # Send a message
    channel.basic_publish(exchange='',
                          routing_key="test",
                          body="Hello World!",
                          properties=pika.BasicProperties(content_type="text/plain",
                                                          delivery_mode=1))

Consuming Example::

    import pika

    # Open a connection to RabbitMQ on localhost using all default parameters
    connection = pika.BlockingConnection()

    # Open the channel
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="test", durable=True,
                          exclusive=False, auto_delete=False)

    # Start our counter at 0
    messages = 0

    # Method that will receive our messages and stop consuming after 10
    def _on_message(channel, method, header, body):
        print "Message:"
        print "\t%r" % method
        print "\t%r" % header
        print "\t%r" % body

        # Acknowledge message receipt
        channel.basic_ack(method.delivery_tag)

        # We've received 10 messages, stop consuming
        global messages
        messages += 1
        if messages > 10:
            channel.stop_consuming()

    # Setup up our consumer callback
    channel.basic_consume(_on_message, queue="test")

    # This is blocking until channel.stop_consuming is called and will allow us to receive messages
    channel.start_consuming()

.. automodule:: pika.adapters.blocking_connection
.. autoclass:: BlockingConnection
   :members:
   :inherited-members:
   :member-order: bysource

.. _adapters_select_connection_SelectConnection:

SelectConnection
^^^^^^^^^^^^^^^^

.. NOTE::
    SelectConnection is the recommended method for using Pika under most circumstances. It supports multiple event notification methods including select, epoll, kqueue and poll.

By default SelectConnection will attempt to use the most appropriate event
notification method for your system. In order to override the default behavior
you may set the poller type by assigning a string value to the
select_connection modules POLLER_TYPE attribute prior to creating the
SelectConnection object instance. Valid values are: kqueue, poll, epoll, select

Poller Type Override Example::

  from pika.adapters import select_connection

  select_connection.POLLER_TYPE = 'epoll'
  connection = select_connection.SelectConnection()

See the :ref:`Continuation-Passing Style example <cps_example>` for an example of using SelectConnection.

.. automodule:: pika.adapters.select_connection
.. autoclass:: SelectConnection
   :member-order: bysource
   :members:
   :inherited-members:

.. _adapters_tornado_connection_TornadoConnection:


TornadoConnection
^^^^^^^^^^^^^^^^^

Tornado is an open source version of the scalable, non-blocking web server and tools that power FriendFeed. For more information on tornado, visit http://tornadoweb.org

Since the Tornado IOLoop blocks once it is started, it is suggested that you use a timer to add Pika to your tornado.Application instance after the HTTPServer has started.

The following is a simple, non-working example on how to add Pika to the Tornado IOLoop without blocking other applications from doing so. To see a fully workng example,
see the Tornado Demo application in the examples.

Example::

    from pika.adapters.tornado_connection import TornadoConnection

    class PikaClient(object):
        def connect(self):
            self.connection = TornadoConnection(on_connected_callback=self.on_connected)

    # Create our Tornado Application
    application = tornado.web.Application([
        (r"/", ExampleHandler)
    ], **settings)

    # Create our Pika Client
    application.pika = PikaClient()

    # Start the HTTPServer
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8080)

    # Get a handle to the instance of IOLoop
    ioloop = tornado.ioloop.IOLoop.instance()

    # Add our Pika connect to the IOLoop since we loop on ioloop.start
    ioloop.add_timeout(500, application.pika.connect)

    # Start the IOLoop
    ioloop.start()

.. automodule:: pika.adapters.tornado_connection
.. autoclass:: TornadoConnection
   :members:
   :inherited-members:
   :member-order: bysource


Twisted
^^^^^^^
::

    import pika
    from pika import exceptions
    from pika.adapters import twisted_connection
    from twisted.internet import defer, reactor, protocol,task


    @defer.inlineCallbacks
    def run(connection):

        channel = yield connection.channel()

    parameters = pika.ConnectionParameters()
    cc = protocol.ClientCreator(reactor, twisted_connection.TwistedProtocolConnection, parameters)
    d = cc.connectTCP('hostname', 5672)
    d.addCallback(lambda protocol: protocol.ready)
    d.addCallback(run)
    reactor.run()

.. automodule:: pika.adapters.twisted_connection
.. autoclass:: TwistedConnection
   :members:
   :inherited-members:
   :member-order: bysource
.. autoclass:: TwistedProtocolConnection
   :members:
   :inherited-members:
   :member-order: bysource


.. rubric:: Footnotes

.. [#f1] "more effective flow control mechanism that does not require cooperation from clients and reacts quickly to prevent the broker from exhausing memory - see http://www.rabbitmq.com/extensions.html#memsup" from http://lists.rabbitmq.com/pipermail/rabbitmq-announce/attachments/20100825/2c672695/attachment.txt

