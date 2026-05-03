Connection Parameters
=====================
To maintain flexibility in how you specify the connection information required for your applications to properly connect to RabbitMQ, pika implements two classes for encapsulating the information, :class:`~pika.connection.ConnectionParameters` and :class:`~pika.connection.URLParameters`.

ConnectionParameters
--------------------
The classic object for specifying all of the connection parameters required to connect to RabbitMQ, :class:`~pika.connection.ConnectionParameters` provides attributes for tweaking every possible connection option.

Example::

    import pika

    # Set the connection parameters to connect to rabbit-server1 on port 5672
    # on the / virtual host using the username "guest" and password "guest"
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('rabbit-server1',
                                           5672,
                                           '/',
                                           credentials)

.. autoclass:: pika.connection.ConnectionParameters
   :members:
   :inherited-members:
   :member-order: bysource

URLParameters
-------------
The :class:`~pika.connection.URLParameters` class allows you to pass in an AMQP URL when creating the object and supports the host, port, virtual host, ssl, username and password in the base URL and other options are passed in via query parameters.

Example::

    import pika

    # Set the connection parameters to connect to rabbit-server1 on port 5672
    # on the / virtual host using the username "guest" and password "guest"
    parameters = pika.URLParameters('amqp://guest:guest@rabbit-server1:5672/%2F')

.. autoclass:: pika.connection.URLParameters
   :members:
   :inherited-members:
   :member-order: bysource

