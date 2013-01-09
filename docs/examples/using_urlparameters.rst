Using URLParameters
===================
Pika has two methods of encapsulating the data that lets it know how to connect
to RabbitMQ, :py:class:`pika.connection.ConnectionParameters` and :py:class:`pika.connection.URLParameters`.

.. note::
    If you're connecting to RabbitMQ on localhost on port 5672, with the default virtual host of */* and the default username and password of *guest* and *guest*, you do not need to specify connection parameters when connecting.

Using :py:class:`pika.connection.URLParameters` is an easy way to minimize the
variables required to connect to RabbitMQ and supports all of the directives
that :py:class:`pika.connection.ConnectionParameters` supports.

The following is the format for the URLParameters connection value::

  scheme://username:password@host:port/virtual_host?key=value&key=value

As you can see, by default, the scheme (amqp, amqps), username, password, host, port and virtual host make up the core of the URL and any other parameter is passed in as query string values.

Example Connection URLS
-----------------------

The default connection URL connects to the / virtual host as guest using the guest password on localhost port 5672. Note the forwardslash in the URL is encoded to %2F::

  amqp://guest:guest@localhost:5672/%2F

Connect to a host *rabbit1* as the user *www-data* using the password *rabbit_pwd* on the virtual host *web_messages*::

  amqp://www-data:rabbit_pwd@rabbit1/web_messages

Connecting via SSL is pretty easy too. To connect via SSL for the previous example, simply change the scheme to *amqps*. If you do not specify a port, Pika will use the default SSL port of 5671::

  amqp://www-data:rabbit_pwd@rabbit1/web_messages

If you're looking to tweak other parameters, such as enabling heartbeats, simply add the key/value pair as a query string value. The following builds upon the SSL connection, enabling heartbeats every 30 seconds::

  amqp://www-data:rabbit_pwd@rabbit1/web_messages?heartbeat_interval=30


Options that are available as query string values:

- backpressure_detection: Pass in a value of *t* to enable backpressure detection, it is disabled by default.
- channel_max: Alter the default channel maximum by passing in a 32-bit integer value here
- connection_attempts: Alter the default of 1 connection attempt by passing in an integer value here [#f1]_.
- frame_max: Alter the default frame maximum size value by passing in a long integer value [#f2]_.
- heartbeat_interval: Pass a value greater than zero to enable heartbeats between the server and your application. The integer value you pass here will be the number of seconds between heartbeats.
- locale: Set the locale of the client using underscore delimited posix Locale code in ll_CC format (en_US, pt_BR, de_DE).
- retry_delay: The number of seconds to wait before attempting to reconnect on a failed connection, if connection_attempts is > 0.
- socket_timeout: Change the default socket timeout duration from 0.25 seconds to another integer or float value. Adjust with caution.
- ssl_options: A url encoded dict of values for the SSL connection. The available keys are:
   - ca_certs
   - cert_reqs
   - certfile
   - keyfile
   - ssl_version

For an information on what the ssl_options can be set to reference the `official Python documentation <http://docs.python.org/2/library/ssl.html>`_. Here is an example of setting the client certificate and key::

  amqp://www-data:rabbit_pwd@rabbit1/web_messages?heartbeat_interval=30&ssl_options=%7B%27keyfile%27%3A+%27%2Fetc%2Fssl%2Fmykey.pem%27%2C+%27certfile%27%3A+%27%2Fetc%2Fssl%2Fmycert.pem%27%7D

The following example demonstrates how to generate the ssl_options string with `Python's urllib <http://docs.python.org/2/library/urllib.html>`_::

    import urllib
    urllib.urlencode({'ssl_options': {'certfile': '/etc/ssl/mycert.pem', 'keyfile': '/etc/ssl/mykey.pem'}})


.. rubric:: Footnotes

.. [#f1] The :py:class:`pika.adapters.blocking_connection.BlockingConnection` adapter does not respect the *connection_attempts* parameter.
.. [#f2] The AMQP specification states that a server can reject a request for a frame size larger than the value it passes during content negotiation.
