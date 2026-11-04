# Using URLParameters

Pika has two methods of encapsulating the data that lets it know how to connect to RabbitMQ, `pika.connection.ConnectionParameters` and `pika.connection.URLParameters`.

!!! note

    If you're connecting to RabbitMQ on localhost on port 5672, with the default virtual host of */* and the default username and password of *guest* and *guest*, `BlockingConnection` and the asynchronous adapters can be constructed without connection parameters at all. `pika.Connection` requires them, so pass `pika.ConnectionParameters()` to take the same defaults.

Using `pika.connection.URLParameters` is an easy way to minimize the variables required to connect to RabbitMQ and supports all of the directives that `pika.connection.ConnectionParameters` supports.

The following is the format for the URLParameters connection value:

```text
scheme://username:password@host:port/virtual_host?key=value&key=value
```

As you can see, by default, the scheme (amqp, amqps), username, password, host, port and virtual host make up the core of the URL and any other parameter is passed in as query string values.

## Example Connection URLS

The default connection URL connects to the / virtual host as guest using the guest password on localhost port 5672. Note the forwardslash in the URL is encoded to %2F:

```text
amqp://guest:guest@localhost:5672/%2F
```

Connect to a host *rabbit1* as the user *www-data* using the password *rabbit_pwd* on the virtual host *web_messages*:

```text
amqp://www-data:rabbit_pwd@rabbit1/web_messages
```

Connecting via SSL is pretty easy too. To connect via SSL for the previous example, simply change the scheme to *amqps*. If you do not specify a port, Pika will use the default SSL port of 5671:

```text
amqps://www-data:rabbit_pwd@rabbit1/web_messages
```

If you're looking to tweak other parameters, such as enabling heartbeats, simply add the key/value pair as a query string value. The following builds upon the SSL connection, enabling heartbeats every 30 seconds:

```text
amqps://www-data:rabbit_pwd@rabbit1/web_messages?heartbeat=30
```

Options that are available as query string values:

- channel_max: Alter the default channel maximum by passing in a 32-bit integer value here.
- client_properties: A dictionary of client properties to override:

```text
amqps://www-data:rabbit_pwd@rabbit1/web_messages?client_properties=%7B%27connection_name%27%3A+%27My+Unique+Connection+Name%27%7D
```

- connection_attempts: Alter the default of 1 connection attempt by passing in an integer value here.
- frame_max: Alter the default frame maximum size value by passing in a long integer value [^f1].
- heartbeat: Propose a heartbeat timeout in seconds between the server and your application. The value is negotiated with the broker per AMQP 0-9-1: 0 defers to the broker's proposal, when both proposals are nonzero the lower one wins, and heartbeats are disabled only if both sides propose 0.
- locale: Set the locale of the client using underscore delimited posix Locale code in ll_CC format (en_US, pt_BR, de_DE).
- max_inbound_message_body_size: The largest message body, in bytes, to accept from the broker, from 1 to 536870912 (512 MiB). Defaults to 67108864 (64 MiB); a larger message closes the connection.
- retry_delay: The number of seconds to wait before attempting to reconnect on a failed connection, if connection_attempts is > 0.
- socket_timeout: Change the `socket.connect()` timeout from its default of 10 seconds to another integer or float value, or `None` to disable it. Adjust with caution.
- ssl_options: A url encoded dict of values for the SSL connection. The keys are the arguments the corresponding `ssl.SSLContext` methods take:
    - protocol, passed to `ssl.SSLContext`
    - ca_certs (or cafile), ca_path (or capath), ca_data (or cadata), passed to `SSLContext.load_verify_locations`
    - certfile, keyfile, password, passed to `SSLContext.load_cert_chain`
    - ciphers, passed to `SSLContext.set_ciphers`
    - server_hostname, passed to `pika.SSLOptions`

!!! note

    `cert_reqs` and `ssl_version` are not among them. They were arguments of the removed `ssl.wrap_socket` and are ignored, so a `cert_reqs` of `CERT_NONE` silently leaves verification on. A URL that sets no other `ssl_options` key is worse than ignored: it raises `TypeError: cafile, capath and cadata cannot be all omitted`.

For information on what the ssl_options can be set to reference the [official Python documentation](https://docs.python.org/3/library/ssl.html). Here is an example of setting the client certificate and key. Note the *amqps* scheme: the query argument configures TLS but does not select it, so with *amqp* you would get a TLS context aimed at the plaintext port 5672.

```text
amqps://www-data:rabbit_pwd@rabbit1/web_messages?heartbeat=30&ssl_options=%7B%27keyfile%27%3A+%27%2Fetc%2Fssl%2Fmykey.pem%27%2C+%27certfile%27%3A+%27%2Fetc%2Fssl%2Fmycert.pem%27%7D
```

The following example generates an ssl_options string like that one with [Python's urllib](https://docs.python.org/3/library/urllib.html), using the repository's own test certificates, and connects with it:

```python
--8<-- "examples/using_urlparameters.py"
```

## Footnotes

[^f1]: The AMQP specification states that a server can reject a request for a frame size larger than the value it passes during content negotiation.
