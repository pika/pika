"""
Connection Adapters
===================

Pika provides multiple adapters to connect to RabbitMQ:

- adapters.asyncio_connection.AsyncioConnection: Native Python3 AsyncIO use
- adapters.blocking_connection.BlockingConnection: Enables blocking,
  synchronous operation on top of library for simple uses.
- adapters.select_connection.SelectConnection: A native event based connection
  adapter that implements select, kqueue, poll and epoll.
- adapters.tornado_connection.TornadoConnection: Connection adapter for use
  with the Tornado web framework.
- adapters.twisted_connection.TwistedConnection: Connection adapter for use
  with the Twisted framework

"""
from pika.adapters.base_connection import BaseConnection
from pika.adapters.blocking_connection import BlockingConnection
from pika.adapters.select_connection import SelectConnection
from pika.adapters.select_connection import IOLoop

# Dynamically handle 3rd party library dependencies for optional imports
try:
    from pika.adapters.asyncio_connection import AsyncioConnection
except ImportError:
    AsyncioConnection = None

try:
    from pika.adapters.tornado_connection import TornadoConnection
except ImportError:
    TornadoConnection = None

try:
    from pika.adapters.twisted_connection import TwistedConnection
    from pika.adapters.twisted_connection import TwistedProtocolConnection
except ImportError:
    TwistedConnection = None
    TwistedProtocolConnection = None

