# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""Pika provides multiple adapters to connect to RabbitMQ:

- adapters.select_connection.SelectConnection: A native event based connection
  adapter that implements select, kqueue, poll and epoll.
- adapters.asyncore_connection.AsyncoreConnection: Legacy adapter kept for
  convenience of previous Pika users. It is recommended to use the
  SelectConnection instead of AsyncoreConnection.
- adapters.tornado_connection.TornadoConnection: Connection adapter for use
  with the Tornado web framework.
- adapters.blocking_connection.BlockingConnection: Enables blocking,
  synchronous operation on top of library for simple uses.
- adapters.twisted_connection.TwistedConnection: Connection adapter for use
  with the Twisted framework

"""
from base_connection import BaseConnection
from asyncore_connection import AsyncoreConnection
from blocking_connection import BlockingConnection
from select_connection import SelectConnection
from select_connection import IOLoop

# Dynamically handle 3rd party library dependencies for optional imports
try:
    from tornado_connection import TornadoConnection
except ImportError:
    TornadoConnection = None

try:
    from twisted_connection import TwistedConnection
    from twisted_connection import TwistedProtocolConnection
except ImportError:
    TwistedConnection = None
    TwistedProtocolConnection = None
