# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
__version__ = '0.9.6-pre0'

from pika.connection import ConnectionParameters
from pika.credentials import PlainCredentials
from pika.reconnection_strategies import ReconnectionStrategy
from pika.spec import BasicProperties

from pika.adapters.base_connection import BaseConnection
from pika.adapters.asyncore_connection import AsyncoreConnection
from pika.adapters.blocking_connection import BlockingConnection
from pika.adapters.select_connection import SelectConnection
from pika.adapters.select_connection import IOLoop


# Python 2.4 support: add struct.unpack_from if it's missing.
try:
    import struct
    getattr(struct, "unpack_from")
except AttributeError:
    def _unpack_from(fmt, buf, offset=0):
        slice = buffer(buf, offset, struct.calcsize(fmt))
        return struct.unpack(fmt, slice)
    struct.unpack_from = _unpack_from
