from pika.spec import \
    BasicProperties
from pika.connection import \
    PlainCredentials, \
    ConnectionParameters, \
    SimpleReconnectionStrategy

import pika.asyncore_adapter
from pika.asyncore_adapter import \
    AsyncoreConnection
asyncore_loop = pika.asyncore_adapter.loop

from pika.blocking_adapter import \
    BlockingConnection
