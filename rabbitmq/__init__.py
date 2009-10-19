from rabbitmq.spec import \
    BasicProperties
from rabbitmq.connection import \
    PlainCredentials, \
    ConnectionParameters, \
    SimpleReconnectionStrategy

import rabbitmq.asyncore_adapter
from rabbitmq.asyncore_adapter import \
    AsyncoreConnection
asyncore_loop = rabbitmq.asyncore_adapter.loop
