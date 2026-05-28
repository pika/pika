__version__ = '1.4.0'

import logging

# Add NullHandler before importing Pika modules to prevent logging warnings
logging.getLogger(__name__).addHandler(logging.NullHandler())

# ruff: noqa: E402

from pika import adapters
from pika.adapters import BaseConnection, BlockingConnection, SelectConnection, ThreadSafeConnection
from pika.adapters.utils.connection_workflow import AMQPConnectionWorkflow
from pika.connection import ConnectionParameters, SSLOptions, URLParameters
from pika.credentials import PlainCredentials
from pika.delivery_mode import DeliveryMode
from pika.spec import BasicProperties

__all__ = [
    'AMQPConnectionWorkflow',
    'BaseConnection',
    'BasicProperties',
    'BlockingConnection',
    'ConnectionParameters',
    'DeliveryMode',
    'PlainCredentials',
    'SSLOptions',
    'ThreadSafeConnection',
    'SelectConnection',
    'URLParameters',
    'adapters',
]
