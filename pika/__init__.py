__version__ = '0.12.0'

import logging
from logging import NullHandler

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from pika.connection import ConnectionParameters
from pika.connection import URLParameters
from pika.connection import SSLOptions
from pika.credentials import PlainCredentials
from pika.spec import BasicProperties

from pika.adapters import BaseConnection
from pika.adapters import BlockingConnection
from pika.adapters import SelectConnection
from pika.adapters import TornadoConnection
from pika.adapters import TwistedConnection
