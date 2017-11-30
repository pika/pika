__version__ = '0.11.2'

import logging
try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):

        def emit(self, record):
            pass

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from pika.connection import ConnectionParameters
from pika.connection import URLParameters
from pika.credentials import PlainCredentials
from pika.spec import BasicProperties

from pika.adapters import BaseConnection
from pika.adapters import BlockingConnection
from pika.adapters import SelectConnection
from pika.adapters import TornadoConnection
from pika.adapters import TwistedConnection
from pika.adapters import LibevConnection
