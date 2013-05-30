# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
__version__ = '0.9.14p0'

import logging
try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

from pika.connection import ConnectionParameters
from pika.connection import URLParameters
from pika.credentials import PlainCredentials
from pika.spec import BasicProperties

from pika.adapters.base_connection import BaseConnection
from pika.adapters.asyncore_connection import AsyncoreConnection
from pika.adapters.blocking_connection import BlockingConnection
from pika.adapters.select_connection import SelectConnection


# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())
