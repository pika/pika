# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

# Prepend our path for the local pika
import os.path
from sys import path as sys_path
sys_path.insert(0, '..')
sys_path.insert(0, os.path.join('..', '..'))

# Get the version of Python we're running
from platform import python_version_tuple
major, minor, revision = python_version_tuple()
PYTHON_VERSION = float("%s.%s" % (major, minor))

# Set the host and port from the environment variables or defaults
from os import environ, uname
HOST = environ.get('RABBITMQ_HOST', 'localhost')
PORT = environ.get('RABBITMQ_PORT', 5672)
PLATFORM = uname()[0].lower()

# Import Warnings
import warnings

# Validate that we can connect to RabbitMQ
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
try:
    sock.connect((HOST, PORT))
    sock.close()
except socket.error:
    warnings.warn("RabbitMQ is not running at %s:%i" % (HOST, PORT))
    from nose import SkipTest
    raise SkipTest("RabbitMQ is not running at %s:%i" % (HOST, PORT))

# Ignore the pika generated warnings
warnings.simplefilter('ignore', UserWarning)

# Set our connection parameters to be used by our tests
from pika.connection import ConnectionParameters
PARAMETERS = ConnectionParameters(HOST, PORT)

if __name__ == '__main__':
    print "Platform: %s" % PLATFORM
    print "Python Version: %s" % PYTHON_VERSION
    print "RabbitMQ Broker: %s:%i" % (HOST, PORT)
