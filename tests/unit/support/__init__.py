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

# Ignore the pika generated warnings
warnings.simplefilter('ignore', UserWarning)

if __name__ == '__main__':
    print "Platform: %s" % PLATFORM
    print "Python Version: %s" % PYTHON_VERSION
    print "RabbitMQ Broker: %s:%i" % (HOST, PORT)
