# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

# Get the version of Python we're running
from platform import python_version_tuple
major, minor, revision = python_version_tuple()
PYTHON_VERSION = float("%s.%s" % (major, minor))

# Set the host and port from the environment variables or defaults
from os import environ, uname
HOST = environ.get('RABBITMQ_HOST', 'localhost')
PORT = environ.get('RABBITMQ_PORT', 5672)

PLATFORM = uname()[0].lower()

if __name__ == '__main__':
    print "Platform: %s" % PLATFORM
    print "Python Version: %s" % PYTHON_VERSION
    print "RabbitMQ Broker: %s:%i" % (HOST, PORT)
