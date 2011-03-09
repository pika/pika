#!/usr/bin/env python
# Copied from the https://github.com/gmr/runnynose project and extended for our
# purposes here.

"""
Simple test runner to separate out the functional tests and the unit tests.
"""
import optparse
import os
import subprocess
import sys
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)


def test_for_version(filename):
    """
    Attempts to run specific files to see if they exist
    """
    stdin, stdout = os.popen4('%s -V' % filename, 'r')
    response = stdout.read()
    if response.find('command not found') > 0:
        return False
    return filename.split('-')[1]


print "Determining options for test environment"

# Get test directories
test_directories = []
for entry in os.listdir('.'):
    if os.path.isdir(entry):
        test_directories.append(entry)

# Get nosetests versions
versions = ['nosetests-2.4', 'nosetests-2.5', 'nosetests-2.6', 'nosetests-2.7']
valid = set()
for filename in versions:
    found = test_for_version(filename)
    if found:
        valid.add(found)

if not valid:
    print "You must install nose and mock to run tests."
    sys.exit(1)

# Set up the valid argument options
parser = optparse.OptionParser()
parser.add_option("-t",
                  "--test",
                  type="choice",
                  dest="test",
                  choices=test_directories,
                  help="Specify the tests to run.\n\
                        Available: %s\n\
                        Defaults to all." % ', '.join(test_directories),
                  default=None)

parser.add_option("-v",
                  "--versions",
                  type="choice",
                  dest="version",
                  choices=list(valid),
                  help="Which Python/nosetests version to test with.\n\
                        Available: %s\n\
                        Defaults to all." % ', '.join(valid),
                  default=None)

parser.add_option("--host",
                  dest="host",
                  default=None,
                  help="Specify the RabbitMQ server to run the\
                        functional tests on.")

parser.add_option("--port",
                  dest="port",
                  default=None,
                  help="Specify the RabbitMQ broker port to connect to for\
                        running the functional tests on.")

parser.add_option("--xunit",
                  dest="xunit",
                  default=None,
                  help="Tell nosetests to use xunit with the specified file.")

parser.add_option("--coverage",
                  dest="coverage",
                  default=None,
                  help="Have nosetests to use coverage to generate Cobertura\
                        XML output to the specified file.")


# Parse the arguments
options, args = parser.parse_args()

if options.test:
    test_directories = [options.test]

if options.version:
    valid = [options.version]

if options.host:
    os.putenv("RABBITMQ_HOST", options.host)

if options.port:
    os.putenv("RABBITMQ_PORT", options.port)

if options.xunit:
    parts = options.xunit.split('/')
    if len(parts) > 1:
        parts[-1] = '{VERSION}.' + parts[-1]
    options.xunit = '/'.join(parts)

if options.coverage:
    parts = options.coverage.split('/')
    if len(parts) > 1:
        parts[-1] = '{VERSION}.' + parts[-1]
    options.coverage = '/'.join(parts)

for version in valid:
    print "Testing %s for Python %s" % (', '.join(test_directories), version)
    print
    command = []
    # Base command
    command.append("nosetests-%s" % version)

    # Append the xunit option if specified
    if options.xunit:
        command.append("--with-xunit --xunit-file=%s" % \
                       options.xunit.replace("{VERSION}", version))

    # Append the coverage options if specified
    if options.coverage:
        command.append("--with-cover --cover-xml --cover-xml-file=%s" %\
                       options.coverage.replace("{VERSION}", version))

    # Append the tests to run
    command.append(' '.join(test_directories))

    # Run the command
    proc = subprocess.Popen(' '.join(command),
                            shell=True,
                            stdout=subprocess.PIPE)

    output = proc.communicate()[0]
    print output
