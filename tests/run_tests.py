#!/usr/bin/env python
# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****#

"""
Simple test runner to separate out the functional tests and the unit tests.
"""
import optparse
import os
import subprocess
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
    valid.add(test_for_version(filename))

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

# Parse the arguments
options, args = parser.parse_args()

if options.test:
    test_directories = [options.test]

if options.version:
    valid = [options.version]

for version in valid:
    print "Testing %s for Python %s" % (', '.join(test_directories), version)
    print
    proc = subprocess.Popen("nosetests-%s %s" % (version,
                                                 ' '.join(test_directories)),
                            shell=True,
                            stdout=subprocess.PIPE)

    output = proc.communicate()[0]
    print output
