# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****#

"""
Simple test runner to separate out the functional tests and the unit tests.
"""
import subprocess
import sys

NOSETESTS = ['unit', 'functional']

if len(sys.argv) > 1:
    if sys.argv[1] in NOSETESTS:
        NOSETESTS = sys.argv[1]

proc = subprocess.Popen("nosetests %s" % ' '.join(NOSETESTS),
                        shell=True,
                        stdout=subprocess.PIPE)

output = proc.communicate()[0]
print output
