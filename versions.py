#!/usr/bin/env python
import os
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)


def test_for_version(filename):
    stdin, stdout = os.popen4('%s --version' % filename, 'r')
    response = stdout.read()
    return '.'.join(response.strip().split(' ')[1].split('.')[:-1])


versions = ['python', 'python2.5', 'python2.6']
valid = {}
for filename in versions:
    version = test_for_version(filename)
    if version not in valid:
        valid[version] = filename

# Prefer the latest version of python
output = []
if '2.6' in valid:
    output.append(valid['2.6'])

for version in valid.keys():
    if valid[version] not in output:
        output.append(valid[version])

print ' '.join(output)
