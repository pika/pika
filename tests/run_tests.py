"""
Simple test runner to separate out the functional tests and the unit tests.
"""
import os
import subprocess

PLATFORMS = ['bsd', 'linux', 'nt']

# Detect what platform we are on
try:
    platform = os.uname()[0].lower()
except AttributeError:
    platform = os.name.lower()

if platform == 'darwin':
    platform = 'bsd'

DIRECTORIES = ['functional'] # Unit tests to be added here


def run_test(file):
    print "Running test: %s" % file
    print
    proc = subprocess.Popen("python %s" % file,
                            shell=True,
                            stdout=subprocess.PIPE)
    output = proc.communicate()[0]
    print output

    # We don't do anything with this yet but I'll probably try and group all
    # these calls up and keep track of the data at the top level
    if output.find('OK'):
        return True
    return False


for directory in DIRECTORIES:
    files = os.listdir(directory)
    for file in files:
        file_path = os.path.join(directory, file)
        if os.path.isfile('%s' % file_path) and file[-3:] == '.py':
            if file[:-3] in PLATFORMS:
                if platform == file[:3]:
                    run_test(file_path)
            else:
                run_test(file_path)
