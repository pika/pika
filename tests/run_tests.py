# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
#
# ***** END LICENSE BLOCK *****

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

DIRECTORIES = ['functional', 'unit']  # Unit tests to be added here


def platform_test(filename):
    for key in PLATFORMS:
        if filename.find(key) > -1:
            return True
    return False


def this_platform(filename):
    if filename.find(platform) > -1:
        return True
    return False


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
            if platform_test(file):
                if this_platform(file):
                    run_test(file_path)
            else:
                run_test(file_path)
