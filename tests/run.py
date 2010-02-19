#!/usr/bin/env python
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

import sys
import glob
import time
import os, os.path
import doctest
import unittest
try:
    import coverage
except ImportError:
    print "No 'coverage' module found. Try:"
    print "     sudo apt-get install python-coverage"


TEST_NAMES = [f.rpartition('.')[0] for f in glob.glob("test_*.py")]
TEST_NAMES.sort()

pwd=os.getcwd()
os.chdir(sys.argv[1])
BAD_MODULES=[]
MODULE_NAMES=[sys.argv[2] + '.' +f[0:-3] for f in glob.glob("*.py") if f not in BAD_MODULES]
MODULE_NAMES.sort()
os.chdir(pwd)

VERBOSE=False

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def main_coverage(TESTS):
    modulenames = MODULE_NAMES

    coverage.erase()
    coverage.start()
    coverage.exclude('#pragma[: ]+[nN][oO] [cC][oO][vV][eE][rR]')

    modules = []
    for modulename in modulenames:
        mod = my_import(modulename)
        modules.append(mod)

    if 'unittest' in TESTS:
        print "***** Unittest *****"
        test_args = {'verbosity': 1}
        suite = unittest.TestLoader().loadTestsFromNames(TEST_NAMES)
        unittest.TextTestRunner(**test_args).run(suite)

    if 'doctest' in TESTS:
        t0 = time.time()
        print "\n***** Doctest *****"
        for mod in modules:
            doctest.testmod(mod, verbose=VERBOSE)
        td = time.time() - t0
        print "      Tests took %.3f seconds" % (td, )

    print "\n***** Coverage Python *****"
    coverage.stop()
    coverage.report(modules, ignore_errors=1, show_missing=1)
    coverage.erase()



if __name__ == '__main__':
    main_coverage(['unittest', 'doctest'])


def run_unittests(g):
    test_args = {'verbosity': 1}
    for t in [t for t in g.keys()
                        if (t.startswith('Test') and issubclass(g[t], unittest.TestCase)) ]:
        suite = unittest.TestLoader().loadTestsFromTestCase(g[t])
        unittest.TextTestRunner(**test_args).run(suite)

