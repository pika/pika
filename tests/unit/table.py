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
Pika table tests
"""

import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import datetime
import decimal
import nose
import pika.table


def test_encode_table():
    r'''
    >>> encode(None)
    '\x00\x00\x00\x00'
    >>> encode({})
    '\x00\x00\x00\x00'
    >>> encode({'a':1, 'c':True, 'd':'x', 'e':{}})
    '\x00\x00\x00\x1d\x01aI\x00\x00\x00\x01\x01cI\x00\x00\x00\x01\x01eF\x00\x00\x00\x00\x01dS\x00\x00\x00\x01x'
    >>> encode({'a':decimal.Decimal('1.0')})
    '\x00\x00\x00\x08\x01aD\x00\x00\x00\x00\x01'
    >>> encode({'a':decimal.Decimal('5E-3')})
    '\x00\x00\x00\x08\x01aD\x03\x00\x00\x00\x05'
    >>> encode({'a':datetime.datetime(2010,12,31,23,58,59)})
    '\x00\x00\x00\x0b\x01aT\x00\x00\x00\x00M\x1enC'
    >>> encode({'test':decimal.Decimal('-0.01')})
    '\x00\x00\x00\x0b\x04testD\x02\xff\xff\xff\xff'
    >>> encode({'a':-1, 'b':[1,2,3,4,-1],'g':-1})
    '\x00\x00\x00.\x01aI\xff\xff\xff\xff\x01bA\x00\x00\x00\x19I\x00\x00\x00\x01I\x00\x00\x00\x02I\x00\x00\x00\x03I\x00\x00\x00\x04I\xff\xff\xff\xff\x01gI\xff\xff\xff\xff'
    >>> encode({'a': 4611686018427387904L, 'b': -4611686018427387904L})
    '\x00\x00\x00\x16\x01al@\x00\x00\x00\x00\x00\x00\x00\x01bl\xc0\x00\x00\x00\x00\x00\x00\x00'
    '''
    pass



def test_decode_table():
    r'''
    >>> reencode(None)
    {}
    >>> reencode({})
    {}
    >>> reencode({'a': 1})
    {'a': 1}
    >>> reencode({'a':1, 'c':True, 'd':'x', 'e':{}, 'f': -1})
    {'a': 1, 'c': 1, 'e': {}, 'd': 'x', 'f': -1}
    >>> reencode({'a':datetime.datetime(2010,12,31,23,58,59)})
    {'a': datetime.datetime(2010, 12, 31, 23, 58, 59)}
    >>> reencode({'a': 0x7EADBEEFDEADBEEFL, 'b': -0x7EADBEEFDEADBEEFL})
    {'a': 9128161957192253167, 'b': -9128161957192253167}
    >>> reencode({'a': 1, 'b':decimal.Decimal('-1.234'), 'g': -1})
    {'a': 1, 'b': Decimal('-1.234'), 'g': -1}
    >>> reencode({'a':[1,2,3,'a',decimal.Decimal('-0.01'),5]})
    {'a': [1, 2, 3, 'a', Decimal('-0.01'), 5]}
    '''
    pass


def encode(v):

    p = []
    n = pika.table.encode_table(p, v)
    r = ''.join(p)
    assert len(r) == n
    return r


def reencode(i):
    r = encode(i)
    (v, n) = pika.table.decode_table(r, 0)
    assert len(r) == n
    return v

if __name__ == "__main__":
    nose.runmodule(argv=['-s', '--with-doctest', '--doctest-tests'])

