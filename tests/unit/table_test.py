# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****#

"""
Pika table tests
"""

import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import datetime
import pika.table
import unittest

from decimal import Decimal


def encode_table():
    r'''
    >>> encode(None)
    '\x00\x00\x00\x00'
    >>> encode({})
    '\x00\x00\x00\x00'
    >>> encode({'a':1, 'c':1, 'd':'x', 'e':{}})
    '\x00\x00\x00\x1d\x01aI\x00\x00\x00\x01\x01cI\x00\x00\x00\x01\x01eF\x00\x00\x00\x00\x01dS\x00\x00\x00\x01x'
    >>> encode({'a':Decimal('1.0')})
    '\x00\x00\x00\x08\x01aD\x00\x00\x00\x00\x01'
    >>> encode({'a':Decimal('5E-3')})
    '\x00\x00\x00\x08\x01aD\x03\x00\x00\x00\x05'
    >>> encode({'a':datetime.datetime(2010,12,31,23,58,59)})
    '\x00\x00\x00\x0b\x01aT\x00\x00\x00\x00M\x1enC'
    >>> encode({'test':Decimal('-0.01')})
    '\x00\x00\x00\x0b\x04testD\x02\xff\xff\xff\xff'
    >>> encode({'a':-1, 'b':[1,2,3,4,-1],'g':-1})
    '\x00\x00\x00.\x01aI\xff\xff\xff\xff\x01bA\x00\x00\x00\x19I\x00\x00\x00\x01I\x00\x00\x00\x02I\x00\x00\x00\x03I\x00\x00\x00\x04I\xff\xff\xff\xff\x01gI\xff\xff\xff\xff'
    >>> encode({'a': 4611686018427387904L, 'b': -4611686018427387904L})
    '\x00\x00\x00\x16\x01al@\x00\x00\x00\x00\x00\x00\x00\x01bl\xc0\x00\x00\x00\x00\x00\x00\x00'
    >>> encode({'a': True, 'b': False})
    '\x00\x00\x00\x08\x01at\x01\x01bt\x00'
    '''

class TestTable(unittest.TestCase):
    def test_reencode_none(self):
        self.assertEqual(self.reencode(None), {})

    def test_reencode_ints(self):
        value = {'a': 1}
        self.assertEqual(self.reencode(value), value)

    def test_reencode_mixed(self):
        value = {'a': 1, 'c': 1, 'e': {}, 'd': 'x', 'f': -1}
        self.assertEqual(self.reencode(value), value)

    def test_reencode_datetime(self):
        value = {'a': datetime.datetime(2010, 12, 31, 23, 58, 59)}
        self.assertEqual(self.reencode(value), value)

    def test_reencode_long(self):
        value = {'a': 9128161957192253167L, 'b': -9128161957192253167L}
        self.assertEqual(self.reencode(value), value)

    def test_reencode_negative_decimals(self):
        value = {'a': 1, 'b': Decimal('-1.234'), 'g': -1}
        self.assertEqual(self.reencode(value), value)

    def test_reencode_mixed_with_decimals(self):
        value = {'a': [1, 2, 3, 'a', Decimal('-0.01'), 5]}
        self.assertEqual(self.reencode(value), value)

    def test_reencode_bool(self):
        value = {'a': True, 'b': False}
        self.assertEqual(self.reencode(value), value)

    def encode(self, v):
        p = []
        n = pika.table.encode_table(p, v)
        r = ''.join(p)
        assert len(r) == n
        return r

    def reencode(self, i):
        r = self.encode(i)
        (v, n) = pika.table.decode_table(r, 0)
        assert len(r) == n
        return v

if __name__ == "__main__":
    unittest.main()
