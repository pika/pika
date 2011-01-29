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

@nose.tools.nottest
def encode(v):
    p=[]
    n = pika.table.encode_table(p, v)
    r = ''.join(p)
    assert len(r) == n
    return r

@nose.tools.nottest
def reencode(i):
    r = encode(i)
    (v, n) = pika.table.decode_table(r, 0)
    assert len(r) == n
    return v

if __name__ == "__main__":
    nose.runmodule(argv=['-s', '--with-doctest', '--doctest-tests'])
