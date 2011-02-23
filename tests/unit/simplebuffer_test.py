# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Pika simplebuffer tests
"""

import os
import sys
import nose
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import nose
import pika.simplebuffer


def test_simplebuffer():
    """
    >>> b = pika.simplebuffer.SimpleBuffer("abcdef")
    >>> b.read_and_consume(3)
    'abc'
    >>> b.write(None, '')
    >>> b.read(0)
    ''
    >>> repr(b)
    "<SimpleBuffer of 3 bytes, 6 total size, 'def'>"
    >>> str(b)
    "<SimpleBuffer of 3 bytes, 6 total size, 'def'>"
    >>> b.flush()
    >>> b.read(1)
    ''
    """

class TestBuffer(object):
    def test_init_with_data(self):
        buffer = pika.simplebuffer.SimpleBuffer('abc')
        nose.tools.eq_(buffer.buf.tell(), 3)

    def test_init_nodata(self):
        buffer = pika.simplebuffer.SimpleBuffer()
        nose.tools.eq_(buffer.buf.tell(), 0)

    def test_write_onestring(self):
        buffer = pika.simplebuffer.SimpleBuffer()
        buffer.write('abc')
        nose.tools.eq_(len(buffer), 3)

    def test_write_multistring(self):
        buffer = pika.simplebuffer.SimpleBuffer()
        buffer.write("abc", "def", "ghi", "jkl", "mno")
        nose.tools.eq_(len(buffer), 15)


if __name__ == "__main__":
    nose.runmodule(argv=['-v', '--with-coverage', '-s', '--with-doctest', '--doctest-tests'])
