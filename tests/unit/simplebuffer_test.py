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
import unittest
from mock import Mock
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import pika.simplebuffer

class TestBuffer(unittest.TestCase):
    def setUp(self):
        self.buffer = pika.simplebuffer.SimpleBuffer()

    def test_init_with_data(self):
        buffer = pika.simplebuffer.SimpleBuffer('abc')
        self.assertEqual(buffer.buf.tell(), 3)

    def test_init_nodata(self):
        self.assertEqual(self.buffer.buf.tell(), 0)

    def test_write_onestring(self):
        self.buffer.write('abc')
        self.assertEqual(len(self.buffer), 3)

    def test_write_multistring(self):
        self.buffer.write("abc", "def", "ghi", "jkl", "mno")
        self.assertEqual(len(self.buffer), 15)

    def test_write_multi_empty(self):
        self.buffer.write('abc', '', 'def', '')
        self.assertEqual(len(self.buffer), 6)

    def test_read(self):
        self.buffer.write('abc')
        res = self.buffer.read(1)

        self.assertEqual(res, 'a')
        self.assertEqual(len(self.buffer), 3)

    def test_read_zerosize(self):
        self.buffer.write('abc')
        res = self.buffer.read(0)

        self.assertEqual(res, '')
        self.assertEqual(len(self.buffer), 3)

    def test_read_nonesize(self):
        self.buffer.write('abcdef')
        res = self.buffer.read()

        self.assertEqual(res, 'abcdef')
        self.assertEqual(len(self.buffer), 6)

    def test_read_and_consume(self):
        self.buffer.write('abcdef')
        result = self.buffer.read_and_consume(3)

        self.assertEqual(result, 'abc')
        self.assertEqual(len(self.buffer), 3)

    def test_read_and_consume_nosize(self):
        """
        read_and_consume called w/ no size arg should raise a TypeError

        """
        self.assertRaises(TypeError, self.buffer.read_and_consume)

    def test_read_and_consume_toobig(self):
        """
        read_and_consume asserts that incoming 'size' arg is smaller than
        the buffer size.

        """
        self.buffer.write('abc')
        self.assertRaises(AssertionError, self.buffer.read_and_consume, 10)

    def test_send_to_socket(self):
        self.buffer.write('abc')
        sd = Mock(spec=['send'])
        sd.send.return_value = 3
        bytes = self.buffer.send_to_socket(sd)

        self.assertEqual(bytes, 3)
        self.assertEqual(self.buffer.buf.tell(), 3)
        self.assertEqual(self.buffer.offset, 3)
        self.assertEqual(len(self.buffer), 0)

    def test_flush(self):
        self.buffer.write('abc')
        self.assertEqual(len(self.buffer), 3)
        self.buffer.flush()

        self.assertEqual(len(self.buffer), 0)

    def test_nonzero(self):
        self.buffer.write('abc')
        self.assertTrue(self.buffer)
        self.buffer.flush()
        self.assertFalse(self.buffer)

    def test_str(self):
        self.buffer.write('abcdef')
        self.buffer.read_and_consume(3)
        self.assertEqual(str(self.buffer), "<SimpleBuffer of 3 bytes, 6 total size, 'def'>")





if __name__ == "__main__":
    unittest.main()
