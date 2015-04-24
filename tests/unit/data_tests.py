# -*- encoding: utf-8 -*-
"""
pika.data tests

"""
import datetime
import decimal
import platform
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import data
from pika import exceptions
from pika.compat import long


class DataTests(unittest.TestCase):

    FIELD_TBL_ENCODED = (b'\x00\x00\x00\xbb\x07longvall\x00\x00\x00\x006e&U'
                         b'\x06intvalI\x00\x00\x00\x01\x07dictvalF\x00\x00'
                         b'\x00\x0c\x03fooS\x00\x00\x00\x03bar\x07unicodeS'
                         b'\x00\x00\x00\x08utf8=\xe2\x9c\x93\x05arrayA\x00'
                         b'\x00\x00\x0fI\x00\x00\x00\x01I\x00\x00\x00\x02I'
                         b'\x00\x00\x00\x03\x04nullV\x06strvalS\x00\x00\x00'
                         b'\x04Test\x0ctimestampvalT\x00\x00\x00\x00Ec)\x92'
                         b'\x07decimalD\x02\x00\x00\x01:\x07boolvalt\x01'
                         b'\x0bdecimal_tooD\x00\x00\x00\x00d')

    FIELD_TBL_VALUE = {b'array': [1, 2, 3],
                       b'boolval': True,
                       b'decimal': decimal.Decimal('3.14'),
                       b'decimal_too': decimal.Decimal('100'),
                       b'dictval': {b'foo': 'bar'},
                       b'intval': 1,
                       b'longval': long(912598613),
                       b'null': None,
                       b'strval': 'Test',
                       b'timestampval': datetime.datetime(2006, 11, 21, 16, 30,
                                                         10),
                       b'unicode': u'utf8=✓'}

    @unittest.skipIf(platform.python_implementation() == 'PyPy',
                     'pypy sort order issue')
    def test_encode_table(self):
        result = []
        data.encode_table(result, self.FIELD_TBL_VALUE)
        self.assertEqual(b''.join(result), self.FIELD_TBL_ENCODED)

    def test_encode_table_bytes(self):
        result = []
        byte_count = data.encode_table(result, self.FIELD_TBL_VALUE)
        self.assertEqual(byte_count, 191)

    def test_decode_table(self):
        value, byte_count = data.decode_table(self.FIELD_TBL_ENCODED, 0)
        self.assertDictEqual(value, self.FIELD_TBL_VALUE)

    def test_decode_table_bytes(self):
        value, byte_count = data.decode_table(self.FIELD_TBL_ENCODED, 0)
        self.assertEqual(byte_count, 191)

    def test_encode_raises(self):
        self.assertRaises(exceptions.UnsupportedAMQPFieldException,
                          data.encode_table,
                          [], {'foo': set([1, 2, 3])})

    def test_decode_raises(self):
        self.assertRaises(exceptions.InvalidFieldTypeException,
                          data.decode_table,
                          b'\x00\x00\x00\t\x03fooZ\x00\x00\x04\xd2', 0)
