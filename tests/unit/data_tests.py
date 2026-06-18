"""pika.data tests."""

import datetime
import decimal
import struct
import unittest
from collections import OrderedDict
from typing import ClassVar

from pika import data, exceptions


class DataTests(unittest.TestCase):

    FIELD_TBL_ENCODED = (
        b'\x00\x00\x00\xe5'
        b'\x05arrayA\x00\x00\x00\x0fI\x00\x00\x00\x01I'
        b'\x00\x00\x00\x02I\x00\x00\x00\x03'
        b'\x07boolvalt\x01'
        b'\x07decimalD\x02\x00\x00\x01:'
        b'\x0bdecimal_tooD\x00\x00\x00\x00d'
        b'\x07dictvalF\x00\x00\x00\x0c\x03fooS\x00\x00\x00\x03bar'
        b'\x06intvalI\x00\x00\x00\x01'
        b'\x06bigintl\x00\x00\x00\x00\x9a\x7e\xc8\x00'
        b'\x07longvalI\x36\x65\x26\x55'
        b'\x07neglongI\xff\xff\xff\xff'
        b'\x04nullV'
        b'\x06strvalS\x00\x00\x00\x04Test'
        b'\x0ctimestampvalT\x00\x00\x00\x00Ec)\x92'
        b'\x07unicodeS\x00\x00\x00\x08utf8=\xe2\x9c\x93')

    FIELD_TBL_ENCODED += b'\x05bytesx\x00\x00\x00\x06foobar'

    FIELD_TBL_VALUE: ClassVar[OrderedDict] = OrderedDict([
        ('array', [1, 2, 3]),
        ('boolval', True),
        ('decimal', decimal.Decimal('3.14')),
        ('decimal_too', decimal.Decimal(100)),
        ('dictval', {
            'foo': 'bar'
        }),
        ('intval', 1),
        ('bigint', 2592000000),
        ('longval', 912598613),
        ('neglong', -1),
        ('null', None),
        ('strval', 'Test'),
        ('timestampval',
         datetime.datetime(2006,
                           11,
                           21,
                           16,
                           30,
                           10,
                           tzinfo=datetime.timezone.utc)),
        ('unicode', 'utf8=✓'),
        ('bytes', b'foobar'),
    ])

    def test_decode_bytes(self):
        input = (b'\x00\x00\x00\x01'
                 b'\x05bytesx\x00\x00\x00\x06foobar')
        result = data.decode_table(input, 0)
        self.assertEqual(result, ({'bytes': b'foobar'}, 21))

    # b'\x08shortints\x04\xd2'
    # ('shortint', 1234),
    def test_decode_shortint(self):
        input = (b'\x00\x00\x00\x01'
                 b'\x08shortints\x04\xd2')
        result = data.decode_table(input, 0)
        self.assertEqual(result, ({'shortint': 1234}, 16))

    def test_encode_table(self):
        result = []
        data.encode_table(result, self.FIELD_TBL_VALUE)
        self.assertEqual(b''.join(result), self.FIELD_TBL_ENCODED)

    def test_encode_table_bytes(self):
        result = []
        byte_count = data.encode_table(result, self.FIELD_TBL_VALUE)
        self.assertEqual(byte_count, 233)

    def test_decode_table(self):
        value, _byte_count = data.decode_table(self.FIELD_TBL_ENCODED, 0)
        self.assertDictEqual(value, self.FIELD_TBL_VALUE)

    def test_decode_table_bytes(self):
        _value, byte_count = data.decode_table(self.FIELD_TBL_ENCODED, 0)
        self.assertEqual(byte_count, 233)

    def test_decode_signed_long_negative(self):
        """
        Verify that type tag 'l' decodes as signed 64-bit (fixes #1531).

        RabbitMQ encodes negative longs (e.g. x-delay after delivery) with type tag 'l' and signed
        64-bit representation.
        """
        # Table with x-delay = -30000 encoded as signed 64-bit 'l'
        input = (b'\x00\x00\x00\x10'
                 b'\x07x-delayl\xff\xff\xff\xff\xff\xff\x8a\xd0')
        result, _ = data.decode_table(input, 0)
        self.assertEqual(result, {'x-delay': -30000})

    def test_encode_decode_negative_long_roundtrip(self):
        """Verify negative long values round-trip correctly."""
        table = {'x-delay': -30000}
        pieces = []
        data.encode_table(pieces, table)
        encoded = b''.join(pieces)
        decoded, _ = data.decode_table(encoded, 0)
        self.assertEqual(decoded, table)

    def test_encode_raises(self):
        self.assertRaises(exceptions.UnsupportedAMQPFieldException,
                          data.encode_table, [], {'foo': {1, 2, 3}})

    def test_decode_raises(self):
        self.assertRaises(exceptions.InvalidFieldTypeException,
                          data.decode_table,
                          b'\x00\x00\x00\t\x03fooZ\x00\x00\x04\xd2', 0)

    def test_long_repr(self):
        value = 912598613
        self.assertEqual(repr(value), '912598613')

    def test_encode_short_string_too_long(self):
        self.assertRaises(exceptions.ShortStringTooLong,
                          data.encode_short_string, [], 'a' * 256)

    def test_decode_short_string_invalid_utf8(self):
        encoded = b'\x02\xff\xfe'
        value, offset = data.decode_short_string(encoded, 0)
        self.assertIsInstance(value, bytes)
        self.assertEqual(value, b'\xff\xfe')
        self.assertEqual(offset, 3)

    def test_decode_value_short_short_int(self):
        # b'b' = signed byte
        encoded = b'\x00\x00\x00\x04\x01kb\xff'
        result, _ = data.decode_table(encoded, 0)
        self.assertEqual(result, {'k': -1})

    def test_decode_value_short_short_uint(self):
        # b'B' = unsigned byte
        encoded = b'\x00\x00\x00\x04\x01kB\xff'
        result, _ = data.decode_table(encoded, 0)
        self.assertEqual(result, {'k': 255})

    def test_decode_value_short_int(self):
        # b'U' = signed short
        encoded = b'\x00\x00\x00\x05\x01kU' + struct.pack('>h', -1000)
        result, _ = data.decode_table(encoded, 0)
        self.assertEqual(result, {'k': -1000})

    def test_decode_value_short_uint(self):
        # b'u' = unsigned short
        encoded = b'\x00\x00\x00\x05\x01ku' + struct.pack('>H', 1000)
        result, _ = data.decode_table(encoded, 0)
        self.assertEqual(result, {'k': 1000})

    def test_decode_value_long_uint(self):
        # b'i' = unsigned long
        encoded = b'\x00\x00\x00\x07\x01ki' + struct.pack('>I', 4294967295)
        result, _ = data.decode_table(encoded, 0)
        self.assertEqual(result, {'k': 4294967295})

    def test_decode_value_long_long_int_uppercase(self):
        # b'L' = signed 64-bit int
        encoded = b'\x00\x00\x00\x0b\x01kL' + struct.pack('>q', -30000)
        result, _ = data.decode_table(encoded, 0)
        self.assertEqual(result, {'k': -30000})

    def test_decode_value_float(self):
        # b'f' = 32-bit float
        encoded = b'\x00\x00\x00\x07\x01kf' + struct.pack('>f', 1.5)
        result, _ = data.decode_table(encoded, 0)
        self.assertAlmostEqual(result['k'], 1.5, places=5)

    def test_decode_value_double(self):
        # b'd' = 64-bit double
        encoded = b'\x00\x00\x00\x0b\x01kd' + struct.pack('>d', 3.14)
        result, _ = data.decode_table(encoded, 0)
        self.assertAlmostEqual(result['k'], 3.14, places=10)

    def test_decode_value_long_string_invalid_utf8(self):
        # b'S' with non-UTF-8 content stays as bytes
        raw = b'\xff\xfe'
        encoded = b'\x00\x00\x00\x09\x01kS' + struct.pack('>I', len(raw)) + raw
        result, _ = data.decode_table(encoded, 0)
        self.assertIsInstance(result['k'], bytes)
        self.assertEqual(result['k'], raw)
