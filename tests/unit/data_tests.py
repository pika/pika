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
        b'\x00\x00\x00\xff'
        b'\x05arrayA\x00\x00\x00\x0fI\x00\x00\x00\x01I'
        b'\x00\x00\x00\x02I\x00\x00\x00\x03'
        b'\x07boolvalt\x01'
        b'\x07decimalD\x02\x00\x00\x01:'
        b'\x0bdecimal_tooD\x00\x00\x00\x00d'
        b'\x07dictvalF\x00\x00\x00\x0c\x03fooS\x00\x00\x00\x03bar'
        b'\x06intvalI\x00\x00\x00\x01'
        b'\x06bigintl\x00\x00\x00\x00\x9a\x7e\xc8\x00'
        b'\x07longvall\x00\x00\x00\x00\x36\x65\x26\x55'
        b'\x07neglongl\xff\xff\xff\xff\xff\xff\xff\xff'
        b'\x04nullV'
        b'\x06strvalS\x00\x00\x00\x04Test'
        b'\x0ctimestampvalT\x00\x00\x00\x00Ec)\x92'
        b'\x07unicodeS\x00\x00\x00\x08utf8=\xe2\x9c\x93')

    FIELD_TBL_ENCODED += b'\x05bytesx\x00\x00\x00\x06foobar'
    FIELD_TBL_ENCODED += b'\x08floatvald' + struct.pack('>d', 2.5)

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
        ('longval', data.LongLong(912598613)),
        ('neglong', data.LongLong(-1)),
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
        ('floatval', 2.5),
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
        self.assertEqual(byte_count, 259)

    def test_decode_table(self):
        value, _byte_count = data.decode_table(self.FIELD_TBL_ENCODED, 0)
        self.assertDictEqual(value, self.FIELD_TBL_VALUE)

    def test_decode_table_bytes(self):
        _value, byte_count = data.decode_table(self.FIELD_TBL_ENCODED, 0)
        self.assertEqual(byte_count, 259)

    # x-delay = -30000 as a signed 64-bit 'l', as RabbitMQ sends it (#1531).
    X_DELAY_TABLE = (b'\x00\x00\x00\x11'
                     b'\x07x-delayl\xff\xff\xff\xff\xff\xff\x8a\xd0')

    def test_decode_signed_long_negative(self):
        result, _ = data.decode_table(self.X_DELAY_TABLE, 0)
        self.assertEqual(result, {'x-delay': -30000})
        self.assertIs(type(result['x-delay']), data.LongLong)

    def test_negative_long_long_encodes_as_l(self):
        pieces = []
        data.encode_table(pieces, {'x-delay': data.LongLong(-30000)})
        self.assertEqual(b''.join(pieces), self.X_DELAY_TABLE)

    def test_long_long_encodes_as_l_whatever_its_magnitude(self):
        for value in (0, 5, -1, 2**31, -2**63, 2**63 - 1):
            with self.subTest(value=value):
                pieces = []
                self.assertEqual(
                    data.encode_value(pieces, data.LongLong(value)), 9)
                self.assertEqual(b''.join(pieces),
                                 b'l' + struct.pack('>q', value))

    def test_plain_int_that_fits_32_bits_encodes_as_I(self):
        for value in (5, -1, -2**31, 2**31 - 1):
            with self.subTest(value=value):
                pieces = []
                data.encode_value(pieces, value)
                self.assertEqual(b''.join(pieces),
                                 b'I' + struct.pack('>i', value))

    def test_I_decodes_as_a_signed_plain_int(self):
        value, _ = data.decode_value(b'I\xff\xff\xff\xff', 0)
        self.assertEqual(value, -1)
        self.assertIs(type(value), int)

    def test_received_l_and_L_values_are_resent_as_l(self):
        payload = struct.pack('>q', 5)
        for tag in (b'l', b'L'):
            with self.subTest(tag=tag):
                value, _ = data.decode_value(tag + payload, 0)
                self.assertIs(type(value), data.LongLong)
                pieces = []
                data.encode_value(pieces, value)
                self.assertEqual(b''.join(pieces), b'l' + payload)

    def test_long_long_outside_signed_64_bits_raises_unencodable_long_error(
            self):
        for value in (2**63, -2**63 - 1):
            with self.subTest(value=value):
                self.assertRaises(exceptions.UnencodableLongError,
                                  data.encode_value, [], data.LongLong(value))

    def test_long_long_arithmetic_returns_int(self):
        self.assertIs(type(data.LongLong(5) + 1), int)

    def test_bytes_table_key_is_encoded_as_is(self):
        pieces = []
        data.encode_table(pieces, {b'k\xff': 1})
        self.assertEqual(b''.join(pieces),
                         b'\x00\x00\x00\x08\x02k\xffI\x00\x00\x00\x01')

    def test_non_string_table_key_raises_type_error_naming_it(self):
        with self.assertRaisesRegex(TypeError, 'short string.*not int'):
            data.encode_table([], {1: 'a'})

    def test_encode_raises(self):
        self.assertRaises(exceptions.UnsupportedAMQPFieldException,
                          data.encode_table, [], {'foo': {1, 2, 3}})

    def test_encode_long_long_boundaries_roundtrip(self):
        """The widest integers that still fit a long-long round-trip."""
        for value in (2**63 - 1, -2**63):
            pieces = []
            data.encode_value(pieces, value)
            decoded, offset = data.decode_value(b''.join(pieces), 0)
            self.assertEqual(decoded, value)
            self.assertEqual(offset, 9)

    def test_encode_long_out_of_range(self):
        # An int wider than signed 64-bit has no AMQP field-table integer
        # type, so it is rejected instead of raising a raw struct.error.
        self.assertRaises(exceptions.UnencodableLongError, data.encode_value,
                          [], 2**63)
        self.assertRaises(exceptions.UnencodableLongError, data.encode_value,
                          [], -2**63 - 1)

    def test_encode_timestamp_epoch_roundtrip(self):
        value = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        pieces = []
        data.encode_value(pieces, value)
        decoded, offset = data.decode_value(b''.join(pieces), 0)
        self.assertEqual(decoded, value)
        self.assertEqual(offset, 9)

    def test_encode_timestamp_before_epoch(self):
        # 'T' is an unsigned 64-bit POSIX time, so a pre-1970 datetime has no
        # representation and is rejected instead of raising a struct.error.
        value = datetime.datetime(1969, 12, 31, tzinfo=datetime.timezone.utc)
        self.assertRaises(exceptions.UnencodableTimestampError,
                          data.encode_value, [], value)

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

    def test_encode_decimal_scale_too_large(self):
        value = decimal.Decimal('0.' + '0' * 300 + '1')
        self.assertRaises(exceptions.UnencodableDecimalError, data.encode_value,
                          [], value)

    def test_encode_decimal_mantissa_out_of_range(self):
        value = decimal.Decimal('99999999999999999999.99')
        self.assertRaises(exceptions.UnencodableDecimalError, data.encode_value,
                          [], value)

    def test_encode_decimal_nan(self):
        self.assertRaises(exceptions.UnencodableDecimalError, data.encode_value,
                          [], decimal.Decimal('NaN'))

    def test_encode_decimal_infinity(self):
        self.assertRaises(exceptions.UnencodableDecimalError, data.encode_value,
                          [], decimal.Decimal('Infinity'))

    def test_encode_decimal_excess_precision_is_not_silently_rounded(self):
        # A finite value with more significant digits than the thread-local
        # decimal context precision (28 by default) must be rejected, not
        # silently rounded down to a small mantissa and encoded as a different
        # number. as_tuple() keeps every digit regardless of context.
        value = decimal.Decimal('1.0000000000000000000000000000005')
        self.assertRaises(exceptions.UnencodableDecimalError, data.encode_value,
                          [], value)

    def test_encode_decimal_context_precision_does_not_affect_result(self):
        # Encoding must not depend on the ambient decimal context, so a low
        # context precision cannot round an in-range value before it is packed.
        value = decimal.Decimal('12345.6789')
        with decimal.localcontext() as ctx:
            ctx.prec = 3
            pieces = []
            data.encode_table(pieces, {'k': value})
        decoded, _ = data.decode_table(b''.join(pieces), 0)
        self.assertEqual(decoded, {'k': value})

    def test_decode_decimal_ignores_context_precision(self):
        for raw, scale in ((123456789, 4), (-123456789, 4), (2147483647, 0),
                           (-2147483648, 255), (110, 2), (0, 255)):
            with self.subTest(raw=raw, scale=scale):
                encoded = struct.pack('>cBi', b'D', scale, raw)
                expected = decimal.Decimal(f'{raw}e-{scale}')
                with decimal.localcontext() as ctx:
                    ctx.prec = 3
                    decoded, offset = data.decode_value(encoded, 0)
                self.assertEqual(decoded.as_tuple(), expected.as_tuple())
                self.assertEqual(offset, len(encoded))

    def test_decode_decimal_ignores_context_traps(self):
        value = decimal.Decimal('12345.6789')
        pieces = []
        data.encode_table(pieces, {'nested': {'decimal': value}})
        encoded = b''.join(pieces)
        with decimal.localcontext() as ctx:
            ctx.prec = 3
            ctx.Emin = -2
            ctx.Emax = 2
            for signal in ctx.traps:
                ctx.traps[signal] = True
            ctx.clear_flags()
            decoded, offset = data.decode_table(encoded, 0)
            self.assertFalse(any(ctx.flags.values()))
        self.assertEqual(decoded, {'nested': {'decimal': value}})
        self.assertEqual(offset, len(encoded))

    def test_encode_decimal_preserves_trailing_zeros(self):
        # Dropping normalize() keeps the scale exactly as given, so a value
        # with trailing fractional zeros round-trips to that same scale.
        value = decimal.Decimal('1.10')
        pieces = []
        data.encode_table(pieces, {'k': value})
        decoded, _ = data.decode_table(b''.join(pieces), 0)
        self.assertEqual(decoded, {'k': value})

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
        self.assertIs(type(result['k']), data.LongLong)

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

    def test_encode_value_double(self):
        # A Python float is a C double, so it encodes as b'd'
        pieces = []
        length = data.encode_value(pieces, 3.14)
        self.assertEqual(b''.join(pieces), b'd' + struct.pack('>d', 3.14))
        self.assertEqual(length, 9)

    def test_encode_decode_float_roundtrip(self):
        for value in (0.0, -0.0, 3.14, -1.5, 1e300, -1e300, 5e-324):
            table = {'k': value}
            pieces = []
            data.encode_table(pieces, table)
            decoded, _ = data.decode_table(b''.join(pieces), 0)
            self.assertEqual(decoded, table)

    def test_encode_non_finite_float_raises(self):
        for value in (float('inf'), float('-inf'), float('nan')):
            with self.subTest(value=value):
                self.assertRaises(exceptions.UnencodableFloatError,
                                  data.encode_value, [], value)

    def test_encode_table_with_non_finite_float_raises(self):
        table = {'nested': [{'k': float('nan')}]}
        self.assertRaises(exceptions.UnencodableFloatError, data.encode_table,
                          [], table)

    def test_reencode_wire_float(self):
        # A 32-bit b'f' field off the wire must survive a decode/encode cycle,
        # which is what forwarding a message with its headers does
        encoded = b'\x00\x00\x00\x07\x01kf' + struct.pack('>f', 1.5)
        decoded, _ = data.decode_table(encoded, 0)
        pieces = []
        data.encode_table(pieces, decoded)
        redecoded, _ = data.decode_table(b''.join(pieces), 0)
        self.assertEqual(redecoded, decoded)

    def test_decode_value_long_string_invalid_utf8(self):
        # b'S' with non-UTF-8 content stays as bytes
        raw = b'\xff\xfe'
        encoded = b'\x00\x00\x00\x09\x01kS' + struct.pack('>I', len(raw)) + raw
        result, _ = data.decode_table(encoded, 0)
        self.assertIsInstance(result['k'], bytes)
        self.assertEqual(result['k'], raw)

    def test_decode_value_timestamp_max_representable(self):
        encoded = b'\x00\x00\x00\x0b\x01kT' + struct.pack('>Q', 253402300799)
        result, _ = data.decode_table(encoded, 0)
        self.assertEqual(
            result['k'],
            datetime.datetime(9999,
                              12,
                              31,
                              23,
                              59,
                              59,
                              tzinfo=datetime.timezone.utc))

    def test_decode_value_timestamp_out_of_datetime_range(self):
        # `timestamp` is a u64 (AMQP 0-9-1 errata), so the wire carries values
        # `datetime` cannot hold; they must not raise out of the decoder.
        values = (253402300800, 1772000000000, 2**63 - 1, 2**64 - 1)
        decoded = {}
        for seconds in values:
            encoded = (b'\x00\x00\x00\x0b\x01kT' + struct.pack('>Q', seconds))
            result, offset = data.decode_table(encoded, 0)
            self.assertEqual(offset, 15)
            decoded[seconds] = result['k']
        self.assertEqual(decoded, {seconds: seconds for seconds in values})
