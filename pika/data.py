"""AMQP Table Encoding/Decoding."""

from __future__ import annotations

import calendar
import decimal
import struct
from datetime import datetime, timedelta, timezone
from typing import Any

from pika import exceptions

# AMQP field-table value type tags, as integer byte values. decode_value
# dispatches on the raw byte rather than a 1-byte slice to avoid a bytes
# allocation per decoded field.
_FIELD_BOOL = ord('t')
_FIELD_SHORT_SHORT_INT = ord('b')
_FIELD_SHORT_SHORT_UINT = ord('B')
_FIELD_SHORT_INT = ord('U')
_FIELD_SHORT_UINT = ord('u')
_FIELD_LONG_INT = ord('I')
_FIELD_LONG_UINT = ord('i')
_FIELD_LONG_LONG_INT = ord('L')
_FIELD_LONG_LONG_INT_ALT = ord('l')
_FIELD_FLOAT = ord('f')
_FIELD_DOUBLE = ord('d')
_FIELD_DECIMAL = ord('D')
_FIELD_SHORT_INT_ALT = ord('s')
_FIELD_LONG_STRING = ord('S')
_FIELD_BYTE_ARRAY = ord('x')
_FIELD_ARRAY = ord('A')
_FIELD_TIMESTAMP = ord('T')
_FIELD_TABLE = ord('F')
_FIELD_VOID = ord('V')

# Pre-compiled struct formats for field-table values. The bare formats are
# used on decode, where the type tag has already been consumed; the `_TAG_`
# ones pack the type tag together with the value on encode.
_PACK_UINT = struct.Struct('>I')
_PACK_INT = struct.Struct('>i')
_PACK_LONG_LONG = struct.Struct('>q')
_PACK_UNSIGNED_LONG_LONG = struct.Struct('>Q')
_PACK_SHORT_SIGNED = struct.Struct('>h')
_PACK_USHORT = struct.Struct('>H')
_PACK_SIGNED_BYTE = struct.Struct('>b')
_PACK_FLOAT = struct.Struct('>f')
_PACK_DOUBLE = struct.Struct('>d')
_PACK_TAG_UINT = struct.Struct('>cI')
_PACK_TAG_BYTE = struct.Struct('>cB')
_PACK_TAG_INT = struct.Struct('>ci')
_PACK_TAG_LONG_LONG = struct.Struct('>cq')
_PACK_TAG_UNSIGNED_LONG_LONG = struct.Struct('>cQ')
_PACK_TAG_BYTE_INT = struct.Struct('>cBi')
_PACK_TAG_DOUBLE = struct.Struct('>cd')

# Reference point for decoding the `timestamp` field type. Adding a
# `timedelta` is a little slower than `datetime.fromtimestamp` but stays in
# Python, so the range that decodes does not vary by platform: on Windows
# `fromtimestamp` goes through a `time_t` that stops at year 3000.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Single-byte `bytes` objects indexed by value. Shared with `pika.spec`,
# which imports this table for its bit-field buffers.
_OCTET_BYTES = tuple(bytes((i,)) for i in range(256))


def encode_short_string(pieces: list[bytes], value: str | bytes) -> int:
    """
    Encode a string value as short string and append it to pieces list returning the size of the
    encoded value.

    :param pieces: Already encoded values
    :param value: String value to encode; bytes are encoded as-is, which is what
        `decode_short_string` yields for a payload that is not valid UTF-8
    """
    encoded_value = value if isinstance(value, bytes) else value.encode('UTF-8')
    length = len(encoded_value)

    # 4.2.5.3
    # Short strings, stored as an 8-bit unsigned integer length followed by zero
    # or more octets of data. Short strings can carry up to 255 octets of UTF-8
    # data, but may not contain binary zero octets.
    # ...
    # 4.2.5.5
    # The server SHOULD validate field names and upon receiving an invalid field
    # name, it SHOULD signal a connection exception with reply code 503 (syntax
    # error).
    # -> validate length (avoid truncated utf-8 / corrupted data), but skip null
    # byte check.
    if length > 255:
        raise exceptions.ShortStringTooLong(encoded_value)

    pieces.append(_OCTET_BYTES[length])
    pieces.append(encoded_value)
    return 1 + length


def decode_short_string(encoded: bytes, offset: int) -> tuple[str | bytes, int]:
    """
    Decode a short string value from ``encoded`` data at ``offset``.

    :param encoded: encoded data
    :param offset: starting offset in the encoded data
    :returns: tuple of (decoded value, new offset)
    """
    length = encoded[offset]
    offset += 1
    raw = encoded[offset:offset + length]
    try:
        value: str | bytes = raw.decode('utf8')
    except UnicodeDecodeError:
        value = raw
    offset += length
    return value, offset


def encode_table(pieces: list[bytes], table: dict[Any, Any] | None) -> int:
    """
    Encode a dict as an AMQP table appending the encoded table to the pieces list passed in.

    :param pieces: Already encoded frame pieces
    :param table: The dict to encode. Keys are encoded as short strings, so a table round-tripped
        through `decode_table` may key on bytes
    """
    table = table or {}
    length_index = len(pieces)
    # placeholder overwritten by pieces[length_index] below
    pieces.append(b'')
    tablesize = 0
    for (key, value) in table.items():
        tablesize += encode_short_string(pieces, key)
        tablesize += encode_value(pieces, value)

    pieces[length_index] = _PACK_UINT.pack(tablesize)
    return tablesize + 4


def encode_value(pieces: list[bytes], value: Any) -> int:
    """
    Encode the value passed in and append it to the pieces list returning the the size of the
    encoded value.

    :param pieces: Already encoded values
    :param value: The value to encode
    """
    if isinstance(value, str):
        value = value.encode('UTF-8')
        pieces.append(_PACK_TAG_UINT.pack(b'S', len(value)))
        pieces.append(value)
        return 5 + len(value)
    if isinstance(value, bytes):
        pieces.append(_PACK_TAG_UINT.pack(b'x', len(value)))
        pieces.append(value)
        return 5 + len(value)
    if isinstance(value, bool):
        pieces.append(_PACK_TAG_BYTE.pack(b't', int(value)))
        return 2
    if isinstance(value, int):
        try:
            packed = _PACK_TAG_INT.pack(b'I', value)
            pieces.append(packed)
            return 5
        except struct.error:
            pieces.append(_PACK_TAG_LONG_LONG.pack(b'l', value))
            return 9
    elif isinstance(value, float):
        # A Python float is a C double, so 'd' is the lossless tag. It is also
        # what decode_value yields for both 'f' and 'd'. Non-finite values
        # (inf, nan) are valid IEEE-754 doubles and encode losslessly, unlike
        # decimal.Decimal, which has no wire representation for them.
        pieces.append(_PACK_TAG_DOUBLE.pack(b'd', value))
        return 9
    elif isinstance(value, decimal.Decimal):
        if not value.is_finite():
            raise exceptions.UnencodableDecimalError(value)
        # Derive the mantissa and scale from as_tuple() using integer
        # arithmetic. normalize() and Decimal multiplication both apply the
        # thread-local context precision (28 digits by default), which would
        # silently round a high-precision value down to a small mantissa that
        # passes the range check below and encodes as a different number.
        sign, digits, exponent = value.as_tuple()
        assert isinstance(exponent, int)  # guaranteed by is_finite() above
        raw = 0
        for digit in digits:
            raw = raw * 10 + digit
        if sign:
            raw = -raw
        if exponent < 0:
            decimals = -exponent
        else:
            # per spec, the "decimals" octet is unsigned (!)
            decimals = 0
            raw *= 10**exponent
        # 4.2.5.3: the scale is an unsigned octet and the value a signed long;
        # a Decimal whose scale or mantissa doesn't fit those bounds has no
        # AMQP decimal representation.
        if decimals > 255 or not -2**31 <= raw < 2**31:
            raise exceptions.UnencodableDecimalError(value)
        pieces.append(_PACK_TAG_BYTE_INT.pack(b'D', decimals, raw))
        return 6
    elif isinstance(value, datetime):
        pieces.append(
            _PACK_TAG_UNSIGNED_LONG_LONG.pack(
                b'T', calendar.timegm(value.utctimetuple())))
        return 9
    elif isinstance(value, dict):
        pieces.append(b'F')
        return 1 + encode_table(pieces, value)
    elif isinstance(value, list):
        list_pieces: list[Any] = []
        for val in value:
            encode_value(list_pieces, val)
        piece = b''.join(list_pieces)
        pieces.append(_PACK_TAG_UINT.pack(b'A', len(piece)))
        pieces.append(piece)
        return 5 + len(piece)
    elif value is None:
        pieces.append(b'V')
        return 1
    else:
        raise exceptions.UnsupportedAMQPFieldException(pieces, value)


def decode_table(encoded: bytes,
                 offset: int) -> tuple[dict[str | bytes, Any], int]:
    """
    Decode the AMQP table passed in from the encoded value returning the decoded result and the
    number of bytes read plus the offset.

    :param encoded: The binary encoded data to decode
    :param offset: The starting byte offset
    """
    result = {}
    tablesize = _PACK_UINT.unpack_from(encoded, offset)[0]
    offset += 4
    limit = offset + tablesize
    while offset < limit:
        key, offset = decode_short_string(encoded, offset)
        value, offset = decode_value(encoded, offset)
        result[key] = value
    return result, offset


def decode_value(encoded: bytes, offset: int) -> tuple[Any, int]:
    """
    Decode the value passed in returning the decoded value and the number of bytes read in addition
    to the starting offset.

    :param encoded: The binary encoded data to decode
    :param offset: The starting byte offset
    :returns: tuple of (decoded value, new offset). A `timestamp` is a u64, so one outside the range
        `datetime` can hold decodes to its raw integer seconds.
    :raises: pika.exceptions.InvalidFieldTypeException
    """
    # Dispatch on the raw type-tag byte (avoids a bytes allocation per field);
    # the most common field types are checked first.
    value: Any
    kind = encoded[offset]
    offset += 1

    # Long String
    if kind == _FIELD_LONG_STRING:
        length = _PACK_UINT.unpack_from(encoded, offset)[0]
        offset += 4
        value = encoded[offset:offset + length]
        try:
            value = value.decode('utf8')
        except UnicodeDecodeError:
            pass
        offset += length

    # Bool
    elif kind == _FIELD_BOOL:
        value = bool(encoded[offset])
        offset += 1

    # Long Int
    elif kind == _FIELD_LONG_INT:
        value = _PACK_INT.unpack_from(encoded, offset)[0]
        offset += 4

    # Long-Long Int (both 'l' and 'L' are signed per RabbitMQ and the
    # AMQP 0-9-1 errata; see rabbitmq/rabbitmq-server#1093)
    elif kind == _FIELD_LONG_LONG_INT_ALT:
        value = _PACK_LONG_LONG.unpack_from(encoded, offset)[0]
        offset += 8

    # Timestamp
    elif kind == _FIELD_TIMESTAMP:
        seconds = _PACK_UNSIGNED_LONG_LONG.unpack_from(encoded, offset)[0]
        try:
            value = _EPOCH + timedelta(seconds=seconds)
        except OverflowError:
            # A u64 past what `datetime` can hold overflows the `timedelta` or
            # its addition to the epoch; both raise `OverflowError` (never
            # `ValueError`, unlike `datetime.fromtimestamp`). Raising would
            # escape the frame decoder, where the transport reports anything
            # unexpected as a lost stream and drops the connection, so fall
            # back to the raw seconds. This loses the `timestamp` type: re-
            # encoding the int yields a long-long ('l'), not a 'T'.
            value = seconds
        offset += 8

    # Field Table
    elif kind == _FIELD_TABLE:
        (value, offset) = decode_table(encoded, offset)

    # Field Array
    elif kind == _FIELD_ARRAY:
        length = _PACK_UINT.unpack_from(encoded, offset)[0]
        offset += 4
        offset_end = offset + length
        value = []
        while offset < offset_end:
            val, offset = decode_value(encoded, offset)
            value.append(val)

    elif kind == _FIELD_BYTE_ARRAY:
        length = _PACK_UINT.unpack_from(encoded, offset)[0]
        offset += 4
        value = encoded[offset:offset + length]
        offset += length

    # Short-Short Int
    elif kind == _FIELD_SHORT_SHORT_INT:
        value = _PACK_SIGNED_BYTE.unpack_from(encoded, offset)[0]
        offset += 1

    # Short-Short Unsigned Int
    elif kind == _FIELD_SHORT_SHORT_UINT:
        value = encoded[offset]
        offset += 1

    # Short Int
    elif kind == _FIELD_SHORT_INT:
        value = _PACK_SHORT_SIGNED.unpack_from(encoded, offset)[0]
        offset += 2

    # Short Unsigned Int
    elif kind == _FIELD_SHORT_UINT:
        value = _PACK_USHORT.unpack_from(encoded, offset)[0]
        offset += 2

    # Long Unsigned Int
    elif kind == _FIELD_LONG_UINT:
        value = _PACK_UINT.unpack_from(encoded, offset)[0]
        offset += 4

    # Long-Long Int
    elif kind == _FIELD_LONG_LONG_INT:
        value = _PACK_LONG_LONG.unpack_from(encoded, offset)[0]
        offset += 8

    # Float
    elif kind == _FIELD_FLOAT:
        value = _PACK_FLOAT.unpack_from(encoded, offset)[0]
        offset += 4

    # Double
    elif kind == _FIELD_DOUBLE:
        value = _PACK_DOUBLE.unpack_from(encoded, offset)[0]
        offset += 8

    # Decimal
    elif kind == _FIELD_DECIMAL:
        decimals = encoded[offset]
        offset += 1
        raw = _PACK_INT.unpack_from(encoded, offset)[0]
        offset += 4
        value = decimal.Decimal(raw) * (decimal.Decimal(10)**-decimals)

    # https://github.com/pika/pika/issues/1205
    # Short Signed Int
    elif kind == _FIELD_SHORT_INT_ALT:
        value = _PACK_SHORT_SIGNED.unpack_from(encoded, offset)[0]
        offset += 2

    # Null / Void
    elif kind == _FIELD_VOID:
        value = None
    else:
        raise exceptions.InvalidFieldTypeException(bytes((kind,)))

    return value, offset
