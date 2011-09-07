# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import struct
import decimal
import calendar
import sys
from datetime import datetime
from pika.exceptions import *


def encode_table(pieces, table):
    table = table or dict()
    length_index = len(pieces)
    pieces.append(None)  # placeholder
    tablesize = 0
    for (key, value) in table.items():
        pieces.append(struct.pack(b'B', len(key)))
        pieces.append(key.encode('ascii'))
        tablesize = tablesize + 1 + len(key)
        tablesize += encode_value(pieces, value)

    pieces[length_index] = struct.pack(b'>I', tablesize)
    return tablesize + 4


def encode_value(pieces, value):
    if isinstance(value, str):
        pieces.append(struct.pack(b'>cI', b'S', len(value)))
        pieces.append(value.encode('ascii'))
        return 5 + len(value)
    elif isinstance(value, bytes):
        pieces.append(struct.pack(b'>cI', b'S', len(value)))
        pieces.append(value)
        return 5 + len(value)
    elif isinstance(value, bool):
        pieces.append(struct.pack(b'>cB', b't', int(value)))
        return 2
    elif isinstance(value, int):
        if abs(value) > sys.maxsize:
            pieces.append(struct.pack(b'>cq', b'l', value))
            return 9
        else:
            pieces.append(struct.pack(b'>ci', b'I', value))
            return 5
    elif isinstance(value, decimal.Decimal):
        value = value.normalize()
        if value._exp < 0:
            decimals = -value._exp
            raw = int(value * (decimal.Decimal(10) ** decimals))
            pieces.append(struct.pack(b'>cBi', b'D', decimals, raw))
        else:
            # per spec, the "decimals" octet is unsigned (!)
            pieces.append(struct.pack(b'>cBi', b'D', 0, int(value)))
        return 6
    elif isinstance(value, datetime):
        pieces.append(struct.pack(b'>cQ', b'T',
                                  calendar.timegm(value.utctimetuple())))
        return 9
    elif isinstance(value, dict):
        pieces.append(struct.pack(b'>c', b'F'))
        return 1 + encode_table(pieces, value)
    elif isinstance(value, list):
        p = []
        for v in value:
            encode_value(p, v)
        piece = b''.join(p)
        pieces.append(struct.pack(b'>cI', b'A', len(piece)))
        pieces.append(piece)
        return 5 + len(piece)
    else:
        raise InvalidTableError("Unsupported field kind during encoding",
                                pieces, value)


def decode_table(encoded, offset):
    result = {}
    tablesize = struct.unpack_from(b'>I', encoded, offset)[0]
    offset += 4
    limit = offset + tablesize
    while offset < limit:
        keylen = struct.unpack_from(b'B', encoded, offset)[0]
        offset += 1
        key = encoded[offset: offset + keylen].decode('ascii')
        offset += keylen
        value, offset = decode_value(encoded, offset)
        result[key] = value
    return result, offset


def decode_value(encoded, offset):
    kind = bytes([encoded[offset]])
    offset += 1
    if kind == b'S':
        length = struct.unpack_from(b'>I', encoded, offset)[0]
        offset += 4
        value = encoded[offset: offset + length].decode('ascii')
        offset += length
    elif kind == b't':
        value = struct.unpack_from(b'>B', encoded, offset)[0]
        value = bool(value)
        offset += 1
    elif kind == b'I':
        value = struct.unpack_from(b'>i', encoded, offset)[0]
        offset += 4
    elif kind == b'l':
        value = int(struct.unpack_from(b'>q', encoded, offset)[0])
        offset += 8
    elif kind == b'D':
        decimals = struct.unpack_from(b'B', encoded, offset)[0]
        offset += 1
        raw = struct.unpack_from(b'>i', encoded, offset)[0]
        offset += 4
        value = decimal.Decimal(raw) * (decimal.Decimal(10) ** -decimals)
    elif kind == b'T':
        value = datetime.utcfromtimestamp(struct.unpack_from(b'>Q', encoded,
                                                             offset)[0])
        offset += 8
    elif kind == b'F':
        (value, offset) = decode_table(encoded, offset)
    elif kind == b'A':
        length = struct.unpack_from(b'>I', encoded, offset)[0]
        offset += 4
        offset_end = offset + length
        value = []
        while offset < offset_end:
            v, offset = decode_value(encoded, offset)
            value.append(v)
    else:
        raise InvalidTableError("Unsupported field kind %s during decoding" % \
                                kind)
    return value, offset


def validate_type(field_name, value, data_type):
    """
    Validate the data types passed into the RPC Command
    """
    if data_type == b'bit' and not isinstance(value, bool):
        raise InvalidRPCParameterType("%s must be a bool" % field_name)

    if data_type == b'shortstr' and \
       (not isinstance(value, str) and not isinstance(value, str)):
        raise InvalidRPCParameterType("%s must be a str or unicode" % \
                                      field_name)

    if data_type == b'short' and not isinstance(value, int):
        raise InvalidRPCParameterType("%s must be a int" % field_name)

    if data_type == b'long' and not (isinstance(value, int) or
                                    isinstance(value, int)):
        raise InvalidRPCParameterType("%s must be a long" % field_name)
