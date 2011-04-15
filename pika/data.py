# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import struct
import decimal
import calendar
from datetime import datetime
from pika.exceptions import *


def encode_table(pieces, table):
    table = table or dict()
    length_index = len(pieces)
    pieces.append(None)  # placeholder
    tablesize = 0
    for (key, value) in table.iteritems():
        pieces.append(struct.pack('B', len(key)))
        pieces.append(key)
        tablesize = tablesize + 1 + len(key)
        tablesize += encode_value(pieces, value)

    pieces[length_index] = struct.pack('>I', tablesize)
    return tablesize + 4


def encode_value(pieces, value):
    if isinstance(value, str):
        pieces.append(struct.pack('>cI', 'S', len(value)))
        pieces.append(value)
        return 5 + len(value)
    elif isinstance(value, bool):
        pieces.append(struct.pack('>cB', 't', int(value)))
        return 2
    elif isinstance(value, int):
        pieces.append(struct.pack('>ci', 'I', value))
        return 5
    elif isinstance(value, long):
        pieces.append(struct.pack('>cq', 'l', value))
        return 9
    elif isinstance(value, decimal.Decimal):
        value = value.normalize()
        if value._exp < 0:
            decimals = -value._exp
            raw = int(value * (decimal.Decimal(10) ** decimals))
            pieces.append(struct.pack('>cBi', 'D', decimals, raw))
        else:
            # per spec, the "decimals" octet is unsigned (!)
            pieces.append(struct.pack('>cBi', 'D', 0, int(value)))
        return 6
    elif isinstance(value, datetime):
        pieces.append(struct.pack('>cQ', 'T',
                                  calendar.timegm(value.utctimetuple())))
        return 9
    elif isinstance(value, dict):
        pieces.append(struct.pack('>c', 'F'))
        return 1 + encode_table(pieces, value)
    elif isinstance(value, list):
        p = []
        for v in value:
            encode_value(p, v)
        piece = ''.join(p)
        pieces.append(struct.pack('>cI', 'A', len(piece)))
        pieces.append(piece)
        return 5 + len(piece)
    else:
        raise InvalidTableError("Unsupported field kind during encoding",
                                pieces, value)


def decode_table(encoded, offset):
    result = {}
    tablesize = struct.unpack_from('>I', encoded, offset)[0]
    offset += 4
    limit = offset + tablesize
    while offset < limit:
        keylen = struct.unpack_from('B', encoded, offset)[0]
        offset += 1
        key = encoded[offset: offset + keylen]
        offset += keylen
        value, offset = decode_value(encoded, offset)
        result[key] = value
    return result, offset


def decode_value(encoded, offset):
    kind = encoded[offset]
    offset += 1
    if kind == 'S':
        length = struct.unpack_from('>I', encoded, offset)[0]
        offset += 4
        value = encoded[offset: offset + length]
        offset += length
    elif kind == 't':
        value = struct.unpack_from('>B', encoded, offset)[0]
        value = bool(value)
        offset += 1
    elif kind == 'I':
        value = struct.unpack_from('>i', encoded, offset)[0]
        offset += 4
    elif kind == 'l':
        value = long(struct.unpack_from('>q', encoded, offset)[0])
        offset += 8
    elif kind == 'D':
        decimals = struct.unpack_from('B', encoded, offset)[0]
        offset += 1
        raw = struct.unpack_from('>i', encoded, offset)[0]
        offset += 4
        value = decimal.Decimal(raw) * (decimal.Decimal(10) ** -decimals)
    elif kind == 'T':
        value = datetime.utcfromtimestamp(struct.unpack_from('>Q', encoded,
                                                             offset)[0])
        offset += 8
    elif kind == 'F':
        (value, offset) = decode_table(encoded, offset)
    elif kind == 'A':
        length = struct.unpack_from('>I', encoded, offset)[0]
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
    if data_type == 'bit' and not isinstance(value, bool):
        raise InvalidRPCParameterType("%s must be a bool" % field_name)

    if data_type == 'shortstr' and \
       (not isinstance(value, str) and not isinstance(value, unicode)):
        raise InvalidRPCParameterType("%s must be a str or unicode" % \
                                      field_name)

    if data_type == 'short' and not isinstance(value, int):
        raise InvalidRPCParameterType("%s must be a int" % field_name)

    if data_type == 'long' and not (isinstance(value, long) or
                                    isinstance(value, int)):
        raise InvalidRPCParameterType("%s must be a long" % field_name)
