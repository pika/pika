import struct
import decimal
import datetime
import calendar

from pika.exceptions import *

def encode_table(pieces, table):
    length_index = len(pieces)
    pieces.append(None) # placeholder
    tablesize = 0
    for (key, value) in table.iteritems():
        pieces.append(struct.pack('B', len(key)))
        pieces.append(key)
        tablesize = tablesize + 1 + len(key)
        if isinstance(value, str):
            pieces.append(struct.pack('>cI', 'S', len(value)))
            pieces.append(value)
            tablesize = tablesize + 5 + len(value)
        elif isinstance(value, int):
            pieces.append(struct.pack('>cI', 'I', value))
            tablesize = tablesize + 5
        elif isinstance(value, decimal.Decimal):
            value = value.normalize()
            if value._exp < 0:
                decimals = -value._exp
                raw = int(value * (decimal.Decimal(10) ** decimals))
                pieces.append(struct.pack('cB>I', 'D', decimals, raw))
            else:
                # per spec, the "decimals" octet is unsigned (!)
                pieces.append(struct.pack('cB>I', 'D', 0, int(value)))
            tablesize = tablesize + 5
        elif isinstance(value, datetime.datetime):
            pieces.append(struct.pack('>cQ', 'T', calendar.timegm(value.utctimetuple())))
            tablesize = tablesize + 9
        elif isinstance(value, dict):
            tablesize = tablesize + encode_table(pieces, value)
        else:
            raise InvalidTableError("Unsupported field kind during encoding", key, value)
    pieces[length_index] = struct.pack('>I', tablesize)
    return tablesize + 4

def decode_table(encoded, offset):
    result = {}
    tablesize = struct.unpack_from('>I', encoded, offset)[0]
    offset = offset + 4
    limit = offset + tablesize
    while offset < limit:
        keylen = struct.unpack_from('B', encoded, offset)[0]
        offset = offset + 1
        key = encoded[offset : offset + keylen]
        offset = offset + keylen
        kind = encoded[offset]
        offset = offset + 1
        if kind == 'S':
            length = struct.unpack_from('>I', encoded, offset)[0]
            offset = offset + 4
            value = encoded[offset : offset + length]
            offset = offset + length
        elif kind == 'I':
            value = struct.unpack_from('>I', encoded, offset)[0]
            offset = offset + 4
        elif kind == 'D':
            decimals = struct.unpack_from('B', encoded, offset)[0]
            offset = offset + 1
            raw = struct.unpack_from('>I', encoded, offset)[0]
            offset = offset + 4
            value = decimal.Decimal(raw) * (decimal.Decimal(10) ** -decimals)
        elif kind == 'T':
            value = datetime.datetime.utcfromtimestamp(struct.unpack_from('>Q', encoded, offset)[0])
            offset = offset + 8
        elif kind == 'F':
            (value, offset) = decode_table(encoded, offset)
        else:
            raise InvalidTableError("Unsupported field kind %s during decoding" % (kind,))
        result[key] = value
    return (result, offset)
