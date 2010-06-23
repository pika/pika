# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
#
# ***** END LICENSE BLOCK *****

import struct
import decimal
import datetime
import calendar

from pika.exceptions import *

def encode_table(pieces, table):
    if table is None:
        table = {}
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
            pieces.append(struct.pack('>c', 'F'))
            tablesize = tablesize + 1 + encode_table(pieces, value)
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
