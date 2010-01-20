##   The contents of this file are subject to the Mozilla Public License
##   Version 1.1 (the "License"); you may not use this file except in
##   compliance with the License. You may obtain a copy of the License at
##   http://www.mozilla.org/MPL/
##
##   Software distributed under the License is distributed on an "AS IS"
##   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
##   License for the specific language governing rights and limitations
##   under the License.
##
##   The Original Code is Pika.
##
##   The Initial Developers of the Original Code are LShift Ltd,
##   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
##
##   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
##   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
##   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
##   Technologies LLC, and Rabbit Technologies Ltd.
##
##   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
##   Ltd. Portions created by Cohesive Financial Technologies LLC are
##   Copyright (C) 2007-2009 Cohesive Financial Technologies
##   LLC. Portions created by Rabbit Technologies Ltd are Copyright
##   (C) 2007-2009 Rabbit Technologies Ltd.
##
##   Portions created by Tony Garnock-Jones are Copyright (C)
##   2009-2010 LShift Ltd and Tony Garnock-Jones.
##
##   All Rights Reserved.
##
##   Contributor(s): ______________________________________.
##

from __future__ import nested_scopes

import sys
sys.path.append("../rabbitmq-codegen")  # in case we're next to an experimental revision
sys.path.append("codegen")              # in case we're building from a distribution package

from amqp_codegen import *
import string
import re

DRIVER_METHODS = {
    "Exchange.Declare": ["Exchange.DeclareOk"],
    "Exchange.Delete": ["Exchange.DeleteOk"],
    "Queue.Declare": ["Queue.DeclareOk"],
    "Queue.Bind": ["Queue.BindOk"],
    "Queue.Purge": ["Queue.PurgeOk"],
    "Queue.Delete": ["Queue.DeleteOk"],
    "Queue.Unbind": ["Queue.UnbindOk"],
    "Basic.Qos": ["Basic.QosOk"],
    "Basic.Get": ["Basic.GetOk", "Basic.GetEmpty"],
    "Basic.Ack": [],
    "Basic.Reject": [],
    "Basic.Recover": [],
    "Tx.Select": ["Tx.SelectOk"],
    "Tx.Commit": ["Tx.CommitOk"],
    "Tx.Rollback": ["Tx.RollbackOk"]
    }

def fieldvalue(v):
    if isinstance(v, unicode):
        return repr(v.encode('ascii'))
    else:
        return repr(v)

def normalize_separators(s):
    s = s.replace('-', '_')
    s = s.replace(' ', '_')
    return s

def pyize(s):
    s = normalize_separators(s)
    if s in ('global', 'class'): s = s + '_'
    return s

def camel(s):
    return normalize_separators(s).title().replace('_', '')

AmqpMethod.structName = lambda m: camel(m.klass.name) + '.' + camel(m.name)
AmqpClass.structName = lambda c: camel(c.name) + "Properties"

def constantName(s):
    return '_'.join(re.split('[- ]', s.upper()))

def flagName(c, f):
    if c:
        return c.structName() + '.' + constantName('flag_' + f.name)
    else:
        return constantName('flag_' + f.name)

def gen(spec):
    def genSingleDecode(prefix, cLvalue, unresolved_domain):
        type = spec.resolveDomain(unresolved_domain)
        if type == 'shortstr':
            print prefix + "length = struct.unpack_from('B', encoded, offset)[0]"
            print prefix + "offset = offset + 1"
            print prefix + "%s = encoded[offset : offset + length]" % (cLvalue,)
            print prefix + "offset = offset + length"
        elif type == 'longstr':
            print prefix + "length = struct.unpack_from('>I', encoded, offset)[0]"
            print prefix + "offset = offset + 4"
            print prefix + "%s = encoded[offset : offset + length]" % (cLvalue,)
            print prefix + "offset = offset + length"
        elif type == 'octet':
            print prefix + "%s = struct.unpack_from('B', encoded, offset)[0]" % (cLvalue,)
            print prefix + "offset = offset + 1"
        elif type == 'short':
            print prefix + "%s = struct.unpack_from('>H', encoded, offset)[0]" % (cLvalue,)
            print prefix + "offset = offset + 2"
        elif type == 'long':
            print prefix + "%s = struct.unpack_from('>I', encoded, offset)[0]" % (cLvalue,)
            print prefix + "offset = offset + 4"
        elif type == 'longlong':
            print prefix + "%s = struct.unpack_from('>Q', encoded, offset)[0]" % (cLvalue,)
            print prefix + "offset = offset + 8"
        elif type == 'timestamp':
            print prefix + "%s = struct.unpack_from('>Q', encoded, offset)[0]" % (cLvalue,)
            print prefix + "offset = offset + 8"
        elif type == 'bit':
            raise "Can't decode bit in genSingleDecode"
        elif type == 'table':
            print prefix + "(%s, offset) = pika.table.decode_table(encoded, offset)" % \
                  (cLvalue,)
        else:
            raise "Illegal domain in genSingleDecode", type

    def genSingleEncode(prefix, cValue, unresolved_domain):
        type = spec.resolveDomain(unresolved_domain)
        if type == 'shortstr':
            print prefix + "pieces.append(struct.pack('B', len(%s)))" % (cValue,)
            print prefix + "pieces.append(%s)" % (cValue,)
        elif type == 'longstr':
            print prefix + "pieces.append(struct.pack('>I', len(%s)))" % (cValue,)
            print prefix + "pieces.append(%s)" % (cValue,)
        elif type == 'octet':
            print prefix + "pieces.append(struct.pack('B', %s))" % (cValue,)
        elif type == 'short':
            print prefix + "pieces.append(struct.pack('>H', %s))" % (cValue,)
        elif type == 'long':
            print prefix + "pieces.append(struct.pack('>I', %s))" % (cValue,)
        elif type == 'longlong':
            print prefix + "pieces.append(struct.pack('>Q', %s))" % (cValue,)
        elif type == 'timestamp':
            print prefix + "pieces.append(struct.pack('>Q', %s))" % (cValue,)
        elif type == 'bit':
            raise "Can't encode bit in genSingleEncode"
        elif type == 'table':
            print prefix + "pika.table.encode_table(pieces, %s)" % (cValue,)
        else:
            raise "Illegal domain in genSingleEncode", type

    def genDecodeMethodFields(m):
        print "        def decode(self, encoded, offset = 0):"
        bitindex = None
        for f in m.arguments:
            if spec.resolveDomain(f.domain) == 'bit':
                if bitindex is None:
                    bitindex = 0
                if bitindex >= 8:
                    bitindex = 0
                if bitindex == 0:
                    print "            bit_buffer = struct.unpack_from('B', encoded, offset)[0]"
                    print "            offset = offset + 1"
                print "            self.%s = (bit_buffer & (1 << %d)) != 0" % \
                      (pyize(f.name), bitindex)
                bitindex = bitindex + 1
            else:
                bitindex = None
                genSingleDecode("            ", "self.%s" % (pyize(f.name),), f.domain)
        print "            return self"
        print

    def genDecodeProperties(c):
        print "    def decode(self, encoded, offset = 0):"
        print "        flags = 0"
        print "        flagword_index = 0"
        print "        while True:"
        print "            partial_flags = struct.unpack_from('>H', encoded, offset)[0]"
        print "            offset = offset + 2"
        print "            flags = flags | (partial_flags << (flagword_index * 16))"
        print "            if (partial_flags & 1) == 0: break"
        print "            flagword_index = flagword_index + 1"
        for f in c.fields:
            if spec.resolveDomain(f.domain) == 'bit':
                print "        self.%s = (flags & %s) != 0" % (pyize(f.name), flagName(c, f))
            else:
                print "        if (flags & %s):" % (flagName(c, f),)
                genSingleDecode("            ", "self.%s" % (pyize(f.name),), f.domain)
                print "        else:"
                print "            self.%s = None" % (pyize(f.name),)
        print "        return self"
        print

    def genEncodeMethodFields(m):
        print "        def encode(self):"
        print "            pieces = []"
        bitindex = None
        def finishBits():
            if bitindex is not None:
                print "            pieces.append(struct.pack('B', bit_buffer))"
        for f in m.arguments:
            if spec.resolveDomain(f.domain) == 'bit':
                if bitindex is None:
                    bitindex = 0
                    print "            bit_buffer = 0;"
                if bitindex >= 8:
                    finishBits()
                    print "            bit_buffer = 0;"
                    bitindex = 0
                print "            if self.%s: bit_buffer = bit_buffer | (1 << %d)" % \
                      (pyize(f.name), bitindex)
                bitindex = bitindex + 1
            else:
                finishBits()
                bitindex = None
                genSingleEncode("            ", "self.%s" % (pyize(f.name),), f.domain)
        finishBits()
        print "            return pieces"
        print

    def genEncodeProperties(c):
        print "    def encode(self):"
        print "        pieces = []"
        print "        flags = 0"
        for f in c.fields:
            if spec.resolveDomain(f.domain) == 'bit':
                print "        if self.%s: flags = flags | %s" % (pyize(f.name), flagName(c, f))
            else:
                print "        if self.%s is not None:" % (pyize(f.name),)
                print "            flags = flags | %s" % (flagName(c, f),)
                genSingleEncode("            ", "self.%s" % (pyize(f.name),), f.domain)
        print "        flag_pieces = []"
        print "        while True:"
        print "            remainder = flags >> 16"
        print "            partial_flags = flags & 0xFFFE"
        print "            if remainder != 0: partial_flags = partial_flags | 1"
        print "            flag_pieces.append(struct.pack('>H', partial_flags))"
        print "            flags = remainder"
        print "            if flags == 0: break"
        print "        return flag_pieces + pieces"
        print

    def fieldDeclList(fields):
        return ''.join([", %s = %s" % (pyize(f.name), fieldvalue(f.defaultvalue)) for f in fields])

    def fieldInitList(prefix, fields):
        if fields:
            return ''.join(["%sself.%s = %s\n" % (prefix, pyize(f.name), pyize(f.name)) \
                            for f in fields])
        else:
            return '%spass' % (prefix,)

    print '# Autogenerated code, do not edit'
    print
    print 'import struct'
    print 'import pika.specbase'
    print 'import pika.table'
    print
    print "PROTOCOL_VERSION = (%d, %d)" % (spec.major, spec.minor)
    print "PORT = %d" % (spec.port)
    print

    for (c,v,cls) in spec.constants:
        print "%s = %s" % (constantName(c), v)
    print

    for c in spec.allClasses():
        print 'class %s(pika.specbase.Class):' % (camel(c.name),)
        print "    INDEX = 0x%.04X ## %d" % (c.index, c.index)
        print "    NAME = %s" % (fieldvalue(camel(c.name)),)
        print

        for m in c.allMethods():
            print '    class %s(pika.specbase.Method):' % (camel(m.name),)
            methodid = m.klass.index << 16 | m.index
            print "        INDEX = 0x%.08X ## %d, %d; %d" % \
                  (methodid,
                   m.klass.index,
                   m.index,
                   methodid)
            print "        NAME = %s" % (fieldvalue(m.structName(),))
            print "        def __init__(self%s):" % (fieldDeclList(m.arguments),)
            print fieldInitList('            ', m.arguments)
            genDecodeMethodFields(m)
            genEncodeMethodFields(m)

    for c in spec.allClasses():
        if c.fields:
            print 'class %s(pika.specbase.Properties):' % (c.structName(),)
            print "    CLASS = %s" % (camel(c.name),)
            print "    INDEX = 0x%.04X ## %d" % (c.index, c.index)
            print "    NAME = %s" % (fieldvalue(c.structName(),))

            index = 0
            if c.fields:
                for f in c.fields:
                    if index % 16 == 15:
                        index = index + 1
                    shortnum = index / 16
                    partialindex = 15 - (index % 16)
                    bitindex = shortnum * 16 + partialindex
                    print '    %s = (1 << %d)' % (flagName(None, f), bitindex)
                    index = index + 1
                print

            print "    def __init__(self%s):" % (fieldDeclList(c.fields),)
            print fieldInitList('        ', c.fields)
            genDecodeProperties(c)
            genEncodeProperties(c)

    print "methods = {"
    print ',\n'.join(["    0x%08X: %s" % (m.klass.index << 16 | m.index, m.structName()) \
                      for m in spec.allMethods()])
    print "}"
    print

    print "props = {"
    print ',\n'.join(["    0x%04X: %s" % (c.index, c.structName()) \
                      for c in spec.allClasses() \
                      if c.fields])
    print "}"
    print

    print "def has_content(methodNumber):"
    for m in spec.allMethods():
        if m.hasContent:
            print '    if methodNumber == %s.INDEX: return True' % (m.structName())
    print "    return False"
    print

    print "class DriverMixin:"
    for m in spec.allMethods():
        if m.structName() in DRIVER_METHODS:
            acceptable_replies = DRIVER_METHODS[m.structName()]
            print "    def %s(self%s):" % (pyize(m.klass.name + '_' + m.name),
                                           fieldDeclList(m.arguments))
            print "        return self.handler._rpc(%s(%s)," % \
                  (m.structName(), ', '.join(["%s = %s" % (pyize(f.name), pyize(f.name))
                                              for f in m.arguments]))
            print "                                 [%s])" % \
                  (', '.join(acceptable_replies),)
            print

def generate(specPath):
    gen(AmqpSpec(specPath))

def dummyGenerate(specPath):
    pass
    
if __name__ == "__main__":
    do_main(dummyGenerate, generate)
