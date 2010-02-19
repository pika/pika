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

"""
This is an implementation of a simple buffer. SimpleBuffer just handles
a string of bytes. The clue, is that you can pop data from the beginning
and append data to the end.

It's ideal to use as a network buffer, from which you send data to the socket.
Use this to avoid concatenating or splitting large strings.
"""
import os
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO


class SimpleBuffer:
    """
    >>> b = SimpleBuffer("abcdef")
    >>> b.read_and_consume(3)
    'abc'
    >>> b.write(None, '')
    >>> b.read(0)
    ''
    >>> repr(b)
    "<SimpleBuffer of 3 bytes, 6 total size, 'def'>"
    >>> str(b)
    "<SimpleBuffer of 3 bytes, 6 total size, 'def'>"
    >>> b.flush()
    >>> b.read(1)
    ''
    """
    buf = None
    offset = 0
    size = 0

    def __init__(self, data=None):
        self.buf = StringIO.StringIO()
        if data is not None:
            self.write(data)
        self.buf.seek(0, os.SEEK_END)

    def write(self, *data_strings):
        """ Append given strings to the buffer. """
        for data in data_strings:
            if not data:
                continue
            self.buf.write(data)
            self.size += len(data)

    def read(self, size=None):
        """ Read the data from the buffer, at most 'size' bytes. """
        if size is 0:
            return ''

        self.buf.seek(self.offset)

        if size is None:
            data = self.buf.read()
        else:
            data = self.buf.read(size)

        self.buf.seek(0, os.SEEK_END)
        return data

    def consume(self, size):
        """ Move pointer and discard first 'size' bytes. """
        self.offset += size
        self.size -= size
        # GC old StringIO instance and free memory used by it.
        if self.size == 0 and self.offset > 65536:
            self.buf.close()
            del self.buf
            self.buf = StringIO.StringIO()
            self.offset = 0

    def read_and_consume(self, size):
        """ Read up to 'size' bytes, also remove it from the buffer. """
        assert(self.size >= size)
        data = self.read(size)
        self.consume(size)
        return data

    def send_to_socket(self, sd):
        """ Faster way of sending buffer data to socket 'sd'. """
        self.buf.seek(self.offset)
        r = sd.send( self.buf.read() )
        self.buf.seek(0, os.SEEK_END)
        self.offset += r
        self.size -= r
        if self.offset > 524288 and self.size == 0:
            self.consume(0)
        return r

    def flush(self):
        """ Remove all the data from buffer. """
        self.consume(self.size)

    def __nonzero__(self):
        """ Are we empty? """
        return True if self.size else False

    def __len__(self):
        return self.size

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '<SimpleBuffer of %i bytes, %i total size, %r%s>' % \
                    (self.size, self.size + self.offset, self.read(16), '...' if self.size > 16 else '')
