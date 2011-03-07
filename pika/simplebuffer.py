# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
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

# Python 2.4 support: os lacks SEEK_END and friends
try:
    getattr(os, "SEEK_END")
except AttributeError:
    os.SEEK_SET, os.SEEK_CUR, os.SEEK_END = range(3)


class SimpleBuffer(object):
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
        """
        Append given strings to the buffer.
        """
        for data in data_strings:
            if not data:
                continue
            self.buf.write(data)
            self.size += len(data)

    def read(self, size=None):
        """
        Read the data from the buffer, at most 'size' bytes.
        """
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
        """
        Move pointer and discard first 'size' bytes.
        """
        self.offset += size
        self.size -= size
        # GC old StringIO instance and free memory used by it.
        if self.size == 0 and self.offset > 65536:
            self.buf.close()
            del self.buf
            self.buf = StringIO.StringIO()
            self.offset = 0

    def read_and_consume(self, size):
        """
        Read up to 'size' bytes, also remove it from the buffer.
        """
        assert(self.size >= size)
        data = self.read(size)
        self.consume(size)
        return data

    def send_to_socket(self, sd):
        """
        Faster way of sending buffer data to socket 'sd'.
        """
        self.buf.seek(self.offset)
        r = sd.send(self.buf.read())
        self.buf.seek(0, os.SEEK_END)
        self.offset += r
        self.size -= r
        if self.offset > 524288 and self.size == 0:
            self.consume(0)
        return r

    def flush(self):
        """
        Remove all the data from buffer.
        """
        self.consume(self.size)

    def __nonzero__(self):
        """ Are we empty? """
        return self.size > 0

    def __len__(self):
        return self.size

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '<SimpleBuffer of %i bytes, %i total size, %r%s>' % \
                    (self.size, self.size + self.offset, self.read(16),
                    (self.size > 16) and '...' or '')
