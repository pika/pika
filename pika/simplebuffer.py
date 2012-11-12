"""This is an implementation of a simple buffer. SimpleBuffer just handles
a string of bytes. The clue, is that you can pop data from the beginning
and append data to the end.

It's ideal to use as a network buffer, from which you send data to the socket.
Use this to avoid concatenating or splitting large strings.

"""
import os
try:
    import cStringIO as StringIO
except ImportError: #pragma: no coverage
    import StringIO

# Python 2.4 support: os lacks SEEK_END and friends
try:
    getattr(os, "SEEK_END")
except AttributeError: #pragma: no coverage
    os.SEEK_SET, os.SEEK_CUR, os.SEEK_END = range(3)


class SimpleBuffer(object):
    """A simple buffer that will handle storing, reading and sending strings
    to a socket.

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
        """Create a new instance of the SimpleBuffer. If you pass in data,
        the buffer will be primed with that value.

        :param data: Optional data to prime the buffer with
        :type data: str or unicode

        """
        self.buf = self._get_stringio()
        if data is not None:
            self.write(data)
        self.buf.seek(0, os.SEEK_END)

    def _get_stringio(self):
        """Return an instance of the StringIO file object.

        :rtype: file

        """
        return StringIO.StringIO()

    def write(self, *data_strings):
        """Append given strings to the buffer.

        :param data_strings: Value to write to the buffer
        :type data_strings: str or unicode

        """
        for data in data_strings:
            if not data:
                continue
            self.buf.write(data)
            self.size += len(data)

    def read(self, size=None):
        """Read the data from the buffer, at most 'size' bytes.

        :param int size: The number of bytes to read

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
        """Move pointer and discard first 'size' bytes.

        :param int size: The number of bytes to consume

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
        """Read up to 'size' bytes, also remove it from the buffer.

        :param int size: The number of bytes to read and consume
        :rtype: str

        """
        assert(self.size >= size)
        data = self.read(size)
        self.consume(size)
        return data

    def send_to_socket(self, sd):
        """Faster way of sending buffer data to socket 'sd'.

        :param socket.socket sd: The socket to send data to
        :rtype: int

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
        """Remove all the data from buffer."""
        self.consume(self.size)

    def __nonzero__(self):
        """Is there any data in the buffer? ie not foo

        :rtype: bool

        """
        return self.size > 0

    def __len__(self):
        """Return the amount of readable data in the buffer.

        :rtype: len

        """
        return self.size

    def __str__(self):
        """Return the string representation of the class.

        :rtype: str

        """
        return self.__repr__()

    def __repr__(self):
        """Return the string representation of the class.

        :rtype: str

        """
        return '<SimpleBuffer of %i bytes, %i total size, %r%s>' % \
                    (self.size, self.size + self.offset, self.read(16),
                    (self.size > 16) and '...' or '')
