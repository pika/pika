import sys as _sys


PY2 = _sys.version_info < (3,)
PY3 = not PY2


if not PY2:
    # these were moved around for Python 3
    from urllib.parse import unquote as url_unquote, urlencode

    # Python 3 does not have basestring anymore; we include
    # *only* the str here as this is used for textual data.
    basestring = (str,)

    # for assertions that the data is either encoded or non-encoded text
    str_or_bytes = (str, bytes)

    # xrange is gone, replace it with range
    xrange = range

    # the unicode type is str
    unicode_type = str


    def dictkeys(dct):
        """
        Returns a list of keys of dictionary

        dict.keys returns a view that works like .keys in Python 2
        *except* any modifications in the dictionary will be visible
        (and will cause errors if the view is being iterated over while
        it is modified).
        """

        return list(dct.keys())

    def dictvalues(dct):
        """
        Returns a list of values of a dictionary

        dict.values returns a view that works like .values in Python 2
        *except* any modifications in the dictionary will be visible
        (and will cause errors if the view is being iterated over while
        it is modified).
        """
        return list(dct.values())

    def byte(*args):
        """
        This is the same as Python 2 `chr(n)` for bytes in Python 3

        Returns a single byte `bytes` for the given int argument (we
        optimize it a bit here by passing the positional argument tuple
        directly to the bytes constructor.
        """
        return bytes(args)

    class long(int):
        """
        A marker class that signifies that the integer value should be
        serialized as `l` instead of `I`
        """

        def __repr__(self):
            return str(self) + 'L'

    def canonical_str(value):
        """
        Return the canonical str value for the string.
        In both Python 3 and Python 2 this is str.
        """

        return str(value)

else:
    from urllib import unquote as url_unquote, urlencode

    basestring = basestring
    str_or_bytes = basestring
    xrange = xrange
    unicode_type = unicode
    dictkeys = dict.keys
    dictvalues = dict.values
    byte = chr
    long = long

    def canonical_str(value):
        """
        Returns the canonical string value of the given string.
        In Python 2 this is the value unchanged if it is an str, otherwise
        it is the unicode value encoded as UTF-8.
        """

        try:
            return str(value)
        except UnicodeEncodeError:
            return str(value.encode('utf-8'))


def as_bytes(value):
    if not isinstance(value, bytes):
        return value.encode('UTF-8')
    return value

