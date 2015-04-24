import sys as _sys


PY2 = _sys.version_info < (3,)
PY3 = not PY2


if not PY2:
    basestring = (str,)
    from urllib.parse import unquote as url_unquote, urlencode
    xrange = range

    def dictkeys(dct):
        return list(dct.keys())

    unicode_type = str
    def byte(*args):
        return bytes(args)

    class long(int):
        pass

    def __repr__(self):
        return str(self) + 'L'

    int_types = (int, long)

    def canonical_str(value):
        return str(value)

else:
    basestring = basestring
    from urllib import unquote as url_unquote, urlencode
    xrange = xrange

    import uuid as _uuid
    uuid2hex = _uuid.UUID.get_hex
    unicode_type = unicode
    dictkeys = dict.keys
    int_types = (int, long)
    byte = chr
    long = long

    def canonical_str(value):
        try:
            return str(value)
        except UnicodeEncodeError:
            return str(value.encode('utf-8'))

