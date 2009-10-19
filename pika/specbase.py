class Class:
    def __repr__(self):
        return _codec_repr(self, self.__class__)

class Method:
    def _set_content(self, properties, body):
        self._properties = properties
        self._body = body

    def get_properties(self):
        return self._properties

    def get_body(self):
        return self._body

    def __repr__(self):
        return _codec_repr(self, self.__class__)

class Properties:
    def __repr__(self):
        return _codec_repr(self, self.__class__)

def _codec_repr(o, c):
    """Returns a repr()esentation of o in the form of a constructor
    call, including only fields that differ from some default instance
    constructed by invoking c with no arguments."""

    d = c()
    n = getattr(o, 'NAME', o.__class__.__name__)
    return n + "(" + \
           ", ".join(["%s = %s" % (k, repr(v)) \
                      for (k, v) in o.__dict__.iteritems() \
                      if getattr(d, k, None) != v]) + \
           ")"
