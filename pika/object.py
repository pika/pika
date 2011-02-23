# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****


class object_(object):

    @property
    def name(self):
        return getattr(self, 'NAME', self.__class__.__name__)

    def __repr__(self):
        items = list()
        for key, value in self.__dict__.iteritems():
            if getattr(self.__class__, key, None) != value:
                items.append('%s=%s' % (key, value))
        return "<%s(%s)>" % (self.name, items)


class Class(object_):
    pass


class Method(object_):

    def _set_content(self, properties, body):
        self._properties = properties
        self._body = body

    def get_properties(self):
        return self._properties

    def get_body(self):
        return self._body


class Properties(object_):
    pass
