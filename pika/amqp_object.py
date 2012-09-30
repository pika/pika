# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****


class AMQPObject(object):
    """Base object that is extended by AMQP low level frames and AMQP classes
    and methods.

    """
    NAME = 'PikaObject'

    def __repr__(self):
        items = list()
        for key, value in self.__dict__.iteritems():
            if getattr(self.__class__, key, None) != value:
                items.append('%s=%s' % (key, value))
        return "<%s(%s)>" % (self.NAME, items)


class Class(AMQPObject):
    """Is extended by AMQP classes"""
    NAME = 'Unextended Class'


class Method(AMQPObject):
    """Is extended by AMQP methods"""
    NAME = 'Unextended Method'

    def _set_content(self, properties, body):
        """If the method is a content frame, set the properties and body to
        be carried as attributes of the class.

        :param pika.frame.Properties properties: AMQP Basic Properties
        :param str|unicode body: The message body

        """
        self._properties = properties
        self._body = body

    def get_properties(self):
        """Return the properties if they are set.

        :rtype: pika.frame.Properties

        """
        return self._properties

    def get_body(self):
        """Return the message body if it is set.

        :rtype: str|unicode

        """
        return self._body


class Properties(AMQPObject):
    """Class to encompass message properties (AMQP Basic.Properties)"""
