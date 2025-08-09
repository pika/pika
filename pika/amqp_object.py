"""Base classes that are extended by low level AMQP frames and higher level
AMQP classes and methods.

"""

from __future__ import annotations

from typing import List


class AMQPObject:
    """Base object that is extended by AMQP low level frames and AMQP classes
    and methods.

    """
    NAME: str = 'AMQPObject'
    INDEX: int | None = None

    def __repr__(self) -> str:
        items = list()
        for key, value in self.__dict__.items():
            if getattr(self.__class__, key, None) != value:
                items.append('{}={}'.format(key, value))
        if not items:
            return "<%s>" % self.NAME
        return "<{}({})>".format(self.NAME, sorted(items))

    def __eq__(self, other: object) -> bool:
        if other is not None:
            return self.__dict__ == other.__dict__
        else:
            return False


class Class(AMQPObject):
    """Is extended by AMQP classes"""
    NAME: str = 'Unextended Class'


class Method(AMQPObject):
    """Is extended by AMQP methods"""
    NAME: str = 'Unextended Method'
    synchronous: bool = False

    def _set_content(self, properties: Properties, body: bytes) -> None:
        """If the method is a content frame, set the properties and body to
        be carried as attributes of the class.

        :param pika.frame.Properties properties: AMQP Basic Properties
        :param bytes body: The message body

        """
        self._properties = properties  # pylint: disable=W0201
        self._body = body  # pylint: disable=W0201

    def get_properties(self) -> Properties:
        """Return the properties if they are set.

        :rtype: pika.frame.Properties

        """
        return self._properties

    def get_body(self) -> bytes:
        """Return the message body if it is set.

        :rtype: str|unicode

        """
        return self._body
    
    def encode(self) -> List[bytes]:
        """Encode the method into a binary format.

        :rtype: List[bytes]

        """
        raise NotImplementedError("Subclasses must implement this method")
    
    def decode(self, encoded: bytes, offset: int = 0) -> Method:
        """Decode the method from a binary format.

        :param bytes encoded: The encoded method data
        :param int offset: The offset to start decoding from
        
        :rtype: Method
        """
        raise NotImplementedError("Subclasses must implement this method")


class Properties(AMQPObject):
    """Class to encompass message properties (AMQP Basic.Properties)"""
    NAME: str = 'Unextended Properties'
