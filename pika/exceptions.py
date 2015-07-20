"""Pika specific exceptions"""


class AMQPError(Exception):

    def __repr__(self):
        return 'An unspecified AMQP error has occurred'


class AMQPConnectionError(AMQPError):

    def __repr__(self):
        if len(self.args) == 1:
            if self.args[0] == 1:
                return ('No connection could be opened after 1 '
                        'connection attempt')
            elif isinstance(self.args[0], int):
                return ('No connection could be opened after %s '
                        'connection attempts' % self.args[0])
            else:
                return ('No connection could be opened: %s' % self.args[0])
        elif len(self.args) == 2:
            return '%s: %s' % (self.args[0], self.args[1])


class IncompatibleProtocolError(AMQPConnectionError):

    def __repr__(self):
        return 'The protocol returned by the server is not supported'


class AuthenticationError(AMQPConnectionError):

    def __repr__(self):
        return ('Server and client could not negotiate use of the %s '
                'authentication mechanism' % self.args[0])


class ProbableAuthenticationError(AMQPConnectionError):

    def __repr__(self):
        return ('Client was disconnected at a connection stage indicating a '
                'probable authentication error')


class ProbableAccessDeniedError(AMQPConnectionError):

    def __repr__(self):
        return ('Client was disconnected at a connection stage indicating a '
                'probable denial of access to the specified virtual host')


class NoFreeChannels(AMQPConnectionError):

    def __repr__(self):
        return 'The connection has run out of free channels'


class ConnectionClosed(AMQPConnectionError):

    def __repr__(self):
        if len(self.args) == 2:
            return 'The AMQP connection was closed (%s) %s' % (self.args[0],
                                                               self.args[1])
        else:
            return 'The AMQP connection was closed: %s' % (self.args,)


class AMQPChannelError(AMQPError):

    def __repr__(self):
        return 'An unspecified AMQP channel error has occurred'


class ChannelClosed(AMQPChannelError):

    def __repr__(self):
        if len(self.args) == 2:
            return 'The channel was closed (%s) %s' % (self.args[0],
                                                       self.args[1])
        else:
            return 'The channel was closed: %s' % (self.args,)


class DuplicateConsumerTag(AMQPChannelError):

    def __repr__(self):
        return ('The consumer tag specified already exists for this '
                'channel: %s' % self.args[0])


class ConsumerCancelled(AMQPChannelError):

    def __repr__(self):
        return 'Server cancelled consumer'


class UnroutableError(AMQPChannelError):
    """Exception containing one or more unroutable messages returned by broker
    via Basic.Return.

    Used by BlockingChannel.

    In publisher-acknowledgements mode, this is raised upon receipt of Basic.Ack
    from broker; in the event of Basic.Nack from broker, `NackError` is raised
    instead
    """

    def __init__(self, messages):
        """
        :param messages: sequence of returned unroutable messages
        :type messages: sequence of `blocking_connection.ReturnedMessage`
           objects
        """
        super(UnroutableError, self).__init__(
            "%s unroutable message(s) returned" % (len(messages)))

        self.messages = messages

    def __repr__(self):
        return '%s: %i unroutable messages returned by broker' % (
            self.__class__.__name__, len(self.messages))


class NackError(AMQPChannelError):
    """This exception is raised when a message published in
    publisher-acknowledgements mode is Nack'ed by the broker.

    Used by BlockingChannel.
    """

    def __init__(self, messages):
        """
        :param messages: sequence of returned unroutable messages
        :type messages: sequence of `blocking_connection.ReturnedMessage`
            objects
        """
        super(NackError, self).__init__(
            "%s message(s) NACKed" % (len(messages)))

        self.messages = messages

    def __repr__(self):
        return '%s: %i unroutable messages returned by broker' % (
            self.__class__.__name__, len(self.messages))


class InvalidChannelNumber(AMQPError):

    def __repr__(self):
        return 'An invalid channel number has been specified: %s' % self.args[0]


class ProtocolSyntaxError(AMQPError):

    def __repr__(self):
        return 'An unspecified protocol syntax error occurred'


class UnexpectedFrameError(ProtocolSyntaxError):

    def __repr__(self):
        return 'Received a frame out of sequence: %r' % self.args[0]


class ProtocolVersionMismatch(ProtocolSyntaxError):

    def __repr__(self):
        return 'Protocol versions did not match: %r vs %r' % (self.args[0],
                                                              self.args[1])


class BodyTooLongError(ProtocolSyntaxError):

    def __repr__(self):
        return ('Received too many bytes for a message delivery: '
                'Received %i, expected %i' % (self.args[0], self.args[1]))


class InvalidFrameError(ProtocolSyntaxError):

    def __repr__(self):
        return 'Invalid frame received: %r' % self.args[0]


class InvalidFieldTypeException(ProtocolSyntaxError):

    def __repr__(self):
        return 'Unsupported field kind %s' % self.args[0]


class UnsupportedAMQPFieldException(ProtocolSyntaxError):

    def __repr__(self):
        return 'Unsupported field kind %s' % type(self.args[1])


class UnspportedAMQPFieldException(UnsupportedAMQPFieldException):
    """Deprecated version of UnsupportedAMQPFieldException"""


class MethodNotImplemented(AMQPError):
    pass


class ChannelError(Exception):

    def __repr__(self):
        return 'An unspecified error occurred with the Channel'


class InvalidMinimumFrameSize(ProtocolSyntaxError):

    def __repr__(self):
        return 'AMQP Minimum Frame Size is 4096 Bytes'


class InvalidMaximumFrameSize(ProtocolSyntaxError):

    def __repr__(self):
        return 'AMQP Maximum Frame Size is 131072 Bytes'


class RecursionError(Exception):
    """The requested operation would result in unsupported recursion or
    reentrancy.

    Used by BlockingConnection/BlockingChannel

    """


class ShortStringTooLong(AMQPError):

    def __repr__(self):
        return ('AMQP Short String can contain up to 255 bytes: '
                '%.300s' % self.args[0])
