"""Pika specific exceptions"""


class AMQPError(Exception):
    def __repr__(self):
        return 'An unspecified AMQP error has occurred'


class AMQPConnectionError(AMQPError):
    def __repr__(self):
        if len(self.args) == 1:
            if (self.args[0] == 1):
                return ('No connection could be opened after 1 '
                        'connection attempt')
            else:
                return ('No connection could be opened after %s '
                        'connection attempts' %
                        self.args[0])
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
        return 'The AMQP connection was closed (%s) %s' % (self.args[0],
                                                           self.args[1])


class AMQPChannelError(AMQPError):
    def __repr__(self):
        return 'An unspecified AMQP channel error has occurred'


class ChannelClosed(AMQPChannelError):
    def __repr__(self):
        return 'The channel was remotely closed (%s) %s' % (self.args[0],
                                                            self.args[1])


class DuplicateConsumerTag(AMQPChannelError):
    def __repr__(self):
        return ('The consumer tag specified already exists for this '
                'channel: %s' % self.args[0])


class ConsumerCancelled(AMQPChannelError):
    def __repr__(self):
        return 'Server cancelled consumer (%s): %s' % (self.args[0].reply_code,
                                                       self.args[0].reply_text)


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


class UnspportedAMQPFieldException(ProtocolSyntaxError):
    def __repr__(self):
        return 'Unsupported field kind %s' % type(self.args[1])


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
