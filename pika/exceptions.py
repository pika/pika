"""Pika specific exceptions"""
from pika import spec


class AMQPError(Exception):
    pass


class AMQPConnectionError(AMQPError):
    def __repr__(self):

        if len(self.args) == 1:
            return ("No connection could be opened after %s retries" %
                    self.args[0])
        elif len(self.args) == 2:
            return "%s: %s" % (self.args[0], self.args[1])

class IncompatibleProtocolError(AMQPConnectionError):
    pass


class AuthenticationError(AMQPConnectionError):
    def __repr__(self):
        return "No %s support for the credentials" % self.args[0]


class ProbableAuthenticationError(AMQPConnectionError):
    pass


class ProbableAccessDeniedError(AMQPConnectionError):
    pass


class NoFreeChannels(AMQPConnectionError):
    pass


class ConnectionClosed(AMQPConnectionError):
    pass


class AMQPChannelError(AMQPError):
    pass


class ChannelBlocked(AMQPChannelError):
    pass


class DuplicateConsumerTag(AMQPChannelError):
    pass


class UnknownConsumerTag(AMQPChannelError):
    pass


class ChannelClosed(AMQPChannelError):
    pass


class InvalidChannelNumber(AMQPError):
    pass


class RecursiveOperationDetected(AMQPError):
    pass


class ProtocolSyntaxError(AMQPError):
    pass


class UnexpectedFrameError(ProtocolSyntaxError):
    pass


class ProtocolVersionMismatch(ProtocolSyntaxError):
    pass


class BodyTooLongError(ProtocolSyntaxError):
    pass


class InvalidFrameError(ProtocolSyntaxError):
    pass


class InvalidProtocolHeader(ProtocolSyntaxError):
    pass


class InvalidTableError(ProtocolSyntaxError):
    pass


class TableDecodingError(ProtocolSyntaxError):
    pass


class DecodingError(ProtocolSyntaxError):
    pass


class MethodNotImplemented(AMQPError):
    pass


class ChannelTransportError(Exception):
    pass


class CallbackReplyAlreadyRegistered(ChannelTransportError):
    pass


class InvalidMinimumFrameSize(ProtocolSyntaxError):
    def __repr__(self):
        return "AMQP Minimum Frame Size is %i Bytes" % spec.FRAME_MIN_SIZE


class InvalidMaximumFrameSize(ProtocolSyntaxError):
    def __repr__(self):
        return "AMQP Maximum Frame Size is %i Bytes" % spec.FRAME_MAX_SIZE


class InvalidRPCParameterType(Exception):
    pass
