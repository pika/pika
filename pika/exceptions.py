# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****


class AMQPError(Exception):
    pass


class AMQPConnectionError(AMQPError):
    pass


class LoginError(AMQPConnectionError):
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


class InvalidFrameSize(ProtocolSyntaxError):
    pass


class InvalidRPCParameterType(Exception):
    pass
