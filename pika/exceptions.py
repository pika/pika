"""Pika specific exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from pika._utils import override

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import ReturnedMessage


class AMQPError(Exception):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: An unspecified AMQP error has occurred; {self.args}'


class AMQPConnectionError(AMQPError):

    @override
    def __init__(self,
                 *args,
                 host: str | None = None,
                 port: int | None = None) -> None:
        super().__init__(*args)
        self.host = host
        self.port = port

    def __repr__(self) -> str:
        if len(self.args) == 2:
            base = f'{self.__class__.__name__}: ({self.args[0]}) {self.args[1]}'
        else:
            base = f'{self.__class__.__name__}: {self.args}'
        if self.host is not None:
            return f'{base} (host={self.host!r}, port={self.port})'
        return base


class ConnectionOpenAborted(AMQPConnectionError):
    """Client closed connection while opening."""


class StreamLostError(AMQPConnectionError):
    """Stream (TCP) connection lost."""


class IncompatibleProtocolError(AMQPConnectionError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: The protocol returned by the server is not supported: {self.args}'
        )


class AuthenticationError(AMQPConnectionError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: Server and client could not negotiate use of the {self.args[0]} '
            'authentication mechanism')


class ProbableAuthenticationError(AMQPConnectionError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: Client was disconnected at a connection stage indicating a '
            f'probable authentication error: {self.args}')


class ProbableAccessDeniedError(AMQPConnectionError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: Client was disconnected at a connection stage indicating a '
            f'probable denial of access to the specified virtual host: {self.args}'
        )


class NoFreeChannels(AMQPConnectionError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: The connection has run out of free channels'


class ConnectionWrongStateError(AMQPConnectionError):
    """Connection is in wrong state for the requested operation."""

    @override
    def __repr__(self) -> str:
        if self.args:
            return super().__repr__()
        return (
            f'{self.__class__.__name__}: The connection is in wrong state for the requested '
            'operation.')


class ConnectionClosed(AMQPConnectionError):

    def __init__(self,
                 reply_code: int,
                 reply_text: str,
                 host: str | None = None,
                 port: int | None = None) -> None:
        """

        :param reply_code: reply-code that was used in user's or broker's
            `Connection.Close` method. NEW in v1.0.0
        :param reply_text: reply-text that was used in user's or broker's
            `Connection.Close` method. Human-readable string corresponding to
            `reply_code`. NEW in v1.0.0
        :param host: hostname or IP of the broker
        :param port: port of the broker
        """
        super().__init__(int(reply_code), str(reply_text), host=host, port=port)

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: ({self.reply_code}) {self.reply_text!r}'

    @property
    def reply_code(self) -> int:
        """NEW in v1.0.0."""
        return self.args[0]

    @property
    def reply_text(self) -> str:
        """NEW in v1.0.0."""
        return self.args[1]


class ConnectionClosedByBroker(ConnectionClosed):
    """Connection.Close from broker."""


class ConnectionClosedByClient(ConnectionClosed):
    """Connection was closed at request of Pika client."""


class ConnectionBlockedTimeout(AMQPConnectionError):
    """RabbitMQ-specific: timed out waiting for connection.unblocked."""


class AMQPHeartbeatTimeout(AMQPConnectionError):
    """Connection was dropped as result of heartbeat timeout."""


class AMQPChannelError(AMQPError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: {self.args!r}'


class ChannelWrongStateError(AMQPChannelError):
    """Channel is in wrong state for the requested operation."""


class ChannelClosed(AMQPChannelError):
    """The channel closed by client or by broker."""

    def __init__(self, reply_code: int, reply_text: str) -> None:
        """

        :param reply_code: reply-code that was used in user's or broker's
            `Channel.Close` method. One of the AMQP-defined Channel Errors.
            NEW in v1.0.0
        :param reply_text: reply-text that was used in user's or broker's
            `Channel.Close` method. Human-readable string corresponding to
            `reply_code`;
            NEW in v1.0.0

        """
        super().__init__(int(reply_code), str(reply_text))

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: ({self.reply_code}) {self.reply_text!r}'

    @property
    def reply_code(self) -> int:
        """NEW in v1.0.0."""
        return self.args[0]

    @property
    def reply_text(self) -> str:
        """NEW in v1.0.0."""
        return self.args[1]


class ChannelClosedByBroker(ChannelClosed):
    """
    `Channel.Close` from broker; may be passed as reason to channel's on-closed callback of non-
    blocking connection adapters or raised by `BlockingConnection`.

    NEW in v1.0.0
    """


class ChannelClosedByClient(ChannelClosed):
    """
    Channel closed by client upon receipt of `Channel.CloseOk`; may be passed as reason to channel's
    on-closed callback of non-blocking connection adapters, but not raised by `BlockingConnection`.

    NEW in v1.0.0
    """


class DuplicateConsumerTag(AMQPChannelError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: The consumer tag specified already exists for this '
            f'channel: {self.args[0]}')


class ConsumerCancelled(AMQPChannelError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: Server cancelled consumer'


class UnroutableError(AMQPChannelError):
    """
    Exception containing one or more unroutable messages returned by broker via Basic.Return.

    Used by BlockingChannel.

    In publisher-acknowledgements mode, this is raised upon receipt of Basic.Ack from broker; in the
    event of Basic.Nack from broker, `NackError` is raised instead
    """

    def __init__(self, messages: Sequence[ReturnedMessage]) -> None:
        """
        :param messages: Sequence of returned unroutable messages
        """
        super().__init__(f"{len(messages)} unroutable message(s) returned")

        self.messages = messages

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: {len(self.messages)} unroutable messages returned by broker'


class NackError(AMQPChannelError):
    """
    This exception is raised when a message published in publisher-acknowledgements mode is Nack'ed
    by the broker.

    Used by BlockingChannel.
    """

    def __init__(self, messages: Sequence[ReturnedMessage]) -> None:
        """
        :param messages: Sequence of returned unroutable messages
        """
        super().__init__(f"{len(messages)} message(s) NACKed")

        self.messages = messages

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: {len(self.messages)} unroutable messages returned by broker'


class InvalidChannelNumber(AMQPError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: An invalid channel number has been specified: {self.args[0]}'


class ProtocolSyntaxError(AMQPError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: An unspecified protocol syntax error occurred'


class UnexpectedFrameError(ProtocolSyntaxError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: Received a frame out of sequence: {self.args[0]!r}'


class ProtocolVersionMismatch(ProtocolSyntaxError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: Protocol versions did not match: {self.args[0]!r} vs {self.args[1]!r}'


class BodyTooLongError(ProtocolSyntaxError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: Received too many bytes for a message delivery: '
            f'Received {self.args[0]}, expected {self.args[1]}')


class InvalidFrameError(ProtocolSyntaxError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: Invalid frame received: {self.args[0]!r}'


class InvalidFieldTypeException(ProtocolSyntaxError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: Unsupported field kind {self.args[0]}'


class UnsupportedAMQPFieldException(ProtocolSyntaxError):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: Unsupported field kind {type(self.args[1])}'


class MethodNotImplemented(AMQPError):
    pass


class ChannelError(Exception):

    @override
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}: An unspecified error occurred with the Channel'


class ReentrancyError(Exception):
    """
    The requested operation would result in unsupported recursion or reentrancy.

    Used by BlockingConnection/BlockingChannel
    """


class ShortStringTooLong(AMQPError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: AMQP Short String can contain up to 255 bytes: '
            f'{self.args[0]!s:.300}')


class DuplicateGetOkCallback(ChannelError):

    @override
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}: basic_get can only be called again after the callback for '
            'the previous basic_get is executed')
