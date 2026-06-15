from enum import IntEnum


class DeliveryMode(IntEnum):
    """
    Enum for specifying the message delivery mode.

    Inherits from :class:`enum.IntEnum` so members are usable wherever an
    integer is expected, including the wire encoder for
    :class:`pika.spec.BasicProperties.delivery_mode`.

    Attributes:
        Transient: The message is not written to disk and may be lost if the
            broker crashes.
        Persistent: The message is written to disk and will survive broker
            crashes.
    """

    Transient = 1
    Persistent = 2
