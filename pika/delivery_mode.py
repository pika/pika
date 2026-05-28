from enum import Enum


class DeliveryMode(Enum):
    """Enum for specifying the message delivery mode.

    Attributes:
        Transient: The message is not written to disk and may be lost if the
            broker crashes.
        Persistent: The message is written to disk and will survive broker
            crashes.
    """

    Transient = 1
    Persistent = 2
