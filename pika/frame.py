"""Frame objects that do the frame demarshaling and marshaling."""

from __future__ import annotations

import logging
import struct
from typing import Generic, TypeVar

from pika import amqp_object, exceptions, spec
from pika._utils import override

LOGGER = logging.getLogger(__name__)

_MethodT = TypeVar('_MethodT', bound=amqp_object.Method)

_FRAME_END_BYTE = bytes((spec.FRAME_END,))


class Frame(amqp_object.AMQPObject):
    """
    Base Frame object mapping.

    Defines a behavior for all child classes for assignment of core attributes and implementation of
    the a core _marshal method which child classes use to create the binary AMQP frame.
    """

    NAME = 'Frame'

    def __init__(self, frame_type: int, channel_number: int) -> None:
        """
        Create a new instance of a frame.

        :param frame_type: The frame type
        :param channel_number: The channel number for the frame
        """
        self.frame_type = frame_type
        self.channel_number = channel_number

    def _marshal(self, pieces: list[bytes]) -> bytes:
        """
        Create the full AMQP wire protocol frame data representation.

        :param pieces: Encoded AMQP frame fragments to assemble
        """
        payload = b''.join(pieces)
        return struct.pack('>BHI', self.frame_type, self.channel_number,
                           len(payload)) + payload + _FRAME_END_BYTE

    def marshal(self) -> bytes:
        """
        To be ended by child classes.

        :raises NotImplementedError
        """
        raise NotImplementedError


class Method(Frame, Generic[_MethodT]):
    """
    Base Method frame object mapping.

    AMQP method frames are mapped on top of this class for creating or accessing their data and
    attributes.
    """

    NAME = 'METHOD'

    def __init__(self, channel_number: int, method: _MethodT) -> None:
        """
        Create a new instance of a frame.

        :param channel_number: The channel number for the frame
        :param method: The AMQP Class.Method
        """
        Frame.__init__(self, spec.FRAME_METHOD, channel_number)
        self.method = method

    @override
    def marshal(self) -> bytes:
        """Return the AMQP binary encoded value of the frame."""
        pieces = self.method.encode()
        pieces.insert(0, struct.pack('>I', self.method.INDEX))
        return self._marshal(pieces)


class Header(Frame):
    """
    Header frame object mapping.

    AMQP content header frames are mapped on top of this class for creating or accessing their data
    and attributes.
    """

    NAME = 'Header'

    def __init__(self, channel_number: int, body_size: int,
                 props: spec.BasicProperties) -> None:
        """
        Create a new instance of a AMQP ContentHeader object.

        :param channel_number: The channel number for the frame
        :param body_size: The number of bytes for the body
        :param props: Basic.Properties object
        """
        Frame.__init__(self, spec.FRAME_HEADER, channel_number)
        self.body_size = body_size
        self.properties = props

    @override
    def marshal(self) -> bytes:
        """Return the AMQP binary encoded value of the frame."""
        pieces = self.properties.encode()
        pieces.insert(
            0, struct.pack('>HxxQ', self.properties.INDEX, self.body_size))
        return self._marshal(pieces)


class Body(Frame):
    """
    Body frame object mapping class.

    AMQP content body frames are mapped on to this base class for getting/setting of
    attributes/data.
    """

    NAME = 'Body'

    def __init__(self, channel_number: int, fragment: bytes) -> None:
        """
        :param channel_number: The channel number for the frame
        :param fragment: The fragment of the body
        """
        Frame.__init__(self, spec.FRAME_BODY, channel_number)
        self.fragment = fragment

    @override
    def marshal(self) -> bytes:
        """Return the AMQP binary encoded value of the frame."""
        return self._marshal([self.fragment])


class Heartbeat(Frame):
    """
    Heartbeat frame object mapping class.

    AMQP Heartbeat frames are mapped on to this class for a common access structure to the
    attributes/data values.
    """

    NAME = 'Heartbeat'

    def __init__(self) -> None:
        """Create a new instance of the Heartbeat frame."""
        Frame.__init__(self, spec.FRAME_HEARTBEAT, 0)

    @override
    def marshal(self) -> bytes:
        """Return the AMQP binary encoded value of the frame."""
        return self._marshal([])


class ProtocolHeader(amqp_object.AMQPObject):
    """AMQP Protocol header frame class which provides a pythonic interface for creating AMQP
    Protocol headers.
    """

    NAME = 'ProtocolHeader'

    def __init__(self,
                 major: int | None = None,
                 minor: int | None = None,
                 revision: int | None = None) -> None:
        """
        Construct a Protocol Header frame object for the specified AMQP version.

        :param major: Major version number
        :param minor: Minor version number
        :param revision: Revision
        """
        self.frame_type = -1
        self.major = major or spec.PROTOCOL_VERSION[0]
        self.minor = minor or spec.PROTOCOL_VERSION[1]
        self.revision = revision or spec.PROTOCOL_VERSION[2]

    def marshal(self) -> bytes:
        """Return the full AMQP wire protocol frame data representation of the ProtocolHeader
        frame.
        """
        return b'AMQP' + struct.pack('BBBB', 0, self.major, self.minor,
                                     self.revision)


def decode_frame(data_in: bytes | bytearray,
                 offset: int = 0) -> tuple[int, Frame | ProtocolHeader | None]:
    """
    Receives raw socket data and attempts to turn it into a frame.

    Returns the number of bytes consumed from the stream starting at ``offset`` and the frame.

    :param data_in: The raw data stream
    :param offset: Offset into ``data_in`` at which the frame starts
    :raises: pika.exceptions.InvalidFrameError
    """
    # Look to see if it's a protocol header frame
    try:
        if data_in[offset:offset + 4] == b'AMQP':
            major, minor, revision = struct.unpack_from('BBB', data_in,
                                                        offset + 5)
            return 8, ProtocolHeader(major, minor, revision)
    except (IndexError, struct.error):
        return 0, None

    # Get the Frame Type, Channel Number and Frame Size
    try:
        (frame_type, channel_number,
         frame_size) = struct.unpack_from('>BHL', data_in, offset)
    except struct.error:
        return 0, None

    # Get the frame data
    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    # We don't have all of the frame yet
    if offset + frame_end > len(data_in):
        return 0, None

    # The Frame termination chr is wrong
    if data_in[offset + frame_end - 1] != spec.FRAME_END:
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    # Get the raw frame data as immutable bytes. For large payloads copy via
    # memoryview, which avoids a bytearray input's extra intermediate copy;
    # for small frames a plain slice is cheaper than memoryview setup.
    data_start = offset + spec.FRAME_HEADER_SIZE
    data_end = offset + frame_end - 1
    if frame_size > 1024:
        frame_data = bytes(memoryview(data_in)[data_start:data_end])
    else:
        frame_data = bytes(data_in[data_start:data_end])

    if frame_type == spec.FRAME_METHOD:

        # Get the Method ID from the frame data
        method_id = struct.unpack_from('>I', frame_data)[0]

        # Get a Method object for this method_id
        method = spec.methods[method_id]()

        # Decode the content
        method.decode(frame_data, 4)

        # Return the amount of data consumed and the Method object
        return frame_end, Method(channel_number, method)

    if frame_type == spec.FRAME_HEADER:

        # Return the header class and body size
        class_id, _weight, body_size = struct.unpack_from('>HHQ', frame_data)

        # Get the Properties type
        properties = spec.props[class_id]()

        # Decode the properties
        properties.decode(frame_data[12:])

        # Return a Header frame
        return frame_end, Header(channel_number, body_size, properties)

    if frame_type == spec.FRAME_BODY:

        # Return the amount of data consumed and the Body frame w/ data
        return frame_end, Body(channel_number, frame_data)

    if frame_type == spec.FRAME_HEARTBEAT:

        # Return the amount of data and a Heartbeat frame
        return frame_end, Heartbeat()

    raise exceptions.InvalidFrameError(f"Unknown frame type: {frame_type}")
