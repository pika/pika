# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import struct
import pika.log as log
import pika.spec as spec
import pika.exceptions as exceptions

from pika.object import object_


class Frame(object_):
    """
    Base Frame object mapping. Defines a behavior for all child classes for
    assignment of core attributes and implementation of the a core _marshal
    method which child classes use to create the binary AMQP frame.
    """

    def __init__(self, frame_type, channel_number):
        """
        Parameters:

        - frame_type: int
        - channel_number: int
        """
        self.frame_type = frame_type
        self.channel_number = channel_number

    def _marshal(self, pieces):
        """
        Create the full AMQP wire protocol frame data representation
        """
        payload = ''.join(pieces)
        return struct.pack('>BHI',
                           self.frame_type,
                           self.channel_number,
                           len(payload)) + payload + chr(spec.FRAME_END)


class Method(Frame):
    """
    Base Method frame object mapping. AMQP method frames are mappend on top
    of this class for creating or accessing their data and attributes.
    """

    def __init__(self, channel_number, method):
        """
        Parameters:

        - channel_number: int
        - method: a spec.Class.Method object
        """
        Frame.__init__(self, spec.FRAME_METHOD, channel_number)
        self.method = method

    def marshal(self):
        """
        Return the AMQP binary encoded value of the frame
        """
        pieces = self.method.encode()
        pieces.insert(0, struct.pack('>I', self.method.INDEX))
        return self._marshal(pieces)


class Header(Frame):
    """
    Header frame object mapping. AMQP content header frames are mapped
    on top of this class for creating or accessing their data and attributes.
    """

    def __init__(self, channel_number, body_size, props):
        """
        Parameters:

        - channel_number: int
        - body_size: int
        - props: spec.BasicProperties object
        """
        Frame.__init__(self, spec.FRAME_HEADER, channel_number)
        self.body_size = body_size
        self.properties = props

    def marshal(self):
        """
        Return the AMQP binary encoded value of the frame
        """
        pieces = self.properties.encode()
        pieces.insert(0, struct.pack('>HxxQ',
                                     self.properties.INDEX,
                                     self.body_size))
        return self._marshal(pieces)


class Body(Frame):
    """
    Body frame object mapping class. AMQP content body frames are mapped on
    to this base class for getting/setting of attributes/data.
    """

    def __init__(self, channel_number, fragment):
        """
        Parameters:

        - channel_number: int
        - fragment: unicode or str
        """
        Frame.__init__(self, spec.FRAME_BODY, channel_number)
        self.fragment = fragment

    def marshal(self):
        """
        Return the AMQP binary encoded value of the frame
        """
        return self._marshal([self.fragment])


class Heartbeat(Frame):
    """
    Heartbeat frame object mapping class. AMQP Heartbeat frames are mapped on
    to this class for a common access structure to the attributes/data values.
    """

    def __init__(self):
        Frame.__init__(self, spec.FRAME_HEARTBEAT, 0)

    def marshal(self):
        """
        Return the AMQP binary encoded value of the frame
        """
        return self._marshal(list())


class ProtocolHeader(object_):
    """
    AMQP Protocol header frame class which provides a pythonic interface
    for creating AMQP Protocol headers
    """

    def __init__(self, major=None, minor=None, revision=None):
        """
        Construct a Protocol Header frame object for the specified AMQP version

        Parameters:

        - major: int
        - miinor: int
        - revision: int
        """
        self.frame_type = -1
        self.major = major or spec.PROTOCOL_VERSION[0]
        self.minor = minor or spec.PROTOCOL_VERSION[1]
        self.revision = revision or spec.PROTOCOL_VERSION[2]

    def marshal(self):
        """
        Return the full AMQP wire protocol frame data representation of the
        ProtocolHeader frame
        """
        return 'AMQP' + struct.pack('BBBB', 0,
                                    self.major,
                                    self.minor,
                                    self.revision)


class Dispatcher(object):
    """
    This handles content frames which come in in synchronous order as follows:

    1) Method Frame
    2) Header Frame
    3) Body Frame(s)

    The way that content handler works is to assign the active frame type to
    the self._handler variable. When we receive a header frame that is either
    a Basic.Deliver, Basic.GetOk, or Basic.Return, we will assign the handler
    to the ContentHandler._handle_header_frame. This will fire the next time
    we are called and parse out the attributes required to receive the body
    frames and assemble the content to be returned. We will then assign the
    self._handler to ContentHandler._handle_body_frame.

    _handle_body_frame has two methods inside of it, handle and finish.
    handle will be invoked until the requirements set by the header frame have
    been met at which point it will call the finish method. This calls
    the callback manager with the method frame, header frame and assembled
    body and then reset the self._handler to the _handle_method_frame method.
    """

    def __init__(self, callback_manager):
        # We start with Method frames always
        self._handler = self._handle_method_frame
        self.callbacks = callback_manager

    def process(self, frame):
        """
        Invoked by the ChannelTransport object when passed frames that are not
        setup in the rpc process and that don't have explicit reply types
        defined. This includes Basic.Publish, Basic.GetOk and Basic.Return
        """
        self._handler(frame)

    def _handle_method_frame(self, frame):
        """
        Receive a frame and process it, we should have content by the time we
        reach this handler, set the next handler to be the header frame handler
        """
        # If we don't have FrameMethod something is wrong so throw an exception
        if not isinstance(frame, Method):
            raise exceptions.UnexpectedFrameError(frame)

        # If the frame is a content related frame go deal with the content
        # By getting the content header frame
        if spec.has_content(frame.method.INDEX):
            self._handler = self._handle_header_frame(frame)

        # We were passed a frame we don't know how to deal with
        else:
            raise NotImplementedError(frame.method.__class__)

    def _handle_header_frame(self, frame):
        """
        Receive a header frame and process that, setting the next handler
        to the body frame handler
        """

        def handler(header_frame):
            # Make sure it's a header frame
            if not isinstance(header_frame, Header):
                raise exceptions.UnexpectedFrameError(header_frame)

            # Call the handle body frame including our header frame
            self._handle_body_frame(frame, header_frame)

        return handler

    def _handle_body_frame(self, method_frame, header_frame):
        """
        Receive body frames. We'll keep receiving them in handler until we've
        received the body size specified in the header frame. When done
        call our finish function which will call our transports callbacks
        """
        seen_so_far = [0]
        body_fragments = list()

        def handler(body_frame):
            # Make sure it's a body frame
            if not isinstance(body_frame, Body):
                raise exceptions.UnexpectedFrameError(body_frame)

            # Increment our counter so we know when we've had enough
            seen_so_far[0] += len(body_frame.fragment)

            # Append the fragment to our list
            body_fragments.append(body_frame.fragment)

            # Did we get enough bytes? If so finish
            if seen_so_far[0] == header_frame.body_size:
                finish()

            # Did we get too many bytes?
            elif seen_so_far[0] > header_frame.body_size:
                error = 'Received %i and only expected %i' % \
                        (seen_so_far[0], header_frame.body_size)
                raise exceptions.BodyTooLongError(error)

        def finish():
            # We're done so set our handler back to the method frame
            self._handler = self._handle_method_frame

            # Get our method name
            method = method_frame.method.__class__.__name__
            if method == 'Deliver':
                key = '_on_basic_deliver'
            elif method == 'GetOk':
                key = '_on_basic_get'
            elif method == 'Return':
                key = '_on_basic_return'
            else:
                raise Exception('Unimplemented Content Return Key')

            # Check for a processing callback for our method name
            self.callbacks.process(method_frame.channel_number,  # Prefix
                                   key,                          # Key
                                   self,                         # Caller
                                   method_frame,                 # Arg 1
                                   header_frame,                 # Arg 2
                                   ''.join(body_fragments))      # Arg 3

        # If we don't have a header frame body size, finish. Otherwise keep
        # going and keep our handler function as the frame handler
        if not header_frame.body_size:
            finish()
        else:
            self._handler = handler


def decode_frame(data_in):
    """
    Receives raw socket data and attempts to turn it into a frame.
    Returns bytes used to make the frame and the frame
    """
    # Look to see if it's a protocol header frame
    try:
        if data_in[0:4] == 'AMQP':
            major, minor, revision = struct.unpack_from('BBB', data_in, 5)
            return 8, ProtocolHeader(major, minor, revision)
    except IndexError:
        # We didn't get a full frame
        return 0, None
    except struct.error:
        # We didn't get a full frame
        return 0, None

    # Get the Frame Type, Channel Number and Frame Size
    try:
        frame_type, channel_number, frame_size = \
            struct.unpack('>BHL', data_in[0:7])
    except struct.error:
        # We didn't get a full frame
        return 0, None

    # Get the frame data
    frame_end = spec.FRAME_HEADER_SIZE +\
                frame_size +\
                spec.FRAME_END_SIZE

    # We don't have all of the frame yet
    if frame_end > len(data_in):
        return 0, None

    # The Frame termination chr is wrong
    if data_in[frame_end - 1] != chr(spec.FRAME_END):
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    # Get the raw frame data
    frame_data = data_in[spec.FRAME_HEADER_SIZE:frame_end - 1]

    if frame_type == spec.FRAME_METHOD:

        # Get the Method ID from the frame data
        method_id = struct.unpack_from('>I', frame_data)[0]

        # Get a Method object for this method_id
        method = spec.methods[method_id]()

        # Decode the content
        method.decode(frame_data, 4)

        # Return the amount of data consumed and the Method object
        return frame_end, Method(channel_number, method)

    elif frame_type == spec.FRAME_HEADER:

        # Return the header class and body size
        class_id, weight, body_size = struct.unpack_from('>HHQ', frame_data)

        # Get the Properties type
        properties = spec.props[class_id]()

        log.debug("<%r>", properties)

        # Decode the properties
        out = properties.decode(frame_data[12:])

        log.debug("<%r>", out)

        # Return a Header frame
        return frame_end, Header(channel_number, body_size, properties)

    elif frame_type == spec.FRAME_BODY:

        # Return the amount of data consumed and the Body frame w/ data
        return frame_end, Body(channel_number, frame_data)

    elif frame_type == spec.FRAME_HEARTBEAT:

        # Return the amount of data and a Heartbeat frame
        return frame_end, Heartbeat()

    raise exceptions.InvalidFrameError("Unknown frame type: %i" % frame_type)
