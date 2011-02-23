# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import struct
import pika.log as log
import pika.spec as spec
from pika.object import object_


class Frame(object_):

    @log.method_call
    def __init__(self, frame_type, channel_number):
        self.frame_type = frame_type
        self.channel_number = channel_number

    @log.method_call
    def _marshal(self, pieces):
        payload = ''.join(pieces)
        return struct.pack('>BHI',
                           self.frame_type,
                           self.channel_number,
                           len(payload)) + payload + chr(spec.FRAME_END)


class Method(Frame):

    @log.method_call
    def __init__(self, channel_number, method):
        Frame.__init__(self, spec.FRAME_METHOD, channel_number)
        self.method = method

    @log.method_call
    def marshal(self):
        pieces = self.method.encode()
        pieces.insert(0, struct.pack('>I', self.method.INDEX))
        return self._marshal(pieces)


class Header(Frame):

    @log.method_call
    def __init__(self, channel_number, body_size, props):
        Frame.__init__(self, spec.FRAME_HEADER, channel_number)
        self.body_size = body_size
        self.properties = props

    @log.method_call
    def marshal(self):
        pieces = self.properties.encode()
        pieces.insert(0, struct.pack('>HxxQ', self.properties.INDEX,
                                     self.body_size))
        return self._marshal(pieces)


class Body(Frame):

    @log.method_call
    def __init__(self, channel_number, fragment):
        Frame.__init__(self, spec.FRAME_BODY, channel_number)
        self.fragment = fragment

    @log.method_call
    def marshal(self):
        return self._marshal([self.fragment])


class Heartbeat(Frame):

    @log.method_call
    def __init__(self):
        Frame.__init__(self, spec.FRAME_HEARTBEAT, 0)

    @log.method_call
    def marshal(self):
        return self._marshal(list())


class ProtocolHeader(Frame):

    @log.method_call
    def __init__(self, major=None, minor=None, revision=None):
        Frame.__init__(self, -1, -1)
        self.major = major or spec.PROTOCOL_VERSION[0]
        self.minor = minor or spec.PROTOCOL_VERSION[1]
        self.revision = revision or spec.PROTOCOL_VERSION[2]

    @log.method_call
    def marshal(self):
        return 'AMQP' + struct.pack('BBBB', 0, self.major,
                                    self.minor, self.revision)
