# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
#
# ***** END LICENSE BLOCK *****

import struct
import pika.spec as spec

from pika.exceptions import *

class Frame:
    def __init__(self, frame_type, channel_number):
        self.frame_type = frame_type
        self.channel_number = channel_number

    def _marshal(self, pieces):
        payload = ''.join(pieces)
        return struct.pack('>BHI', self.frame_type, self.channel_number, len(payload)) + \
               payload + chr(spec.FRAME_END)

    def __repr__(self):
        import pika.specbase
        return pika.specbase._codec_repr(self, lambda: Frame(-1, -1))

class FrameMethod(Frame):
    def __init__(self, channel_number, method):
        Frame.__init__(self, spec.FRAME_METHOD, channel_number)
        self.method = method

    def marshal(self):
        pieces = self.method.encode()
        pieces.insert(0, struct.pack('>I', self.method.INDEX))
        return self._marshal(pieces)

class FrameHeader(Frame):
    def __init__(self, channel_number, body_size, props):
        Frame.__init__(self, spec.FRAME_HEADER, channel_number)
        self.body_size = body_size
        self.properties = props

    def marshal(self):
        pieces = self.properties.encode()
        pieces.insert(0, struct.pack('>HxxQ', self.properties.INDEX, self.body_size))
        return self._marshal(pieces)

class FrameBody(Frame):
    def __init__(self, channel_number, fragment):
        Frame.__init__(self, spec.FRAME_BODY, channel_number)
        self.fragment = fragment

    def marshal(self):
        return self._marshal([self.fragment])

class FrameHeartbeat(Frame):
    def __init__(self):
        Frame.__init__(self, spec.FRAME_HEARTBEAT, 0)

    def marshal(self):
        return self._marshal([])

class FrameProtocolHeader(Frame):
    def __init__(self, th, tl, vh, vl):
        Frame.__init__(self, -1, -1)
        self.transport_high = th
        self.transport_low = tl
        self.protocol_version_major = vh
        self.protocol_version_minor = vl

    def marshal(self):
        return 'AMQP' + struct.pack('BBBB',
                                    self.transport_high,
                                    self.transport_low,
                                    self.protocol_version_major,
                                    self.protocol_version_minor)

class ConnectionState:
    HEADER_SIZE = 7
    FOOTER_SIZE = 1

    def __init__(self):
        self.channel_max = None
        self.frame_max = None
        self._return_to_idle()

    def tune(self, channel_max, frame_max):
        self.channel_max = channel_max
        self.frame_max = frame_max

    def _return_to_idle(self):
        self.inbound_buffer = []
        self.inbound_available = 0
        self.target_size = ConnectionState.HEADER_SIZE
        self.state = self._waiting_for_header

    def _inbound(self):
        return ''.join(self.inbound_buffer)

    def handle_input(self, received_data):
        total_bytes_consumed = 0

        while True:
            if not received_data:
                return (total_bytes_consumed, None)

            bytes_consumed = self.target_size - self.inbound_available
            if len(received_data) < bytes_consumed:
                bytes_consumed = len(received_data)

            self.inbound_buffer.append(received_data[:bytes_consumed])
            self.inbound_available = self.inbound_available + bytes_consumed
            received_data = received_data[bytes_consumed:]
            total_bytes_consumed = total_bytes_consumed + bytes_consumed

            if self.inbound_available < self.target_size:
                return (total_bytes_consumed, None)

            maybe_result = self.state(self._inbound())
            if maybe_result:
                return (total_bytes_consumed, maybe_result)

    def _waiting_for_header(self, inbound):
        # Here we switch state without resetting the inbound_buffer,
        # because we want to keep the frame header.
        
        if inbound[:3] == 'AMQ':
            # Protocol header.
            self.target_size = 8
            self.state = self._waiting_for_protocol_header
        else:
            self.target_size = struct.unpack_from('>I', inbound, 3)[0] + \
                               ConnectionState.HEADER_SIZE + \
                               ConnectionState.FOOTER_SIZE
            self.state = self._waiting_for_body

    def _waiting_for_body(self, inbound):
        if ord(inbound[-1]) != spec.FRAME_END:
            raise InvalidFrameError("Invalid frame end byte", inbound[-1])

        self._return_to_idle()

        (frame_type, channel_number) = struct.unpack_from('>BH', inbound, 0)
        if frame_type == spec.FRAME_METHOD:
            method_id = struct.unpack_from('>I', inbound, ConnectionState.HEADER_SIZE)[0]
            method = spec.methods[method_id]()
            method.decode(inbound, ConnectionState.HEADER_SIZE + 4)
            return FrameMethod(channel_number, method)
        elif frame_type == spec.FRAME_HEADER:
            (class_id, body_size) = struct.unpack_from('>HxxQ', inbound,
                                                       ConnectionState.HEADER_SIZE)
            props = spec.props[class_id]()
            props.decode(inbound, ConnectionState.HEADER_SIZE + 12)
            return FrameHeader(channel_number, body_size, props)
        elif frame_type == spec.FRAME_BODY:
            return FrameBody(channel_number,
                             inbound[ConnectionState.HEADER_SIZE : -ConnectionState.FOOTER_SIZE])
        elif frame_type == spec.FRAME_HEARTBEAT:
            return FrameHeartbeat()
        else:
            # Ignore the frame.
            return None

    def _waiting_for_protocol_header(self, inbound):
        if inbound[3] != 'P':
            raise InvalidProtocolHeader(inbound)

        self._return_to_idle()

        (th, tl, vh, vl) = struct.unpack_from('BBBB', inbound, 4)
        return FrameProtocolHeader(th, tl, vh, vl)
