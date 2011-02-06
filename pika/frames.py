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
from pika.object import object_


class Frame(object_):

    def __init__(self, frame_type, channel_number):
        self.frame_type = frame_type
        self.channel_number = channel_number

    def _marshal(self, pieces):
        payload = ''.join(pieces)
        return struct.pack('>BHI',
                           self.frame_type,
                           self.channel_number,
                           len(payload)) + payload + chr(spec.FRAME_END)


class Method(Frame):

    def __init__(self, channel_number, method):
        Frame.__init__(self, spec.FRAME_METHOD, channel_number)
        self.method = method

    def marshal(self):
        pieces = self.method.encode()
        pieces.insert(0, struct.pack('>I', self.method.INDEX))
        return self._marshal(pieces)


class Header(Frame):

    def __init__(self, channel_number, body_size, props):
        Frame.__init__(self, spec.FRAME_HEADER, channel_number)
        self.body_size = body_size
        self.properties = props

    def marshal(self):
        pieces = self.properties.encode()
        pieces.insert(0, struct.pack('>HxxQ', self.properties.INDEX,
                                     self.body_size))
        return self._marshal(pieces)


class Body(Frame):

    def __init__(self, channel_number, fragment):
        Frame.__init__(self, spec.FRAME_BODY, channel_number)
        self.fragment = fragment

    def marshal(self):
        return self._marshal([self.fragment])


class Heartbeat(Frame):

    def __init__(self):
        Frame.__init__(self, spec.FRAME_HEARTBEAT, 0)

    def marshal(self):
        return self._marshal(list())


class ProtocolHeader(Frame):

    def __init__(self, major=None, minor=None, revision=None):
        Frame.__init__(self, -1, -1)
        self.major = major or spec.PROTOCOL_VERSION[0]
        self.minor = minor or spec.PROTOCOL_VERSION[1]
        self.revision = revision or spec.PROTOCOL_VERSION[2]

    def marshal(self):
        return 'AMQP' + struct.pack('BBBB', 0, self.major,
                                    self.minor, self.revision)
