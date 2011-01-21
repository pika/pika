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

import logging

import pika.codec as codec
import pika.spec as spec

from pika.exceptions import *


class FrameHandler(object):

    def __init__(self, channel):

        self._channel = channel
        self._handler = _handle_method_frame

    def process(self, frame):

        self._handler(frame)

    def _handle_method_frame(self, frame):
        """
        Receive a frame and process it, we should have content by the time we
        reach this handler, set the next handler to be the header frame handler
        """
        logging.debug("%s._handle_method: %r" % (self.__class__.__name__,
                                                 frame))

        # If we don't have FrameMethod something is wrong so throw an exception
        if not isinstance(frame, codec.FrameMethod):
            raise UnexpectedFrameError(frame)

        # If the frame is a content related frame go deal with the content
        # By getting the content header frame
        if spec.has_content(frame.method.INDEX):
            self._handler = self._make_header_handler(frame)

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
            if not isinstance(header_frame, codec.FrameHeader):
                raise UnexpectedFrameError(header_frame)

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
        body_fragments = []

        def handler(body_frame):

            # Make sure it's a body frame
            if not isinstance(body_frame, codec.FrameBody):
                raise UnexpectedFrameError(body_frame)

            fragment = body_frame.fragment
            seen_so_far[0] += len(fragment)
            body_fragments.append(fragment)
            if seen_so_far[0] == header_frame.body_size:
                finish()
            elif seen_so_far[0] > header_frame.body_size:
                raise BodyTooLongError()
            else:
                pass

        def finish():
            # We're done so set our handler back to the method frame
            self._handler = self._handle_method_frame

            # Get our method name
            method_name = method_frame.method.NAME

            # Check for a processing callback for our method name
            if method_name in self._channel._callbacks:
                self._channel.transport._callbacks[method_name](method_frame,
                                                                header_frame,
                                                                body_fragments)

        # if we dont have a header frame body size, finish otherwise keep going
        # And keep our handler function as the frame handler
        if not header_frame.body_size:
            finish()
        else:
            self._handler = handler
