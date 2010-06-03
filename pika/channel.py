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

import pika.spec as spec
import pika.codec as codec
import pika.event as event
from pika.exceptions import *

class ChannelHandler:
    def __init__(self, connection, channel_number = None):
        self.connection = connection
        self.frame_handler = self._handle_method
        self.channel_close = None
        self.async_map = {}
        self.reply_map = None
        self.flow_active = True ## we are permitted to transmit, so True.

        self.channel_state_change_event = event.Event()
        self.flow_active_change_event = event.Event()

        if channel_number is None:
            self.channel_number = connection._next_channel_number()
        else:
            self.channel_number = channel_number
        connection._set_channel(self.channel_number, self)

    def _async_channel_close(self, method_frame, header_frame, body):
        self._set_channel_close(method_frame.method)
        self.connection.send_method(self.channel_number, spec.Channel.CloseOk())

    def _async_channel_flow(self, method_frame, header_frame, body):
        self.flow_active = method_frame.method.active
        self.flow_active_change_event.fire(self, self.flow_active)
        self.connection.send_method(self.channel_number,
                                    spec.Channel.FlowOk(active = self.flow_active))

    def _ensure(self):
        if self.channel_close:
            raise ChannelClosed(self.channel_close)
        return self

    def _set_channel_close(self, c):
        if not self.channel_close:
            self.channel_close = c
            self.connection.reset_channel(self.channel_number)
            self.channel_state_change_event.fire(self, False)

    def addStateChangeHandler(self, handler, key = None):
        self.channel_state_change_event.addHandler(handler, key)
        handler(self, not self.channel_close)

    def addFlowChangeHandler(self, handler, key = None):
        self.flow_active_change_event.addHandler(handler, key)
        handler(self, self.flow_active)

    def wait_for_reply(self, acceptable_replies):
        if not acceptable_replies:
            # One-way.
            return

        if self.reply_map is not None:
            raise RecursiveOperationDetected([p.NAME for p in acceptable_replies])

        reply = [None]
        def set_reply(r):
            reply[0] = r

        self.reply_map = {}
        for possibility in acceptable_replies:
            self.reply_map[possibility] = set_reply

        while True:
            self._ensure()
            self.connection.drain_events()
            if reply[0]: return reply[0]

    def _handle_async(self, method_frame, header_frame, body):
        method = method_frame.method
        methodClass = method.__class__

        if self.reply_map is not None and methodClass in self.reply_map:
            if header_frame is not None:
                method._set_content(header_frame.properties, body)
            handler = self.reply_map[methodClass]
            self.reply_map = None
            handler(method)
        elif methodClass in self.async_map:
            self.async_map[methodClass](method_frame, header_frame, body)
        else:
            self.connection.close(spec.NOT_IMPLEMENTED,
                                  'Pika: method not implemented: ' + methodClass.NAME)

    def _handle_method(self, frame):
        if not isinstance(frame, codec.FrameMethod):
            raise UnexpectedFrameError(frame)
        if spec.has_content(frame.method.INDEX):
            self.frame_handler = self._make_header_handler(frame)
        else:
            self._handle_async(frame, None, None)

    def _make_header_handler(self, method_frame):
        def handler(header_frame):
            if not isinstance(header_frame, codec.FrameHeader):
                raise UnexpectedFrameError(header_frame)
            self._install_body_handler(method_frame, header_frame)
        return handler

    def _install_body_handler(self, method_frame, header_frame):
        seen_so_far = [0]
        body_fragments = []

        def handler(body_frame):
            if not isinstance(body_frame, codec.FrameBody):
                raise UnexpectedFrameError(body_frame)
            fragment = body_frame.fragment
            seen_so_far[0] = seen_so_far[0] + len(fragment)
            body_fragments.append(fragment)
            if seen_so_far[0] == header_frame.body_size:
                finish()
            elif seen_so_far[0] > header_frame.body_size:
                raise BodyTooLongError()
            else:
                pass
        def finish():
            self.frame_handler = self._handle_method
            self._handle_async(method_frame, header_frame, ''.join(body_fragments))

        if header_frame.body_size == 0:
            finish()
        else:
            self.frame_handler = handler

    def _rpc(self, method, acceptable_replies):
        self._ensure()
        return self.connection._rpc(self.channel_number, method, acceptable_replies)

    def content_transmission_forbidden(self):
        return not self.flow_active

class Channel(spec.DriverMixin):
    def __init__(self, handler):
        self.handler = handler
        self.callbacks = {}
        self.pending = {}
        self.next_consumer_tag = 0

        handler.async_map[spec.Channel.Close] = handler._async_channel_close
        handler.async_map[spec.Channel.Flow] = handler._async_channel_flow

        handler.async_map[spec.Basic.Deliver] = self._async_basic_deliver
        handler.async_map[spec.Basic.Return] = self._async_basic_return

        self.handler._rpc(spec.Channel.Open(), [spec.Channel.OpenOk])

    def addStateChangeHandler(self, handler, key = None):
        self.handler.addStateChangeHandler(handler, key)

    def addFlowChangeHandler(self, handler, key = None):
        self.handler.addFlowChangeHandler(handler, key)

    def _async_basic_deliver(self, method_frame, header_frame, body):
        """Cope with reentrancy. If a particular consumer is still active when another
        delivery appears for it, queue the deliveries up until it finally exits."""
        consumer_tag = method_frame.method.consumer_tag
        if consumer_tag not in self.pending:
            q = []
            self.pending[consumer_tag] = q
            consumer = self.callbacks[consumer_tag]
            consumer(self, method_frame.method, header_frame.properties, body)
            while q:
                (m, p, b) = q.pop(0)
                consumer(self, m, p, b)
            del self.pending[consumer_tag]
        else:
            self.pending[consumer_tag].append((method_frame.method, header_frame.properties, body))

    def _async_basic_return(self, method_frame, header_frame, body):
        raise NotImplementedError("Basic.Return")

    def close(self, code = 0, text = 'Normal close'):
        c = spec.Channel.Close(reply_code = code,
                               reply_text = text,
                               class_id = 0,
                               method_id = 0)
        self.handler._rpc(c, [spec.Channel.CloseOk])
        self.handler._set_channel_close(c)

    def basic_publish(self, exchange, routing_key, body, properties = None, mandatory = False, immediate = False, block_on_flow_control = False):
        if self.handler.content_transmission_forbidden():
            if block_on_flow_control:
                while self.handler.content_transmission_forbidden():
                    self.handler.connection.drain_events()
            else:
                raise ContentTransmissionForbidden(self)
        properties = properties or spec.BasicProperties()
        self.handler.connection.send_method(self.handler.channel_number,
                                            spec.Basic.Publish(exchange = exchange,
                                                               routing_key = routing_key,
                                                               mandatory = mandatory,
                                                               immediate = immediate),
                                            (properties, body))

    def basic_consume(self, consumer, queue = '', no_ack = False, exclusive = False, consumer_tag = None):
        tag = consumer_tag
        if not tag:
            tag = 'ctag' + str(self.next_consumer_tag)
            self.next_consumer_tag += 1

        if tag in self.callbacks:
            raise DuplicateConsumerTag(tag)

        self.callbacks[tag] = consumer
        return self.handler._rpc(spec.Basic.Consume(queue = queue,
                                                    consumer_tag = tag,
                                                    no_ack = no_ack,
                                                    exclusive = exclusive),
                                 [spec.Basic.ConsumeOk]).consumer_tag

    def basic_cancel(self, consumer_tag):
        if not consumer_tag in self.callbacks:
            raise UnknownConsumerTag(consumer_tag)

        self.handler._rpc(spec.Basic.Cancel(consumer_tag = consumer_tag),
                          [spec.Basic.CancelOk])
        del self.callbacks[consumer_tag]
