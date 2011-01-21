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

import pika.spec as spec
import pika.codec as codec
import pika.event as event
from pika.exceptions import *

BLOCKED = 0x01

class ChannelHandler(object):

    def __init__(self, connection, channel_number):

        self.connection = connection

        self.channel = None

        # The frame-handler changes depending on the type of frame processed
        self.frame_handler = self._handle_method



        self.channel_close_callback = None
        self.channel_close = False

        self.flow_active = True ## we are permitted to transmit, so True.

        self.channel_state_change_event = event.Event()
        self.flow_active_change_event = event.Event()

        # Make sure that the caller passed in an int for the channel number
        if not isinstance(channel_number, int):
            raise InvalidChannelNumber

        self.channel_number = channel_number

        # Call back to our connection and add ourself, I dont like this
        connection.add_channel(self.channel_number, self)

    def close(self, code=0, text="Normal Shutdown"):
        """
        Will invoke a clean shutdown of the channel with the AMQP Broker
        via the Channel Handlers close callback
        """
        logging.debug("ChannelHandler.close: (%s) %s" % (str(code), text))

        # If we have an open connection send a RPC call to close the channel
        if self.connection.is_open():
            self.rpc(self.connection.on_channel_close,
                     spec.Channel.Close(code, text, 0, 0),
                     [spec.Channel.CloseOk])

        # Otherwise call the same callback with a dummy frame which has our
        # Information
        else:
            close_frame = spec.Channel.CloseOk()
            close_frame.channel_number = self.channel_number
            self.connection.on_channel_close(close_frame)


    def _channel_flow(self, method_frame, header_frame, body):
        self.flow_active = method_frame.method.active
        self.flow_active_change_event.fire(self, self.flow_active)
        self.connection.send_method(self.channel_number,
                                    spec.Channel.FlowOk(active=self.flow_active))

    def _ensure(self):
        if self.channel_close:
            raise ChannelClosed(self.channel_close)
        return self

    def _set_channel_close(self, c):
        if not self.channel_close:
            self.channel_close = c
            self.connection.remove_channel(self.channel_number)
            self.channel_state_change_event.fire(self, False)

    def add_state_change_handler(self, handler, key = None):
        self.channel_state_change_event.addHandler(handler, key)
        handler(self, not self.channel_close)

    def add_flow_change_handler(self, handler, key = None):
        self.flow_active_change_event.addHandler(handler, key)
        handler(self, self.flow_active)

    def flush_and_drain(self):
        # Flush, and handle any traffic that's already arrived, but
        # don't wait for more.
        self.connection.flush_outbound()
        self.connection.drain_events(timeout = 0)






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
        """
        Receive a frame from the AMQP Broker and process it
        """
        logging.debug("%s._handle_method: %r" % (self.__class__.__name__,
                                                 frame))

        # If we don't have FrameMethod something is wrong so throw an exception
        if not isinstance(frame, codec.FrameMethod):
            raise UnexpectedFrameError(frame)

        # If the frame is a content related frame go deal with the content
        if spec.has_content(frame.method.INDEX):
            self.frame_handler = self._make_header_handler(frame)

        # We were passed a frame we don't know how to deal with
        else:
            self.connection.close(spec.NOT_IMPLEMENTED,
                                  'Pika: method not implemented: %s' % \
                                  frame.method.__class__)

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
            seen_so_far[0] += len(fragment)
            body_fragments.append(fragment)
            if seen_so_far[0] == header_frame.body_size:
                finish()
            elif seen_so_far[0] > header_frame.body_size:
                raise BodyTooLongError()
            else:
                pass

        def finish():
            self.frame_handler = self._handle_method

            method_name = method_frame.method.NAME

            if method_name in self.channel._callbacks:
                self.channel._callbacks[method_name](method_frame,
                                                     header_frame,
                                                     body_fragments)

        if not header_frame.body_size:
            finish()
        else:
            self.frame_handler = handler

    def rpc(self, callback, method, acceptable_replies):

        # Send the rpc call to RabbitMQ
        self.connection.rpc(callback, self.channel_number,
                            method, acceptable_replies)

    def content_transmission_forbidden(self):
        return not self.flow_active

    def flush_and_drain(self):
        # Flush, and handle any traffic that's already arrived, but
        # don't wait for more.
        self.connection.flush_outbound()
        # Don't drain (or block in a wait for things to finish)

    def add_callback(self, callback, acceptable_replies):
        """
        Adds a callback entry in our connections callback handler for our
        channel
        """
        self.connection.add_callback(callback, self.channel_number,
                                     acceptable_replies)

channel_attr_whitelist = ('blocking', '_blocked', '_callbacks', 'synchronous',
                          'handle', 'handler', '__class__', '_consumers', 'blocked_function',
                          'on_open_ok_callback', '_on_synchronous_complete', 'add_to_blocked')

class Channel(spec.DriverMixin):

    def __init__(self, handler, on_open_ok_callback):

        handler.channel = self
        self.handler = handler

        self._callbacks = {}
        self._consumers = {}

        self.pending = {}

        self.blocking = None
        self._blocked = []
        self.blocked_function = None

        self._callbacks[spec.Channel.OpenOk.NAME] = on_open_ok_callback
        self._callbacks[spec.Basic.Deliver.NAME] = self._basic_deliver

        handler.add_callback(handler._channel_flow, [spec.Channel.Flow])

        handler.rpc(self.on_event_ok, spec.Channel.Open(),
                         [spec.Channel.OpenOk])


    def __getattribute__(self, function):

        # If we don't check to see if the item we're going to use in this function
        if function  in channel_attr_whitelist:
            return object.__getattribute__(self, function)

        if self.blocking:
            logging.debug('%s is blocking this channel' % self.blocking)
            self.blocked_function = str(function)
            return self.add_to_blocked

        if function in self.synchronous:
            self.blocking = "%s.%s" % (self.__class__.__name__, function)
            logging.debug('%s turning on blocking' % self.blocking)

        return object.__getattribute__(self, function)


    def add_to_blocked(self, *args, **keywords):
        self._blocked.append([self.blocked_function, args, keywords])
        self.blocked_function = None



    def _on_synchronous_complete(self, frame):
        logging.debug("%s._on_synchronous_complete for %s: %r" % \
                      (self.__class__.__name__, self.blocking, frame))

        # Release the lock
        self.blocking = None

        # Get the function to call next
        method = self._blocked.pop(0)

        # Call our original method
        function = method[0]
        args = method[1]
        keywords = method[2]

        print function
        print args
        print keywords

        function = getattr(self, function)

        if args and not len(keywords):
            function(self, *args)
        elif args and len(keywords):
            function(self, *args, **keywords)
        elif len(keywords):
            function(self, **keywords)
        else:
            function(self)

    def add_state_change_handler(self, handler, key = None):
        self.handler.add_state_change_handler(handler, key)

    def add_flow_change_handler(self, handler, key = None):
        self.handler.add_flow_change_handler(handler, key)

    def _basic_deliver(self, method_frame, header_frame, body):
        """
        Cope with reentrancy. If a particular consumer is still active when another
        delivery appears for it, queue the deliveries up until it finally exits.
        """
        consumer_tag = method_frame.method.consumer_tag
        if consumer_tag not in self.pending:
            q = []
            self.pending[consumer_tag] = q
            consumer = self._consumers[consumer_tag]
            consumer(self, method_frame.method, header_frame.properties, body)
            while q:
                (m, p, b) = q.pop(0)
                consumer(self, m, p, b)
            del self.pending[consumer_tag]
        else:
            self.pending[consumer_tag].append((method_frame.method,
                                               header_frame.properties, body))

    def _basic_return(self, method_frame, header_frame, body):
        raise NotImplementedError("Basic.Return")

    def _basic_consume(self, frame):
        self.handler._make_header_handler(frame)

    def _basic_cancel(self, frame):

        logging.info("Cancel Ok: %s" % str(frame.method))


    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False,
                      block_on_flow_control=False):

        if self.handler.content_transmission_forbidden():
            if block_on_flow_control:
                while self.handler.content_transmission_forbidden():
                    self.handler.connection.drain_events()
            else:
                raise ContentTransmissionForbidden(self)
        properties = properties or spec.BasicProperties()
        self.handler.connection.send_method(self.handler.channel_number,
                                            spec.Basic.Publish(exchange=exchange,
                                                               routing_key=routing_key,
                                                               mandatory=mandatory,
                                                               immediate=immediate),
                                            (properties, body))
        self.handler.flush_and_drain()

    def rpc(self, callback, method, acceptable_replies):
        self.handler.rpc(callback, method, acceptable_replies)

    def basic_cancel(self, consumer_tag):
        if not consumer_tag in self.callbacks:
            raise UnknownConsumerTag(consumer_tag)

        self.handler.rpc(self.on_cancel_ok, spec.Basic.Cancel(consumer_tag = consumer_tag),

                        [spec.Basic.CancelOk])

    def basic_consume(self, consumer,
                      queue='', no_ack=False, exclusive=False,
                      consumer_tag=None):
        logging.debug("%s.basic_consume" % self.__class__.__name__)

        # If a consumer tag was not passed, create one
        if not consumer_tag:
            consumer_tag = 'ctag%i' % len(self._consumers)

        # Make sure we've not already registered this consumer tag
        if consumer_tag in self._consumers:
            raise DuplicateConsumerTag(consumer_tag)

        # The consumer tag has not been used before, add it to our consumers
        self._consumers[consumer_tag] = consumer

        print self._consumers

        # Send our Basic.Consume RPC call
        self.handler.rpc(self.on_event_ok,
                         spec.Basic.Consume(queue=queue,
                                            consumer_tag=consumer_tag,
                                            no_ack=no_ack,
                                            exclusive=exclusive),
                         [spec.Basic.ConsumeOk])

    def on_event_ok(self, frame):

        logging.debug("%s.on_event_ok: %r" % (self.__class__.__name__,
                                              frame.method.NAME))

        # If we've defined a callback, make the call passing ourselves
        # and the frame to the registered function
        if frame.method.NAME in self._callbacks:
            self._callbacks[frame.method.NAME](self, frame)
