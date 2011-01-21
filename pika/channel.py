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
NOT_IMPLEMENTED = [spec.Basic.Return.NAME]

class ChannelHandler(object):

    def __init__(self, connection, channel_number):

        self.connection = connection

        self.channel = None
        self.closed = False

        # The frame-handler changes depending on the type of frame processed
        self.frame_handler = self._handle_method


        self.channel_close = False

        self.flow_active = True

        self.channel_state_change_event = event.Event()
        self.flow_active_change_event = event.Event()

        # Make sure that the caller passed in an int for the channel number
        if not isinstance(channel_number, int):
            raise InvalidChannelNumber

        self.channel_number = channel_number

        # Call back to our connection and add ourself, I dont like this
        connection.add_channel(self.channel_number, self)

    def add_callback(self, callback, acceptable_replies):
        """
        Adds a callback entry in our connections callback handler for our
        channel
        """
        self.connection.add_callback(callback, self.channel_number,
                                     acceptable_replies)

    def add_flow_change_handler(self, handler, key = None):
        self.flow_active_change_event.addHandler(handler, key)
        handler(self, self.flow_active)

    def add_state_change_handler(self, handler, key = None):
        self.channel_state_change_event.addHandler(handler, key)
        handler(self, not self.channel_close)



    def _channel_flow(self, method_frame, header_frame, body):
        self.flow_active = method_frame.method.active
        self.flow_active_change_event.fire(self, self.flow_active)
        self.connection.send_method(self.channel_number,
                                    spec.Channel.FlowOk(active=self.flow_active))


    def _set_channel_close(self, c):
        if not self.closed:
            self.channel_close = c
            self.connection.remove_channel(self.channel_number)
            self.channel_state_change_event.fire(self, False)



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
    def _make_header_handler(self, method_frame):

        def handler(header_frame):

            if not isinstance(header_frame, codec.FrameHeader):
                raise UnexpectedFrameError(header_frame)

            self._install_body_handler(method_frame, header_frame)

        return handler
    def rpc(self, callback, method, acceptable_replies):
        """
        Shortcut wrapper to the Connection's rpc command using its callback
        stack, passing in our channel number
        """
        self.connection.rpc(callback, self.channel_number,
                            method, acceptable_replies)

    def send_method(self, method, content=None):
        """
        Shortcut wrapper to send a method through our connection, passing in
        our channel number
        """
        self.connection.send_method(self.channel_number, method, content)


    def blocked_on_flow_control(self):
        """
        Returns a bool if we're currently blocking in Flow
        """
        return not self.flow_active



# Due to our use of __getattribute__ to block calls while synchronous calls are
# taking place, we use a whitelist to prevent the __getattribute__ function
# from becoming a recursive mess. Whitelisting variables and functions that
# allow us to quickly return from the function prior to evaluating them.

channel_attr_whitelist = ('__class__',
                          '_add_to_blocked'
                          '_blocked',
                          '_callbacks',
                          '_consumers',
                          '_on_basic_deliver',
                          '_on_cancel_ok',
                          '_on_event_ok',
                          '_on_synchronous_complete',
                          '_pending',
                          'add_state_change_handler',
                          'add_flow_change_handler',
                          'blocked_function',
                          'blocking',
                          'synchronous',
                          'handle',
                          'handler')

class Channel(spec.DriverMixin):

    def __init__(self, handler, on_open_ok):

        # Let our handler have our handle
        handler.channel = self

        # Get the handle to our handler
        self.handler = handler

        # For event based processing
        self._callbacks = {}
        self._consumers = {}
        self._pending = {}

        # We need to block on synchronous commands, but do so asynchronously
        self.blocking = None
        self._blocked = []
        self.blocked_function = None

        # Register our function callbacks for specific events
        self._callbacks[spec.Channel.OpenOk.NAME] = on_open_ok
        self._callbacks[spec.Basic.Deliver.NAME] = self._on_basic_deliver
        self._callbacks[spec.Basic.Return.NAME] = self._on_event_ok

        # Let our channel handler handle our flow
        handler.add_callback(handler._channel_flow, [spec.Channel.Flow])

        # Open our channel
        handler.rpc(self._on_event_ok, spec.Channel.Open(),
                         [spec.Channel.OpenOk])

    def __getattribute__(self, function):
        """
        We grab every inbound attribute request to redirect items that are
        synchronous at the spec.DriverMixin layer to prevent commands
        from sending while we're still waiting for a synchronous reply
        """

        # If it's in the whitelist, we know it's local in scope and not in spec
        if function  in channel_attr_whitelist:
            return object.__getattribute__(self, function)

        # If we're blocking subsequent commands add
        if self.blocking:
            logging.debug('%s: %s is blocking this channel' % \
                          (self.__class__.__name__, self.blocking))
            if self.blocked_function:
                logging.error("%s: Blocked call %s has not been processed" % \
                              (self.__class__.__name__, self.blocked_function))
            self.blocked_function = function

            # Hide the scope of our function to append our _blocked list
            def _add_to_blocked(*args, **keywords):
                self._blocked.append([self.blocked_function, args, keywords])
                self.blocked_function = None

            return _add_to_blocked

        if function in self.synchronous:
            self.blocking = "%s.%s" % (self.__class__.__name__, function)
            logging.debug('%s turning on blocking' % self.blocking)

        return object.__getattribute__(self, function)

    def add_state_change_handler(self, handler, key = None):
        """
        Add a State Change Handler for the given key, will be called back
        when the state changes for that key
        """
        self.handler.add_state_change_handler(handler, key)

    def add_flow_change_handler(self, handler, key = None):
        """
        Add a callback handler for when the state of the Channel Flow control
        changes
        """
        self.handler.add_flow_change_handler(handler, key)

    def basic_cancel(self, consumer_tag):
        """
        Cancel a basic_consumer call on the Broker
        """
        # Asked to cancel a consumer_tag we don't have? throw an exception
        if not consumer_tag in self.callbacks:
            raise UnknownConsumerTag(consumer_tag)

        # Send a Basic.Cancel RPC call to close the Basic.Consume
        self.handler.rpc(self._on_cancel_ok,
                         spec.Basic.Cancel(consumer_tag = consumer_tag),
                        [spec.Basic.CancelOk])

    def basic_consume(self, consumer,
                      queue='', no_ack=False, exclusive=False,
                      consumer_tag=None):
        """
        Pass a callback consuming function for a given queue. You can specify
        your own consumer tag but why not let the driver do it for you?
        """
        logging.debug("%s.basic_consume" % self.__class__.__name__)

        # If a consumer tag was not passed, create one
        if not consumer_tag:
            consumer_tag = 'ctag%i' % len(self._consumers)

        # Make sure we've not already registered this consumer tag
        if consumer_tag in self._consumers:
            raise DuplicateConsumerTag(consumer_tag)

        # The consumer tag has not been used before, add it to our consumers
        self._consumers[consumer_tag] = consumer

        # Send our Basic.Consume RPC call
        self.handler.rpc(self.on_event_ok,
                         spec.Basic.Consume(queue=queue,
                                            consumer_tag=consumer_tag,
                                            no_ack=no_ack,
                                            exclusive=exclusive),
                         [spec.Basic.ConsumeOk])

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """
        Publish to the channel with the given exchange, routing key and body.

        If flow control is enabled and you publish a message while another is
        sending, a ContentTransmissionForbidden exception ill be generated
        """
        logging.debug("%s.basic_publish" % self.__class__.__name__)

        # If our handler says we're blocked on flow control
        if self.handler.blocked_on_flow_control():
                raise ContentTransmissionForbidden("Blocked on Flow Control")

        # If properties are not passed in, use the spec's default
        properties = properties or spec.BasicProperties()

        self.handler.send_method(spec.Basic.Publish(exchange=exchange,
                                                    routing_key=routing_key,
                                                    mandatory=mandatory,
                                                    immediate=immediate),
                                 (properties, body))

    def close(self, code=0, text="Normal Shutdown"):
        """
        Will invoke a clean shutdown of the channel with the AMQP Broker
        """
        logging.debug("%s.close: (%s) %s" % (self.__class__.__name__,
                                             str(code), text))

        # If we have an open connection send a RPC call to close the channel
        if self.handler.connection.is_open():
            self.handler.rpc(self.handler.connection.on_channel_close,
                             spec.Channel.Close(code, text, 0, 0),
                             [spec.Channel.CloseOk])

        # Otherwise call the same callback with a dummy frame which has our
        # Information
        else:
            close_frame = spec.Channel.CloseOk()
            close_frame.channel_number = self.handler.channel_number
            self.connection.on_channel_close(close_frame)

    def _ensure(self):
        if self.closed:
            raise ChannelClosed(self.channel_close)
        return self

    def _on_basic_deliver(self, method_frame, header_frame, body):
        """
        Cope with reentrancy. If a particular consumer is still active when
        another delivery appears for it, queue the deliveries up until it
        finally exits.
        """
        logging.debug("%s._on_basic_deliver" % self.__class__.__name__)

        # Shortcut for our consumer tag
        consumer_tag = method_frame.method.consumer_tag

        # If we don't have anything pending,
        if consumer_tag not in self._pending:

            # Setup a list for appending frames into
            self._pending[consumer_tag] = []

            # Call our consumer callback with the data
            self._consumers[consumer_tag](self,
                                          method_frame.method,
                                          header_frame.properties,
                                          body)

            # Loop through and empty the list, we may have gotten more
            # while we were delivering the message to the callback
            while len(self._pending[consumer_tag]):

                # Remove the parts of our list item
                (method, properties, body) = self._pending[consumer_tag].pop(0)

                # Call our consumer callback with the data
                self._consumers[consumer_tag](self,
                                              method,
                                              properties,
                                              body)

            # Remove our pending messages
            del self._pending[consumer_tag]
        else:
            # Append our _pending list with additional data
            self._pending[consumer_tag].append((method_frame.method,
                                                header_frame.properties,
                                                body))

    def _on_cancel_ok(self, frame):
        """
        Called from the Broker when we issue a Basic.Cancel
        """
        logging.debug("%s._on_cancel_ok: %r" % (self.__class__.__name__,
                                               frame.method.NAME))
        # We need to delete the consumer tag from our _consumers
        del(self._consumers[frame.method.consumer_tag])

    def _on_event_ok(self, frame):
        """
        Generic events that returned ok that may have internal callbacks.
        We keep a list of what we've yet to implement so that we don't silently
        drain events that we don't support.
        """
        logging.debug("%s._on_event_ok: %r" % (self.__class__.__name__,
                                              frame.method.NAME))

        # If we've defined a callback, make the call passing ourselves
        # and the frame to the registered function
        if frame.method.NAME in self._callbacks:
            self._callbacks[frame.method.NAME](self, frame)

        if frame.method.NAME in NOT_IMPLEMENTED:
            raise NotImplementedError(frame.method.NAME)

    def _on_synchronous_complete(self, frame):
        """
        This is called whena  synchronous command is completed. It will undo
        the blocking state and send all the frames that stacked up while we
        were in the blocking state.
        """
        logging.debug("%s._on_synchronous_complete for %s: %r" % \
                      (self.__class__.__name__, self.blocking, frame))

        # Release the lock
        self.blocking = None

        # Get the function to call next
        method = self._blocked.pop(0)

        # Call our original method
        function_name = method[0]
        args = method[1]
        keywords = method[2]

        # Get a handle to the function
        function = getattr(self, function_name)

        # Call the function with the correct parameters
        if args and not len(keywords):
            logging.debug("Calling %s with just args" % function_name)
            return function(*args)
        elif args and len(keywords):
            logging.debug("Calling %s with args and keywords" % function_name)
            return function(*args, **keywords)
        elif len(keywords):
            logging.debug("Calling %s with just keywords" % function_name)
            return function(**keywords)

        logging.debug("Calling %s without args or keywords" % function_name)
        return function(self)
