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

import pika.frame as frame
import pika.event as event
import pika.spec as spec

from pika.exceptions import *

NOT_IMPLEMENTED = [spec.Basic.Return.NAME, spec.Basic.Get.NAME]


class ChannelTransport(object):

    def __init__(self, connection, channel_number):

        self.connection = connection
        self.channel_number = channel_number

        # Dictionary of callbacks indexed by the reply's frame method name
        self._callbacks = {}

        # The frame-handler changes depending on the type of frame processed
        self.frame_handler = frame.FrameHandler(self)

        # We need to block on synchronous commands, but do so asynchronously
        self.blocking = None
        self._blocked = []
        self.blocked_function = None

        # By default we're closed
        self.closed = True

        # By default our Flow is active
        self.flow_active = True

        # Create our event callback handlers for our state and flow
        self.channel_state_change_event = event.Event()
        self.flow_active_change_event = event.Event()

        # Define our callbacks for specific frame types
        self.add_callback(self._on_channel_flow, [spec.Channel.Flow])
        self.add_callback(self._on_event_ok, NOT_IMPLEMENTED)

    def add_callback(self, callback, acceptable_replies):
        """
        Adds a callback entry in our connections callback handler for our
        channel
        """
        for reply in acceptable_replies:
            if reply in self._callbacks:
                raise CallbackReplyAlreadyRegistered(reply)

            self._callbacks[reply] = callback

    def add_flow_change_handler(self, handler, key=None):
        """
        Add a callback for when the state of the channel flow changes
        """
        self.flow_active_change_event.addHandler(handler, key)
        handler(self, self.flow_active)

    def add_state_change_handler(self, handler, key=None):
        """
        Add a callback for when the state of the channel changes between open
        and closed
        """
        self.channel_state_change_event.addHandler(handler, key)
        handler(self, not self.channel_close)

    def deliver(self, frame):
        """
        Deliver a frame to the frame handler. When it's gotten to a point that
        it has build our frame responses in a way we can use them, it will
        call our callback stack to figure out what to do.
        """
        self.frame_handler.process(frame)

    def _blocked_on_flow_control(self):
        """
        Returns a bool if we're currently blocking in Flow
        """
        return not self.flow_active

    def _ensure(self):
        """
        Make sure the transport is open
        """
        if self.closed:
            raise ChannelClosed("Command issued while channel is closed")

        return True

    def _on_channel_flow(self, frame):
        """
        We've received a Channel.Flow frame, obey it's request and set our
        flow active state, then fire the event and send the FlowOk method in
        response
        """
        logging.debug("%s._on_channel_flow: %r" % (self.__class__.__name__,
                                                   frame))
        self.flow_active = frame.method.active
        self.flow_active_change_event.fire(self, self.flow_active)
        self.send_method(spec.Channel.FlowOk(active=self.flow_active))

    def on_close(self):
        """
        Called by the Channel._on_close_ok function, this will fire the event
        and remove the channel from the connection
        """
        if not self.closed:
            self.closed = True
            self.connection.remove_channel(self.channel_number)
            self.channel_state_change_event.fire(self, False)

    def open(self):
        """
        Update our internal variables when we open and fire the open event
        on the state change handler
        """
        self.closed = False
        self.channel_state_change_event.fire(self, True)

    def rpc(self, callback, method, acceptable_replies):
        """
        Shortcut wrapper to the Connection's rpc command using its callback
        stack, passing in our channel number
        """
        # We can't ensure if we're opening
        if method.NAME != spec.Channel.Open.NAME:
            self._ensure()

        if self._blocked_on_flow_control():
            raise ChannelBlocked("Flow Control Active, Waiting for Ok")

        self.connection.rpc(callback, self.channel_number,
                            method, acceptable_replies)

    def send_method(self, method, content=None):
        """
        Shortcut wrapper to send a method through our connection, passing in
        our channel number
        """
        self.connection.send_method(self.channel_number, method, content)

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
        This is called when a synchronous command is completed. It will undo
        the blocking state and send all the frames that stacked up while we
        were in the blocking state.
        """
        logging.debug("%s._on_synchronous_complete for %s: %r" % \
                      (self.__class__.__name__, self.blocking, frame))

        # Release the lock
        self.blocking = None

        # Get the list, then empty it so we can spin through all the blocked
        # Calls, blocking again in the class level list
        blocked = self._blocked
        self._blocked = []

        # Loop through and call all that were blocked during our last command
        while len(blocked):

            # Get the function to call next
            method = blocked.pop(0)

            # Call our original method
            object_ = method[0]
            function_name = method[1]
            args = method[2]
            keywords = method[3]

            # Get a handle to the function
            function = getattr(object_, function_name)

            # Call the function with the correct parameters
            if args and not len(keywords):
                logging.debug("Calling %s with just args" % \
                              function_name)
                function(*args)
            elif args and len(keywords):
                logging.debug("Calling %s with args and keywords" % \
                              function_name)
                function(*args, **keywords)
            elif len(keywords):
                logging.debug("Calling %s with just keywords" % \
                              function_name)
                function(**keywords)
            else:
                logging.debug("Calling %s without args or keywords" % \
                              function_name)
                function()

# Due to our use of __getattribute__ to block calls while synchronous calls are
# taking place, we use a whitelist to prevent the __getattribute__ function
# from becoming a recursive mess. Whitelisting variables and functions that
# allow us to quickly return from the function prior to evaluating them.

channel_attr_whitelist = ('__class__',
                          '_consumers',
                          '_on_basic_deliver',
                          '_on_cancel_ok',
                          '_on_event_ok',
                          '_pending',
                          'add_state_change_handler',
                          'add_flow_change_handler',
                          'closed',
                          'connection',
                          'synchronous',
                          'transport')


class Channel(spec.DriverMixin):

    def __init__(self, connection, channel_number, on_channel_ready):

        # Make sure that the caller passed in an int for the channel number
        if not isinstance(channel_number, int):
            raise InvalidChannelNumber

        # Get the handle to our handler
        self.transport = ChannelTransport(connection, channel_number)

        # Assign our connection we communicate with
        self.connection = connection

        # When we open, we'll call this back passing our handle
        self.on_channel_ready = on_channel_ready

        # For event based processing
        self._consumers = {}
        self._pending = {}

        # Add a callback for Basic.Deliver
        self.transport.add_callback(self._on_basic_deliver,
                                    [spec.Basic.Deliver.NAME])

        # Open our channel
        self.transport.rpc(self._on_open_ok, spec.Channel.Open(),
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
        if self.transport.blocking:
            logging.debug('%s: %s is blocking this channel' % \
                          (self.__class__.__name__, self.transport.blocking))

            # No need to scope this function outside of here and it makes
            # it easier to get at the value of function
            def _add_to_blocked(*args, **keywords):
                # Append the signature of what was called the transport's
                # _blocked list for later processing
                self.transport._blocked.append([self,
                                                function,
                                                args,
                                                keywords])
                # @TODO Should add a timeout here for blocking functions

            return _add_to_blocked

        if function in self.synchronous:
            self.transport.blocking = "%s.%s" % (self.__class__.__name__,
                                                 function)
            logging.debug('%s turning on blocking' % self.transport.blocking)

        return object.__getattribute__(self, function)

    def basic_cancel(self, consumer_tag):
        """
        Cancel a basic_consumer call on the Broker
        """
        # Asked to cancel a consumer_tag we don't have? throw an exception
        if not consumer_tag in self.callbacks:
            raise UnknownConsumerTag(consumer_tag)

        # Send a Basic.Cancel RPC call to close the Basic.Consume
        self.transport.rpc(self._on_cancel_ok,
                           spec.Basic.Cancel(consumer_tag=consumer_tag),
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
        try:
            self.transport.rpc(self.transport._on_event_ok,
                               spec.Basic.Consume(queue=queue,
                                                  consumer_tag=consumer_tag,
                                                  no_ack=no_ack,
                                                  exclusive=exclusive),
                               [spec.Basic.ConsumeOk])
        except ChannelClosed, e:
            del(self._consumers[consumer_tag])
            raise ChannelClosed(e)

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """
        Publish to the channel with the given exchange, routing key and body.

        If flow control is enabled and you publish a message while another is
        sending, a ContentTransmissionForbidden exception ill be generated
        """
        logging.debug("%s.basic_publish" % self.__class__.__name__)

        # If properties are not passed in, use the spec's default
        properties = properties or spec.BasicProperties()

        self.transport.send_method(spec.Basic.Publish(exchange=exchange,
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
        self.transport.rpc(self._on_close_ok,
                           spec.Channel.Close(code, text, 0, 0),
                           [spec.Channel.CloseOk])

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

    def _on_close_ok(self, frame):
        logging.debug("%s._on_event_ok: %r" % (self.__class__.__name__,
                                              frame.method.NAME))

        # Let our transport know we're closed and it'll deal with the rest
        self.transport.on_close()

    def _on_open_ok(self, frame):
        logging.debug("%s._on_open_ok: %r" % (self.__class__.__name__,
                                              frame.method.NAME))

        # Let our transport know we're open
        self.transport.open()

        # Let our on_channel_ready callback have our handle
        self.on_channel_ready(self)
