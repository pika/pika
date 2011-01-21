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

# Keep track of things we need to implement and that are not supported
# in Pika at this time
NOT_IMPLEMENTED = [spec.Basic.Return,
                   spec.Basic.Get]

class ChannelTransport(object):

    def __init__(self, connection, channel_number):

        self.connection = connection
        self.channel_number = channel_number

        # Dictionary of callbacks indexed by the reply frame method's class
        self._callbacks = dict()
        self._one_shot = dict()

        # The frame-handler changes depending on the type of frame processed
        self.frame_handler = frame.FrameHandler(self)

        # We need to block on synchronous commands, but do so asynchronously
        self.blocking = None
        self.blocking_callback = None
        self._blocked = list()

        # By default we're closed
        self.closed = True

        # By default our Flow is active
        self.flow_active = True

        # Create our event callback handlers for our state and flow
        self.channel_state_change_event = event.Event()
        self.flow_active_change_event = event.Event()

        # Define our callbacks for specific frame types
        self.add_callback(self._on_channel_flow, [spec.Channel.Flow], False)
        self.add_callback(self._on_event_ok, NOT_IMPLEMENTED, False)

    def add_callback(self, callback, acceptable_replies, one_shot=True):
        """
        Adds a callback entry in our connections callback handler for our
        channel
        """
        logging.debug("%s.add_callback: %s: %r" % (self.__class__.__name__,
                                                   str(callback),
                                                   acceptable_replies))
        # Iterate through the list of acceptable replies
        for reply in acceptable_replies:

            # One-Shot callbacks are made once for a given reply then removed
            if one_shot:

                # Initialize our list for the reply if we need to
                if reply not in self._one_shot:
                    self._one_shot[reply] = list()

                # Append our one shot callback list for this reply
                self._one_shot[reply].append(callback)
            else:

                # Initialize the list for the reply if we need to
                if reply not in self._callbacks:
                    self._callbacks[reply] = list()

                # Append our callback list for this reply
                self._callbacks[reply].append(callback)

    def _process_callbacks(self, frame):
        """
        Invoked when we receive a frame so we can iterate through the defined
        callbacks for this frame
        """
        logging.debug("%s._process_callbacks: %s" % (self.__class__.__name__,
                                                     frame.method.NAME))

        # Process callbacks that dont get removed
        if frame.method.__class__ in self._callbacks:
            for callback in self._callbacks[frame.method.__class__]:
                logging.debug("%s._process_callbacks Calling: %s" % \
                              (self.__class__.__name__, callback))
                callback(frame)

        # Process one shot callbacks
        if frame.method.__class__ in self._one_shot:
            while len(self._one_shot[frame.method.__class__]):
                callback = self._one_shot[frame.method.__class__].pop(0)
                logging.debug("%s._process_callbacks Calling: %s" % \
                              (self.__class__.__name__, callback))
                # One-shot callbacks dont get the frame since they shouldnt
                # Know how to decode them
                callback()

    def add_flow_change_handler(self, handler, key=None):
        """
        Add a callback for when the state of the channel flow changes
        """
        self.flow_active_change_event.add_handler(handler, key)
        handler(self, self.flow_active)

    def add_state_change_handler(self, handler, key=None):
        """
        Add a callback for when the state of the channel changes between open
        and closed
        """
        self.channel_state_change_event.add_handler(handler, key)
        handler(self, not self.closed)

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


    def _has_content(self, method):
        """
        Return a bool if it's a content method as defined by the spec
        """
        return spec.has_content(method.INDEX)

    def rpc(self, callback, method, acceptable_replies):
        """
        Shortcut wrapper to the Connection's rpc command using its callback
        stack, passing in our channel number
        """
        logging.debug("%s.rpc(%s, %s, %r)" % (self.__class__.__name__,
                                               callback,
                                               method,
                                               acceptable_replies))

        # We can't ensure if we're opening
        if not isinstance(method, spec.Channel.Open):
            self._ensure()

        # If we're using flow control, content methods should know not to call
        # the rpc function and should be using the flow_active_change_event
        # notification system to know when they can send
        if self._has_content(method) and self._blocked_on_flow_control():
            raise ChannelBlocked("Flow Control Active, Waiting for Ok")

        # If we're blocking, add subsequent commands to our stack
        if self.blocking:
            logging.debug('%s: %s is blocking this channel' % \
                          (self.__class__.__name__, self.blocking))

            self.blocking.append([callback, method, acceptable_replies])
            return

        # If this is a synchronous method, block connections until we're done
        if method in spec.SYNCHRONOUS_METHODS:
            logging.debug('%s: %s turning on blocking' % \
                          (self.__class__.__name__, method))
            self.blocking = method

            # Let's hold the callback so we can make it from
            # _on_synchronous_complete
            self.blocking_callback = callback

            # Override the callback
            callback = self._on_synchronous_complete

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

        # Process callbacks
        self._process_callbacks(frame)

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

        # Call the original callback then remove the reference
        if self.blocking_callback:
            self.blocking_callback(frame)
            self.blocking_callback = None

        # Process callbacks
        self._process_callbacks(frame)

        # Get the list, then empty it so we can spin through all the blocked
        # Calls, blocking again in the class level list
        blocked = self._blocked
        self._blocked = []

        # Loop through and call all that were blocked during our last command
        while len(blocked):

            # Get the function to call next
            method = blocked.pop(0)

            # Call the RPC for each of our blocked calls
            self.rpc(method[0], method[1], method[2])


class Channel(spec.DriverMixin):

    def __init__(self, connection, channel_number):

        # Make sure that the caller passed in an int for the channel number
        if not isinstance(channel_number, int):
            raise InvalidChannelNumber

        # Get the handle to our handler
        self.transport = ChannelTransport(connection, channel_number)

        # Assign our connection we communicate with
        self.connection = connection

        # For event based processing
        self._consumers = {}
        self._pending = {}

        # Add a callback for Basic.Deliver
        self.transport.add_callback(self._on_basic_deliver,
                                    [spec.Basic.Deliver],
                                    False)

        # Create our event callback handlers for our state
        self.state_change_event = event.Event()

        # Open our channel
        self.transport.rpc(self._on_open_ok, spec.Channel.Open(),
                           [spec.Channel.OpenOk])

    def add_state_change_handler(self, handler, key=None):
        """
        Add a callback for when the state of the channel changes between open
        and closed
        """
        self.state_change_event.add_handler(handler, key)

    def basic_cancel(self, consumer_tag, callback):
        """
        Cancel a basic_consumer call on the Broker
        """
        logging.debug("%s.basic_cancel: %s" % \
                      (self.__class__.__name__, consumer_tag))
        # Asked to cancel a consumer_tag we don't have? throw an exception
        if not consumer_tag in self._consumers:
            raise UnknownConsumerTag(consumer_tag)

        # Add our CPS style callback
        self.transport.add_callback(callback,
                                    [spec.Basic.CancelOk])
        self.transport.add_callback(self._on_cancel_ok,
                                    [spec.Basic.CancelOk],
                                    False)

        # Send a Basic.Cancel RPC call to close the Basic.Consume
        self.transport.rpc(self.transport._on_event_ok,
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
        Called in response to a frame from the Broker when we call Basic.Cancel
        """
        logging.debug("%s._on_cancel_ok: %r" % (self.__class__.__name__,
                                               frame.method.NAME))

        # We need to delete the consumer tag from our _consumers
        del(self._consumers[frame.method.consumer_tag])

    def _on_close_ok(self, frame):
        """
        Called in response to a frame from the Broker when we
        call Channel.Close
        """
        logging.debug("%s._on_event_ok: %r" % (self.__class__.__name__,
                                              frame.method.NAME))

        # Let our transport know we're closed and it'll deal with the rest
        self.transport.on_close()

        # Let those who registered for our state change know it's happened
        self.state_change_event.fire(self, False)

    def _on_open_ok(self, frame):
        """
        Called in response to a frame from the broker when we call Channel.Open
        """
        logging.debug("%s._on_open_ok: %r" % (self.__class__.__name__,
                                              frame.method.NAME))

        # Let our transport know we're open
        self.transport.open()

        # Let those who registered for our state change know it's happened
        self.state_change_event.fire(self, True)
