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

import pika.frames as frames
import pika.callback as callback
import pika.spec as spec
import pika.exceptions


# Keep track of things we need to implement and that are not supported
# in Pika at this time
NOT_IMPLEMENTED = [spec.Basic.Return]


class ChannelTransport(object):

    def __init__(self, connection, channel_number):

        self.connection = connection
        self.channel_number = channel_number

        # Use the global callback handler
        self.callbacks = callback.CallbackManager.instance()

        # The frame-handler changes depending on the type of frame processed
        self.frame_handler = frames.FrameHandler()

        # We need to block on synchronous commands, but do so asynchronously
        self.blocking = None
        self._blocked = list()

        # By default we're closed
        self.closed = True

        # By default our Flow is active
        self.flow_active = True

        # Define our callbacks for specific frame types
        self.callbacks.add(self.channel_number, spec.Channel.Flow,
                           self._on_channel_flow, False)

        # Add callbacks for channel open event
        self.callbacks.add(self.channel_number, spec.Channel.OpenOk,
                           self._on_open)

        # Add in callbacks for methods not implemented
        for method in NOT_IMPLEMENTED:
            self.callbacks.add(self.channel_number, method,
                               self._on_event_ok, False)

    def _on_open(self, frame):
        """
        Called as a result of receiving a spec.Channel.OpenOk frame.
        """
        self.closed = False

    def _blocked_on_flow_control(self):
        """
        Returns a bool if we're currently blocking in Flow
        """
        return not self.flow_active

    def deliver(self, frame):
        """
        Deliver a frame to the frame handler. When it's gotten to a point that
        it has build our frame responses in a way we can use them, it will
        call our callback stack to figure out what to do.
        """
        self.frame_handler.process(frame)

    def _ensure(self):
        """
        Make sure the transport is open
        """
        if self.closed:
            raise pika.exceptions.ChannelClosed

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

        # Process our flow state change callbacks
        self.callbacks.process(self.channel_number, 'flow_change',
                               frame.method.active)

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

        # Make sure the channel is open
        self._ensure()

        # If we're using flow control, content methods should know not to call
        # the rpc function and should be using the flow_active_change_event
        # notification system to know when they can send
        if self._has_content(method) and self._blocked_on_flow_control():
            raise pika.exceptions.ChannelBlocked("Flow Control Active")

        # If we're blocking, add subsequent commands to our stack
        if self.blocking:
            logging.debug('%s: %s is blocking this channel' % \
                          (self.__class__.__name__, self.blocking))

            self._blocked.append([callback, method, acceptable_replies])
            return

        # If this is a synchronous method, block connections until we're done
        if method.synchronous:
            logging.debug('%s: %s turning on blocking' % \
                          (self.__class__.__name__, method.NAME))

            self.blocking = method.NAME

        for reply in acceptable_replies:
            self.callbacks.add(self.channel_number, reply,
                               self._on_synchronous_complete)
            self.callbacks.add(self.channel_number, reply,
                               callback)

        self.send_method(method)

    def send_method(self, method, content=None):
        """
        Shortcut wrapper to send a method through our connection, passing in
        our channel number
        """
        logging.debug("%s.send_method: %s(%s)" % (self.__class__.__name__,
                                                  method, content))
        self.connection.send_method(self.channel_number, method, content)

    def _on_event_ok(self, frame):
        """
        Generic events that returned ok that may have internal callbacks.
        We keep a list of what we've yet to implement so that we don't silently
        drain events that we don't support.
        """
        logging.debug("%s._on_event_ok: %r" % (self.__class__.__name__,
                                              frame.method.NAME))

        # If we've not implemented this frame, raise an exception
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
        self._blocked = list()

        # Loop through and call all that were blocked during our last command
        while blocked:

            # Get the function to call next
            method = blocked.pop(0)

            # Call the RPC for each of our blocked calls
            self.rpc(*method)


class Channel(spec.DriverMixin):

    def __init__(self, connection, channel_number, on_open_callback=None,
                 transport=None):
        """
        Initialize the Channel and Transport, opening the channel with the
        Server connection is passed down from our invoker and
        channel_number is also passed from our invoker. channel_number is best
        managed from Connection
        """
        # Make sure that the caller passed in an int for the channel number
        if not isinstance(channel_number, int):
            raise pika.exceptions.InvalidChannelNumber

        # Get the handle to our transport, either passed in or made here
        if transport:
            self.transport = transport
        else:
            self.transport = ChannelTransport(connection, channel_number)

        # Channel Number
        self.channel_number = channel_number

        # Assign our connection we communicate with
        self.connection = connection

        # Get the callback manager
        self.callbacks = callback.CallbackManager.instance()

        # Our on_open_callback, special case
        self._on_open_callback = on_open_callback

        # Reason for closing channels
        self.closing = None

        # For event based processing
        self._consumers = dict()
        self._pending = dict()

        # Set this here just as a default value
        self._basic_get_callback = None

        # Add a callback for Basic.Deliver
        self.callbacks.add(self.channel_number,
                           '_on_basic_deliver',
                           self._on_basic_deliver,
                           False,
                           frames.FrameHandler)

        # Add a callback for Basic.Deliver
        self.callbacks.add(self.channel_number,
                           '_on_basic_get',
                           self._on_basic_get,
                           False,
                           frames.FrameHandler)

        # Add a callback for Basic.GetEmpty
        self.callbacks.add(self.channel_number,
                           spec.Basic.GetEmpty,
                           self._on_basic_get_empty,
                           False)

        # Add the callback for our Channel.OpenOk to call our on_open_callback
        self.callbacks.add(self.channel_number,
                           spec.Channel.OpenOk,
                           self._open)

        # Open our channel
        self.transport.send_method(spec.Channel.Open())

    def add_callback(self, callback, acceptable_replies):
        """
        Our DriverMixin will call this to add items to the callback stack
        """
        for reply in acceptable_replies:
            self.callbacks.add(self.channel_number, reply, callback)

    def add_on_close_callback(self, callback):
        """
        Add a close callback to the stack that will be called when we have
        successfully closed the channel
        """
        self.callbacks.add(self.channel_number, spec.Channel.CloseOk, callback)

    def add_flow_change_callback(self, callback):
        """
        Pass in a callback that will be called with a boolean flag which
        indicates if the channel flow is active or not
        """
        self.callbacks.add(self.channel_number, 'flow_change', callback)

    def close(self, code=0, text="Normal Shutdown"):
        """
        Will invoke a clean shutdown of the channel with the AMQP Broker
        """
        logging.debug("%s.close: (%s) %s" % (self.__class__.__name__,
                                             str(code), text))

        # Set our closing code and text
        self.closing = code, text

        # Send our basic cancel for all of our consumers
        for consumer_tag in self._consumers.keys():
            self.basic_cancel(consumer_tag)

        # If we have an open connection send a RPC call to close the channel
        if not len(self._consumers):
            self._close()

    def _close(self):
        """
        Internal close, is called when all the consumers are closed by both
        Channel.close and Channel._on_cancel_ok
        """
        logging.debug("%s._close" % self.__class__.__name__)
        self.transport.send_method(spec.Channel.Close(self.closing[0],
                                                      self.closing[1],
                                                      0, 0))

    def _open(self, frame):
        logging.debug("%s._open: %r" % (self.__class__.__name__, frame))
        # Call our on open callback
        if self._on_open_callback:
            self._on_open_callback(self)

    def basic_cancel(self, consumer_tag, callback=None):
        """
        Cancel a basic_consumer call on the Broker
        """
        logging.debug("%s.basic_cancel: %s" % \
                      (self.__class__.__name__, consumer_tag))

        if consumer_tag not in self._consumers:
            return

        if callback:
            self.callbacks.add(self.channel_number,
                               spec.Basic.CancelOk,
                               callback)

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
            raise pika.exceptions.DuplicateConsumerTag(consumer_tag)

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
        except pika.exceptions.ChannelClosed, e:
            del(self._consumers[consumer_tag])
            raise pika.exceptions.ChannelClosed(e)

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

    @property
    def consumer_tags(self):
        """
        Return a list of the currently active consumer tags
        """
        return self._consumers.keys()

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
        if consumer_tag not in self._pending and \
           consumer_tag in self._consumers:

            # Setup a list for appending frames into
            self._pending[consumer_tag] = list()

            # Call our consumer callback with the data
            self._consumers[consumer_tag](self,
                                          method_frame.method,
                                          header_frame.properties,
                                          body)

            # Loop through and empty the list, we may have gotten more
            # while we were delivering the message to the callback
            while self._pending[consumer_tag]:

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

        # If we're closing and dont have any consumers left, close
        if self.closing and not len(self._consumers):
            self._close()

    def basic_get(self, callback, ticket=0, queue='', no_ack=False):
        """
        Implementation of the basic_get command that bypasses the rpc mechanism
        since we have to assemble the content
        """
        logging.debug("%s.basic_get(cb=%s, ticket=%i, queue=%s, no_ack=%s)" % \
                      (self.__class__.__name__, callback, ticket,
                       queue, no_ack))
        self._basic_get_callback = callback
        self.transport.send_method(spec.Basic.Get(ticket=ticket,
                                                  queue=queue,
                                                  no_ack=no_ack))

    def _on_basic_get(self, method_frame, header_frame, body):
        logging.debug("%s._on_basic_get" % self.__class__.__name__)
        if self._basic_get_callback:
            self._basic_get_callback(self,
                                     method_frame.method,
                                     header_frame.properties,
                                     body)
            self._basic_get_callback = None
        else:
            logging.error("%s._on_basic_get: No callback defined." %\
                          self.__class__.__name__)

    def _on_basic_get_empty(self, frame):
        logging.debug("%s._on_basic_get_empty" % self.__class__.__name__)
