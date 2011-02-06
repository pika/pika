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
import pika.callback as callback
import pika.frames as frames
import pika.log as log
import pika.spec as spec
import pika.exceptions as exceptions


class ChannelTransport(object):
    """
    The ChannelTransport class is meant to handle the communication paths to
    and from the AMQP Broker. It is not supposed to implement client behavior
    other than what is pre-specified by the Channel class. It is not meant
    to be invoked by applications using Pika.

    Note that as of RabbitMQ 2.0 Channel.Flow is no longer used and has been
    removed and uses tcp backpressure instead. Since we only support 0-9-1
    which was not introduced in RabbitMQ until 2.0, server side channel
    flow detection has been removed.
    """

    def __init__(self, connection, channel_number):

        self.connection = connection
        self.channel_number = channel_number

        # Use the global callback handler
        self.callbacks = callback.CallbackManager.instance()

        # The frame-handler changes depending on the type of frame processed
        self.frame_handler = ContentHandler()

        # We need to block on synchronous commands, but do so asynchronously
        self.blocking = None
        self._blocked = list()

        # By default we're closed
        self.closed = True

        # Add callbacks for channel open event
        self.callbacks.add(self.channel_number, spec.Channel.OpenOk,
                           self._on_open)

    def _on_open(self, frame):
        """
        Called as a result of receiving a spec.Channel.OpenOk frame.
        """
        self.closed = False

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
            raise exceptions.ChannelClosed

        return True

    def _has_content(self, method):
        """
        Return a bool if it's a content method as defined by the spec
        """
        return spec.has_content(method.INDEX)

    def rpc(self, method, callback=None, acceptable_replies=[]):
        """
        Shortcut wrapper to the Connection's rpc command using its callback
        stack, passing in our channel number
        """
        log.debug("%s.rpc(%s, %s, %r)",
                      self.__class__.__name__, callback,
                      method, acceptable_replies)

        # Make sure the channel is open
        self._ensure()

        # If we're blocking, add subsequent commands to our stack
        if self.blocking:
            log.debug('%s: %s is blocking this channel',
                          self.__class__.__name__, self.blocking)

            self._blocked.append([method, callback, acceptable_replies])
            return

        # If this is a synchronous method, block connections until we're done
        if method.synchronous:
            log.debug('%s: %s turning on blocking',
                          self.__class__.__name__, method.NAME)
            self.blocking = method.NAME

        for reply in acceptable_replies:
            self.callbacks.add(self.channel_number, reply,
                               self._on_synchronous_complete)
            if callback:
                self.callbacks.add(self.channel_number, reply, callback)

        self.send_method(method)

    def send_method(self, method, content=None):
        """
        Shortcut wrapper to send a method through our connection, passing in
        our channel number
        """
        log.debug("%s.send_method: %s(%s)", self.__class__.__name__,
                      method, content)

        self.connection._send_method(self.channel_number, method, content)

    def _on_event_ok(self, frame):
        """
        Generic events that returned ok that may have internal callbacks.
        We keep a list of what we've yet to implement so that we don't silently
        drain events that we don't support.
        """
        log.debug("%s._on_event_ok: %r", self.__class__.__name__,
                      frame.method.NAME)

    def _on_synchronous_complete(self, frame):
        """
        This is called when a synchronous command is completed. It will undo
        the blocking state and send all the frames that stacked up while we
        were in the blocking state.
        """
        log.debug("%s._on_synchronous_complete for %s: %r",
                      self.__class__.__name__, self.blocking, frame)

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
        A Channel is the primary communication method for interacting with
        RabbitMQ. It is recommended that you do not directly invoke
        the creation of a channel object in your application code but rather
        construct the a channel by calling the active connection's channel()
        method.
        """
        # Make sure that the caller passed in an int for the channel number
        if not isinstance(channel_number, int):
            raise exceptions.InvalidChannelNumber

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
        self._on_get_ok_callback = None
        self._on_flow_ok_callback = None

        # Add a callback for Basic.Deliver
        self.callbacks.add(self.channel_number,
                           '_on_basic_deliver',
                           self._on_basic_deliver,
                           False,
                           ContentHandler)

        # Add a callback for Basic.Get
        self.callbacks.add(self.channel_number,
                           '_on_basic_get',
                           self._on_basic_get_ok,
                           False,
                           ContentHandler)

        # Add a callback for Basic.GetEmpty
        self.callbacks.add(self.channel_number,
                           spec.Basic.GetEmpty,
                           self._on_basic_get_empty,
                           False)

        # Add a callback for when the server closes our channel
        self.callbacks.add(self.channel_number,
                           spec.Channel.Close,
                           self._on_remote_close,
                           False)

        # Add the callback for our Channel.OpenOk to call our on_open_callback
        self.callbacks.add(self.channel_number,
                           spec.Channel.OpenOk,
                           self._open)

        # Open our channel
        self.transport.send_method(spec.Channel.Open())

    def add_callback(self, callback, replies):
        """
        Pass in a callback handler and a list replies from the
        RabbitMQ broker which you'd like the callback notified of. Callbacks
        should allow for the frame parameter to be passed in.
        """
        for reply in replies:
            self.callbacks.add(self.channel_number, reply, callback)

    def add_on_close_callback(self, callback):
        """
        Pass a callback function that will be called when the channel is
        closed. The callback function should receive a frame parameter.
        """
        self.callbacks.add(self.channel_number, '_on_channel_close', callback)

    def add_on_return_callback(self, callback):
        """
        Pass a callback function that will be called when basic_publish as sent
        a message that has been rejected and returned by the server. The
        callback handler should receive a method, header and body frame. The
        base signature for the callback should be the same as the method
        signature one creates for a basic_consume callback.
        """
        self.callbacks.add(self.channel_number, '_on_basic_return', callback)

    def close(self, code=0, text="Normal Shutdown", from_server=False):
        """
        Will invoke a clean shutdown of the channel with the AMQP Broker
        """
        log.debug("%s.close: (%s) %s", self.__class__.__name__,
                      str(code), text)

        # Set our closing code and text
        self.closing = code, text

        # Let an application that registered itself our callbacks know we're
        # Closing/Closed
        self.callbacks.process(self.channel_number, '_on_channel_close',
                               self, code, text)

        # Send our basic cancel for all of our consumers
        for consumer_tag in self._consumers.keys():
            self.basic_cancel(consumer_tag)

        # If we have an open connection send a RPC call to close the channel
        if not len(self._consumers) and not from_server:
            self._close()

    def _close(self):
        """
        Internal close, is called when all the consumers are closed by both
        Channel.close and Channel._on_cancel_ok
        """
        log.debug("%s._close", self.__class__.__name__)
        self.transport.send_method(spec.Channel.Close(self.closing[0],
                                                      self.closing[1],
                                                      0, 0))

    def _on_remote_close(self, frame):
        """
        Handle the case where our channel has been closed for us
        """
        log.warning("%s._on_remote_close(%s, %s)", self.__class__.__name__,
                    frame.method.reply_code, frame.method.reply_text)

        # Set our closing code and text
        self.closing = frame.method.reply_code, frame.method.reply_text

        # Let an application that registered itself our callbacks know we're
        # Closing/Closed
        self.callbacks.process(self.channel_number, '_on_channel_close',
                               self, frame.method.reply_code,
                               frame.method.reply_text)

    def _open(self, frame):
        """
        Called by our callback handler when we receive a Channel.OpenOk and
        subsequently calls our _on_open_callback which was passed into the
        Channel construtor. The reason we do this is because we want to make
        sure that the on_open_callback parameter passed into the Channel
        constructor is not the first callback we make. ChannelTransport needs
        to know before the app that passed in the callback.
        """
        log.debug("%s._open: %r", self.__class__.__name__, frame)
        # Call our on open callback
        if self._on_open_callback:
            self._on_open_callback(self)

    def basic_cancel(self, consumer_tag, nowait=False, callback=None):
        """
        Pass in the consumer tag to cancel a basic_consume request with. The
        consumer_tag is optionally passed to basic_consume as a parameter and
        it is always returned by the Basic.ConsumeOk frame. For more
        information see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.cancel
        """
        log.debug("%s.basic_cancel: %s", self.__class__.__name__,
                      consumer_tag)

        if consumer_tag not in self._consumers:
            return

        if callback:
            self.callbacks.add(self.channel_number,
                               spec.Basic.CancelOk,
                               callback)

        # Send a Basic.Cancel RPC call to close the Basic.Consume
        self.transport.rpc(spec.Basic.Cancel(consumer_tag=consumer_tag,
                                             nowait=nowait),
                           self._on_cancel_ok, [spec.Basic.CancelOk])

    def basic_consume(self, consumer_callback,
                      queue='', no_ack=False, exclusive=False,
                      consumer_tag=None):
        """
        Sends the AMQP command Basic.Consume to the broker and binds messages
        for the consumer_tag to the consumer callback. If you do not pass in
        a consumer_tag, one will be automatically generated for you. For
        more information on basic_consume, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume
        """
        log.debug("%s.basic_consume", self.__class__.__name__)

        # If a consumer tag was not passed, create one
        if not consumer_tag:
            consumer_tag = 'ctag%i' % len(self._consumers)

        # Make sure we've not already registered this consumer tag
        if consumer_tag in self._consumers:
            raise exceptions.DuplicateConsumerTag(consumer_tag)

        # The consumer tag has not been used before, add it to our consumers
        self._consumers[consumer_tag] = consumer_callback

        # Send our Basic.Consume RPC call
        try:
            self.transport.rpc(spec.Basic.Consume(queue=queue,
                                                  consumer_tag=consumer_tag,
                                                  no_ack=no_ack,
                                                  exclusive=exclusive),
                               self.transport._on_event_ok,
                               [spec.Basic.ConsumeOk])
        except exceptions.ChannelClosed, e:
            del(self._consumers[consumer_tag])
            raise exceptions.ChannelClosed(e)

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """
        Publish to the channel with the given exchange, routing key and body.
        For more information on basic_publish and what the parameters do, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish
        """
        log.debug("%s.basic_publish", self.__class__.__name__)

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
        Property method that returns a list of currently active consumers
        """
        return self._consumers.keys()

    def _on_basic_deliver(self, method_frame, header_frame, body):
        """
        Cope with reentrancy. If a particular consumer is still active when
        another delivery appears for it, queue the deliveries up until it
        finally exits.
        """
        log.debug("%s._on_basic_deliver", self.__class__.__name__)

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
        log.debug("%s._on_cancel_ok: %r", self.__class__.__name__,
                                               frame.method.NAME)

        # We need to delete the consumer tag from our _consumers
        del(self._consumers[frame.method.consumer_tag])

        # If we're closing and dont have any consumers left, close
        if self.closing and not len(self._consumers):
            self._close()

    def basic_get(self, callback, ticket=0, queue='', no_ack=False):
        """
        Get a single message from the AMQP broker. The callback method
        signature should have 3 parameters: The method frame, header frame and
        the body, like the consumer callback for Basic.Consume. If you want to
        be notified of Basic.GetEmpty, use the Channel.add_callback method
        adding your Basic.GetEmpty callback which should expect only one
        parameter, frame. For more information on basic_get and its
        parameters, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get
        """
        log.debug("%s.basic_get(cb=%s, ticket=%i, queue=%s, no_ack=%s)",
                      self.__class__.__name__, callback, ticket, queue, no_ack)
        self._on_get_ok_callback = callback
        self.transport.send_method(spec.Basic.Get(ticket=ticket,
                                                  queue=queue,
                                                  no_ack=no_ack))

    def _on_basic_get_ok(self, method_frame, header_frame, body):
        log.debug("%s._on_basic_get_ok", self.__class__.__name__)
        if self._on_get_ok_callback:
            self._on_get_ok_callback(self,
                                        method_frame.method,
                                        header_frame.properties,
                                        body)
            self._basic_get_ok_callback = None
        else:
            log.error("%s._on_basic_get: No callback defined.",
                          self.__class__.__name__)

    def _on_basic_get_empty(self, frame):
        """
        When we receive an empty reply do nothing but log it
        """
        log.debug("%s._on_basic_get_empty", self.__class__.__name__)

    def flow(self, callback, active):
        """
        Turn Channel flow control off and on. Pass a callback to be notified
        of the response from the server. active is a bool. Callback should
        expect a bool in response indicating channel flow state. For more
        information, please reference:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#channel.flow
        """
        log.debug("%s.flow(%s, %s)", self.__class__.__name__, callback,
                  active)
        self._on_flow_ok_callback = callback
        self.transport.rpc(spec.Channel.Flow(active), self._on_channel_flow_ok,
                           [spec.Channel.FlowOk])

    def _on_channel_flow_ok(self, frame):
        """
        Called in response to us asking the server to toggle on Channel.Flow
        """
        log.debug("%s._on_channel_flow_ok(%s)", self.__class__.__name__, frame)

        # Update the channel flow_active state
        self.transport.flow_active = frame.method.active

        # If we have a callback defined, process it
        if self._on_flow_ok_callback:
            self._on_flow_ok_callback(frame.method.active)
            self._on_flow_ok_callback = None
        else:
            log.error("%s._on_flow_ok: No callback defined.",
                      self.__class__.__name__)


class ContentHandler(object):
    """
    This handles content frames which come in in synchronous order as follows:

    1) Method Frame
    2) Header Frame
    3) Body Frame(s)

    The way that content handler works is to assign the active frame type to
    the self._handler variable. When we receive a header frame that is either
    a Basic.Deliver, Basic.GetOk, or Basic.Return, we will assign the handler
    to the ContentHandler._handle_header_frame. This will fire the next time
    we are called and parse out the attributes required to receive the body
    frames and assemble the content to be returned. We will then assign the
    self._handler to ContentHandler._handle_body_frame.

    _handle_body_frame has two methods inside of it, handle and finish.
    handle will be invoked until the requirements set by the header frame have
    been met at which point it will call the finish method. This calls
    the callback manager with the method frame, header frame and assembled
    body and then reset the self._handler to the _handle_method_frame method.
    """

    def __init__(self):
        # We start with Method frames always
        self._handler = self._handle_method_frame

    def process(self, frame):
        """
        Invoked by the ChannelTransport object when passed frames that are not
        setup in the rpc process and that don't have explicit reply types
        defined. This includes Basic.Publish, Basic.GetOk and Basic.Return
        """
        self._handler(frame)

    def _handle_method_frame(self, frame):
        """
        Receive a frame and process it, we should have content by the time we
        reach this handler, set the next handler to be the header frame handler
        """
        log.debug("%s._handle_method_frame: %r", self.__class__.__name__,
                      frame)

        # If we don't have FrameMethod something is wrong so throw an exception
        if not isinstance(frame, frames.Method):
            raise exceptions.UnexpectedFrameError(frame)

        # If the frame is a content related frame go deal with the content
        # By getting the content header frame
        if spec.has_content(frame.method.INDEX):
            self._handler = self._handle_header_frame(frame)

        # We were passed a frame we don't know how to deal with
        else:
            raise NotImplementedError(frame.method.__class__)

    def _handle_header_frame(self, frame):
        """
        Receive a header frame and process that, setting the next handler
        to the body frame handler
        """
        log.debug("%s._handle_header_frame: %r", self.__class__.__name__,
                      frame)

        def handler(header_frame):
            # Make sure it's a header frame
            if not isinstance(header_frame, frames.Header):
                raise exceptions.UnexpectedFrameError(header_frame)

            # Call the handle body frame including our header frame
            self._handle_body_frame(frame, header_frame)

        return handler

    def _handle_body_frame(self, method_frame, header_frame):
        """
        Receive body frames. We'll keep receiving them in handler until we've
        received the body size specified in the header frame. When done
        call our finish function which will call our transports callbacks
        """
        log.debug("%s._handle_body_frame: %r", self.__class__.__name__,
                      header_frame)
        seen_so_far = [0]
        body_fragments = list()

        def handler(body_frame):
            # Make sure it's a body frame
            if not isinstance(body_frame, frames.Body):
                raise exceptions.UnexpectedFrameError(body_frame)

            # Increment our counter so we know when we've had enough
            seen_so_far[0] += len(body_frame.fragment)

            # Append the fragment to our list
            body_fragments.append(body_frame.fragment)

            # Did we get enough bytes? If so finish
            if seen_so_far[0] == header_frame.body_size:
                finish()

            # Did we get too many bytes?
            elif seen_so_far[0] > header_frame.body_size:
                error = 'Received %i and only expected %i' % \
                        (seen_so_far[0], header_frame.body_size)
                raise exceptions.BodyTooLongError(error)

        def finish():
            # We're done so set our handler back to the method frame
            self._handler = self._handle_method_frame

            # Get our method name
            method = method_frame.method.__class__.__name__
            if method == 'Deliver':
                key = '_on_basic_deliver'
            elif method == 'GetOk':
                key = '_on_basic_get'
            elif method == 'Return':
                key = '_on_basic_return'
            else:
                raise Exception('Unimplemented Content Return Key')

            # Get our callback manager instance
            callbacks = callback.CallbackManager.instance()

            # Check for a processing callback for our method name
            callbacks.process(method_frame.channel_number,  # Prefix
                              key,                          # Key
                              self,                         # Caller
                              method_frame,                 # Arg 1
                              header_frame,                 # Arg 2
                              ''.join(body_fragments))      # Arg 3

        # If we don't have a header frame body size, finish. Otherwise keep
        # going and keep our handler function as the frame handler
        if not header_frame.body_size:
            finish()
        else:
            self._handler = handler
