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
import socket
import pika.log as log
import pika.spec as spec

from pika.adapters import BaseConnection
from pika.callback import CallbackManager
from pika.channel import Channel, ChannelTransport, ContentHandler
from pika.exceptions import AMQPConnectionError, AMQPChannelError

SOCKET_TIMEOUT = 1
SOCKET_TIMEOUT_THRESHOLD = 100
SOCKET_TIMEOUT_MESSAGE = "BlockingConnection: Timeout exceeded, disconnected"


class BlockingConnection(BaseConnection):
    """
    The BlockingConnection adapter is meant for simple implementations where
    you want to have blocking behavior. The behavior layered on top of the
    async library. Because of the nature of AMQP there are a few callbacks
    one needs to do, even in a blocking implementation. These include receiving
    messages from Basic.Deliver, Basic.GetOk, and Basic.Return.
    """

    @log.method_call
    def __init__(self, parameters=None, reconnection_strategy=None):
        BaseConnection.__init__(self, parameters, None, reconnection_strategy)

    @log.method_call
    def _adapter_connect(self, host, port):

        BaseConnection._adapter_connect(self, host, port)
        self.socket.setblocking(1)
        self.socket.settimeout(SOCKET_TIMEOUT)
        self._socket_timeouts = 0
        self._on_connected()
        while not self.is_open:
            self._flush_outbound()
            self._handle_read()
        return self

    @log.method_call
    def close(self, code=200, text='Normal shutdown'):
        BaseConnection.close(self, code, text)
        while self.is_open:
            try:
                self.process_data_events()
            except AMQPConnectionError:
                break

    @log.method_call
    def disconnect(self):
        self.socket.close()

    @log.method_call
    def _handle_disconnect(self):
        """
        Called internally when we know our socket is disconnected already
        """
        # Close the socket
        self.socket.close()
        # Close up our Connection state
        self._on_connection_closed(None, True)

    @log.method_call
    def _flush_outbound(self):
        try:
            self._handle_write()
            self._socket_timeouts = 0
        except socket.timeout:
            self._socket_timeouts += 1
            if self._socket_timeouts > SOCKET_TIMEOUT_THRESHOLD:
                log.error(SOCKET_TIMEOUT_MESSAGE)
                self._handle_disconnect()

    @log.method_call
    def process_data_events(self):
        if not self.is_open:
            raise AMQPConnectionError
        self._flush_outbound()
        try:
            self._handle_read()
            self._socket_timeouts = 0
        except socket.timeout:
            self._socket_timeouts += 1
            if self._socket_timeouts > SOCKET_TIMEOUT_THRESHOLD:
                log.error(SOCKET_TIMEOUT_MESSAGE)
                self._handle_disconnect()

    @log.method_call
    def channel(self, channel_number=None):
        """
        Create a new channel with the next available or specified channel #
        """
        # We'll loop on this
        self._channel_open = False

        # If the user didn't specify a channel_number get the next avail
        if not channel_number:
            channel_number = self._next_channel_number()

        # Add the channel spec.Channel.CloseOk callback for _on_channel_close
        self.callbacks.add(channel_number,
                           spec.Channel.CloseOk,
                           self._on_channel_close)

        # Add it to our Channel dictionary
        transport = BlockingChannelTransport(self, channel_number)
        self._channels[channel_number] = BlockingChannel(self,
                                                         channel_number,
                                                         transport)
        return self._channels[channel_number]


class BlockingChannelTransport(ChannelTransport):

    no_response_frame = ['Basic.Ack', 'Basic.Reject', 'Basic.RecoverAsync']

    @log.method_call
    def __init__(self, connection, channel_number):
        ChannelTransport.__init__(self, connection, channel_number)
        self._replies = list()
        self._frames = dict()
        self._wait = False

    @log.method_call
    def add_reply(self, reply):
        reply = self.callbacks.sanitize(reply)
        self._replies.append(reply)

    @log.method_call
    def remove_reply(self, frame):
        key = self.callbacks.sanitize(frame)
        if key in self._replies:
            self._replies.remove(key)

    @log.method_call
    def rpc(self, method, callback=None, acceptable_replies=[]):
        """
        Shortcut wrapper to the Connection's rpc command using its callback
        stack, passing in our channel number
        """
        # Make sure the channel is open
        self._ensure()

        replies = list()
        for reply in acceptable_replies:
            prefix, key = self.callbacks.add(self.channel_number, reply,
                                             self._on_rpc_complete)
            replies.append(key)

        # Send the method
        self._received_response = False

        if method.NAME in BlockingChannelTransport.no_response_frame:
            wait = False
        else:
            wait = True

        self.send_method(method, None, wait)

        # Find our reply in our list of replies
        for reply in self._replies:
            if reply in replies:
                frame = self._frames[reply]
                self._received_response = True
                if callback:
                    callback(frame)
                del(self._frames[reply])
                return frame

    @log.method_call
    def _on_rpc_complete(self, frame):
        key = self.callbacks.sanitize(frame)
        self._replies.append(key)
        self._frames[key] = frame
        self._received_response = True

    @log.method_call
    def send_method(self, method, content=None, wait=True):
        """
        Shortcut wrapper to send a method through our connection, passing in
        our channel number
        """
        self.wait = wait
        self._received_response = False
        self.connection._send_method(self.channel_number, method, content)
        while self.wait and not self._received_response:
            try:
                self.connection.process_data_events()
            except AMQPConnectionError:
                break


class BlockingChannel(Channel):

    @log.method_call
    def __init__(self, connection, channel_number, transport=None):

        # We need to do this before the channel is invoked and send_method is
        # called
        CallbackManager.instance().add(channel_number,
                                       spec.Channel.OpenOk,
                                       transport._on_rpc_complete)
        Channel.__init__(self, connection, channel_number, None, transport)
        self.basic_get_ = Channel.basic_get

    @log.method_call
    def _open(self, frame):
        Channel._open(self, frame)
        self.transport.remove_reply(frame)

    @log.method_call
    def _on_remote_close(self, frame):
        Channel._on_remote_close(self, frame)
        raise AMQPChannelError(frame.reply_code,
                               frame.reply_text)

    @log.method_call
    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """
        Publish to the channel with the given exchange, routing key and body.

        If flow control is enabled and you publish a message while another is
        sending, a ContentTransmissionForbidden exception ill be generated
        """
        # If properties are not passed in, use the spec's default
        properties = properties or spec.BasicProperties()
        self.transport.send_method(spec.Basic.Publish(exchange=exchange,
                                                      routing_key=routing_key,
                                                      mandatory=mandatory,
                                                      immediate=immediate),
                                   (properties, body), False)

    @log.method_call
    def basic_consume(self, consumer,
                      queue='', no_ack=False, exclusive=False,
                      consumer_tag=None):
        """
        Sends the AMQP command Basic.Consume to the broker and binds messages
        for the consumer_tag to the consumer callback. If you do not pass in
        a consumer_tag, one will be automatically generated for you. For
        more information on basic_consume, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        NOTE: This blocks further execution until you call the
        BlockingChannel.stop_consuming() method.
        """
        # Setup a default consumer tag if one was not passed
        consumer_tag = consumer_tag or 'ctag0'

        self._consumer = consumer
        self._consuming = True
        self.transport.rpc(spec.Basic.Consume(queue=queue,
                                              consumer_tag=consumer_tag,
                                              no_ack=no_ack,
                                              exclusive=exclusive),
                           None, [spec.Basic.ConsumeOk])

        # Block while we are consuming
        while self._consuming:
            self.connection.process_data_events()


    @log.method_call
    def _on_basic_deliver(self, method_frame, header_frame, body):
        # Call our consumer callback with the data
        self._consumer(self,
                       method_frame.method,
                       header_frame.properties,
                       body)

    @log.method_call
    def stop_consuming(self, consumer_tag=None):
        """
        Sends off the Basic.Cancel to let RabbitMQ know to stop consuming and
        sets our internal state to exit out of the basic_consume.
        """
        self.basic_cancel(consumer_tag or 'ctag0')
        self.transport.wait = False
        self._consuming = False

    @log.method_call
    def basic_get(self, ticket=0, queue=None, no_ack=False):
        """
        Get a single message from the AMQP broker. The response will include
        either a single method frame of Basic.GetEmpty or three frames:
        the method frame (Basic.GetOk), header frame and
        the body, like the reponse from Basic.Consume.  For more information
        on basic_get and its parameters, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get
        """
        self._get_response = None
        self.basic_get_(self, self._on_basic_get, ticket, queue, no_ack)
        while not self._get_response:
            self.connection.process_data_events()

        return self._get_response[0], \
               self._get_response[1], \
               self._get_response[2]

    @log.method_call
    def _on_basic_get(self, caller, method_frame, header_frame, body):
        self.transport._received_response = True
        self._get_response = method_frame, \
                             header_frame, \
                             body

    @log.method_call
    def _on_basic_get_empty(self, frame):
        self.transport._received_response = True
        self._get_response = frame.method, None, None
