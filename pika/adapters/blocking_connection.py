# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import socket
import time
import types

import pika.log as log
import pika.spec as spec

from pika.adapters import BaseConnection
from pika.channel import Channel, ChannelTransport
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

    def __init__(self, parameters=None, reconnection_strategy=None):
        BaseConnection.__init__(self, parameters, None, reconnection_strategy)

    def _adapter_connect(self):
        BaseConnection._adapter_connect(self)
        self.socket.setblocking(1)
        self.socket.settimeout(SOCKET_TIMEOUT)
        self._socket_timeouts = 0
        self._on_connected()
        self._timeouts = dict()
        while not self.is_open:
            self._flush_outbound()
            self._handle_read()
        return self

    def close(self, code=200, text='Normal shutdown'):
        BaseConnection.close(self, code, text)
        while self.is_open:
            try:
                self.process_data_events()
            except AMQPConnectionError:
                break

    def disconnect(self):
        self.socket.close()

    def _adapter_disconnect(self):
        """
        Called if we are forced to disconnect for some reason from Connection
        """
        # Close our socket
        self.socket.close()

    def _handle_disconnect(self):
        """
        Called internally when we know our socket is disconnected already
        """
        # Close the socket
        self.socket.close()
        # Close up our Connection state
        self._on_connection_closed(None, True)

    def _flush_outbound(self):
        try:
            self._handle_write()
            self._socket_timeouts = 0
        except socket.timeout:
            self._socket_timeouts += 1
            if self._socket_timeouts > SOCKET_TIMEOUT_THRESHOLD:
                log.error(SOCKET_TIMEOUT_MESSAGE)
                self._handle_disconnect()

    def process_data_events(self):
        # Make sure we're open, if not raise the exception
        if not self.is_open and not self.closing:
            raise AMQPConnectionError

        # Write our data
        self._flush_outbound()

        # Read data
        try:
            self._handle_read()
            self._socket_timeouts = 0
        except socket.timeout:
            self._socket_timeouts += 1
            if self._socket_timeouts > SOCKET_TIMEOUT_THRESHOLD:
                log.error(SOCKET_TIMEOUT_MESSAGE)
                self._handle_disconnect()

        # Process our timeout events
        self.process_timeouts()

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

    def add_timeout(self, delay_sec, callback):
        """
        Add a timeout calling callback to our stack that will execute
        in delay_sec.
        """
        deadline = time.time() + delay_sec
        timeout_id = '%.8f' % time.time()
        self._timeouts[timeout_id] = {'deadline': deadline,
                                      'handler': callback}
        return timeout_id

    def remove_timeout(self, timeout_id):
        """
        Remove a timeout from the stack
        """
        if timeout_id in self._timeouts:
            del self._timeouts[timeout_id]

    def process_timeouts(self):
        """
        Process our self._timeouts event stack
        """
        # Process our timeout events
        keys = self._timeouts.keys()

        start_time = time.time()
        for timeout_id in keys:
            if timeout_id in self._timeouts and \
                self._timeouts[timeout_id]['deadline'] <= start_time:
                log.debug('%s: Timeout calling %s',
                          self.__class__.__name__,
                          self._timeouts[timeout_id]['handler'])
                self._timeouts[timeout_id]['handler']()
                del(self._timeouts[timeout_id])


class BlockingChannelTransport(ChannelTransport):

    no_response_frame = ['Basic.Ack', 'Basic.Reject', 'Basic.RecoverAsync']

    def __init__(self, connection, channel_number):
        ChannelTransport.__init__(self, connection, channel_number)
        self._replies = list()
        self._frames = dict()
        self._wait = False

    def add_reply(self, reply):
        reply = self.callbacks.sanitize(reply)
        self._replies.append(reply)

    def remove_reply(self, frame):
        key = self.callbacks.sanitize(frame)
        if key in self._replies:
            self._replies.remove(key)

    def rpc(self, method, callback=None, acceptable_replies=None):
        """
        Shortcut wrapper to the Connection's rpc command using its callback
        stack, passing in our channel number
        """
        # Make sure the channel is open
        self._ensure()

        # Validate we got None or a list of acceptable_replies
        if acceptable_replies and not isinstance(acceptable_replies, list):
            raise TypeError("acceptable_replies should be list or None")

        # Validate the callback is a function or instancemethod
        if callback and not isinstance(callback, types.FunctionType) and \
           not isinstance(callback, types.MethodType):
            raise TypeError("callback should be None, a function or method.")

        replies = list()
        if acceptable_replies:
            for reply in acceptable_replies:
                prefix, key = self.callbacks.add(self.channel_number,
                                                 reply,
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

    def _on_rpc_complete(self, frame):
        key = self.callbacks.sanitize(frame)
        self._replies.append(key)
        self._frames[key] = frame
        self._received_response = True

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

    def __init__(self, connection, channel_number, transport=None):

        # We need to do this before the channel is invoked and send_method is
        # called
        connection.callbacks.add(channel_number,
                                 spec.Channel.OpenOk,
                                 transport._on_rpc_complete)
        Channel.__init__(self, connection, channel_number, None, transport)
        self.basic_get_ = Channel.basic_get
        self._consumers = {}

    def _open(self, frame):
        Channel._open(self, frame)
        self.transport.remove_reply(frame)

    def _on_remote_close(self, frame):
        Channel._on_remote_close(self, frame)
        raise AMQPChannelError(frame.method.reply_code,
                               frame.method.reply_text)

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

    def start_consuming(self):
        """
        Starts consuming from registered callbacks.
        """
        # Block while we have registered consumers
        while len(self._consumers):
            self.transport.connection.process_data_events()

    def stop_consuming(self, consumer_tag=None):
        """
        Sends off the Basic.Cancel to let RabbitMQ know to stop consuming and
        sets our internal state to exit out of the basic_consume.
        """
        if consumer_tag:
            self.basic_cancel(consumer_tag)
        else:
            for consumer_tag in self._consumers.keys():
                self.basic_cancel(consumer_tag)
        self.transport.wait = False

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
            self.transport.connection.process_data_events()

        return self._get_response[0], \
               self._get_response[1], \
               self._get_response[2]

    def _on_basic_get(self, caller, method_frame, header_frame, body):
        self.transport._received_response = True
        self._get_response = method_frame, \
                             header_frame, \
                             body

    def _on_basic_get_empty(self, frame):
        self.transport._received_response = True
        self._get_response = frame.method, None, None
