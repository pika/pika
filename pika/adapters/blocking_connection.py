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

import errno
import logging
import socket

import pika.spec as spec

from pika.adapters import BaseConnection
from pika.callback import CallbackManager
from pika.channel import Channel, ChannelTransport


class BlockingConnection(BaseConnection):

    def __init__(self, parameters, reconnection_strategy=None):

        BaseConnection.__init__(self, parameters, None, reconnection_strategy)

    def connect(self, host, port):

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.socket.connect((host, port))
        self._on_connected()
        while not self.is_open:
            self.drain_events()
        return self

    def close(self, code=200, text='Normal shutdown'):
        BaseConnection.close(self, code, text)
        while self.is_open:
             self.drain_events()

    def disconnect(self):
        self.socket.close()

    def _recv(self, bufsize, timeout=None):
        prev_timeout = self.socket.gettimeout()

        self.socket.settimeout(timeout)
        try:
            return self.socket.recv(bufsize)
        except socket.timeout:
            self.socket.settimeout(prev_timeout)
        finally:
            self.socket.settimeout(prev_timeout)

    def flush_outbound(self):
        while self.outbound_buffer and self.is_open:
            fragment = self.outbound_buffer.read()
            r = self.socket.send(fragment)
            self.outbound_buffer.consume(r)

    def flush_outbound(self):
        while self.outbound_buffer:
            fragment = self.outbound_buffer.read()
            r = self.socket.send(fragment)
            self.outbound_buffer.consume(r)

    def drain_events(self, timeout=None):
        self.flush_outbound()

        try:
            buf = self._recv(self.suggested_buffer_size(), timeout)
        except socket.timeout:
            # subclass of socket.error catched below, so re-raise.
            raise
        except socket.error, exn:
            if hasattr(exn, 'errno'):
                # 2.6 and newer have an errno field.
                code = exn.errno
            else:
                # 2.5 and earlier do not, but place the errno in the first
                # exn argument.
                code = exn.args[0]

            if code == errno.EAGAIN:
                # Weird, but happens very occasionally.
                return
            else:
                return self._on_connection_closed(None, True)

        if not buf:
            return self._on_connection_closed(None, True)

        self.on_data_available(buf)

    def channel(self, channel_number=None):
        """
        Create a new channel with the next available or specified channel #
        """
        logging.debug('%s.channel' % self.__class__.__name__)

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

    def __init__(self, connection, channel_number):
        ChannelTransport.__init__(self, connection, channel_number)
        self._replies = list()
        self._frames = dict()

    def add_reply(self, reply):
        reply = self.callbacks.santize(reply)
        self._replies.append(reply)

    def remove_reply(self, frame):
        key = self.callbacks.santize(frame)
        if key in self._replies:
            self._replies.remove(key)

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

        replies = list()
        for reply in acceptable_replies:
            prefix, key = self.callbacks.add(self.channel_number, reply,
                                             self._on_rpc_complete)
            replies.append(key)

        # Send the method
        self._received_response = False

        if method.NAME in self.no_response_frame:
            wait = False
        else:
            wait = True

        self.send_method(method, None, wait)

        for reply in self._replies:
            if reply in replies:
                self._received_response = True
                if callback:
                    callback(self._frames[reply])
                del(self._frames[reply])
                break

    def _on_rpc_complete(self, frame):
        key = self.callbacks.santize(frame)
        self._replies.append(key)
        self._frames[key] = frame
        self._received_response = True

    def send_method(self, method, content=None, wait=True):
        """
        Shortcut wrapper to send a method through our connection, passing in
        our channel number
        """
        logging.debug("%s.send_method: %s(%s)" % (self.__class__.__name__,
                                                  method, content))
        self._received_response = False
        self.connection.send_method(self.channel_number, method, content)
        while wait and not self._received_response:
            self.connection.drain_events()


class BlockingChannel(Channel):

    def __init__(self, connection, channel_number, transport=None):

        # We need to do this before the channel is invoked and send_method is
        # called
        CallbackManager.instance().add(channel_number,
                                       spec.Channel.OpenOk,
                                       transport._on_rpc_complete)
        Channel.__init__(self, connection, channel_number, None, transport)

    def _open(self, frame):
        Channel._open(self, frame)
        self.transport.remove_reply(frame)

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
                                   (properties, body), False)

    def basic_consume(self, consumer,
                      queue='', no_ack=False, exclusive=False,
                      consumer_tag=None):

        if not consumer_tag:
            consumer_tag = 'ctag0'

        self._consumer = consumer

        self.transport.rpc(self._on_consume_ok,
                           spec.Basic.Consume(queue=queue,
                                              consumer_tag=consumer_tag,
                                              no_ack=no_ack,
                                              exclusive=exclusive),
                           [spec.Basic.ConsumeOk])

    def _on_consume_ok(self, frame):
        logging.debug("%s._on_consume_ok" % self.__class__.__name__)
        self._consuming = True
        while self._consuming:
            self.connection.drain_events()

    def _on_basic_deliver(self, method_frame, header_frame, body):
        logging.debug("%s._on_basic_deliver" % self.__class__.__name__)
        # Call our consumer callback with the data
        self._consumer(self,
                       method_frame.method,
                       header_frame.properties,
                       body)

    def stop_consuming(self):
        logging.debug("%s._on_consume_ok" % self.__class__.__name__)
        self._consuming = False

    def basic_get(self, ticket=0, queue=None, no_ack=False):
        self._get_response = None

        self.transport.send_method(spec.Basic.Get(ticket=ticket,
                                                  queue=queue,
                                                  no_ack=no_ack))
        return self._get_response[0], \
               self._get_response[1], \
               self._get_response[2]

    def _on_basic_get(self, method_frame, header_frame, body):
        self.transport._received_response = True
        self._get_response = method_frame.method, \
                             header_frame.properties, \
                             body

    def _on_basic_get_empty(self, frame):
        self.transport._received_response = True
        self._get_response = frame.method, None, None
