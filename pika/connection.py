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

import random

import pika.spec as spec
import pika.codec as codec
import pika.channel as channel
import pika.simplebuffer as simplebuffer
import pika.event as event
from pika.specbase import _codec_repr
from pika.exceptions import *

class PlainCredentials:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def response_for(self, start):
        if 'PLAIN' not in start.mechanisms.split():
            return None
        return ('PLAIN', '\0%s\0%s' % (self.username, self.password))

default_credentials = PlainCredentials('guest', 'guest')

class ConnectionParameters:
    def __init__(self,
                 host,
                 port = None,
                 virtual_host = "/",
                 credentials = None,
                 channel_max = 0,
                 frame_max = 131072,
                 heartbeat = 0):
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.credentials = credentials
        self.channel_max = channel_max
        self.frame_max = frame_max
        self.heartbeat = heartbeat

    def __repr__(self):
        return _codec_repr(self, lambda: ConnectionParameters(None))

class SimpleReconnectionStrategy:
    def __init__(self, initial_retry_delay = 1.0, multiplier = 2.0, max_delay = 30.0, jitter = 0.5):
        self.initial_retry_delay = initial_retry_delay
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter

        self._reset()

    def _reset(self):
        #print 'RESET'
        self.current_delay = self.initial_retry_delay
        self.attempts_since_last_success = 0

    def can_reconnect(self):
        return True

    def on_connect_attempt(self, conn):
        #print 'ATTEMPT', conn.parameters
        self.attempts_since_last_success = self.attempts_since_last_success + 1

    def on_connect_attempt_failure(self, conn):
        #print 'ATTEMPTFAILED', conn.parameters
        pass

    def on_transport_connected(self, conn):
        #print 'TXCONNECTED', conn.parameters
        pass

    def on_transport_disconnected(self, conn):
        #print "TXDISCONNECTED", conn.parameters, self.attempts_since_last_success
        pass

    def on_connection_open(self, conn):
        #print 'CONNECTED', conn.parameters
        self._reset()

    def on_connection_closed(self, conn):
        t = self.current_delay * ((random.random() * self.jitter) + 1)
        #print "RETRYING %r IN %r SECONDS (%r attempts)" % (conn.parameters, t, self.attempts_since_last_success)
        self.current_delay = min(self.max_delay, self.current_delay * self.multiplier)
        conn.delayed_call(t, conn.reconnect)

class NullReconnectionStrategy:
    def can_reconnect(self): return False
    def on_connect_attempt(self, conn): pass
    def on_connect_attempt_failure(self, conn): pass
    def on_transport_connected(self, conn): pass
    def on_transport_disconnected(self, conn): pass
    def on_connection_open(self, conn): pass
    def on_connection_closed(self, conn): pass

class Connection:
    def __init__(self, parameters, wait_for_open = True, reconnection_strategy = None):
        self.parameters = parameters
        self.reconnection_strategy = reconnection_strategy or NullReconnectionStrategy()

        self.connection_state_change_event = event.Event()

        self._reset_per_connection_state()
        self.reconnect()

        if wait_for_open:
            self.wait_for_open()

    def _reset_per_connection_state(self):
        self.state = codec.ConnectionState()
        self.server_properties = None
        self.outbound_buffer = simplebuffer.SimpleBuffer()
        self.frame_handler = self._login1
        self.channels = {}
        self.next_channel = 0
        self.connection_open = False
        self.connection_close = None
        self.bytes_sent = 0
        self.bytes_received = 0
        self.heartbeat_checker = None

    def delayed_call(self, delay_sec, callback):
        """Subclasses should override to call the callback after the
        specified number of seconds have elapsed, using a timer, or a
        thread, or similar."""
        raise NotImplementedError('Subclass Responsibility')

    def reconnect(self):
        self.ensure_closed()
        self.reconnection_strategy.on_connect_attempt(self)
        self._reset_per_connection_state()
        try:
            self.connect(self.parameters.host, self.parameters.port or spec.PORT)
            self.send_frame(self._local_protocol_header())
        except:
            self.reconnection_strategy.on_connect_attempt_failure(self)
            raise

    def connect(self, host, port):
        """Subclasses should override to set up the outbound
        socket."""
        raise NotImplementedError('Subclass Responsibility')

    def _local_protocol_header(self):
        return codec.FrameProtocolHeader(1,
                                         1,
                                         spec.PROTOCOL_VERSION[0],
                                         spec.PROTOCOL_VERSION[1])

    def on_connected(self):
        self.reconnection_strategy.on_transport_connected(self)

    def handle_connection_open(self):
        self.reconnection_strategy.on_connection_open(self)
        self.connection_state_change_event.fire(self, True)

    def handle_connection_close(self):
        self.reconnection_strategy.on_connection_closed(self)
        self.connection_state_change_event.fire(self, False)

    def addStateChangeHandler(self, handler, key = None):
        self.connection_state_change_event.addHandler(handler, key)
        if self.connection_open:
            handler(self, True)
        elif self.connection_close:
            handler(self, False)

    def delStateChangeHandler(self, key):
        self.connection_state_change_event.delHandler(key)

    def _set_connection_close(self, c):
        if not self.connection_close:
            self.connection_close = c
            for chan in self.channels.values():
                chan._set_channel_close(c)
            self.connection_open = False
            self.handle_connection_close()

    def close(self, code = 200, text = 'Normal shutdown'):
        if self.connection_open:
            self.connection_open = False
            c = spec.Connection.Close(reply_code = code,
                                      reply_text = text,
                                      class_id = 0,
                                      method_id = 0)
            self._rpc(0, c, [spec.Connection.CloseOk])
            self._set_connection_close(c)
        self._disconnect_transport()

    def ensure_closed(self):
        if self.is_alive():
            self.close()

    def _disconnect_transport(self, reason = None):
        self.disconnect_transport()
        self.on_disconnected(reason)

    def disconnect_transport(self):
        """Subclasses should override this to cause the underlying
        transport (socket) to close."""
        raise NotImplementedError('Subclass Responsibility')

    def on_disconnected(self, reason = 'Socket closed'):
        self._set_connection_close(spec.Connection.Close(reply_code = 0,
                                                         reply_text = reason,
                                                         class_id = 0,
                                                         method_id = 0))
        self.reconnection_strategy.on_transport_disconnected(self)

    def suggested_buffer_size(self):
        b = self.state.frame_max
        if not b: b = 131072
        return b

    def on_data_available(self, buf):
        while buf:
            (consumed_count, frame) = self.state.handle_input(buf)
            self.bytes_received = self.bytes_received + consumed_count
            buf = buf[consumed_count:]
            if frame:
                self.frame_handler(frame)

    def _next_channel_number(self):
        tries = 0
        limit = self.state.channel_max or 32767
        while self.next_channel in self.channels:
            self.next_channel = (self.next_channel + 1) % limit
            tries = tries + 1
            if self.next_channel == 0:
                self.next_channel = 1
            if tries > limit:
                raise NoFreeChannels()
        return self.next_channel

    def _set_channel(self, channel_number, channel):
        self.channels[channel_number] = channel

    def _ensure_channel(self, channel_number):
        if self.connection_close:
            raise ConnectionClosed(self.connection_close)
        return self.channels[channel_number]._ensure()

    def reset_channel(self, channel_number):
        if channel_number in self.channels:
            del self.channels[channel_number]

    def send_frame(self, frame):
        marshalled_frame = frame.marshal()
        self.bytes_sent = self.bytes_sent + len(marshalled_frame)
        self.outbound_buffer.write(marshalled_frame)
        #print 'Wrote %r' % (frame, )

    def send_method(self, channel_number, method, content = None):
        self.send_frame(codec.FrameMethod(channel_number, method))
        props = None
        body = None
        if isinstance(content, tuple):
            props = content[0]
            body = content[1]
        else:
            body = content
        if props:
            length = 0
            if body: length = len(body)
            self.send_frame(codec.FrameHeader(channel_number, length, props))
        if body:
            maxpiece = (self.state.frame_max - \
                        codec.ConnectionState.HEADER_SIZE - \
                        codec.ConnectionState.FOOTER_SIZE)
            body_buf = simplebuffer.SimpleBuffer( body )
            while body_buf:
                piecelen = min(len(body_buf), maxpiece)
                piece = body_buf.read_and_consume( piecelen )
                self.send_frame(codec.FrameBody(channel_number, piece))

    def _rpc(self, channel_number, method, acceptable_replies):
        channel = self._ensure_channel(channel_number)
        self.send_method(channel_number, method)
        return channel.wait_for_reply(acceptable_replies)

    def _login1(self, frame):
        if isinstance(frame, codec.FrameProtocolHeader):
            raise ProtocolVersionMismatch(self._local_protocol_header(),
                                          frame)

        if (frame.method.version_major, frame.method.version_minor) != spec.PROTOCOL_VERSION:
            raise ProtocolVersionMismatch(self._local_protocol_header(),
                                          frame)

        self.server_properties = frame.method.server_properties

        credentials = self.parameters.credentials or default_credentials
        response = credentials.response_for(frame.method)
        if not response:
            raise LoginError("No acceptable SASL mechanism for the given credentials",
                             credentials)
        self.send_method(0, spec.Connection.StartOk(client_properties = \
                                                      {"product": "Pika Python AMQP Client Library"},
                                                    mechanism = response[0],
                                                    response = response[1]))
        self.erase_credentials()
        self.frame_handler = self._login2

    def erase_credentials(self):
        """Override if in some context you need the object to forget
        its login credentials after successfully opening a connection."""
        pass

    def _login2(self, frame):
        channel_max = combine_tuning(self.parameters.channel_max, frame.method.channel_max)
        frame_max = combine_tuning(self.parameters.frame_max, frame.method.frame_max)
        heartbeat = combine_tuning(self.parameters.heartbeat, frame.method.heartbeat)
        if heartbeat:
            self.heartbeat_checker = HeartbeatChecker(self, heartbeat)
        self.state.tune(channel_max, frame_max)
        self.send_method(0, spec.Connection.TuneOk(
            channel_max = channel_max,
            frame_max = frame_max,
            heartbeat = heartbeat))
        self.frame_handler = self._generic_frame_handler
        self._install_channel0()
        self.known_hosts = \
                         self._rpc(0, spec.Connection.Open(virtual_host = \
                                                               self.parameters.virtual_host,
                                                           insist = True),
                                   [spec.Connection.OpenOk]).known_hosts
        self.connection_open = True
        self.handle_connection_open()

    def is_alive(self):
        return self.connection_open and not self.connection_close

    def _install_channel0(self):
        c = channel.ChannelHandler(self, 0)
        c.async_map[spec.Connection.Close] = self._async_connection_close

    def channel(self):
        return channel.Channel(channel.ChannelHandler(self))

    def wait_for_open(self):
        while (not self.connection_open) and \
                (self.reconnection_strategy.can_reconnect() or (not self.connection_close)):
            self.drain_events()

    def flush_outbound(self):
        """Subclasses should override to flush the contents of
        outbound_buffer out along the socket."""
        raise NotImplementedError('Subclass Responsibility')

    def drain_events(self, timeout=None):
        """Subclasses should override as required to wait for a few
        events -- perhaps running the dispatch loop once, or a small
        number of times -- and dispatch them, and then to return
        control to this method's caller, which will be waiting for
        something to have been set by one of the event
        handlers. Implementations should also take care to flush the
        outbound_buffer before blocking on input!"""
        raise NotImplementedError('Subclass Responsibility')

    def _async_connection_close(self, method_frame, header_frame, body):
        self.send_method(0, spec.Connection.CloseOk())
        self._set_connection_close(method_frame.method)

    def _generic_frame_handler(self, frame):
        #print "GENERIC_FRAME_HANDLER", frame
        if isinstance(frame, codec.FrameHeartbeat):
            pass # we already counted the received bytes for our heartbeat checker
        else:
            self.channels[frame.channel_number].frame_handler(frame)

def combine_tuning(a, b):
    if a == 0: return b
    if b == 0: return a
    return min(a, b)

class HeartbeatChecker:
    def __init__(self, connection, heartbeat_interval_sec):
        self.previous_sent = 0
        self.previous_received = 0
        self.missed_heartbeat_count = 0
        self.connection = connection
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self.setup_timer()

    def setup_timer(self):
        self.connection.delayed_call(self.heartbeat_interval_sec, self.check_heartbeats)

    def check_heartbeats(self):
        if self.previous_sent == self.connection.bytes_sent:
            # We need to send something. Send a heartbeat frame.
            self.connection.send_frame(codec.FrameHeartbeat())

        if self.previous_received == self.connection.bytes_received:
            # The server has been silent a wee while. Bump our missed-heartbeat-counter.
            self.missed_heartbeat_count = self.missed_heartbeat_count + 1
        else:
            # The server has said something. Reset our count.
            self.missed_heartbeat_count = 0

        # It's important to take the current value for bytes_sent,
        # because if we sent a heartbeat before we don't want that
        # counted as legit traffic for next time round.
        self.previous_sent = self.connection.bytes_sent
        self.previous_received = self.connection.bytes_received

        if self.missed_heartbeat_count >= 2:
            self.connection._disconnect_transport("Too many missed heartbeats")
        elif self.connection.heartbeat_checker is self:
            # Only install the timer again if the connection still
            # thinks it cares about this instance.
            self.setup_timer()
        else:
            pass
