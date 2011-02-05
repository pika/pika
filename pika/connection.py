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
import struct

import pika.channel as channel
import pika.frames as frames
import pika.log as log
import pika.simplebuffer as simplebuffer
import pika.spec as spec

from pika.callback import CallbackManager
from pika.credentials import PlainCredentials
from pika.exceptions import *
from pika.heartbeat import HeartbeatChecker
from pika.reconnection_strategies import NullReconnectionStrategy

CHANNEL_MAX = 32767
FRAME_MAX = 131072
PRODUCT = "Pika Python AMQP Client Library"

# Module wide default credentials for default RabbitMQ configurations
default_credentials = PlainCredentials('guest', 'guest')


class ConnectionParameters(object):
    """
    Connection parameters object that is passed into the connection adapter
    upon construction. The following parameters are passed and are used to
    negotiate communication with RabbitMQ:

    - host: Hostname or IP Address to connect to, defaults to localhost.
    - port: TCP port to connect to, defaults to 5672
    - virtual_host: RabbitMQ virtual host to use, defaults to /
    - credentials: A instance of a credentials class to authenticate with.
      Defaults to PlainCredentials for the guest user.
    - channel_max: Maximum number of channels to allow, defaults to 0 for None
    - frame_max: The maximum byte size for an AMQP frame. Defaults to 131072
    - heartbeat: Turn heartbeat checking on or off. Defaults to 0 for Off.
    """
    def __init__(self,
                 host='localhost',
                 port=spec.PORT,
                 virtual_host='/',
                 credentials=None,
                 channel_max=0,
                 frame_max=FRAME_MAX,
                 heartbeat=0):

        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.credentials = credentials
        self.channel_max = channel_max
        self.frame_max = frame_max
        self.heartbeat = heartbeat


class Connection(object):

    """
    Pika Connection Class

    This is the core class that implements communication with RabbitMQ. This
    class should not be invoked directly but rather through the use of an
    adapter such as SelectConnection or BlockingConnection.
    """
    def __init__(self, parameters=None, on_open_callback=None,
                 reconnection_strategy=None):
        """
        Connection initialization expects a ConnectionParameters object and
        a callback function to notify when we have successfully connected
        to the AMQP Broker.

        A reconnection_strategy of None will use the NullReconnectionStrategy
        """
        # Define our callback dictionary
        self.callbacks = CallbackManager.instance()

        # On connection callback
        if on_open_callback:
            self.add_on_open_callback(on_open_callback)

        # Set our configuration options
        self.parameters = parameters or ConnectionParameters()

        # If we did not pass in a reconnection_strategy, setup the default
        self.reconnection = reconnection_strategy or NullReconnectionStrategy()

        # Add our callback for if we close by being disconnected
        self.add_on_close_callback(self.reconnection.on_connection_closed)

        # Set all of our default connection state values
        self._init_connection_state()

        # Connect to the AMQP Broker
        self._connect()

    def _init_connection_state(self):
        """
        Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped
        """
        log.debug('%s._init_connection_state', self.__class__.__name__)

        # Outbound buffer for buffering writes until we're able to send them
        self.outbound_buffer = simplebuffer.SimpleBuffer()

        # Connection state, server properties and channels all change on
        # each connection
        self.state = ConnectionState()
        self.server_properties = None
        self._channels = dict()

        # Data used for Heartbeat checking and backpressure detection
        self.bytes_sent = 0
        self.bytes_received = 0
        self.frames_sent = 0
        self.heartbeat = None

        # Default backpressure multiplier value
        self._backpressure = 10

        # AMQP Lifecycle States
        self.closed = True
        self.closing = False
        self.open = False

        # Our starting point once connected, first frame received
        self.callbacks.add(0, spec.Connection.Start, self._on_connection_start)

    def _adapter_connect(self, host, port):
        """
        Subclasses should override to set up the outbound socket connection
        """
        raise NotImplementedError('%s needs to implement this function' %\
                                  self.__class__.__name__)

    def _adapter__disconnect(self):
        """
        Subclasses should override this to cause the underlying
        transport (socket) to close.
        """
        raise NotImplementedError('%s needs to implement this function' %\
                                  self.__class__.__name__)

    def _connect(self):
        """
        Call the Adapter's connect method after letting the
        ReconnectionStrategy know that we're going to do so.
        """
        log.debug('%s._connect', self.__class__.__name__)

        # Let our RS know what we're up to
        self.reconnection.on_connect_attempt(self)

        # Try and connect and send the first frame
        self._adapter_connect(self.parameters.host,
                              self.parameters.port or  spec.PORT)

    def _reconnect(self):
        """
        Called by the Reconnection Strategy classes or Adapters to disconnect
        and reconnect to the broker
        """
        log.debug('%s.reconnect', self.__class__.__name__)

        # We're already closing but it may not be from reconnect, so first
        # Add a callback that won't be duplicated
        if self.closing:
            self.add_on_close_callback(self._reconnect)
            return

        # If we're open, we want to close normally if we can, then actually
        # reconnect via callback that can't be added more than once
        if self.open:
            self.add_on_close_callback(self._reconnect)
            self._ensure_closed()
            return

        # We're not closing and we're not open, so reconnect
        self._init_connection_state()
        self._connect()

    def _on_connected(self):
        """
        This is called by our connection Adapter to let us know that we've
        connected and we can notify our connection strategy
        """
        log.debug('%s.on_connected', self.__class__.__name__)

        # Start the communication with the RabbitMQ Broker
        self._send_frame(frames.ProtocolHeader())

        # Let our reconnection_strategy know we're connected
        self.reconnection.on_transport_connected(self)

    def _on_connection_open(self, frame):
        """
        This is called once we have tuned the connection with the server and
        called the Connection.Open on the server and it has replied with
        Connection.Ok.
        """
        log.debug('%s._on_connection_open', self.__class__.__name__)

        self.known_hosts = frame.method.known_hosts

        # Add a callback handler for the Broker telling us to disconnect
        self.callbacks.add(0, spec.Connection.Close, self._on_remote_close)

        # We're now connected at the AMQP level
        self.open = True

        # Call our initial callback that we're open
        self.callbacks.process(0, '_on_connection_open', self, self)

    def _on_connection_start(self, frame):
        """
        This is called as a callback once we have received a Connection.Start
        from the server.
        """
        log.debug('%s._on_connection_start', self.__class__.__name__)

        # We're now connected to the broker
        self.closed = False

        # We didn't expect a FrameProtocolHeader, did we get one?
        if isinstance(frame, frames.ProtocolHeader):
            raise ProtocolVersionMismatch(frames.ProtocolHeader, frame)

        # Make sure that the major and minor version matches our spec version
        if (frame.method.version_major,
            frame.method.version_minor) != spec.PROTOCOL_VERSION[0:2]:
            raise ProtocolVersionMismatch(frames.ProtocolHeader(), frame)

        # Get our server properties for use elsewhere
        self.server_properties = frame.method.server_properties

        # Use the default credentials if the user didn't pass any in
        credentials = self.parameters.credentials or default_credentials

        # Build our StartOk authentication response
        response = credentials.response_for(frame.method)

        # Server asked for credentials for a method we don't support so raise
        # an exception to let the implementing app know
        if not response:
            raise LoginError("No %s support for the credentials" %\
                             self.parameters.credentials.TYPE)

        # Erase our credentials if we don't want to retain them in the state
        # of the connection. By default this is a noop function but adapters
        # may override this
        self._erase_credentials()

        # Add our callback for our Connection Tune event
        self.callbacks.add(0, spec.Connection.Tune, self._on_connection_tune)

        # Send our Connection.StartOk
        method = spec.Connection.StartOk(client_properties={"product":
                                                            PRODUCT},
                                        mechanism=response[0],
                                        response=response[1])
        self._send_method(0, method)

    def _erase_credentials(self):
        """
        Override if in some context you need the object to forget
        its login credentials after successfully opening a connection.
        """
        pass

    def _combine(self, a, b):
        """
        Pass in two values, if a is 0, return b otherwise if b is 0, return a.
        If neither case matches return the smallest value.
        """
        return min(a, b) or (a or b)

    def _on_connection_tune(self, frame):
        """
        Once the Broker sends back a Connection.Tune, we will set our tuning
        variables that have been returned to us and kick off the Heartbeat
        monitor if required, send our TuneOk and then the Connection. Open rpc
        call on channel 0
        """
        log.debug('%s._on_connection_tune', self.__class__.__name__)
        cmax = self._combine(self.parameters.channel_max,
                             frame.method.channel_max)
        fmax = self._combine(self.parameters.frame_max,
                             frame.method.frame_max)
        hint = self._combine(self.parameters.heartbeat,
                             frame.method.heartbeat)

        # If we have a heartbeat interval, create a heartbeat checker
        if hint:
            self.heartbeat = HeartbeatChecker(self, hint)

        # Update our connection state with our tuned values
        self.state.tune(cmax, fmax)

        # Send the TuneOk response with what we've agreed upon
        self._send_method(0, spec.Connection.TuneOk(channel_max=cmax,
                                                    frame_max=fmax,
                                                    heartbeat=hint))

        # Send the Connection.Open RPC call for the vhost
        cmd = spec.Connection.Open(virtual_host=self.parameters.virtual_host,
                                   insist=True)
        self._rpc(0, cmd, self._on_connection_open, [spec.Connection.OpenOk])

    def close(self, code=200, text='Normal shutdown'):
        """
        Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.
        """
        log.debug("%s.close Closing Connection: (%s) %s",
                      self.__class__.__name__, code, text)

        if self.closing or self.closed:
            log.warning("%s.Close invoked while closing or closed",
                            self.__class__.__name__)
            return

        # Carry our code and text around with us
        self.closing = code, text

        # Remove the reconnection strategy callback for when we close
        self.callbacks.remove(0, '_on_connection_close',
                              self.reconnection.on_connection_closed)

        # If we're not already closed
        for channel_number in self._channels.keys():
            self._channels[channel_number].close(code, text)

        # If we already dont have any channels, close out
        if not self._channels:
            self._on_close_ready()

    def _on_close_ready(self):
        """
        On a clean shutdown we'll call this once all of our channels are closed
        Let the Broker know we want to close
        """
        log.debug('%s._on_close_ready', self.__class__.__name__)

        if self.closed:
            log.warn("%s.on_close_ready invoked while closed",
                         self.__class__.__name__)
            return

        self._rpc(0, spec.Connection.Close(self.closing[0],
                                          self.closing[1], 0, 0),
                  self._on_connection_closed,
                  [spec.Connection.CloseOk])

    def _on_connection_closed(self, frame, from_adapter=False):
        """
        Let both our RS and Event object know we closed
        """
        log.debug('%s._on_connection_close', self.__class__.__name__)

        # Set that we're actually closed
        self.closed = True
        self.closing = False
        self.open = False

        # Call any callbacks registered for this
        self.callbacks.process(0, '_on_connection_closed', self, self)

        # Disconnect our transport if it didn't call on_disconnected
        if not from_adapter:
            self._adapter_disconnect()

    def _on_remote_close(self, frame):
        """
        We've received a remote close from the server
        """
        log.debug('%s._on_remote_close: %r',
                      self.__class__.__name__, frame)
        self.close(frame.method.reply_code, frame.method.reply_text)

    def _ensure_closed(self):
        """
        If we're not already closed, make sure we're closed
        """
        log.debug('%s._ensure_closed', self.__class__.__name__)

        # We carry the connection state and so we want to close if we know
        if self.is_open and not self.closing:
            self.close()

    @property
    def is_open(self):
        """
        Returns a boolean reporting the current connection state
        """
        return self.open and (not self.closing and not self.closed)

    def add_on_close_callback(self, callback):
        """
        Add a callback notification when the connection has closed
        """
        log.debug('%s.add_on_close_callback: %s', self.__class__.__name__,
                      callback)

        self.callbacks.add(0, '_on_connection_closed', callback, False)

    def add_on_open_callback(self, callback):
        """
        Add a callback notification when the connection has opened
        """
        log.debug('%s.add_on_open_callback: %s',
                      self.__class__.__name__, callback)

        self.callbacks.add(0, '_on_connection_open', callback, False)

    def add_backpressure_callback(self, callback):
        """
        Add a callback notification when we think backpressue is being applied
        due to the size of the output buffer being exceeded.
        """
        log.debug('%s.add_backpressure_callback: %s', self.__class__.__name__,
                  callback)

        self.callbacks.add(0, 'backpressure', callback, False)

    def set_backpressure_multiplier(self, value=10):
        """
        Alter the backpressure multiplier value. We set this to 10 by default.
        This value is used to raise warnings and trigger the backpressure
        callback.
        """
        self._backpressure = value

    def add_timeout(self, delay_sec, callback):
        """
        Adapters should override to call the callback after the
        specified number of seconds have elapsed, using a timer, or a
        thread, or similar.
        """
        raise NotImplementedError('%s needs to implement this function ' %\
                                  self.__class__.__name__)

    def remove_timeout(self, callback):
        """
        Adapters should override to call the callback after the
        specified number of seconds have elapsed, using a timer, or a
        thread, or similar.
        """
        raise NotImplementedError('%s needs to implement this function' %\
                                  self.__class__.__name__)

    # Channel related functionality

    def channel(self, on_open_callback, channel_number=None):
        """
        Create a new channel with the next available channel number or pass in
        a channel number to use. Must be non-zero if you would like to specify
        but it is recommended that you let Pika manage the channel numbers.
        """
        log.debug('%s.channel', self.__class__.__name__)

        # If the user didn't specify a channel_number get the next avail
        if not channel_number:
            channel_number = self._next_channel_number()

        # Add the channel spec.Channel.CloseOk callback for _on_channel_close
        self.callbacks.add(channel_number, spec.Channel.CloseOk,
                           self._on_channel_close)

        # Add it to our Channel dictionary
        self._channels[channel_number] = channel.Channel(self, channel_number,
                                                         on_open_callback)

        # Add the callback for our Channel.Close event in case the Broker
        # wants to close us for some reason
        self.callbacks.add(channel_number, spec.Channel.Close,
                           self._on_channel_close)

    def _next_channel_number(self):
        """
        Return the next available channel number or raise on exception
        """
        # Our limit is the the Codec's Channel Max or MAX_CHANNELS if it's None
        limit = self.state.channel_max or CHANNEL_MAX

        # We've used all of our channels
        if len(self._channels) == limit:
            raise NoFreeChannels()

        # Get a list of all of our keys, all should be numeric channel ids
        channel_numbers = self._channels.keys()

        # We don't start with any open channels
        if not channel_numbers:
            return 1

        # Our next channel is the max key value + 1
        return max(channel_numbers) + 1

    def _on_channel_close(self, frame):
        """
        RPC Response from when a channel closes itself, remove from our stack
        """
        log.debug('%s._on_channel_close: %s', self.__class__.__name__,
                      frame.channel_number)

        channel_number = frame.channel_number
        # If we have this channel number in our channels:
        if channel_number in self._channels:
            # If our server called Channel.Close on us remotely
            if isinstance(frame.method, spec.Channel.Close):
                # Call the channel.close() function letting it know it came
                # From the server.
                self._channels[channel_number].close(frame.method.reply_code,
                                                     frame.method.reply_text,
                                                     True)  # Forced close
            # Remove the channel from our dict
            del(self._channels[channel_number])

        # If we're closing and don't have any channels, go to the next step
        if self.closing and not self._channels:
            self._on_close_ready()

    # Data packet and frame handling functions

    def _on_data_available(self, data):
        """
        This is called by our Adapter, passing in the data from the socket
        As long as we have buffer try and map out frame data
        """
        while data:
            consumed_count, frame = self.state.handle_input(data)
            # If we don't have a frame, exit
            if not frame:
                break

            # Remove the frame we just consumed from our data
            data = data[consumed_count:]

            # Increment our bytes received buffer for heartbeat checking
            self.bytes_received += consumed_count

            # If we have a Method Frame and have callbacks for it
            if isinstance(frame, frames.Method) and \
                self.callbacks.pending(frame.channel_number, frame.method):

                # Process the callbacks for it
                self.callbacks.process(frame.channel_number,  # Prefix
                                       frame.method,          # Key
                                       self,                  # Caller
                                       frame)                 # Args

            # We don't check for heartbeat frames because we can not count
            # atomic frames reliably due to different message behaviors
            # such as large content frames being transferred slowly
            elif isinstance(frame, frames.Heartbeat):
                continue

            elif frame.channel_number > 0:
                # Call our Channel Handler with the frame
                self._channels[frame.channel_number].transport.deliver(frame)

    def _rpc(self, channel_number, method,
            callback=None, acceptable_replies=[]):
        """
        Make an RPC call for the given callback, channel number and method.
        acceptable_replies lists out what responses we'll process from the
        server with the specified callback.
        """

        # If we were passed a callback, add it to our stack
        if callback:
            for reply in acceptable_replies:
                self.callbacks.add(channel_number, reply, callback)

        # Send the rpc call to RabbitMQ
        self._send_method(channel_number, method)

    def _send_frame(self, frame):
        """
        This appends the fully generated frame to send to the broker to the
        output buffer which will be then sent via the connection adapter
        """
        log.debug('%s._send_frame: %r', self.__class__.__name__, frame)

        marshalled_frame = frame.marshal()
        self.bytes_sent += len(marshalled_frame)
        self.frames_sent += 1
        self.outbound_buffer.write(marshalled_frame)
        self._flush_outbound()
        avg_frame_size = self.bytes_sent / self.frames_sent
        if self.outbound_buffer.size > (avg_frame_size * self._backpressure):
            est_frames_behind = self.outbound_buffer.size / avg_frame_size
            message = "Pika: Write buffer exceeded warning threshold" + \
                      " at %i bytes and an estimated %i frames behind"
            log.warning(message, self.outbound_buffer.size, est_frames_behind)
            self.callbacks.process(0, 'backpressure', self)

    def _flush_outbound(self):
        """
        Adapters should override to flush the contents of
        outbound_buffer out along the socket.
        """
        raise NotImplementedError('%s needs to implement this function ' %\
                                  self.__class__.__name__)

    def _send_method(self, channel_number, method, content=None):
        """
        Constructs a RPC method frame and then sends it to the broker
        """
        log.debug('%s.send_method(%i, %s, %s)', self.__class__.__name__,
                      channel_number, method, content)
        self._send_frame(frames.Method(channel_number, method))

        if isinstance(content, tuple):
            props = content[0]
            body = content[1]
        else:
            props = None
            body = content

        if props:
            length = 0
            if body:
                length = len(body)
            self._send_frame(frames.Header(channel_number, length, props))

        if body:
            max_piece = (self.state.frame_max - \
                         ConnectionState.HEADER_SIZE - \
                         ConnectionState.FOOTER_SIZE)
            body_buf = simplebuffer.SimpleBuffer(body)

            while body_buf:
                piece_len = min(len(body_buf), max_piece)
                piece = body_buf.read_and_consume(piece_len)
                self._send_frame(frames.Body(channel_number, piece))

    @property
    def _suggested_buffer_size(self):
        """
        Return the suggested buffer size from the connection state/tune or the
        default if that is None
        """
        return self.state.frame_max or FRAME_MAX


class ConnectionState(object):

    HEADER_SIZE = 7
    FOOTER_SIZE = 1

    def __init__(self):

        self.channel_max = None
        self.frame_max = None
        self._return_to_idle()

    def tune(self, channel_max, frame_max):

        self.channel_max = channel_max
        self.frame_max = frame_max

    def _return_to_idle(self):

        self.inbound_buffer = list()
        self.inbound_available = 0
        self.target_size = ConnectionState.HEADER_SIZE
        self.state = self._waiting_for_header

    def _inbound(self):

        return ''.join(self.inbound_buffer)

    def handle_input(self, received_data):

        total_bytes_consumed = 0

        while True:
            if not received_data:
                return total_bytes_consumed, None

            bytes_consumed = self.target_size - self.inbound_available
            if len(received_data) < bytes_consumed:
                bytes_consumed = len(received_data)

            self.inbound_buffer.append(received_data[:bytes_consumed])
            self.inbound_available += bytes_consumed
            received_data = received_data[bytes_consumed:]
            total_bytes_consumed += bytes_consumed

            if self.inbound_available < self.target_size:
                return total_bytes_consumed, None

            maybe_result = self.state(self._inbound())
            if maybe_result:
                return total_bytes_consumed, maybe_result

    def _waiting_for_header(self, inbound):
        # Here we switch state without resetting the inbound_buffer,
        # because we want to keep the frame header.

        if inbound[:3] == 'AMQ':
            # Protocol header.
            self.target_size = 8
            self.state = self._waiting_for_protocol_header
        else:
            self.target_size = struct.unpack_from('>I', inbound, 3)[0] + \
                               ConnectionState.HEADER_SIZE + \
                               ConnectionState.FOOTER_SIZE
            self.state = self._waiting_for_body

    def _waiting_for_body(self, inbound):

        if ord(inbound[-1]) != spec.FRAME_END:
            raise InvalidFrameError("Invalid frame end byte", inbound[-1])

        self._return_to_idle()

        (frame_type, channel_number) = struct.unpack_from('>BH', inbound, 0)
        if frame_type == spec.FRAME_METHOD:
            method_id = struct.unpack_from('>I', inbound,
                                           ConnectionState.HEADER_SIZE)[0]
            method = spec.methods[method_id]()
            method.decode(inbound, ConnectionState.HEADER_SIZE + 4)
            return frames.Method(channel_number, method)
        elif frame_type == spec.FRAME_HEADER:
            (class_id, body_size) = \
                struct.unpack_from('>HxxQ', inbound,
                                   ConnectionState.HEADER_SIZE)
            props = spec.props[class_id]()
            props.decode(inbound, ConnectionState.HEADER_SIZE + 12)
            return frames.Header(channel_number, body_size, props)
        elif frame_type == spec.FRAME_BODY:
            return frames.Body(channel_number,
                                   inbound[ConnectionState.HEADER_SIZE: \
                                           -ConnectionState.FOOTER_SIZE])
        elif frame_type == spec.FRAME_HEARTBEAT:
            return frames.Heartbeat()

        # Ignore the frame
        return None

    def _waiting_for_protocol_header(self, inbound):

        if inbound[3] != 'P':
            raise InvalidProtocolHeader(inbound)

        self._return_to_idle()
        major, minor, revision = struct.unpack_from('BBB', inbound, 5)
        return frames.ProtocolHeader(major, minor, revision)
