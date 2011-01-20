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
import pika.channel as channel
import pika.simplebuffer as simplebuffer
import pika.event as event
from pika.specbase import _codec_repr
from pika.exceptions import *

from pika.credentials import PlainCredentials
from pika.heartbeat import HeartbeatChecker
from pika.reconnection_strategies import NullReconnectionStrategy

CHANNEL_MAX = 32767
FRAME_MAX = 131072
PRODUCT = "Pika Python AMQP Client Library"


# Module wide default credentials for default RabbitMQ configurations
default_credentials = PlainCredentials('guest', 'guest')


class ConnectionParameters(object):

    def __init__(self,
                 host,
                 port=None,
                 virtual_host="/",
                 credentials=None,
                 channel_max=0,
                 frame_max=131072,
                 heartbeat=0):

        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.credentials = credentials
        self.channel_max = channel_max
        self.frame_max = frame_max
        self.heartbeat = heartbeat

    def __repr__(self):

        return _codec_repr(self, lambda: ConnectionParameters(None))


class Connection(object):

    """
    Pika Connection Class

    This class is extended by the adapter Connection classes such as
    blocking_adapter.BlockingConnection & asyncore_adapter.AsyncoreConnection.
    To build an adapter Connection class implement the following functions:

        Required:

        def connect(self, host, port)
        def disconnect_transport(self)
        def flush_outbound(self)

        def add_timeout(self, delay_sec, callback)

        Optional:

        def erase_credentials(self)

    """

    def __init__(self, parameters, open_callback, close_callback,
                 reconnection_strategy=None):
        """
        Connection initialization expects a ConnectionParameters object and
        a callback function to notify when we have successfully connected
        to the AMQP Broker.

        A reconnection_strategy of None will use the NullReconnectionStrategy
        """

        # If we did not pass in a reconnection_strategy, setup the default
        if not reconnection_strategy:
            reconnection_strategy = NullReconnectionStrategy()

        # Define our callback dictionary
        self._callbacks = dict()

        # Set our configuration options
        self.on_connected_callback = open_callback
        self.on_close_callback = close_callback
        self.parameters = parameters
        self.reconnection_strategy = reconnection_strategy

        # Event handler for callbacks on connection state change
        self.connection_state_change_event = event.Event()

        # Set all of our default connection state values
        self._init_connection_state()

        # Connect to the AMQP Broker
        self._connect()

    def _local_protocol_header(self):
        """
        Returns the Frame Protocol Header for our AMQP Client for communicating
        with our AMQP Broker
        """
        return codec.FrameProtocolHeader(1, 1,
                                         spec.PROTOCOL_VERSION[0],
                                         spec.PROTOCOL_VERSION[1])

    def add_state_change_handler(self, handler, key=None):
        """
        This method allows you to add a custom event
        handler that will be fired when the connection state changes.
        """
        logging.debug('%s.add_state_change_handler' % \
                      self.__class__.__name__)
        self.connection_state_change_event.addHandler(handler, key)
        if closed:
            handler(self, False)
        else:
            handler(self, True)

    def remove_state_change_handler(self, key):
        """
        Remove a custom connection state change event handler.
        """
        logging.debug('%s.remove_state_change_handler' % \
                      self.__class__.__name__)
        self.connection_state_change_event.delHandler(key)

    # Connection opening related functionality

    def combine_tuning(self, a, b):
        """
        Pass in two values, if a is 0, return b otherwise if b is 0, return a.
        If neither case matches return the smallest value.
        """
        if not a:
            return b
        elif not b:
            return a
        return min(a, b)

    def _connect(self):
        """
        Internal connection method that will kick off the socket based
        connections in our Adapter and kick off the initial communication
        frame.

        Connect in our Adapter's Connection is a blocking operation
        """
        logging.debug('%s._connect' % self.__class__.__name__)

        # Let our RS know what we're up to
        self.reconnection_strategy.on_connect_attempt(self)

        # Try and connect and send the first frame
        try:
            self.connect(self.parameters.host, self.parameters.port or \
                                               spec.PORT)
            self._send_frame(self._local_protocol_header())
        except:
            # Something went wrong, let our SRS know
            self.reconnection_strategy.on_connect_attempt_failure(self)
            raise AMQPConnectionError

    def _handle_connection_open(self):
        """
        Let both our RS and Event object know we connected successfully
        """
        logging.debug('%s._handle_connection_open' % self.__class__.__name__)

        # Let our connection strategy know the connection is open
        self.reconnection_strategy.on_connection_open(self)

        # Call our custom state change event callbacks
        self.connection_state_change_event.fire(self, True)

    def _init_connection_state(self):
        """
        Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped
        """
        logging.debug('%s._init_connection_state' % self.__class__.__name__)

        # Inbound and outbound buffers
        self.buffer = str()
        self.outbound_buffer = simplebuffer.SimpleBuffer()

        # Server state and channels
        self.state = codec.ConnectionState()
        self.server_properties = None
        self.channels = {}

        # Data used for Heartbeat checking
        self.bytes_sent = 0
        self.bytes_received = 0
        self.heartbeat = None

        # AMQP Lifecycle States
        self.closed = True
        self.closing = False
        self.open = False

        # Close code and text for async shutdown behaviors
        # @todo Previously this used the Close frame, re-investigate that
        self.close_code = None
        self.close_text = None

        # Our starting point once connected, first frame received
        self.add_callback(self._on_connection_start, [spec.Connection.Start])

    def is_open(self):
        """
        Returns a boolean reporting the current connection state
        """
        return self.open and (not self.closing and not self.closed)

    def on_connected(self):
        """
        This is called by our connection Adapter to let us know that we've
        connected and we can notify our connection strategy
        """
        logging.debug('%s.on_connected' % self.__class__.__name__)

        # Let our reconnection_strategy know we're connected
        self.reconnection_strategy.on_transport_connected(self)

    def _on_connection_open(self, frame):
        """
        This is called once we have tuned the conneciton with the server and
        called the Connection.Open on the server and it has replied with
        Connection.Ok.
        """
        logging.debug('%s._on_connection_open' % self.__class__.__name__)
        self.known_hosts = frame.method.known_hosts
        self._handle_connection_open()

        # Add a callback handler for the Broker telling us to disconnect
        self.add_callback(self.on_remote_close, [spec.Connection.Close])

        # We're now connected at the AMQP level
        self.open = True

        # Call our invokers callback
        self.on_connected_callback()

    def _on_connection_start(self, frame):
        """
        This is called as a callback once we have received a Connection.Start
        from the server.
        """
        logging.debug('%s._on_connection_start' % self.__class__.__name__)

        # We're now connected to the broker
        self.closed = False

        if isinstance(frame, codec.FrameProtocolHeader):
            raise ProtocolVersionMismatch(self._local_protocol_header(), frame)

        if (frame.method.version_major,
            frame.method.version_minor) != spec.PROTOCOL_VERSION:
            raise ProtocolVersionMismatch(self._local_protocol_header(),
                                          frame)

        self.server_properties = frame.method.server_properties

        credentials = self.parameters.credentials or default_credentials
        response = credentials.response_for(frame.method)
        if not response:
            raise LoginError("No acceptable %s support for the credentials",
                             (frame.method, credentials))
        self.send_method(0, spec.Connection.StartOk(
                                  client_properties={"product": PRODUCT},
                                  mechanism=response[0],
                                  response=response[1]))
        self.erase_credentials()
        self.add_callback(self._on_connection_tune, [spec.Connection.Tune])

    def _on_connection_tune(self, frame):
        """
        Once the Broker sends back a Connection.Tune, we will set our tuning
        variables that have been returned to us and kick off the Heartbeat
        monitor if required, send our TuneOk and then the Connection. Open rpc
        call on channel 0
        """
        logging.debug('%s._on_connection_tune' % self.__class__.__name__)
        channel_max = self.combine_tuning(self.parameters.channel_max,
                                          frame.method.channel_max)
        frame_max = self.combine_tuning(self.parameters.frame_max,
                                        frame.method.frame_max)
        heartbeat_interval = self.combine_tuning(self.parameters.heartbeat,
                                                 frame.method.heartbeat)
        if heartbeat_interval:
            self.heartbeat = HeartbeatChecker(self, heartbeat_interval)

        self.state.tune(channel_max, frame_max)
        self.send_method(0,
                         spec.Connection.TuneOk(channel_max=channel_max,
                                                frame_max=frame_max,
                                                heartbeat=heartbeat_interval))
        self.rpc(self._on_connection_open, 0,
                  spec.Connection.Open( \
                          virtual_host=self.parameters.virtual_host,
                          insist=True),
                  [spec.Connection.OpenOk])

    def reconnect(self):
        """
        Called by the Reconnection Strategy classes or Adapters to disconnect
        and reconnect to the broker
        """
        logging.debug('%s.reconnect' % self.__class__.__name__)
        self._ensure_closed()
        self._init_connection_state()
        self._connect()

    # Functions related to closing a connection

    def close(self, code=200, text='Normal shutdown'):
        """
        Main close function, will attempt to close the channels and if there
        are no channels left, will go straight to on_close_ready
        """
        logging.info("%s.close Closing Connection: (%s) %s" % \
                     (self.__class__.__name__, code, text))

        if self.closing or self.closed:
            logging.info("%s.Close invoked while closing or closed" %\
                         self.__class__.__name__)
            return

        self.close_code = code
        self.close_text = text

        # If we're not already closed
        for channel in self.channels.values():
            channel.close(code, text)

        # If we already dont have any channels, close out
        if not len(self.channels):
            self.on_close_ready()

    def on_close_ready(self):
        """
        On a clean shutdown we'll call this once all of our channels are closed
        Let the Broker know we want to close
        """
        logging.debug('%s._on_close_ready' % self.__class__.__name__)

        if self.closing or self.closed:
            logging.info("%s.on_close_ready invoked while closing or closed" %\
                         self.__class__.__name__)
            return

        self.closing = True
        self.rpc(self._on_close_ok, 0,
                 spec.Connection.Close(self.close_code, self.close_text, 0, 0),
                 [spec.Connection.CloseOk])

    def on_channel_close(self, frame):
        """
        RPC Response from when a channel closes itself, remove from our stack
        """
        logging.debug('%s._on_channel_close: %r' % (self.__class__.__name__,
                                                    frame))

        if frame.channel_number in self.channels:
            del(self.channels[frame.channel_number])

        if not len(self.channels):
            self.on_close_ready()

    def _on_close_ok(self, frame):
        """
        This is usually invoked by the Broker as a frame across channel 0
        """
        logging.debug('%s._on_close_ok' % self.__class__.__name__)
        self.on_close_callback()
        self.closed = True
        self.closing = False
        self.open = False

    def on_remote_close(self, frame):
        """
        We've received a remote close from the server
        """
        logging.debug('%s._on_remote_close: %r' % (self.__class__.__name__,
                                                   frame))
        self.close(frame.reply_code, frame.reply_text)

    def _ensure_closed(self):
        """
        If we're not already closed, make sure we're closed
        """
        logging.debug('%s._ensure_closed' % self.__class__.__name__)

        # We carry the connection state and so we want to close if we know
        if self.is_open():
            self.close()

        # Let our Reconnection Strategy know we were disconnected
        self.reconnection_strategy.on_transport_disconnected(self)

    def _handle_connection_close(self):
        """
        Let both our RS and Event object know we closed
        """
        logging.debug('%s._handle_connection_close' % self.__class__.__name__)
        # Let our connection strategy know the connection closed
        self.reconnection_strategy.on_connection_closed(self)

        # Call our custom state change event callbacks
        self.connection_state_change_event.fire(self, False)

    def on_disconnected(self, reason='Socket closed'):
        """
        This is called by our Adapter to let us know the socket has fully
        closed
        """
        logging.debug('%s._disconnected: %s' % (self.__class__.__name__,
                                                reason))

        # Set that we're actually closed
        self.closed = True
        self.closing = False
        self.open = False

        # Let our Events and RS know what's up
        self._handle_connection_close()

    # Channel related functionality

    def add_channel(self, channel_number, channel):
        """
        Add channel identified by channel_number to our dictionary
        """
        self.channels[channel_number] = channel

    def channel(self, callback, channel_number=None):
        """
        Create a new channel with the next available or specified channel #
        """

        # If the user didn't specify a channel_number get the next avail
        if not channel_number:
            channel_number = self._next_channel_number()

        # Return a handle to the channel
        return channel.Channel(channel.ChannelHandler(self, channel_number),
                               callback)

    def _ensure_channel(self, channel_number):
        """
        Ensure we have channel_number in our channel stack or it's channel 0
        """
        # If we don't have an open connection raise an exception
        if self.closed:
            raise ConnectionClosed

        if channel_number > 0 and channel_number in self.channels:
            return self.channels[channel_number]._ensure()

    def _next_channel_number(self):
        """
        Return the next available channel number or raise on exception
        """
        # Our limit is the the Codec's Channel Max or MAX_CHANNELS if it's None
        limit = self.state.channel_max or CHANNEL_MAX

        # We've used all of our channels
        if len(self.channels) == limit:
            raise NoFreeChannels()

        # Get a list of all of our keys, all should be numeric channel ids
        channel_numbers = self.channels.keys()

        # We don't start with any open channels
        if not len(channel_numbers):
            return 1

        # Our next channel is the max key value + 1
        return max(channel_numbers) + 1

    def remove_channel(self, channel_number):
        """
        Remove the channel identified by channel_number from our stack
        """
        if channel_number in self.channels:
            del self.channels[channel_number]

    # Data packet and frame handling functions

    def add_callback(self, callback, acceptable_replies):
        """
        Add a callback of the specific frame type to the rpc_callback stack
        """

        # If we didn't pass in more than one, make it a list anyway
        if not isinstance(acceptable_replies, list):
            raise TypeError("acceptable_replies must be a list.")

        # If the frame type isn't in our callback dict, add it
        for reply in acceptable_replies:

            # Make sure we have an empty dict setup for the reply
            if reply not in self._callbacks:
                self._callbacks[reply.NAME] = set()

            # Sets will noop a duplicate add, since we're not likely to dupe,
            # rely on this behavior
            self._callbacks[reply.NAME].add(callback)

    def remove_callback(self, frame, callback):
        """
        Remove a given callback id for a given frame type
        """
        if frame in self._callbacks:

            # If the callback id exists, remove it
            if callback in self._callbacks[frame.NAME]:
                self._callbacks[frame.NAME].remove(callback)

            # If we dont have any more callbacks for this frame type, remove it
            if not len(self._callbacks[frame.NAME]):
                del(self._callbacks[frame.NAME])

    def on_data_available(self, buffer):
        """
        This is called by our Adapter, passing in the data from the socket
        As long as we have buffer try and map out frame data
        """

        # Create a buffer consisting of the global buffer & what we were passed
        buffer = ''.join([self.buffer, buffer])

        # Reset our cross-call buffer
        self.buffer = str()

        while buffer:

            # Try and read data from the
            try:
                (consumed_count, frame) = self.state.handle_input(buffer)
            except Exception, e:
                logging.error("Error handling input: %s" % e)
                break

            # If we don't have a full frame, set our global buffer and exit
            if not frame:
                self.buffer = buffer
                break

            # We've gotten a frame from the buffer so update counter and buffer
            self.bytes_received += consumed_count
            buffer = buffer[consumed_count:]

            # Send the frame to the appropriate channel handler
            if frame.channel_number > 0:

                # Make sure we have a valid channel response
                if frame.channel_number in self.channels:

                    # Call our Channel Handler with the frame
                    self.channels[frame.channel_number].frame_handler(frame)

                else:
                    error = "Frame received for untracked channel: %i" % \
                            frame.channel_number
                    logging.error(error)

            # We didn't have it in our reply_map so let the channel deal w\ it
            else:
                # If we've registered this method in our callback stack
                if frame.method.NAME in self._callbacks:

                    # Loop through each callback in our reply_map
                    for callback in self._callbacks[frame.method.NAME]:

                        # Make the callback
                        callback(frame)

                    # If we processed callbacks remove them
                    del(self._callbacks[frame.method.NAME])

                # We don't check for heartbeat frames because we can not count
                # atomic frames reliably due to different message behaviors
                # such as large content frames being transferred slowly
                elif isinstance(frame, codec.FrameHeartbeat):
                    return

    def rpc(self, callback, channel_number, method, acceptable_replies):
        """
        Make an RPC call for the given callback, channel number and method.
        acceptable_replies lists out what responses we'll process from the
        server with the specified callback.
        """
        if callback:
            self.add_callback(callback, acceptable_replies)

        # Ensure our channel is still open before making the call
        self._ensure_channel(channel_number)

        # Send the rpc call to RabbitMQ
        self.send_method(channel_number, method)

    def _send_frame(self, frame):
        """
        This appends the fully generated frame to send to the broker to the
        output buffer which will be then sent via the connection adapter
        """
        marshalled_frame = frame.marshal()
        self.bytes_sent += len(marshalled_frame)
        self.outbound_buffer.write(marshalled_frame)
        logging.debug('%s Wrote: %r' % (self.__class__.__name__,
                                        frame))

    def send_method(self, channel_number, method, content=None):
        """
        Constructs a RPC method frame and then sends it to the broker
        """
        self._send_frame(codec.FrameMethod(channel_number, method))

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
            self._send_frame(codec.FrameHeader(channel_number, length, props))

        if body:
            max_piece = (self.state.frame_max - \
                         codec.ConnectionState.HEADER_SIZE - \
                         codec.ConnectionState.FOOTER_SIZE)
            body_buf = simplebuffer.SimpleBuffer(body)

            while body_buf:
                piece_len = min(len(body_buf), max_piece)
                piece = body_buf.read_and_consume(piece_len)
                self._send_frame(codec.FrameBody(channel_number, piece))

    def suggested_buffer_size(self):
        """
        Return the suggested buffer size from the codec/tune or the default
        if that is None
        """
        if not self.state.frame_max:
            return FRAME_MAX

        return self.state.frame_max

    """
    In order to implement a connection adapter, you must extend connect,
    delayed_call, disconnect_transport and flush_outbound.
    """

    def connect(self, host, port):
        """
        Subclasses should override to set up the outbound
        socket.
        """
        raise NotImplementedError('Subclass Responsibility')

    def disconnect_transport(self):
        """
        Subclasses should override this to cause the underlying
        transport (socket) to close.
        """
        raise NotImplementedError('Subclass Responsibility')

    def flush_outbound(self):
        """Subclasses should override to flush the contents of
        outbound_buffer out along the socket."""
        raise NotImplementedError('Subclass Responsibility')

    def add_timeout(self, delay_sec, callback):
        """
        Subclasses should override to call the callback after the
        specified number of seconds have elapsed, using a timer, or a
        thread, or similar.
        """
        raise NotImplementedError('Subclass Responsibility')

    def erase_credentials(self):
        """
        Override if in some context you need the object to forget
        its login credentials after successfully opening a connection.
        """
        pass
