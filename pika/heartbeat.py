"""Handle AMQP Heartbeats"""
import logging

import pika.exceptions
from pika import frame

LOGGER = logging.getLogger(__name__)


class HeartbeatChecker(object):
    """Sends heartbeats to the broker and checks to make sure that the broker's
    heartbeat is received before the expected timeout expires.

    """
    # Note: even though we're sending heartbeats in half the specified
    # timeout value, the broker will be sending them to us at the specified
    # value. This means we'll be checking for an idle connection via send_and_check
    # twice as many times as the broker will send heartbeats to us,
    # so we need to set max idle count to 4 here
    _MAX_IDLE_COUNT = 4
    _STALE_CONNECTION = "Too Many Missed Heartbeats, No reply in %i seconds"

    def __init__(self, connection, timeout):
        """Create a heartbeat on the connection that sends two heartbeat frames
        within the specified timeout window. Also checks to ensure heartbeats
        are received from the broker.

        :param pika.connection.Connection: Connection object
        :param int timeout: Heartbeat check timeout. Note: heartbeats will
                            actually be sent at timeout / 2 frequency.

        """
        self._connection = connection

        # Note: see the following document:
        # https://www.rabbitmq.com/heartbeats.html#heartbeats-timeout
        self._timeout = timeout
        self._check_interval = float(self._timeout / 2)

        # Initialize counters
        self._bytes_received = 0
        self._bytes_sent = 0
        self._heartbeat_frames_received = 0
        self._heartbeat_frames_sent = 0
        self._idle_byte_intervals = 0

        # The handle for the last timer
        self._timer = None

        # Setup the timer to fire in _interval seconds
        self._setup_timer()

    @property
    def active(self):
        """Return True if the connection's heartbeat attribute is set to this
        instance.

        :rtype True

        """
        return self._connection._heartbeat_checker is self

    @property
    def bytes_received_on_connection(self):
        """Return the number of bytes received by the connection bytes object.

        :rtype int

        """
        return self._connection.bytes_received

    @property
    def connection_is_idle(self):
        """Returns true if the byte count hasn't changed in enough intervals
        to trip the max idle threshold.

        """
        return self._idle_byte_intervals >= HeartbeatChecker._MAX_IDLE_COUNT

    def received(self):
        """Called when a heartbeat is received"""
        LOGGER.debug('Received heartbeat frame')
        self._heartbeat_frames_received += 1

    def send_and_check(self):
        """Invoked by a timer to send a heartbeat when we need to, check to see
        if we've missed any heartbeats and disconnect our connection if it's
        been idle too long.

        """
        LOGGER.debug('Received %i heartbeat frames, sent %i, '
                     'idle intervals %i, max idle count %i',
                     self._heartbeat_frames_received,
                     self._heartbeat_frames_sent,
                     self._idle_byte_intervals,
                     HeartbeatChecker._MAX_IDLE_COUNT)

        if self.connection_is_idle:
            self._close_connection()
            return

        # Connection has not received any data, increment the counter
        if not self._has_received_data:
            self._idle_byte_intervals += 1
        else:
            self._idle_byte_intervals = 0

        # Update the counters of bytes sent/received and the frames received
        self._update_counters()

        # Send a heartbeat frame
        self._send_heartbeat_frame()

        # Update the timer to fire again
        self._start_timer()

    def stop(self):
        """Stop the heartbeat checker"""
        if self._timer:
            LOGGER.debug('Removing timeout for next heartbeat interval')
            self._connection._adapter_remove_timeout(self._timer)  # pylint: disable=W0212
            self._timer = None

    def _close_connection(self):
        """Close the connection with the AMQP Connection-Forced value."""
        LOGGER.info('Connection is idle, %i stale byte intervals',
                    self._idle_byte_intervals)
        text = HeartbeatChecker._STALE_CONNECTION % self._timeout

        # Abort the stream connection. There is no point trying to gracefully
        # close the AMQP connection since lack of heartbeat suggests that the
        # stream is dead.
        self._connection._terminate_stream(  # pylint: disable=W0212
            pika.exceptions.AMQPHeartbeatTimeout(text))

    @property
    def _has_received_data(self):
        """Returns True if the connection has received data on the connection.

        :rtype: bool

        """
        return not self._bytes_received == self.bytes_received_on_connection

    @staticmethod
    def _new_heartbeat_frame():
        """Return a new heartbeat frame.

        :rtype pika.frame.Heartbeat

        """
        return frame.Heartbeat()

    def _send_heartbeat_frame(self):
        """Send a heartbeat frame on the connection.

        """
        LOGGER.debug('Sending heartbeat frame')
        self._connection._send_frame(  # pylint: disable=W0212
            self._new_heartbeat_frame())
        self._heartbeat_frames_sent += 1

    def _setup_timer(self):
        """Use the connection objects delayed_call function which is
        implemented by the Adapter for calling the check_heartbeats function
        every interval seconds.

        """
        self._timer = self._connection._adapter_add_timeout(  # pylint: disable=W0212
            self._check_interval,
            self.send_and_check)

    def _start_timer(self):
        """If the connection still has this object set for heartbeats, add a
        new timer.

        """
        if self.active:
            self._setup_timer()

    def _update_counters(self):
        """Update the internal counters for bytes sent and received and the
        number of frames received

        """
        self._bytes_sent = self._connection.bytes_sent
        self._bytes_received = self._connection.bytes_received
