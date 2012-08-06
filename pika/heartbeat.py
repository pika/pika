# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Class that handles heartbeat communication

"""
from pika import frame


class HeartbeatChecker(object):
    """Checks to make sure that our heartbeat is received at the expected
    intervals.

    """
    _CONNECTION_FORCED = 320
    _MAX_MISSED_HEARTBEATS = 2
    _STALE_CONNECTION = "Too Many Missed Heartbeats, No reply in %i seconds"

    def __init__(self, connection, interval):
        """Create a heartbeat on connection sending a heartbeat frame every
        interval seconds.

        :param pika.connection.Connection: Connection object
        :param int interval: Heartbeat check interval

        """
        # We need to reference our connection object to close a connection
        self._connection = connection
        self._interval = interval

        # Initialize counters
        self._missed = 0
        self._received = 0
        self._sent = 0

        # Setup the timer to fire in _interval seconds
        self._setup_timer()

    def _close_connection(self):
        """Close the connection with the AMQP Connection-Forced value."""
        duration = self._missed * self._interval
        self._connection.close(HeartbeatChecker._CONNECTION_FORCED,
                               HeartbeatChecker._STALE_CONNECTION % duration)

    def _reset_connection(self):
        """Tell the connection to re-connect"""
        self._connection.force_reconnect()

    def _connection_bytes_received(self):
        """Return the number of bytes received by the connection bytes object.

        :returns: int

        """
        return self._connection.bytes_received

    def _in_use(self):
        """Return True if the connection's heartbeat attribute is set to this
        instance.

        :returns: True

        """
        return self._connection.heartbeat is self

    def _new_heartbeat_frame(self):  #pragma: no cover
        """Return a new heartbeat frame.

        :returns: pika.frame.Heartbeat

        """
        return frame.Heartbeat()

    def _missed_heartbeat_responses(self):
        """Check to see if the received byte count matches the connection
        object received byte count. If the bytes are equal, there has not been
        a heartbeat sent since the last check.

        """
        if self._received == self._connection_bytes_received():
            self._missed += 1
        else:
            # The server has said something. Reset our count.
            self._missed = 0

        # Return the missed heartbeat response count
        return self._missed

    def _send_heartbeat_frame(self):
        """Send a heartbeat frame on the connection.

        """
        self._connection._send_frame(self._new_heartbeat_frame())

    def _setup_timer(self):
        """Use the connection objects delayed_call function which is
        implemented by the Adapter for calling the check_heartbeats function
        every interval seconds.

        """
        self._connection.add_timeout(self._interval, self.send_and_check)

    def _should_send_heartbeat_frame(self):
        """Returns True if the amount of bytes recorded the last time the
        timer fired still matches the number of bytes the connection has
        sent.

        :returns: True

        """
        return self._sent == self._connection.bytes_sent

    def _start_timer(self):
        """If the connection still has this object set for heartbeats, add a
        new timer.

        """
        if self._in_use():
            self._setup_timer()

    def _too_many_missed_heartbeats(self):
        """Return if the number of missed heartbeats exceeds the maximum amount
        of missed heartbeats.

        :returns: True

        """
        return (self._missed_heartbeat_responses() >=
                HeartbeatChecker._MAX_MISSED_HEARTBEATS)

    def _update_byte_counts(self):
        """Update the internal byte counters from the current values of the
        connection object.

        """
        self._sent = self._connection.bytes_sent
        self._received = self._connection.bytes_received

    def send_and_check(self):
        """Invoked by a timer to send a heartbeat when we need to, check to see
        if we've missed any heartbeats and disconnect our connection if it's
        been idle too long.

        """
        # If too many heartbeats have been missed, close & reset the connection
        if self._too_many_missed_heartbeats():
            self._close_connection()
            self._reset_connection()
            return

        # If there have been no bytes received since the last check
        if self._should_send_heartbeat_frame():
            self._send_heartbeat_frame()

        # Update the byte counts for the next check
        self._update_byte_counts()

        # Update the timer to fire again
        self._start_timer()
