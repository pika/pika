"""Handle AMQP Heartbeats"""
import logging

from pika import frame

LOGGER = logging.getLogger(__name__)


class HeartbeatChecker(object):
    """Checks to make sure that our heartbeat is received at the expected
    intervals.

    """
    MAX_IDLE_COUNT = 5
    _CONNECTION_FORCED = 320
    _STALE_CONNECTION = "Too Many Missed Heartbeats, No reply in %i seconds"

    def __init__(self, connection, interval, idle_count=MAX_IDLE_COUNT):
        """Create a heartbeat on connection sending a heartbeat frame every
        interval seconds.

        :param pika.connection.Connection: Connection object
        :param int interval: Heartbeat check interval
        :param int idle_count: Number of heartbeat intervals missed until the
                               connection is considered idle and disconnects
        """
        self._connection = connection
        self._interval = interval
        self._max_idle_count = idle_count

        # Initialize counters
        self._heartbeat_frames_received = 0
        self._heartbeat_frames_sent = 0

        # Setup the timer to fire in _interval seconds
        self._setup_timer()

    @property
    def active(self):
        """Return True if the connection's heartbeat attribute is set to this
        instance.

        :rtype True

        """
        return self._connection.heartbeat is self

    @property
    def connection_is_idle(self):
        """Returns true if the byte count hasn't changed in enough intervals
        to trip the max idle threshold.

        """
        return (self._heartbeat_frames_sent -
                self._heartbeat_frames_received > self._max_idle_count)

    def received(self):
        """Called when a heartbeat is received"""
        LOGGER.debug('Received heartbeat frame')
        self._heartbeat_frames_received += 1

    def send_and_check(self):
        """Invoked by a timer to send a heartbeat when we need to, check to see
        if we've missed any heartbeats and disconnect our connection if it's
        been idle too long.

        """
        LOGGER.debug('Received %i heartbeat frames, sent %i',
                     self._heartbeat_frames_received,
                     self._heartbeat_frames_sent)

        if self.connection_is_idle:
            return self._close_connection()

        self._send_heartbeat_frame()
        self._start_timer()

    def _close_connection(self):
        """Close the connection with the AMQP Connection-Forced value."""
        LOGGER.info('Connection is idle, %i sent but only %i received',
                    self._heartbeat_frames_sent,
                    self._heartbeat_frames_received)
        duration = self._max_idle_count * self._interval
        self._connection.close(HeartbeatChecker._CONNECTION_FORCED,
                               HeartbeatChecker._STALE_CONNECTION % duration)

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
        self._connection._send_frame(self._new_heartbeat_frame())
        self._heartbeat_frames_sent += 1

    def _setup_timer(self):
        """Use the connection objects delayed_call function which is
        implemented by the Adapter for calling the check_heartbeats function
        every interval seconds.

        """
        self._connection.add_timeout(self._interval, self.send_and_check)

    def _start_timer(self):
        """If the connection still has this object set for heartbeats, add a
        new timer.

        """
        if self.active:
            self._setup_timer()
