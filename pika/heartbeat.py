"""Handle AMQP Heartbeats"""
import logging

import pika.exceptions
from pika import frame

LOGGER = logging.getLogger(__name__)


class HeartbeatChecker(object):
    """Sends heartbeats to the broker twice during the provided interval
    and checks at a different interval to make sure that the broker's
    heartbeat is received as expected. See the parameter list for more
    details.

    """
    _MAX_IDLE_COUNT = 2
    _STALE_CONNECTION = "Too Many Missed Heartbeats, No reply in %i seconds"

    def __init__(self, connection, interval):
        """Create a heartbeat on the connection that sends two heartbeat frames
        within the specified interval. Also checks to ensure heartbeats are
        received from the broker at a different interval explained below.

        :param pika.connection.Connection: Connection object
        :param int interval: Heartbeat check interval. Note: heartbeats will
                             actually be sent at interval / 2 frequency, and
                             heartbeat checks made at interval. The default
                             interval is 60 seconds so Pika will send heartbeats
                             at 30 second intervals and will check every 60 seconds.
                             If no heartbeat is received from the broker, nor data
                             activity on the connection after 120 seconds (two
                             intervals, what RabbitMQ uses) the connection will be
                             assumed dead and closed.

        """
        self._connection = connection

        # Note: see the following documents:
        # https://www.rabbitmq.com/heartbeats.html#heartbeats-timeout
        # https://github.com/pika/pika/pull/1072
        # https://groups.google.com/d/topic/rabbitmq-users/Fmfeqe5ocTY/discussion
        # There is a certain amount of confusion around how client developers
        # interpret the spec. The spec talks about 2 missed heartbeats as a
        # *timeout*, plus that any activity on the connection counts for a
        # heartbeat. This is to avoid edge cases and not to depend on network
        # latency.
        self._send_interval = float(interval) / 2
        self._check_interval = interval

        # Initialize counters
        self._bytes_received = 0
        self._bytes_sent = 0
        self._heartbeat_frames_received = 0
        self._heartbeat_frames_sent = 0
        self._idle_byte_intervals = 0

        self._send_timer = None
        self._check_timer = None
        self._start_send_timer()
        self._start_check_timer()

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

    def _send_heartbeat(self):
        """Invoked by a timer to send a heartbeat when we need to.

        """
        LOGGER.debug('Sending heartbeat frame')
        self._send_heartbeat_frame()
        self._start_send_timer()

    def _check_heartbeat(self):
        """Invoked by a timer to check for broker heartbeats. Checks to see
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

        self._update_counters()
        self._start_check_timer()

    def stop(self):
        """Stop the heartbeat checker"""
        if self._send_timer:
            LOGGER.debug('Removing timer for next heartbeat send interval')
            self._connection._adapter_remove_timeout(self._send_timer)  # pylint: disable=W0212
            self._send_timer = None
        if self._check_timer:
            LOGGER.debug('Removing timer for next heartbeat check interval')
            self._connection._adapter_remove_timeout(self._check_timer)  # pylint: disable=W0212
            self._check_timer = None

    def _close_connection(self):
        """Close the connection with the AMQP Connection-Forced value."""
        LOGGER.info('Connection is idle, %i stale byte intervals',
                    self._idle_byte_intervals)
        text = HeartbeatChecker._STALE_CONNECTION % (
            self._check_interval * HeartbeatChecker._MAX_IDLE_COUNT)

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
        return self._bytes_received != self.bytes_received_on_connection

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

    def _start_send_timer(self):
        """Start a new heartbeat send timer."""
        self._send_timer = self._connection._adapter_add_timeout(  # pylint: disable=W0212
            self._send_interval,
            self._send_heartbeat)

    def _start_check_timer(self):
        """Start a new heartbeat check timer."""
        self._check_timer = self._connection._adapter_add_timeout(  # pylint: disable=W0212
            self._check_interval,
            self._check_heartbeat)

    def _update_counters(self):
        """Update the internal counters for bytes sent and received and the
        number of frames received

        """
        self._bytes_sent = self._connection.bytes_sent
        self._bytes_received = self._connection.bytes_received
