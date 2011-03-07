# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

from pika.frame import Heartbeat

MAX_MISSED_HEARTBEATS = 2


class HeartbeatChecker(object):

    def __init__(self, connection, interval):
        """
        Create a heartbeat on connection sending a heartbeat frame every
        interval seconds.
        """

        # We need to reference our connection object to close a connection
        self.connection = connection
        self.interval = interval

        # Initialize our counters
        self.missed = 0
        self.received = 0
        self.sent = 0

        # Setup our timer to fire every interval seconds
        self.setup_timer()

    def setup_timer(self):
        """
        Use the connection objects delayed_call function which is implemented
        by the Adapter for calling the
        check_heartbeats function every interval seconds
        """
        self.connection.add_timeout(self.interval, self.send_and_check)

    def send_and_check(self):
        """
        Invoked by a timer to send a heartbeat when we need to, check to see
        if we've missed any heartbeats and disconnect our connection if it's
        been idle too long
        """

        # If our received byte count ==  our connection object's received byte
        # count, the connection has been
        # stale since the last heartbeat
        if self.received == self.connection.bytes_received:

            self.missed += 1
        else:
            # The server has said something. Reset our count.
            self.missed = 0

        # If we've missed MAX_MISSED_HEARTBEATS, close the connection
        if self.missed >= MAX_MISSED_HEARTBEATS:
            duration = self.missed * self.interval
            reason = "Too Many Missed Heartbeats, No reply in %i seconds" % \
                     duration
            self.connection.close(320, reason)
            return

        # If we've not sent a heartbeat since the last time we ran this
        # function, send a new heartbeat frame
        if self.sent == self.connection.bytes_sent:
            self.connection._send_frame(Heartbeat())

        # Get the current byte counters from the connection, we expect these
        # to increment on our next call
        self.sent = self.connection.bytes_sent
        self.received = self.connection.bytes_received

        # If we're still relevant to the connection, add another timeout for
        # our interval
        if self.connection.heartbeat is self:
            self.setup_timer()
