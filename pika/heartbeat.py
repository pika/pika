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


from pika.frames import Heartbeat

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
            self.connection.send_frame(Heartbeat())

        # Get the current byte counters from the connection, we expect these
        # to increment on our next call
        self.sent = self.connection.bytes_sent
        self.received = self.connection.bytes_received

        # If we're still relevant to the connection, add another timeout for
        # our interval
        if self.connection.heartbeat is self:
            self.setup_timer()
