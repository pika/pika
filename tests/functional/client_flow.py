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

"""
Test for being able to toggle Channel.Flow on the server which will become
more explicitly supported in AMQP 1-0
"""
import nose
import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import support.tools as tools
from pika.adapters import SelectConnection

HOST = 'localhost'
PORT = 5672


class TestAsyncClientFlow(tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_flow(self):
        self.connection = self._connect(SelectConnection, HOST, PORT)
        self.connection.ioloop.start()
        pass

    def _on_channel(self, channel):
        self.channel = channel
        self._queue_declare()

    def _on_queue_declared(self, frame):
        self._channel_flow_test_1()

    @nose.tools.nottest
    def _channel_flow_test_1(self):
        # Confirm we can turn it on
        self.channel.flow(self._channel_flow_test_1_response, True)

    @nose.tools.nottest
    def _channel_flow_test_1_response(self, active):
        if not active:
            assert False, "Test #1: Channel flow did not turn on"
        self.connection.add_timeout(0.5, self._channel_flow_test_2)

    @nose.tools.nottest
    def _channel_flow_test_2(self):
        # Confirm we can turn it off
        self.channel.flow(self._channel_flow_test_2_response, False)

    @nose.tools.nottest
    def _channel_flow_test_2_response(self, active):
        if active:
            assert False, "Test #2: Channel flow did not turn off"

        def _on_closed(frame):
            self.connection.add_on_close_callback(self._on_closed)
            self.connection.close()

        self.connection.add_on_close_callback(_on_closed)
        self.connection.close()


if __name__ == "__main__":
    nose.runmodule()
