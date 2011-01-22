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

import errno
import logging
import sys
import socket
import asyncore
import time
import traceback

import pika.spec

from heapq import heappush, heappop
from pika.adapters.base_connection import BaseConnection


class RabbitDispatcher(asyncore.dispatcher):

    def __init__(self, connection):
        logging.debug("%s.__init__" % self.__class__.__name__)
        asyncore.dispatcher.__init__(self)
        self.connection = connection
        self.buffer_size = self.connection.suggested_buffer_size()

    def create_socket(self, socket_domain, socket_type):
        logging.debug("%s.create_socket" % self.__class__.__name__)
        asyncore.dispatcher.create_socket(self, socket_domain, socket_type)
        # Disable TCP nagling for improved latency.
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def handle_connect(self):
        logging.debug("%s.handle_connect" % self.__class__.__name__)
        self.connection.on_connected()

    def handle_close(self):
        logging.debug("%s.handle_close" % self.__class__.__name__)
        self.connection.disconnect()

    def _handle_error(self, error):
        logging.debug("%s.handle_error" % self.__class__.__name__)
        if error[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
            return
        elif error[0] == errno.EBADF:
            logging.error("%s: Write to a closed socket" %
                          self.__class__.__name__)
        else:
            logging.error("%s: Write error on %d: %s" %
                          (self.__class__.__name__,
                           self.fileno(), error[0]))
        self.close()
        self.connection.disconnect()

    def handle_read(self):
        logging.debug("%s.handle_read" % self.__class__.__name__)
        try:
            buf = self.recv(self.buffer_size)
        except socket.error, exn:
            self._handle_error(exn)
            return

        if not buf:
            self.close()
        else:
            self.connection.on_data_available(buf)

    def writable(self):
        return bool(self.connection.outbound_buffer)

    def handle_write(self):
        logging.debug("%s.handle_write" % self.__class__.__name__)

        fragment = self.connection.outbound_buffer.read(self.buffer_size)
        try:
            r = self.send(fragment)
        except socket.error, exn:
            self._handle_error(exn)
            return

        self.connection.outbound_buffer.consume(r)

    def on_state_change(self, caller, is_open):
        logging.debug("%s.on_state_change" % self.__class__.__name__)
        if not is_open:
            self.close()
            self.connection.disconnect()

    def handle_error(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback)


class AsyncoreConnection(BaseConnection):

    def add_timeout(self, delay_sec, callback):
        add_oneshot_timer_rel(delay_sec, callback)

    def connect(self, host, port):
        logging.debug("%s.connect" % self.__class__.__name__)
        self.dispatcher = RabbitDispatcher(self)
        self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dispatcher.connect((host, port or pika.spec.PORT))
        self.add_state_change_handler(self.dispatcher.on_state_change)

    def disconnect(self):
        logging.debug("%s.disconnect" % self.__class__.__name__)
        self.dispatcher.close()
        self.dispatcher = None
        self.on_disconnected()

    def flush_outbound(self):
        logging.debug("%s.flush" % self.__class__.__name__)
        while self.outbound_buffer:
            self.drain_events()

    def drain_events(self, timeout=None):
        loop(count=1, timeout=timeout)

# Timer related functionality
timer_heap = []


def add_oneshot_timer_abs(firing_time, callback):
    heappush(timer_heap, (firing_time, callback))


def add_oneshot_timer_rel(firing_delay, callback):
    add_oneshot_timer_abs(time.time() + firing_delay, callback)


def next_event_timeout(default_timeout=None):
    cutoff = run_timers_internal()
    if timer_heap:
        timeout = timer_heap[0][0] - cutoff
        if default_timeout is not None and timeout > default_timeout:
            timeout = default_timeout
    elif default_timeout is None:
        timeout = 30.0  # default timeout
    else:
        timeout = default_timeout
    return timeout


def log_timer_error(info):
    sys.stderr.write('EXCEPTION IN ASYNCORE_ADAPTER TIMER\n')
    traceback.print_exception(*info)


def run_timers_internal():
    cutoff = time.time()
    while timer_heap and timer_heap[0][0] < cutoff:
        try:
            heappop(timer_heap)[1]()
        except:
            log_timer_error(sys.exc_info())
        cutoff = time.time()
    return cutoff


def loop1(map, timeout=None):
    if map:
        asyncore.loop(timeout=next_event_timeout(timeout), map=map, count=1)
    else:
        time.sleep(next_event_timeout(timeout))


def loop(map=None, count=None, timeout=None):
    if map is None:
        map = asyncore.socket_map
    if count is None:
        while (map or timer_heap):
            loop1(map, timeout)
    else:
        while (map or timer_heap) and count > 0:
            loop1(map, timeout)
            count = count - 1
        run_timers_internal()
