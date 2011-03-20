# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
from pika.log import info
from random import random


class ReconnectionStrategy(object):

    can_reconnect = False

    def on_connect_attempt(self, conn):
        pass

    def on_connect_attempt_failure(self, conn):
        pass

    def on_transport_connected(self, conn):
        pass

    def on_transport_disconnected(self, conn):
        pass

    def on_connection_open(self, conn):
        pass

    def on_connection_closed(self, conn):
        pass


class NullReconnectionStrategy(ReconnectionStrategy):
    pass


class SimpleReconnectionStrategy(ReconnectionStrategy):

    can_reconnect = True

    def __init__(self, initial_retry_delay=1.0, multiplier=2.0,
                 max_delay=30.0, jitter=0.5):

        self.initial_retry_delay = initial_retry_delay
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter
        self._reset()

    def _reset(self):
        self.current_delay = self.initial_retry_delay
        self.attempts_since_last_success = 0

    def on_connect_attempt(self, conn):
        self.attempts_since_last_success += 1

    def on_connection_open(self, conn):
        self._reset()

    def on_connection_closed(self, conn):
        t = self.current_delay * ((random() * self.jitter) + 1)
        info("%s retrying %r in %r seconds (%r attempts)",
                 self.__class__.__name__, conn.parameters, t,
                 self.attempts_since_last_success)
        self.current_delay = min(self.max_delay,
                                 self.current_delay * self.multiplier)
        conn.add_timeout(t, conn._reconnect)
