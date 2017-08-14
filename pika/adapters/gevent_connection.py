"""Use pika with the GEVENT"""

import gevent

from pika.adapters import BaseConnection
from pika.heartbeat import HeartbeatChecker


class PIKAGeventHeartbeatChecker(HeartbeatChecker):
    def send_and_check(self):
        gevent.spawn(super(PIKAGeventHeartbeatChecker, self).send_and_check)


class PIKAGeventConnection(BaseConnection):
    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None):
        self.connected = False
        super(PIKAGeventConnection, self).__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback
        )

    def add_timeout(self, deadline, callback_method):
        timer = gevent.get_hub().loop.timer(deadline)
        timer.start(callback_method)
        return timer

    def remove_timeout(self, timeout_id):
        timer = timeout_id
        timer.stop()

    def _adapter_connect(self):
        error = super(PIKAGeventConnection, self)._adapter_connect()
        if not error:
            self.socket.setblocking(1)
            self.connected = True
            gevent.spawn(self._read_loop)
        return error

    def _adapter_disconnect(self):
        self.connected = False
        super(PIKAGeventConnection, self)._adapter_disconnect()

    def _read_loop(self):
        while self.connected:
            self._handle_read()

    def _manage_event_state(self):
        pass

    def _create_heartbeat_checker(self):
        """Create a heartbeat checker instance if there is a heartbeat interval
        set.

        :rtype: pika.heartbeat.Heartbeat

        """
        if self.params.heartbeat is not None and self.params.heartbeat > 0:
            return PIKAGeventHeartbeatChecker(self, self.params.heartbeat)
