"""Use pika with the GEVENT"""

import gevent

from pika.adapters import BaseConnection
from pika.heartbeat import HeartbeatChecker


class PikaGeventHeartbeatChecker(HeartbeatChecker):
    def send_and_check(self):
        """Send and check heartbeat in a Greenlet object"""
        gevent.spawn(super(PikaGeventHeartbeatChecker, self).send_and_check)


class PikaGeventConnection(BaseConnection):
    """ A standard Pika connection adapter for gevent.

        :param pika.connection.Parameters parameters: Connection parameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method
        :param on_open_error_callback: Method to call if the connection cant be opened
        :type on_open_error_callback: method
    """
    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None):
        self.connected = False
        super(PikaGeventConnection, self).__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback
        )

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to gevent hub to fire after deadline
        seconds.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: timer instance

        """
        timer = gevent.get_hub().loop.timer(deadline)
        timer.start(callback_method)
        return timer

    def remove_timeout(self, timeout_id):
        """Remove the timer from gevent hub using the instance returned from
        add_timeout.

        param: timer instance handle

        """
        timer = timeout_id
        timer.stop()

    def _adapter_connect(self):
        """Connect and read data in a Greenlet object

        """
        error = super(PikaGeventConnection, self)._adapter_connect()
        if not error:
            self.socket.setblocking(1)
            self.connected = True
            gevent.spawn(self._read_loop)
        return error

    def _adapter_disconnect(self):
        """Disconnect the connection

        """
        self.connected = False
        super(PikaGeventConnection, self)._adapter_disconnect()

    def _read_loop(self):
        """A read loop run in a Greenlet object if the connection is connected

        """
        while self.connected:
            self._handle_read()

    def _manage_event_state(self):
        """No need manage event state, connection works like in a block mode.

        """
        pass

    def _create_heartbeat_checker(self):
        """Create a heartbeat checker instance if there is a heartbeat interval
        set.

        :rtype: PikaGeventHeartbeatChecker

        """
        if self.params.heartbeat is not None and self.params.heartbeat > 0:
            return PikaGeventHeartbeatChecker(self, self.params.heartbeat)