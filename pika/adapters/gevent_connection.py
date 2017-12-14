"""Use pika with the GEVENT"""

import gevent

from pika.adapters.base_connection import BaseConnection
from pika.heartbeat import HeartbeatChecker


class _GeventHeartbeatChecker(HeartbeatChecker):
    def send_and_check(self):
        """Send and check heartbeat in a Greenlet object"""
        gevent.spawn(super(_GeventHeartbeatChecker, self).send_and_check)


class GeventFakeIOLoop(object):
    """Fake IOLoop for gevent"""

    def start(self):
        pass

    def stop(self):
        pass


class GeventConnection(BaseConnection):
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
        super(GeventConnection, self).__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            GeventFakeIOLoop(),
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

    def _on_connect_timer(self):
        gevent.spawn(super(GeventConnection, self)._on_connect_timer)

    def _adapter_connect(self):
        """Connect and read data in a Greenlet object

        """
        error = super(GeventConnection, self)._adapter_connect()
        if not error:
            self.socket.setblocking(1)
            gevent.spawn(self._read_loop)
        return error

    def _adapter_disconnect(self):
        """Disconnect the connection

        """
        super(GeventConnection, self)._adapter_disconnect()

    def _read_loop(self):
        """A read loop run in a Greenlet object if the connection is connected

        """
        while self.socket:
            try:
                self._handle_read()
            except:
                raise Exception("socke read error")

    def _flush_outbound(self):
        """Override BaseConnection._flush_outbound to send all bufferred data
        the Twisted way, by writing to the transport. No need for buffering,
        Twisted handles that for us.
        """
        self._handle_write()

    def _manage_event_state(self):
        """No need manage event state, connection works like in a block mode.

        """
        pass

    def _create_heartbeat_checker(self):
        """Create a heartbeat checker instance if there is a heartbeat interval
        set.

        :rtype: _GeventHeartbeatChecker

        """
        if self.params.heartbeat is not None and self.params.heartbeat > 0:
            return _GeventHeartbeatChecker(self, self.params.heartbeat)

    def _handle_ioloop_stop(self):
        """DO NOTHING"""
        pass
