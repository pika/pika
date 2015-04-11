"""Use pika with the Tornado IOLoop"""
from tornado import ioloop
import logging
import time
import socket
import errno

from pika.adapters import base_connection

LOGGER = logging.getLogger(__name__)


class TornadoConnection(base_connection.BaseConnection):
    """The TornadoConnection runs on the Tornado IOLoop. If you're running the
    connection in a web app, make sure you set stop_ioloop_on_close to False,
    which is the default behavior for this adapter, otherwise the web app
    will stop taking requests.

    :param pika.connection.Parameters parameters: Connection parameters
    :param on_open_callback: The method to call when the connection is open
    :type on_open_callback: method
    :param on_open_error_callback: Method to call if the connection cant
                                   be opened
    :type on_open_error_callback: method
    :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
    :param custom_ioloop: Override using the global IOLoop in Tornado

    """
    WARN_ABOUT_IOLOOP = True

    def __init__(self, parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=False,
                 custom_ioloop=None):
        """Create a new instance of the TornadoConnection class, connecting
        to RabbitMQ automatically

        :param pika.connection.Parameters parameters: Connection parameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method
        :param on_open_error_callback: Method to call if the connection cant
                                       be opened
        :type on_open_error_callback: method
        :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
        :param custom_ioloop: Override using the global IOLoop in Tornado

        """
        self.sleep_counter = 0
        self.ioloop = custom_ioloop or ioloop.IOLoop.instance()

        super(TornadoConnection, self).__init__(parameters,
                                                on_open_callback,
                                                on_open_error_callback,
                                                on_close_callback,
                                                self.ioloop,
                                                stop_ioloop_on_close)

    def _adapter_connect(self):
        """Connect to the remote socket, adding the socket to the IOLoop if
        connected.

        Set the socket non-blocking after super() call

        :rtype: bool

        """
        error = super(TornadoConnection, self)._adapter_connect()
        if not error:
            self.socket.setblocking(0) 
            self.ioloop.add_handler(self.socket.fileno(),
                                    self._handle_events,
                                    self.event_state)
        return error

    def _adapter_disconnect(self):
        """Disconnect from the RabbitMQ broker"""
        if self.socket:
            self.ioloop.remove_handler(self.socket.fileno())
        super(TornadoConnection, self)._adapter_disconnect()

    def _handle_write(self):
        """Try and write as much as we can, if we get blocked requeue 
        what's left"""
        bytes_written = 0
        try:
            while self.outbound_buffer:
                frame = self.outbound_buffer.popleft()
                bw = self.socket.send(frame)
                frame = frame[bw:]
                bytes_written += bw

                if frame:
                    LOGGER.warning("Partial write, requeing remaining data")
                    self.outbound_buffer.appendleft(frame)
                    break

        except socket.timeout:
            # Will get a timeout if the socket is blocking
            LOGGER.warning("socket timeout, requeuing frame")
            self.outbound_buffer.appendleft(frame)
            
        except socket.error as error:
            # If the socket is non-blocking, we'll come here instead
            if error.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                LOGGER.warning("Would block, requeuing frame")
                self.outbound_buffer.appendleft(frame)
            else:
                return self._handle_error(error)
            
        #LOGGER.warning("wrote %s bytes", bytes_written)
        return bytes_written     

    def _flush_outbound(self):
        """write early, if the socket will take the data why not get it out
        there asap.
        """
        self._handle_write()
        return super(TornadoConnection, self)._flush_outbound()

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        return self.ioloop.add_timeout(time.time() + deadline, callback_method)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :rtype: str

        """
        return self.ioloop.remove_timeout(timeout_id)
