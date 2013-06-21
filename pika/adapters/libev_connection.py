"""Use pika with the libev IOLoop via pyev"""
import pyev
import signal
import array
import logging

from pika.adapters.base_connection import BaseConnection

LOGGER = logging.getLogger(__name__)

class LibevConnection(BaseConnection):
    """The LibevConnection runs on the libev IOLoop. If you're running the
    connection in a web app, make sure you set stop_ioloop_on_close to False,
    which is the default behavior for this adapter, otherwise the web app
    will stop taking requests.

    :param pika.connection.Parameters parameters: Connection parameters
    :param on_open_callback: The method to call when the connection is open
    :type on_open_callback: method
    :param on_open_error_callback: Method to call if the connection can't
                                   be opened
    :type on_open_error_callback: method
    :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
    :param custom_ioloop: Override using the default_loop in libev

    """
    WARN_ABOUT_IOLOOP = True
    
    # use static arrays to translate masks between pika and libev
    PIKA_TO_LIBEV_ARRAY = array.array(
        'i', 
        [0] * (
            (BaseConnection.READ|BaseConnection.WRITE|BaseConnection.ERROR) + 1
        )
    )
    
    PIKA_TO_LIBEV_ARRAY[BaseConnection.READ] = pyev.EV_READ
    PIKA_TO_LIBEV_ARRAY[BaseConnection.WRITE] = pyev.EV_WRITE
    
    PIKA_TO_LIBEV_ARRAY[
        BaseConnection.READ|BaseConnection.WRITE
    ] = pyev.EV_READ|pyev.EV_WRITE
    
    PIKA_TO_LIBEV_ARRAY[
        BaseConnection.READ|BaseConnection.ERROR
    ] = pyev.EV_READ
    
    PIKA_TO_LIBEV_ARRAY[
        BaseConnection.WRITE|BaseConnection.ERROR
    ] = pyev.EV_WRITE
    
    PIKA_TO_LIBEV_ARRAY[
        BaseConnection.READ|BaseConnection.WRITE|BaseConnection.ERROR
    ] = pyev.EV_READ|pyev.EV_WRITE
    
    LIBEV_TO_PIKA_ARRAY = array.array(
        'i', 
        [0] * ((pyev.EV_READ|pyev.EV_WRITE) + 1)
    )
    
    LIBEV_TO_PIKA_ARRAY[pyev.EV_READ] = BaseConnection.READ
    LIBEV_TO_PIKA_ARRAY[pyev.EV_WRITE] = BaseConnection.WRITE
    
    LIBEV_TO_PIKA_ARRAY[
        pyev.EV_READ|pyev.EV_WRITE
    ] = BaseConnection.READ|BaseConnection.WRITE

    def __init__(
        self, parameters=None,
        on_open_callback=None,
        on_open_error_callback=None,
        on_close_callback=None,
        stop_ioloop_on_close=False,
        custom_ioloop=None
    ):
        """Create a new instance of the LibevConnection class, connecting
        to RabbitMQ automatically

        :param pika.connection.Parameters parameters: Connection parameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method
        :param on_open_error_callback: Method to call if the connection cant
                                       be opened
        :type on_open_error_callback: method
        :param bool stop_ioloop_on_close: Call ioloop.stop() if disconnected
        :param custom_ioloop: Override using the default IOLoop in libev

        """
        self.ioloop = custom_ioloop or pyev.default_loop(debug=True)
        self.sigint_watcher = None
        self.sigterm_watcher = None
        self.io_watcher = None
        self._active_timers = {}
        self._stopped_timers = []

        super(LibevConnection, self).__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            self.ioloop,
            stop_ioloop_on_close
        )

    def _adapter_connect(self):
        """Connect to the remote socket, adding the socket to the IOLoop if
        connected

        :rtype: bool

        """
        LOGGER.debug('create io and signal watchers')
                
        if super(LibevConnection, self)._adapter_connect():
            self.sigterm_watcher = self.ioloop.signal(
                signal.SIGTERM,
                self._handle_signal
            )

            self.sigint_watcher = self.ioloop.signal(
                signal.SIGINT,
                self._handle_signal
            )

            self.io_watcher = self.ioloop.io(
                self.socket.fileno(),
                self.PIKA_TO_LIBEV_ARRAY[self.event_state],
                self._handle_events
            )
            
            self.sigterm_watcher.start()
            self.sigint_watcher.start()
            self.io_watcher.start()
            return True
        return False

    def _adapter_disconnect(self):
        """Disconnect from the RabbitMQ broker"""
        LOGGER.debug('disconnect')
        if self.sigint_watcher: self.sigint_watcher.stop()
        if self.sigterm_watcher: self.sigterm_watcher.stop()
        if self.io_watcher: self.io_watcher.stop()
        super(LibevConnection, self)._adapter_disconnect()
        
    def _handle_signal(self, signal_watcher, libev_events):
        LOGGER.debug('SIGINT or SIGTERM')
        raise KeyboardInterrupt
        
    def _handle_events(self, io_watcher, libev_events):
        super(LibevConnection, self)._handle_events(
            io_watcher.fd, 
            self.LIBEV_TO_PIKA_ARRAY[libev_events]
        )

    def _manage_event_state(self):
        """Manage the bitmask for reading/writing/error which is used by the
        io/event handler to specify when there is an event such as a read or
        write.
        
        """
        if self.outbound_buffer:
            if not self.event_state & self.WRITE: 
                self.event_state |= self.WRITE
                self.io_watcher.stop()

                self.io_watcher.set(
                    self.io_watcher.fd,
                    self.PIKA_TO_LIBEV_ARRAY[self.event_state]
                )

                self.io_watcher.start()
        elif self.event_state & self.WRITE:            
            self.event_state = self.base_events
            self.io_watcher.stop()

            self.io_watcher.set(
                self.io_watcher.fd,
                self.PIKA_TO_LIBEV_ARRAY[self.event_state]
            )

            self.io_watcher.start()

    def _timer_callback(self, timer, libev_events):
        """Indirect callback glue."""
        if timer in self._active_timers: 
            self._active_timers[timer]()
            self.remove_timeout(timer)
        else:
            LOGGER.warning('Timer callback_method not found')
            
    def _get_timer(self, deadline):
        """Timer pool"""
        if self._stopped_timers:
            timer = self._stopped_timers.pop()
            timer.set(deadline, 0.0)
        else:
            timer = self.ioloop.timer(deadline, 0.0, self._timer_callback)
            
        return timer

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method indirectly to the IOLoop timer to fire
         after deadline seconds. Returns the timer handle.
        
        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: timer instance handle.

        """
        LOGGER.debug('deadline: {0}'.format(deadline))
        timer = self._get_timer(deadline)
        self._active_timers[timer] = callback_method
        timer.start()
        return timer
        
    def remove_timeout(self, timer):
        """Remove the timer from the IOLoop using the handle returned from
        add_timeout.

        """
        LOGGER.debug('stop')
        self._active_timers.pop(timer, None)
        timer.stop()
        self._stopped_timers.append(timer)
        
    def _create_and_connect_to_socket(self, sock_addr_tuple):
        result = super(LibevConnection, self)._create_and_connect_to_socket(
            sock_addr_tuple
        )
        
        if result: self.socket.setblocking(0) # set socket to non-blocking
        return result
