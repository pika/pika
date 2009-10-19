import sys
import traceback
import socket
import asyncore
import time
from heapq import heappush, heappop
from errno import EAGAIN
import pika.connection

class RabbitDispatcher(asyncore.dispatcher):
    def __init__(self, connection):
        asyncore.dispatcher.__init__(self)
        self.connection = connection

    def handle_connect(self):
        self.connection.on_connected()

    def handle_close(self):
        self.connection.on_disconnected()
        self.connection.dispatcher = None
        self.close()

    def handle_read(self):
        try:
            buf = self.recv(self.connection.suggested_buffer_size())
        except socket.error, exn:
            if exn.errno == EAGAIN:
                # Weird, but happens very occasionally.
                return
            else:
                self.handle_close()
                return

        if not buf:
            self.close()
            return

        self.connection.on_data_available(buf)

    def writable(self):
        return bool(self.connection.outbound_buffer)

    def handle_write(self):
        fragment = self.connection.outbound_buffer.read()
        r = self.send(fragment)
        self.connection.outbound_buffer.consume(r)

class AsyncoreConnection(pika.connection.Connection):
    def reconnect_after(self, delay_sec):
        add_oneshot_timer_rel(delay_sec, self.reconnect)

    def connect(self, host, port):
        self.dispatcher = RabbitDispatcher(self)
        self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dispatcher.connect((host, port or spec.PORT))

    def shutdown_event_loop(self):
        self.dispatcher.close()

    def drain_events(self):
        loop(count = 1)

timer_heap = []

def add_oneshot_timer_abs(firing_time, callback):
    heappush(timer_heap, (firing_time, callback))

def add_oneshot_timer_rel(firing_delay, callback):
    add_oneshot_timer_abs(time.time() + firing_delay, callback)

def next_event_timeout(): 
    cutoff = run_timers_internal()
    if timer_heap:
        timeout = timer_heap[0][0] - cutoff
    else:
        timeout = 30.0 # default timeout
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

def loop1(map):
    if map:
        asyncore.loop(timeout = next_event_timeout(), map = map, count = 1)
    else:
        time.sleep(next_event_timeout())

def loop(map = None, count = None):
    if map is None:
        map = asyncore.socket_map
    if count is None:
        while (map or timer_heap):
            loop1(map)
    else:
        while (map or timer_heap) and count > 0:
            loop1(map)
            count = count - 1
        run_timers_internal()
