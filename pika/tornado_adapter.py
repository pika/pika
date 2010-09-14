import errno
import pika.connection
import select
import socket
import sys
import time
import tornado.ioloop
import logging

class TornadoConnection(pika.connection.AsyncConnection):

    def delayed_call(self, delay_sec, callback):
        deadline = time.time() + delay_sec
        self.io_loop.add_timeout(deadline, callback)

    def connect(self, host, port):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.sock.connect((host, port))
        self.sock.setblocking(0)
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.handle_connection_open()
          
        events = tornado.ioloop.IOLoop.READ | tornado.ioloop.IOLoop.WRITE | tornado.ioloop.IOLoop.ERROR
        self.io_loop.add_handler(self.sock.fileno(), self._handle_events, events)
        
        self._handle_write()
        
    def wait_for_open(self):
        logging.debug("In wait for open")
        while (not self.connection_open) and \
            (self.reconnection_strategy.can_reconnect() or (not self.connection_close)):
            self.drain_events()        
    
        
    def _handle_events(self, fd, events):
        
        if not self.sock:
            logging.warning("Got events for closed stream %d", fd)
            return
        if events & tornado.ioloop.IOLoop.READ:
            self._handle_read()
            
        if not self.sock:
            return
        if events & tornado.ioloop.IOLoop.WRITE:
            self._handle_write()
        if not self.sock:
            return
        if events & tornado.ioloop.IOLoop.ERROR:
            self.sock.close()
            return

    def _handle_read(self):
        try:
            chunk = self.sock.recv(self.suggested_buffer_size())
        except socket.error, e:
            if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return
            else:
                logging.warning("Read error on %d: %s",
                                self.sock.fileno(), e)
                self.close()
                return
        
        # Data is wonky
        if not chunk:
            self.close()
            return

        # Let our parent class have the data
        self.on_data_available(chunk)
        
    def _handle_write(self):
        fragment = self.outbound_buffer.read()
        try:
            r = self.sock.send(fragment)
        except socket.error, e:
            if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return
            else:
                logging.warning("Write error on %d: %s",
                                self.sock.fileno(), e)
                self.close()
                return
                
        self.outbound_buffer.consume(r)
         
    def disconnect_transport(self):
        self.sock.close()
        
    def flush_outbound(self):
        #select.select([self.sock], [self.sock], [self.sock])
        pass
        
    def drain_events(self, timeout=None):
        #print self.connection_open
        #time.sleep(1)
        #sys.exit(-1)
        pass
