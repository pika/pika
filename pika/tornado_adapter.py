import errno
import pika.connection
import select
import socket
import sys
import time
import tornado.ioloop
import logging

class TornadoConnection(pika.connection.AsyncConnection):

    product = "Pika/Tornado"
    
    def delayed_call(self, delay_sec, callback):
    
        deadline = time.time() + delay_sec
        self.io_loop.add_timeout(deadline, callback)

    def connect(self, host, port):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.sock.connect((host, port))
        self.sock.setblocking(0)
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.handle_connection_open()
          
        # Append our handler to tornado's ioloop for our socket
        events = tornado.ioloop.IOLoop.READ | tornado.ioloop.IOLoop.ERROR
        self.io_loop.add_handler(self.sock.fileno(), self._handle_events, events)
        
        # Loop on handle_write every 0.1 second        
        self.delayed_call(0.1, self._handle_write)
        
    def _handle_events(self, fd, events):
        
        # Incoming events from IOLoop, make sure we have our socket
        if not self.sock:
            logging.warning("Got events for closed stream %d", fd)
            return

        if events & tornado.ioloop.IOLoop.READ:
            self._handle_read()

        if events & tornado.ioloop.IOLoop.ERROR:
            self.sock.close()

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
        
    def _handle_write(self, add_timer = True):

        if len(self.outbound_buffer):
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
            
            # Remove the content we used from our buffer
            self.outbound_buffer.consume(r)
            
        # Loop on this ever 0.1 second
        if add_timer is True:
            self.delayed_call(0.1, self._handle_write)
         
    def disconnect_transport(self):

        # Close our socket since the Conneciton class told us to do so
        self.sock.close()
        
    def flush_outbound(self):
    
        # Lets just make sure we've written out our buffer but dont make a timer
        self._handle_write(False)
