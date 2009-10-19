import socket
import pika.connection

class BlockingConnection(pika.connection.Connection):
    def connect(self, host, port):
        self.socket = socket.socket()
        self.socket.connect((host, port))
        self.on_connected()

    def shutdown_event_loop(self):
        self.socket.close()
        self.on_disconnected()

    def mainloop(self):
        while self.is_alive():
            self.drain_events()

    def drain_events(self):
        while self.outbound_buffer:
            fragment = self.outbound_buffer.read()
            r = self.socket.send(fragment)
            self.outbound_buffer.consume(r)

        try:
            buf = self.socket.recv(self.suggested_buffer_size())
        except socket.error, exn:
            if exn.errno == EAGAIN:
                # Weird, but happens very occasionally.
                return
            else:
                self.shutdown_event_loop()
                return

        if not buf:
            self.close()
            return

        self.on_data_available(buf)
