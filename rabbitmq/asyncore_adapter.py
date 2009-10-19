import socket
import asyncore
import rabbitmq.connection

class RabbitDispatcher(asyncore.dispatcher):
    def __init__(self, connection):
        asyncore.dispatcher.__init__(self)
        self.connection = connection

    def handle_connect(self):
        self.connection.on_connected()

    def handle_close(self):
        self.connection.on_disconnected()
        self.close()

    def handle_read(self):
        try:
            buf = self.recv(self.connection.suggested_buffer_size())
        except socket.error:
            self.handle_close()
            raise

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

class AsyncoreConnection(rabbitmq.connection.Connection):
    def __init__(self, *args, **kw):
        self.dispatcher = RabbitDispatcher(self)
        rabbitmq.connection.Connection.__init__(self, *args, **kw)

    def connect(self, host, port):
        self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dispatcher.connect((host, port or spec.PORT))

    def shutdown_event_loop(self):
        self.dispatcher.close()

    def drain_events(self):
        asyncore.loop(count = 1)
