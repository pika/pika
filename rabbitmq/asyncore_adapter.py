import socket
import asyncore
import rabbitmq.connection

class AsyncoreConnection(rabbitmq.connection.Connection, asyncore.dispatcher):
    def __init__(self, *args, **kw):
        ## TODO: use composition, not inheritance, to integrate with asyncore
        asyncore.dispatcher.__init__(self)
        rabbitmq.connection.Connection.__init__(self, *args, **kw)

    def amqp_connect(self, host, port):
        ## TODO: think harder about split here
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port or spec.PORT))

    def shutdown_event_loop(self):
        asyncore.dispatcher.close(self)

    def drain_events(self):
        asyncore.loop(count = 1)

    def handle_error(self):
        ## TODO: do something sensible

## readable(), writable() are interesting methods
