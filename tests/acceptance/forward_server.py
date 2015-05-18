# ----------------------------------------------------------------------
# Copyright (c) 2015, Vitaly Kruglikov All rights reserved.
# ----------------------------------------------------------------------

"""TCP/IP forwarding/echo service for testing."""

import array
import errno
import multiprocessing
import socket
import sys
import threading
from pika.compat import PY3

if PY3:
    def buffer(object, offset, size):
        # array etc. have the buffer protocol
        return object[offset:offset + size]

try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer



class ForwardServer(object):
    """ Implement a TCP/IP forwarding/echo service for testing. Listens for
    an incoming TCP/IP connection, accepts it, then connects to the given
    remote address and forwards data back and forth between the two
    endpoints.

    This is similar to the subset of `netcat` functionality, but without
    dependency on any specific flavor of netcat

    Connection forwarding example; forward local connection to default
      rabbitmq addr, connect to rabbit via forwarder, then disconnect
      forwarder, then attempt another pika operation to see what happens

        with ForwardServer(("localhost", 5672)) as fwd:
            params = pika.ConnectionParameters(
                host="localhost",
                port=fwd.server_address[1])
            conn = pika.BlockingConnection(params)

        # Once outside the context, the forwarder is disconnected

        # Let's see what happens in pika with a disconnected server
        channel = conn.channel()

    Echo server example
        def talk_to_echo_server(port):
            pass

        with ForwardServer(None) as fwd:
            worker = threading.Thread(target=talk_to_echo_server,
                                      args=[fwd.server_address[1]])
            worker.start()
            time.sleep(5)

        worker.join()

    """


    def __init__(self,
                 remote_addr,
                 remote_addr_family=socket.AF_INET,
                 remote_socket_type=socket.SOCK_STREAM,
                 server_addr=("127.0.0.1", 0),
                 server_addr_family=socket.AF_INET,
                 server_socket_type=socket.SOCK_STREAM):
        """
        :param tuple remote_addr: remote server's IP address, whose structure
          depends on remote_addr_family; pair (host-or-ip-addr, port-number).
          Pass None to have ForwardServer behave as echo server.
        :param remote_addr_family: socket.AF_INET (the default), socket.AF_INET6
          or socket.AF_UNIX.
        :param remote_socket_type: only socket.SOCK_STREAM is supported at this
          time
        :param server_addr: optional address for binding this server's listening
          socket; the format depends on server_addr_family; defaults to
          ("127.0.0.1", 0)
        :param server_addr_family: Address family for this server's listening
          socket; socket.AF_INET (the default), socket.AF_INET6 or
          socket.AF_UNIX; defaults to socket.AF_INET
        :param server_socket_type: only socket.SOCK_STREAM is supported at this
          time
        """
        self._remote_addr = remote_addr
        self._remote_addr_family = remote_addr_family
        assert remote_socket_type == socket.SOCK_STREAM, remote_socket_type
        self._remote_socket_type = remote_socket_type


        assert server_addr is not None
        self._server_addr = server_addr

        assert server_addr_family is not None
        self._server_addr_family = server_addr_family

        assert server_socket_type == socket.SOCK_STREAM, server_socket_type
        self._server_socket_type = server_socket_type

        self._subproc = None


    @property
    def running(self):
        """Property: True if ForwardServer is active"""
        return self._subproc is not None

    @property
    def server_address_family(self):
        """Property: Get listening socket's address family

        NOTE: undefined before server starts and after it shuts down
        """
        assert self._server_addr_family is not None, "Not in context"

        return self._server_addr_family


    @property
    def server_address(self):
        """ Property: Get listening socket's address; the returned value
        depends on the listening socket's address family

        NOTE: undefined before server starts and after it shuts down
        """
        assert self._server_addr is not None, "Not in context"

        return self._server_addr


    def __enter__(self):
        """ Context manager entry. Starts the forwarding server

        :returns: self
        """
        return self.start()


    def __exit__(self, *args):
        """ Context manager exit; stops the forwarding server
        """
        self.stop()


    def start(self):
        """ Start the server

        NOTE: The context manager is the recommended way to use
        ForwardServer. start()/stop() are alternatives to the context manager
        use case and are mutually exclusive with it.

        :returns: self
        """

        server_addr = self._server_addr
        server_addr_family = self._server_addr_family
        server_socket_type = self._server_socket_type

        # NOTE: We define _ThreadedTCPServer class as a closure in order to
        # override some of its class members dynamically
        class _ThreadedTCPServer(SocketServer.ThreadingMixIn,
                                 SocketServer.TCPServer,
                                 object):

            # Override TCPServer's class members
            address_family = server_addr_family
            socket_type = server_socket_type
            allow_reuse_address = True


            def __init__(self,
                         remote_addr,
                         remote_addr_family,
                         remote_socket_type):
                self.remote_addr = remote_addr
                self.remote_addr_family = remote_addr_family
                self.remote_socket_type = remote_socket_type

                super(_ThreadedTCPServer, self).__init__(
                    server_addr,
                    _TCPHandler,
                    bind_and_activate=True)


        server = _ThreadedTCPServer(self._remote_addr,
                                    self._remote_addr_family,
                                    self._remote_socket_type)

        self._server_addr_family = server.socket.family
        self._server_addr = server.server_address

        self._subproc = multiprocessing.Process(target=_run_server,
                                                args=(server,))
        self._subproc.daemon = True
        self._subproc.start()

        return self


    def stop(self):
        """Stop the server

        NOTE: The context manager is the recommended way to use
        ForwardServer. start()/stop() are alternatives to the context manager
        use case and are mutually exclusive with it.
        """
        self._subproc.terminate()
        self._subproc.join(timeout=10)
        self._subproc = None



def _run_server(server):
    """ Run the server

    :param _ThreadedTCPServer server:
    """
    server.serve_forever()



class _TCPHandler(SocketServer.StreamRequestHandler):
    """TCP/IP session handler instantiated by TCPServer upon incoming
    connection. Implements forwarding/echo of the incoming connection.
    """

    _SOCK_RX_BUF_SIZE = 16 * 1024

    def handle(self):
        def forward(src_sock, dest_sock):
            try:
                # NOTE: python 2.6 doesn't support bytearray with recv_into, so
                # we use array.array instead; this is only okay as long as the
                # array instance isn't shared across threads. See
                # http://bugs.python.org/issue7827 and
                # groups.google.com/forum/#!topic/comp.lang.python/M6Pqr-KUjQw
                rx_buf = array.array("B", [0] * self._SOCK_RX_BUF_SIZE)

                while True:
                    try:
                        nbytes = src_sock.recv_into(rx_buf)
                    except socket.error as e:
                        if e.errno == errno.EINTR:
                            continue
                        elif e.errno == errno.ECONNRESET:
                            # Source peer forcibly closed connection
                            break
                        else:
                            raise

                    if not nbytes:
                        # Source input EOF
                        break

                    try:
                        dest_sock.sendall(buffer(rx_buf, 0, nbytes))
                    except socket.error as e:
                        if e.errno == errno.EPIPE:
                            # Destination peer closed its end of the connection
                            break
                        elif e.errno == errno.ECONNRESET:
                            # Destination peer forcibly closed connection
                            break
                        else:
                            raise
            finally:
                try:
                    # Let source peer know we're done receiving
                    _safe_shutdown_socket(src_sock, socket.SHUT_RD)
                finally:
                    # Let destination peer know we're done sending
                    _safe_shutdown_socket(dest_sock, socket.SHUT_WR)

        local_sock = self.connection

        if self.server.remote_addr is not None:
            # Forwarding set-up
            remote_dest_sock = remote_src_sock = socket.socket(
                family=self.server.remote_addr_family,
                type=self.server.remote_socket_type,
                proto=socket.IPPROTO_IP)
            remote_dest_sock.connect(self.server.remote_addr)
        else:
            # Echo set-up
            remote_dest_sock, remote_src_sock = socket_pair()

        try:
            local_forwarder = threading.Thread(
                target=forward,
                args=(local_sock, remote_dest_sock,))
            local_forwarder.setDaemon(True)
            local_forwarder.start()

            try:
                forward(remote_src_sock, local_sock)
            finally:
                # Wait for local forwarder thread to exit
                local_forwarder.join()
        finally:
            try:
                try:
                    _safe_shutdown_socket(remote_dest_sock,
                                          socket.SHUT_RDWR)
                finally:
                    if remote_src_sock is not remote_dest_sock:
                        _safe_shutdown_socket(remote_src_sock,
                                              socket.SHUT_RDWR)
            finally:
                remote_dest_sock.close()
                if remote_src_sock is not remote_dest_sock:
                    remote_src_sock.close()




def echo(port=0):
    """ This function implements a simple echo server for testing the
    Forwarder class.

    :param int port: port number on which to listen

    We run this function and it prints out the listening socket binding.
    Then, we run Forwarder and point it at this echo "server".
    Then, we run telnet and point it at forwarder and see if whatever we
    type gets echoed back to us.

    This function exits when the remote end connects, then closes connection
    """
    lsock = socket.socket()
    lsock.bind(("", port))
    lsock.listen(1)
    print >> sys.stderr, "Listening on sockname:", lsock.getsockname()

    sock, remote_addr = lsock.accept()
    try:
        print >> sys.stderr, "Connection from peer:", remote_addr
        while True:
            try:
                data = sock.recv(4 * 1024)
            except socket.error as e:
                if e.errno == errno.EINTR:
                    continue
                else:
                    raise

            if not data:
                break

            sock.sendall(data)
    finally:
        try:
            _safe_shutdown_socket(sock, socket.SHUT_RDWR)
        finally:
            sock.close()



def _safe_shutdown_socket(sock, how=socket.SHUT_RDWR):
    """ Shutdown a socket, suppressing ENOTCONN
    """
    try:
        sock.shutdown(how)
    except socket.error as e:
        if e.errno != errno.ENOTCONN:
            raise



def socket_pair(family=None, sock_type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_IP):
    """ socket.socketpair abstraction with support for Windows

    :param family: address family; e.g., socket.AF_UNIX, socket.AF_INET, etc.;
      defaults to socket.AF_UNIX if available, with fallback to socket.AF_INET.
    :param sock_type: socket type; defaults to socket.SOCK_STREAM
    :param proto: protocol; defaults to socket.IPPROTO_IP
    """
    if family is None:
        if hasattr(socket, "AF_UNIX"):
            family = socket.AF_UNIX
        else:
            family = socket.AF_INET

    if hasattr(socket, "socketpair"):
        socket1, socket2 = socket.socketpair(family, sock_type, proto)
    else:
        # Probably running on Windows where socket.socketpair isn't supported

        # Work around lack of socket.socketpair()

        socket1 = socket2 = None

        listener = socket(family, sock_type, proto)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        listener.bind(("localhost", 0))
        listener.listen(1)
        listener_port = listener.getsockname()[1]

        socket1 = socket(family, sock_type, proto)

        # Use thread to connect in background, while foreground issues the
        # blocking accept()
        conn_thread = threading.Thread(
            target=socket1.connect,
            args=(('localhost', listener_port),))
        conn_thread.setDaemon(1)
        conn_thread.start()

        try:
            socket2 = listener.accept()[0]
        finally:
            listener.close()

            # Join/reap background thread
            conn_thread.join(timeout=10)

    return (socket1, socket2)
