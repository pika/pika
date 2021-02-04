"""TCP/IP forwarding/echo service for testing."""

from __future__ import print_function

import array
from datetime import datetime
import errno
from functools import partial
import logging
import multiprocessing
import os
import socket
import struct
import sys
import threading
import traceback

import pika.compat

if pika.compat.PY3:

    def buffer(object, offset, size):  # pylint: disable=W0622
        """array etc. have the buffer protocol"""
        return object[offset:offset + size]


try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer  # pylint: disable=F0401


def _trace(fmt, *args):
    """Format and output the text to stderr"""
    print((fmt % args) + "\n", end="", file=sys.stderr)


class ForwardServer(object):  # pylint: disable=R0902
    """ Implement a TCP/IP forwarding/echo service for testing. Listens for
    an incoming TCP/IP connection, accepts it, then connects to the given
    remote address and forwards data back and forth between the two
    endpoints.

    This is similar to a subset of `netcat` functionality, but without
    dependency on any specific flavor of netcat

    Connection forwarding example; forward local connection to default
      rabbitmq addr, connect to rabbit via forwarder, then disconnect
      forwarder, then attempt another pika operation to see what happens

        with ForwardServer(("localhost", 5672)) as fwd:
            params = pika.ConnectionParameters(
                host=fwd.server_address[0],
                port=fwd.server_address[1])
            conn = pika.BlockingConnection(params)

        # Once outside the context, the forwarder is disconnected

        # Let's see what happens in pika with a disconnected server
        channel = conn.channel()

    Echo server example
        def produce(sock):
            sock.sendall("12345")
            sock.shutdown(socket.SHUT_WR)

        with ForwardServer(None) as echo:
            sock = socket.socket()
            sock.connect(echo.server_address)

            worker = threading.Thread(target=produce,
                                      args=[sock])
            worker.start()

            data = sock.makefile().read()
            assert data == "12345", data

        worker.join()

    """
    # Amount of time, in seconds, we're willing to wait for the subprocess
    _SUBPROC_TIMEOUT = 10

    def __init__(
            self,  # pylint: disable=R0913
            remote_addr,
            remote_addr_family=socket.AF_INET,
            remote_socket_type=socket.SOCK_STREAM,
            server_addr=("127.0.0.1", 0),
            server_addr_family=socket.AF_INET,
            server_socket_type=socket.SOCK_STREAM,
            local_linger_args=None):
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
        :param tuple local_linger_args: SO_LINGER sockoverride for the local
          connection sockets, to be configured after connection is accepted.
          None for default, which is to not change the SO_LINGER option.
          Otherwise, its a two-tuple, where the first element is the `l_onoff`
          switch, and the second element is the `l_linger` value, in seconds
        """
        self._logger = logging.getLogger(__name__)

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

        self._local_linger_args = local_linger_args

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
        queue = multiprocessing.Queue()

        self._subproc = multiprocessing.Process(
            target=_run_server,
            kwargs=dict(
                local_addr=self._server_addr,
                local_addr_family=self._server_addr_family,
                local_socket_type=self._server_socket_type,
                local_linger_args=self._local_linger_args,
                remote_addr=self._remote_addr,
                remote_addr_family=self._remote_addr_family,
                remote_socket_type=self._remote_socket_type,
                queue=queue))
        self._subproc.daemon = True
        self._subproc.start()

        try:
            # Get server socket info from subprocess
            self._server_addr_family, self._server_addr = queue.get(
                block=True, timeout=self._SUBPROC_TIMEOUT)
            queue.close()
        except Exception:  # pylint: disable=W0703
            try:
                self._logger.exception(
                    "Failed while waiting for local socket info")
                # Preserve primary exception and traceback
                raise
            finally:
                # Clean up
                try:
                    self.stop()
                except Exception:  # pylint: disable=W0703
                    # Suppress secondary exception in favor of the primary
                    self._logger.exception(
                        "Emergency subprocess shutdown failed")

        return self

    def stop(self):
        """Stop the server

        NOTE: The context manager is the recommended way to use
        ForwardServer. start()/stop() are alternatives to the context manager
        use case and are mutually exclusive with it.
        """
        self._logger.info("ForwardServer STOPPING")

        try:
            self._subproc.terminate()
            self._subproc.join(timeout=self._SUBPROC_TIMEOUT)
            if self._subproc.is_alive():
                self._logger.error(
                    "ForwardServer failed to terminate, killing it")
                os.kill(self._subproc.pid)
                self._subproc.join(timeout=self._SUBPROC_TIMEOUT)
                assert not self._subproc.is_alive(), self._subproc

            # Log subprocess's exit code; NOTE: negative signal.SIGTERM (usually
            # -15) is normal on POSIX systems - it corresponds to SIGTERM
            exit_code = self._subproc.exitcode
            self._logger.info("ForwardServer terminated with exitcode=%s",
                              exit_code)
        finally:
            self._subproc = None


def _run_server(
        local_addr,
        local_addr_family,
        local_socket_type,  # pylint: disable=R0913
        local_linger_args,
        remote_addr,
        remote_addr_family,
        remote_socket_type,
        queue):
    """ Run the server; executed in the subprocess

    :param local_addr: listening address
    :param local_addr_family: listening address family; one of socket.AF_*
    :param local_socket_type: listening socket type; typically
        socket.SOCK_STREAM
    :param tuple local_linger_args: SO_LINGER sockoverride for the local
        connection sockets, to be configured after connection is accepted.
        Pass None to not change SO_LINGER. Otherwise, its a two-tuple, where the
        first element is the `l_onoff` switch, and the second element is the
        `l_linger` value in seconds
    :param remote_addr: address of the target server. Pass None to have
        ForwardServer behave as echo server
    :param remote_addr_family: address family for connecting to target server;
        one of socket.AF_*
    :param remote_socket_type: socket type for connecting to target server;
        typically socket.SOCK_STREAM
    :param multiprocessing.Queue queue: queue for depositing the forwarding
        server's actual listening socket address family and bound address. The
        parent process waits for this.
    """

    # NOTE: We define _ThreadedTCPServer class as a closure in order to
    # override some of its class members dynamically
    # NOTE: we add `object` to the base classes because `_ThreadedTCPServer`
    # isn't derived from `object`, which prevents `super` from working properly
    class _ThreadedTCPServer(SocketServer.ThreadingMixIn,
                             SocketServer.TCPServer, object):
        """Threaded streaming server for forwarding"""

        # Override TCPServer's class members
        address_family = local_addr_family
        socket_type = local_socket_type
        allow_reuse_address = True

        def __init__(self):

            handler_class_factory = partial(
                _TCPHandler,
                local_linger_args=local_linger_args,
                remote_addr=remote_addr,
                remote_addr_family=remote_addr_family,
                remote_socket_type=remote_socket_type)

            super(_ThreadedTCPServer, self).__init__(
                local_addr, handler_class_factory, bind_and_activate=True)

    server = _ThreadedTCPServer()

    # Send server socket info back to parent process
    queue.put([server.socket.family, server.server_address])

    queue.close()

    server.serve_forever()


# NOTE: we add `object` to the base classes because `StreamRequestHandler` isn't
# derived from `object`, which prevents `super` from working properly
class _TCPHandler(SocketServer.StreamRequestHandler, object):
    """TCP/IP session handler instantiated by TCPServer upon incoming
    connection. Implements forwarding/echo of the incoming connection.
    """

    _SOCK_RX_BUF_SIZE = 16 * 1024

    def __init__(
            self,  # pylint: disable=R0913
            request,
            client_address,
            server,
            local_linger_args,
            remote_addr,
            remote_addr_family,
            remote_socket_type):
        """
        :param request: for super
        :param client_address: for super
        "paarm server:  for super
        :param tuple local_linger_args: SO_LINGER sockoverride for the local
            connection sockets, to be configured after connection is accepted.
            Pass None to not change SO_LINGER. Otherwise, its a two-tuple, where
            the first element is the `l_onoff` switch, and the second element is
            the `l_linger` value in seconds
        :param remote_addr: address of the target server. Pass None to have
            ForwardServer behave as echo server.
        :param remote_addr_family: address family for connecting to target
            server; one of socket.AF_*
        :param remote_socket_type: socket type for connecting to target server;
            typically socket.SOCK_STREAM
        :param **kwargs: kwargs for super class
        """
        self._local_linger_args = local_linger_args
        self._remote_addr = remote_addr
        self._remote_addr_family = remote_addr_family
        self._remote_socket_type = remote_socket_type

        super(_TCPHandler, self).__init__(
            request=request, client_address=client_address, server=server)

    def handle(self):  # pylint: disable=R0912
        """Connect to remote and forward data between local and remote"""
        local_sock = self.connection

        if self._local_linger_args is not None:
            # Set SO_LINGER socket options on local socket
            l_onoff, l_linger = self._local_linger_args
            local_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                  struct.pack('ii', l_onoff, l_linger))

        if self._remote_addr is not None:
            # Forwarding set-up
            remote_dest_sock = remote_src_sock = socket.socket(
                family=self._remote_addr_family,
                type=self._remote_socket_type,
                proto=socket.IPPROTO_IP)
            remote_dest_sock.connect(self._remote_addr)
            _trace("%s _TCPHandler connected to remote %s", datetime.utcnow(),
                   remote_dest_sock.getpeername())
        else:
            # Echo set-up
            # NOTE: Use pika.compat._nonblocking_socketpair() since
            # socket.socketpair() isn't available on Windows under python 2 yet.
            remote_dest_sock, remote_src_sock = \
                    pika.compat._nonblocking_socketpair()
            # We rely on blocking I/O
            remote_dest_sock.setblocking(True)
            remote_src_sock.setblocking(True)

        try:
            local_forwarder = threading.Thread(
                target=self._forward, args=(
                    local_sock,
                    remote_dest_sock,
                ))
            local_forwarder.setDaemon(True)
            local_forwarder.start()

            try:
                self._forward(remote_src_sock, local_sock)
            finally:
                # Wait for local forwarder thread to exit
                local_forwarder.join()
        finally:
            try:
                try:
                    _safe_shutdown_socket(remote_dest_sock, socket.SHUT_RDWR)
                finally:
                    if remote_src_sock is not remote_dest_sock:
                        _safe_shutdown_socket(remote_src_sock, socket.SHUT_RDWR)
            finally:
                remote_dest_sock.close()
                if remote_src_sock is not remote_dest_sock:
                    remote_src_sock.close()

    def _forward(self, src_sock, dest_sock):  # pylint: disable=R0912
        """Forward from src_sock to dest_sock"""
        src_peername = src_sock.getpeername()

        _trace("%s forwarding from %s to %s", datetime.utcnow(), src_peername,
               dest_sock.getpeername())
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
                except pika.compat.SOCKET_ERROR as exc:
                    if exc.errno == errno.EINTR:
                        continue
                    elif exc.errno == errno.ECONNRESET:
                        # Source peer forcibly closed connection
                        _trace("%s errno.ECONNRESET from %s", datetime.utcnow(),
                               src_peername)
                        break
                    else:
                        _trace("%s Unexpected errno=%s from %s\n%s",
                               datetime.utcnow(), exc.errno, src_peername,
                               "".join(traceback.format_stack()))
                        raise

                if not nbytes:
                    # Source input EOF
                    _trace("%s EOF on %s", datetime.utcnow(), src_peername)
                    break

                try:
                    dest_sock.sendall(buffer(rx_buf, 0, nbytes))
                except pika.compat.SOCKET_ERROR as exc:
                    if exc.errno == errno.EPIPE:
                        # Destination peer closed its end of the connection
                        _trace("%s Destination peer %s closed its end of "
                               "the connection: errno.EPIPE", datetime.utcnow(),
                               dest_sock.getpeername())
                        break
                    elif exc.errno == errno.ECONNRESET:
                        # Destination peer forcibly closed connection
                        _trace("%s Destination peer %s forcibly closed "
                               "connection: errno.ECONNRESET",
                               datetime.utcnow(), dest_sock.getpeername())
                        break
                    else:
                        _trace("%s Unexpected errno=%s in sendall to %s\n%s",
                               datetime.utcnow(), exc.errno,
                               dest_sock.getpeername(), "".join(
                                   traceback.format_stack()))
                        raise
        except:
            _trace("forward failed\n%s", "".join(traceback.format_exc()))
            raise
        finally:
            _trace("%s done forwarding from %s", datetime.utcnow(),
                   src_peername)
            try:
                # Let source peer know we're done receiving
                _safe_shutdown_socket(src_sock, socket.SHUT_RD)
            finally:
                # Let destination peer know we're done sending
                _safe_shutdown_socket(dest_sock, socket.SHUT_WR)


def echo(port=0):
    """ This function implements a simple echo server for testing the
    Forwarder class.

    :param int port: port number on which to listen

    We run this function and it prints out the listening socket binding.
    Then, we run Forwarder and point it at this echo "server".
    Then, we run telnet and point it at forwarder and see if whatever we
    type gets echoed back to us.

    This function waits for the client to connect and exits after the client
    closes the connection
    """
    lsock = socket.socket()
    lsock.bind(("", port))
    lsock.listen(1)
    _trace("Listening on sockname=%s", lsock.getsockname())

    sock, remote_addr = lsock.accept()
    try:
        _trace("Connection from peer=%s", remote_addr)
        while True:
            try:
                data = sock.recv(4 * 1024)  # pylint: disable=E1101
            except pika.compat.SOCKET_ERROR as exc:
                if exc.errno == errno.EINTR:
                    continue
                else:
                    raise

            if not data:
                break

            sock.sendall(data)  # pylint: disable=E1101
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
    except pika.compat.SOCKET_ERROR as exc:
        if exc.errno != errno.ENOTCONN:
            raise
