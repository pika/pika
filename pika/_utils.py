"""Internal utility helpers for platform and socket compatibility."""
from __future__ import annotations

import abc
import os
import re
import socket
import sys
import time

RE_NUM = re.compile(r'(\d+).+')

ON_LINUX = sys.platform.startswith('linux')
ON_OSX = sys.platform == 'darwin'
ON_WINDOWS = sys.platform == 'win32'

# Portable Abstract Base Class
AbstractBase = abc.ABCMeta('AbstractBase', (object,), {})

SOCKET_ERROR = OSError

try:
    SOL_TCP = socket.SOL_TCP
except AttributeError:
    SOL_TCP = socket.IPPROTO_TCP

HAVE_SIGNAL = os.name == 'posix'

_LOCALHOST = '127.0.0.1'
_LOCALHOST_V6 = '::1'


def time_now() -> float:
    """Returns monotonic time.

    :rtype: float
    """
    return time.monotonic()


def as_bytes(value: str | bytes) -> bytes:
    """Returns value as bytes.

    :param value: string or bytes value to convert
    :rtype: bytes
    """
    if not isinstance(value, bytes):
        return value.encode('UTF-8')
    return value


def to_digit(value: str) -> int:
    """Returns value as an integer.

    :param value: string containing digits
    :rtype: int
    """
    if value.isdigit():
        return int(value)
    match = RE_NUM.match(value)
    return int(match.groups()[0]) if match else 0


def get_linux_version(release_str: str) -> tuple[int, ...]:
    """Gets linux version.

    :param release_str: kernel release string
    :rtype: tuple[int, ...]
    """
    ver_str = release_str.split('-', 1)[0]
    return tuple(map(to_digit, ver_str.split('.', 3)[:3]))


LINUX_VERSION = None
if ON_LINUX:
    import platform
    LINUX_VERSION = get_linux_version(platform.release())


def nonblocking_socketpair(
        family: int = socket.AF_INET,
        socket_type: int = socket.SOCK_STREAM,
        proto: int = 0) -> tuple[socket.socket, socket.socket]:
    """Non-blocking socket pair for use with pika's I/O loop.

    Returns a pair of sockets in the manner of socketpair with the additional
    feature that they will be non-blocking.

    :param family: socket family (default AF_INET)
    :param socket_type: socket type (default SOCK_STREAM)
    :param proto: socket protocol (default 0)
    :returns: a pair of connected non-blocking sockets
    :rtype: tuple[socket.socket, socket.socket]
    """
    if family == socket.AF_INET:
        host = _LOCALHOST
    elif family == socket.AF_INET6:
        host = _LOCALHOST_V6
    else:
        raise ValueError('Only AF_INET and AF_INET6 socket address families '
                         'are supported')
    if socket_type != socket.SOCK_STREAM:
        raise ValueError('Only SOCK_STREAM socket socket_type is supported')
    if proto != 0:
        raise ValueError('Only protocol zero is supported')

    lsock = socket.socket(family, socket_type, proto)
    try:
        lsock.bind((host, 0))
        lsock.listen(min(socket.SOMAXCONN, 128))
        # On IPv6, ignore flow_info and scope_id
        addr, port = lsock.getsockname()[:2]
        csock = socket.socket(family, socket_type, proto)
        try:
            csock.connect((addr, port))
            ssock, _ = lsock.accept()
        except Exception:
            csock.close()
            raise
    finally:
        lsock.close()

    csock.setblocking(False)
    ssock.setblocking(False)

    return ssock, csock
