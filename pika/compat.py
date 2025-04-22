"""The compat module provides various compatibility functions"""
# pylint: disable=C0103

import abc
import os
import re
import socket
import sys
import time

RE_NUM = re.compile(r'(\d+).+')

ON_LINUX = sys.platform.startswith("linux")
ON_OSX = sys.platform == "darwin"
ON_WINDOWS = sys.platform == "win32"

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

# for assertions that the data is either encoded or non-encoded text
str_or_bytes = (str, bytes)


def time_now():
    """
    Returns monotonic time
    """
    return time.monotonic()


def byte(*args):
    """
    Returns a single byte `bytes` for the given int argument (we
    optimize it a bit here by passing the positional argument tuple
    directly to the bytes constructor.
    """
    return bytes(args)


class long(int):
    """
    A marker class that signifies that the integer value should be
    serialized as `l` instead of `I`
    """

    def __str__(self):
        return str(int(self))

    def __repr__(self):
        return str(self) + 'L'


def canonical_str(value):
    """
    Return the canonical str value for the string.
    """

    return str(value)


def is_integer(value):
    """
    Is value an integer?
    """
    return isinstance(value, int)


def as_bytes(value):
    """
    Returns value as bytes
    """
    if not isinstance(value, bytes):
        return value.encode('UTF-8')
    return value


def to_digit(value):
    """
    Returns value as in integer
    """
    if value.isdigit():
        return int(value)
    match = RE_NUM.match(value)
    return int(match.groups()[0]) if match else 0


def get_linux_version(release_str):
    """
    Gets linux version
    """
    ver_str = release_str.split('-')[0]
    return tuple(map(to_digit, ver_str.split('.', 3)[:3]))


LINUX_VERSION = None
if ON_LINUX:
    import platform
    LINUX_VERSION = get_linux_version(platform.release())



def nonblocking_socketpair(family=socket.AF_INET,
                            socket_type=socket.SOCK_STREAM,
                            proto=0):
    """
    Returns a pair of sockets in the manner of socketpair with the additional
    feature that they will be non-blocking. Prior to Python 3.5, socketpair
    did not exist on Windows at all.
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

    # Make sockets non-blocking to prevent deadlocks
    # See https://github.com/pika/pika/issues/917
    csock.setblocking(False)
    ssock.setblocking(False)

    return ssock, csock
