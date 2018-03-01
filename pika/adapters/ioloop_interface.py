"""IO Loop interface for adapters.

TODO This will eventually contain the IOLoopABC class that defines the IO/Event
loop interface expected by `BaseConnection`
"""

# TODO Reconcile PollEvents placement once https://github.com/pika/pika/pull/974
# is merged.
class PollEvents(object):
    """Event flags for I/O"""

    # Use epoll's constants to keep life easy
    READ = 0x0001  # available for read
    WRITE = 0x0004  # available for write
    ERROR = 0x0008  # error on associated fd
