"""
A downstream caller publishing both body types `basic_publish` accepts.

The channels encode a `str` body as UTF-8, so the annotation must admit `str` as well as `bytes`.
See `tests/typing/README.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.adapters.twisted_connection import TwistedChannel
    from pika.channel import Channel


def publish_bytes_and_str(channel: Channel, blocking: BlockingChannel,
                          twisted: TwistedChannel) -> None:
    """
    Publish a `bytes` and a `str` body on each channel type.

    :param channel: An open asynchronous channel
    :param blocking: An open blocking channel
    :param twisted: An open Twisted channel
    """
    channel.basic_publish('', 'queue', b'body')
    channel.basic_publish('', 'queue', 'body')
    blocking.basic_publish('', 'queue', b'body')
    blocking.basic_publish('', 'queue', 'body')
    twisted.basic_publish('', 'queue', b'body')
    twisted.basic_publish('', 'queue', 'body')
