"""
A downstream caller using what the blocking RPC wrappers return.

Eleven of these wrappers once claimed `-> None` while returning a reply frame, and pika's own `mypy`
run stayed green because the value came from a helper typed `Any`. `warn_return_any` now reports
that inside pika, but not a return annotation removed outright, which reads as `Any` here. So each
reply is pinned with `assert_type`, which rejects `Any` as well as the wrong frame. See
`tests/typing/README.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import assert_type

from pika import frame, spec

if TYPE_CHECKING:
    from pika.adapters.thread_safe_connection import Channel


def read_every_rpc_reply(channel: Channel, queue: str, exchange: str) -> None:
    """
    Pin the reply frame each wrapper returns.

    :param channel: An open channel
    :param queue: A queue name
    :param exchange: An exchange name
    """
    assert_type(channel.queue_declare(queue, durable=True),
                frame.Method[spec.Queue.DeclareOk])
    assert_type(channel.queue_purge(queue), frame.Method[spec.Queue.PurgeOk])
    assert_type(channel.queue_delete(queue), frame.Method[spec.Queue.DeleteOk])
    assert_type(channel.queue_bind(queue, exchange),
                frame.Method[spec.Queue.BindOk])
    assert_type(channel.queue_unbind(queue, exchange),
                frame.Method[spec.Queue.UnbindOk])
    assert_type(channel.exchange_declare(exchange),
                frame.Method[spec.Exchange.DeclareOk])
    assert_type(channel.exchange_delete(exchange),
                frame.Method[spec.Exchange.DeleteOk])
    assert_type(channel.exchange_bind(exchange, exchange),
                frame.Method[spec.Exchange.BindOk])
    assert_type(channel.exchange_unbind(exchange, exchange),
                frame.Method[spec.Exchange.UnbindOk])
    assert_type(channel.basic_cancel('ctag'), frame.Method[spec.Basic.CancelOk])
    assert_type(channel.basic_qos(prefetch_count=1),
                frame.Method[spec.Basic.QosOk])
    assert_type(channel.confirm_delivery(lambda _frame: None),
                frame.Method[spec.Confirm.SelectOk])


def read_consumer_tag(channel: Channel, queue: str) -> str:
    """
    `basic_consume` returns the broker's consumer tag rather than a frame.

    :param channel: An open channel
    :param queue: A queue name
    """
    return channel.basic_consume(queue, lambda _ch, _m, _p, _b: None)


def read_channel_number(channel: Channel) -> int:
    """
    Returned directly, so `warn_return_any` reports the property losing its annotation.

    :param channel: An open channel
    """
    return channel.channel_number


def read_is_open(channel: Channel) -> bool:
    """
    As `read_channel_number`.

    :param channel: An open channel
    """
    return channel.is_open


def read_is_closed(channel: Channel) -> bool:
    """
    As `read_channel_number`.

    :param channel: An open channel
    """
    return channel.is_closed
