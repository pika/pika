"""
A downstream caller using what the blocking RPC wrappers return.

Pins annotations that pika's own `mypy` run cannot catch when they are wrong. Each of these methods
returns `self._blocking_rpc(...)`, which is typed `Any`, and mypy accepts returning `Any` from a
function declared `-> None` without a word. So eleven of them claimed `-> None` while returning a
reply frame, and the gate stayed green for as long as nobody outside pika used the value. Here the
value is used, so a return to `None` reads as "None has no attribute" and fails. See
`tests/typing/README.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pika.adapters.thread_safe_connection import Channel


def read_every_rpc_reply(channel: Channel, queue: str, exchange: str) -> None:
    """
    Reach into each reply frame for a field only that method's reply carries.

    `.method` on each catches a return annotated `None`, and the field catches one annotated as the
    wrong reply.

    :param channel: An open channel
    :param queue: A queue name
    :param exchange: An exchange name
    """
    # Optional because `pika.spec` is annotated as of 1.5.0 and a decoded field
    # may be absent; that is the field's real type, not a gap in the wrapper.
    depth: int | None = channel.queue_declare(queue,
                                              durable=True).method.message_count
    purged: int | None = channel.queue_purge(queue).method.message_count
    deleted: int | None = channel.queue_delete(queue).method.message_count
    cancelled: str | bytes | None = channel.basic_cancel(
        'ctag').method.consumer_tag
    assert depth is not None and purged is not None
    assert deleted is not None and cancelled is not None

    # Replies carrying no fields of their own, so `.method` is the whole test.
    assert channel.queue_bind(queue, exchange).method is not None
    assert channel.queue_unbind(queue, exchange).method is not None
    assert channel.exchange_declare(exchange).method is not None
    assert channel.exchange_delete(exchange).method is not None
    assert channel.exchange_bind(exchange, exchange).method is not None
    assert channel.exchange_unbind(exchange, exchange).method is not None
    assert channel.basic_qos(prefetch_count=1).method is not None
    assert channel.confirm_delivery(lambda _frame: None).method is not None


def read_consumer_tag(channel: Channel, queue: str) -> str:
    """
    `basic_consume` returns the broker's consumer tag rather than a frame.

    :param channel: An open channel
    :param queue: A queue name
    """
    return channel.basic_consume(queue, lambda _ch, _m, _p, _b: None)


def read_channel_state(channel: Channel) -> tuple[int, bool, bool]:
    """
    The three properties, which were unannotated and so read as `Any`.

    This catches an annotation changed to the wrong type, and cannot catch one removed again: an
    unannotated function returns `Any`, which satisfies the tuple above. Catching that needs
    `warn_return_any`, which the codebase does not yet satisfy.

    :param channel: An open channel
    """
    return channel.channel_number, channel.is_open, channel.is_closed
