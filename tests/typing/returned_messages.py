"""
Downstream consumers of `UnroutableError.messages` and `NackError.messages`.

Pins the annotation that pika's own `mypy` run cannot cover: neither exception class is consumed
from outside `pika`, so a regression in `.messages` type only surfaces in code like this. See
`tests/typing/README.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Sequence

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import ReturnedMessage
    from pika.exceptions import NackError, ReturnedMessageLike, UnroutableError


def against_concrete(err: UnroutableError) -> None:
    """
    A caller annotating the messages against the concrete blocking type.

    This is what regressed when `.messages` was annotated with the protocol: a protocol is not
    assignable to the nominal class it describes.
    """
    messages: Sequence[ReturnedMessage] = err.messages
    for message in messages:
        _ = message.method.exchange
        _ = message.body


def stored_as_concrete_list(err: NackError) -> list[ReturnedMessage]:
    """A caller collecting the messages into a concrete-typed list."""
    return list(err.messages)


def passed_to_concrete_callback(
        err: UnroutableError, callback: Callable[[Sequence[ReturnedMessage]],
                                                 None]) -> None:
    """A caller forwarding the messages to a concrete-typed callback."""
    callback(err.messages)


def against_the_protocol(err: NackError) -> None:
    """A caller annotating against the exported protocol reads the members it promises."""
    messages: Sequence[ReturnedMessageLike] = err.messages
    for message in messages:
        _ = message.method
        _ = message.properties
        _ = message.body
