"""Negative fixtures: shapes the exception constructors must reject.

Each offending line carries a `# type: ignore[<code>]`. `mypy` runs here with
`warn_unused_ignores = True`, so if the constructor ever stops constraining its
argument the ignore becomes unused and the run fails. See
`tests/typing/README.md`.
"""

from __future__ import annotations

from pika.exceptions import NackError, UnroutableError


class NotAMessage:
    """Carries none of the members the message protocol requires."""


def rejects_non_message_sequence() -> None:
    UnroutableError([NotAMessage()])  # type: ignore[list-item]
    NackError([NotAMessage()])  # type: ignore[list-item]
