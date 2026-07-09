"""Shared helper for creating asyncio event loops in tests."""

from __future__ import annotations

import asyncio
import sys


def new_pika_asyncio_loop() -> asyncio.AbstractEventLoop:
    """
    Create a new asyncio event loop suitable for Pika's AsyncioConnection.

    On Windows, Pika requires a `SelectorEventLoop` because it uses `add_reader`/`add_writer`, which
    the default `ProactorEventLoop` does not support. Elsewhere, the default
    `asyncio.new_event_loop()` is selector-based.
    """
    if sys.platform == 'win32':
        return asyncio.SelectorEventLoop()
    return asyncio.new_event_loop()
