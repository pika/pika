"""
A downstream custom I/O loop.

Pins the annotation that pika's own `mypy` run cannot cover: `SelectorIOServicesAdapter` accepts an
implementation of `AbstractSelectorIOLoop`, and the point of the conversion to `typing.Protocol` is
that a loop implementing the interface without deriving from it, as `tornado.ioloop.IOLoop` does,
satisfies it. That guarantee lives at the boundary, so nothing inside `pika` observes it. See
`tests/typing/README.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from pika.adapters.utils.selector_ioloop_adapter import SelectorIOServicesAdapter


class _StructuralLoop:
    """
    Implements the whole interface without deriving from it.

    Mirrors the class of the same name in
    `tests/unit/selector_ioloop_adapter_tests.py`, which exercises the runtime
    behavior. This one exists to be type-checked, not run.
    """

    READ = 1
    WRITE = 2
    ERROR = 4

    def close(self) -> None:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def call_later(self, delay: float, callback: Callable[..., Any]) -> object:
        ...

    def remove_timeout(self, timeout_handle: Any) -> None:
        ...

    def add_callback(self, callback: Callable[..., Any]) -> None:
        ...

    def add_handler(self, fd: int, handler: Callable[[int, int], None],
                    events: int) -> None:
        ...

    def update_handler(self, fd: int, events: int) -> None:
        ...

    def remove_handler(self, fd: int) -> None:
        ...


def accepts_a_structurally_conforming_loop() -> SelectorIOServicesAdapter:
    """
    The reason for the `Protocol` conversion.

    A loop implementing the interface without deriving from it satisfies the argument type. On
    `main` before the conversion this fails with `[arg-type]`, since the class was a nominal base.
    """
    from pika.adapters.utils.selector_ioloop_adapter import SelectorIOServicesAdapter
    return SelectorIOServicesAdapter(_StructuralLoop())
