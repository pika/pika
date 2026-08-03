# Typing tests

Type-checker fixtures that pin annotations pika's own `mypy` run cannot see.

`hatch run typecheck` checks `packages = pika`, so it never covers code that
consumes pika from the outside. Some guarantees only fail there, and two
already have. A downstream caller annotating `UnroutableError.messages` against
the concrete `ReturnedMessage` broke when the attribute was annotated with the
message protocol. A caller wiring a custom I/O loop into
`SelectorIOServicesAdapter` needs the loop interface to be structural rather
than nominal. Neither pika's `mypy` run nor the runtime unit tests observed
either one.

Each file here is a small downstream consumer that must type-check clean.
`hatch run typecheck` covers this directory alongside `pika/`, so an annotation
regression fails with a non-zero exit. A fixture that is meant to fail instead
uses an inline `# type: ignore[<code>]` on the offending line; `mypy` reports an
unused ignore, again non-zero, if the error stops occurring.

These are not collected by `pytest`; they carry no `test_` functions and assert
nothing at runtime.
