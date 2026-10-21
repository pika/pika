"""
Post-release smoke test for the published pika wheel.

Run against a live broker after installing pika from PyPI to prove the
distributed package can open a connection, declare a queue, publish a
message, and read it back.  Exits 0 on success, non-zero on any failure.

Both shipped connections are exercised.  `pika.Connection` is the one
1.5.0 documents and the one a new application uses, so a release that
could not open one is broken whatever `BlockingConnection` does; and
`BlockingConnection` is deprecated but still shipped, so it has to be
provably installable too.  Its `DeprecationWarning` is filtered here
rather than left to print, so the output says what failed instead of
burying it.

This lives under .ci/ rather than examples/ or tests/ so it is not picked
up by pytest, yapf, or ruff globs, and so it exercises the *installed*
package rather than the source tree.

Usage:
    python .ci/smoke_test.py [amqp_url]

The broker URL is taken from the first argument, then the
PIKA_SMOKE_AMQP_URL environment variable, then a localhost default.
"""

import os
import sys
import time
import warnings

import pika

DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2F'
#: One queue per check: each is exclusive, so the broker holds it until that
#: connection is gone, and sharing a name makes the second check race the first.
CONNECTION_QUEUE = 'pika-smoke-test-connection'
BLOCKING_QUEUE = 'pika-smoke-test-blocking'
BODY = b'pika smoke test'

#: Seconds to wait for the consumer callback in the `pika.Connection` check.
DELIVERY_TIMEOUT = 15.0


def _check_blocking_connection(url: str) -> int:
    """
    Publish and read back over the deprecated `BlockingConnection`.

    :param url: Broker URL.
    :returns: 0 on success.
    """
    with warnings.catch_warnings():
        # Deprecated since 1.5.0 and still shipped, so still worth proving
        # installable; the notice is not news here.
        warnings.filterwarnings('ignore',
                                message='.*will be removed in Pika 2\\.0',
                                category=DeprecationWarning)
        connection = pika.BlockingConnection(pika.URLParameters(url))
    try:
        channel = connection.channel()
        # Exclusive so the queue is auto-deleted when this connection closes
        # (no cleanup, no leftover state between repeated test runs) and to
        # avoid the transient non-exclusive queue combination, which modern
        # RabbitMQ rejects by default.
        channel.queue_declare(queue=BLOCKING_QUEUE, exclusive=True)
        channel.basic_publish(exchange='',
                              routing_key=BLOCKING_QUEUE,
                              body=BODY)

        method, _properties, body = channel.basic_get(queue=BLOCKING_QUEUE,
                                                      auto_ack=True)
        if method is None:
            print('smoke test FAILED: BlockingConnection basic_get returned '
                  'no message')
            return 1
        if body != BODY:
            print(f'smoke test FAILED: BlockingConnection expected {BODY!r}, '
                  f'got {body!r}')
            return 1
    finally:
        # The broker may have already closed the connection (e.g. on a
        # rejected declare); closing an already-closed connection raises and
        # would mask the original error.
        if connection.is_open:
            connection.close()
    print('  BlockingConnection: ok')
    return 0


def _check_connection(url: str) -> int:
    """
    Publish and consume over `pika.Connection`, including the acknowledgement.

    Consuming rather than `basic_get`, because the worker thread that dispatches a delivery is what
    distinguishes this connection from the other one, and a release where that thread never starts
    would pass a `basic_get`.

    :param url: Broker URL.
    :returns: 0 on success.
    """
    connection = pika.Connection(pika.URLParameters(url))
    delivered = []
    try:
        channel = connection.channel()
        channel.queue_declare(queue=CONNECTION_QUEUE, exclusive=True)
        channel.basic_publish(exchange='',
                              routing_key=CONNECTION_QUEUE,
                              body=BODY)

        def on_message(ch, method, _properties, body) -> None:
            """Record the delivery and acknowledge it."""
            delivered.append(body)
            ch.basic_ack(method.delivery_tag)

        channel.basic_consume(queue=CONNECTION_QUEUE,
                              on_message_callback=on_message)
        deadline = time.monotonic() + DELIVERY_TIMEOUT
        while not delivered and time.monotonic() < deadline:
            time.sleep(0.05)
        if not delivered:
            print(f'smoke test FAILED: Connection delivered no message within '
                  f'{DELIVERY_TIMEOUT:g}s')
            return 1
        if delivered[0] != BODY:
            print(f'smoke test FAILED: Connection expected {BODY!r}, got '
                  f'{delivered[0]!r}')
            return 1
    finally:
        if connection.is_open:
            connection.close(timeout=DELIVERY_TIMEOUT)
    print('  Connection: ok')
    return 0


def main() -> int:
    """
    Check both shipped connections against a live broker.

    :returns: Process exit status.
    """
    url = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
        'PIKA_SMOKE_AMQP_URL', DEFAULT_URL))
    print(f'pika {pika.__version__}: connecting to {url}')

    for check in (_check_connection, _check_blocking_connection):
        status = check(url)
        if status:
            return status

    print('smoke test PASSED')
    return 0


if __name__ == '__main__':
    sys.exit(main())
