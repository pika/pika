#!/usr/bin/env python
"""Post-release smoke test for the published pika wheel.

Run against a live broker after installing pika from PyPI to prove the
distributed package can open a connection, declare a queue, publish a
message, and read it back. Exits 0 on success, non-zero on any failure.

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

import pika

DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2F'
QUEUE = 'pika-smoke-test'
BODY = b'pika smoke test'


def main() -> int:
    url = (sys.argv[1] if len(sys.argv) > 1 else
           os.environ.get('PIKA_SMOKE_AMQP_URL', DEFAULT_URL))
    print(f'pika {pika.__version__}: connecting to {url}')

    connection = pika.BlockingConnection(pika.URLParameters(url))
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, auto_delete=True)
        channel.basic_publish(exchange='', routing_key=QUEUE, body=BODY)

        method, _properties, body = channel.basic_get(queue=QUEUE,
                                                       auto_ack=True)
        if method is None:
            print('smoke test FAILED: no message returned by basic_get')
            return 1
        if body != BODY:
            print(f'smoke test FAILED: expected {BODY!r}, got {body!r}')
            return 1
    finally:
        connection.close()

    print('smoke test PASSED')
    return 0


if __name__ == '__main__':
    sys.exit(main())
