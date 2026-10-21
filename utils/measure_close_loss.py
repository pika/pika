"""
How many messages a close loses, measured against a live broker.

Reproduces the figures quoted in `docs/internal/thread-safe-connection.md` and in the 1.5.0 upgrade
notes. Those figures move with every one of the message count, body size, work-queue size, callback
cost and timeout, so each is published with its configuration and this script takes all five as
arguments rather than defaulting to the one that was measured.

Only an `auto_ack` consumer can lose a message: the broker drops its copy when it dispatches, so a
delivery that reaches no callback is gone. A manual-ack consumer measures something else, which the
``--ack manual`` mode reports instead: how many acknowledgements the close let land, the rest being
redelivered rather than lost.

The loss is read at the moment `close` returns, not after the process settles. The pool worker is a
daemon thread that keeps draining, so a late reading credits a bounded close with callbacks it never
waited for; both numbers are reported, because the gap between them is the backlog the close
abandoned but the worker happened to finish.

Needs a broker. `hatch run rabbitmq` starts one. Not wired into CI: a run takes as long as the
callbacks it is timing, and the counts are a measurement rather than a verdict.
"""

from __future__ import annotations

import argparse
import statistics
import threading
import time
from typing import TYPE_CHECKING

import pika
from pika.adapters.thread_safe_connection import Connection

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

#: What `--help` shows, rather than a slice of the module docstring.
DESCRIPTION = 'Measure how many messages a close loses, against a live broker.'

#: Queue used for every run, deleted and redeclared each time.
QUEUE = 'pika-close-loss-measurement'


def _publish(params: pika.ConnectionParameters, count: int,
             body_size: int) -> None:
    """
    Fill the queue, and confirm the broker has all of it before returning.

    Publisher confirms are what make the count below trustworthy: without them `queue_declare`
    can report a depth that is still short, which reads as a message lost before the test began.

    :param params: Where to connect
    :param count: How many messages to publish
    :param body_size: Bytes per message body
    """
    conn = pika.BlockingConnection(params)
    try:
        channel = conn.channel()
        channel.queue_delete(QUEUE)
        channel.queue_declare(QUEUE, durable=True)
        channel.confirm_delivery()
        body = b'x' * body_size
        for _ in range(count):
            channel.basic_publish('', QUEUE, body)
        depth = _depth_on(channel)
        if depth != count:
            raise RuntimeError(f'published {count} but the queue holds {depth}')
    finally:
        conn.close()


def _depth_on(channel: BlockingChannel) -> int:
    """
    Report the queue's depth over an existing channel.

    :param channel: An open channel
    """
    return channel.queue_declare(QUEUE, durable=True,
                                 passive=True).method.message_count


def _depth(params: pika.ConnectionParameters) -> int:
    """
    Report the queue's depth over a connection of its own.

    :param params: Where to connect
    """
    conn = pika.BlockingConnection(params)
    try:
        return _depth_on(conn.channel())
    finally:
        conn.close()


def _run_once(args: argparse.Namespace) -> dict[str, int]:
    """
    Consume until the close, then report what reached a callback and what the broker still holds.

    :param args: The parsed command line
    """
    params = pika.ConnectionParameters(args.host)
    _publish(params, args.messages, args.body_size)

    seen: list[int] = []
    first = threading.Event()
    auto_ack = args.ack == 'auto'

    conn = Connection(params, work_queue_maxsize=args.queue_maxsize)
    channel = conn.channel()

    def on_message(ch, method, _properties, _body) -> None:
        seen.append(method.delivery_tag)
        first.set()
        time.sleep(args.callback_ms / 1000.0)
        if not auto_ack:
            ch.basic_ack(method.delivery_tag)

    channel.basic_consume(QUEUE, on_message, auto_ack=auto_ack)
    if not first.wait(args.settle + 30):
        raise RuntimeError('no delivery arrived; is the queue being consumed '
                           'by something else?')
    time.sleep(args.settle)

    if args.close == 'connection':
        conn.close(timeout=args.timeout)
        at_close, remaining = len(seen), _depth(params)
    else:
        channel.close(timeout=args.timeout)
        # Both readings come before the connection close, which is unbounded:
        # it would otherwise let the daemon worker finish what this close
        # abandoned, crediting the bounded close with those callbacks and with
        # the acknowledgements they issue.
        at_close, remaining = len(seen), _depth(params)
        conn.close(timeout=None)

    settled = len(seen)
    result = {
        'published': args.messages,
        'to_callback_at_close': at_close,
        'to_callback_settled': settled,
        'left_in_queue': remaining,
    }
    if auto_ack:
        result['lost'] = args.messages - at_close - remaining
        result['lost_after_settle'] = args.messages - settled - remaining
    else:
        result['acks_landed'] = args.messages - remaining
    return result


def _parse_timeout(value: str) -> float | None:
    """
    Read a close timeout, where ``none`` means wait indefinitely.

    :param value: The argument as given
    """
    if value.lower() == 'none':
        return None
    return float(value)


def main(argv: list[str] | None = None) -> int:
    """
    Run the measurement and print one line per run plus a summary.

    :param argv: Command line, or None to read `sys.argv`
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--messages', type=int, default=400)
    parser.add_argument('--body-size', type=int, default=8192)
    parser.add_argument('--queue-maxsize', type=int, default=50)
    parser.add_argument('--callback-ms', type=float, default=50)
    parser.add_argument('--timeout',
                        type=_parse_timeout,
                        default=10,
                        help="seconds, or 'none' to wait indefinitely")
    parser.add_argument('--close',
                        choices=('channel', 'connection'),
                        default='channel')
    parser.add_argument('--ack', choices=('auto', 'manual'), default='auto')
    parser.add_argument('--settle',
                        type=float,
                        default=0.5,
                        help='seconds between the first delivery and the close')
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--host', default='localhost')
    args = parser.parse_args(argv)

    print(f'pika {pika.__version__} from {pika.__file__}')
    print(f'{args.messages} messages of {args.body_size} B, '
          f'work_queue_maxsize={args.queue_maxsize}, '
          f'{args.callback_ms:g} ms callback, {args.ack} ack, '
          f'{args.close}.close(timeout={args.timeout})')
    key = 'lost' if args.ack == 'auto' else 'acks_landed'
    totals = []
    for run in range(1, args.runs + 1):
        result = _run_once(args)
        totals.append(result[key])
        print(f'  run {run}: {result}')
    if args.runs > 1:
        print(f'{key}: range {min(totals)}-{max(totals)}, '
              f'median {int(statistics.median(totals))}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
