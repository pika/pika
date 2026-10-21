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

import pika
from pika.adapters.thread_safe_connection import Channel, Connection

#: What `--help` shows, rather than a slice of the module docstring.
DESCRIPTION = 'Measure how many messages a close loses, against a live broker.'

#: Default queue. Overridable, because two runs sharing one would each delete
#: the other's messages.
QUEUE = 'pika-close-loss-measurement'

#: How long to wait for the first delivery before giving up on the run.
FIRST_DELIVERY_TIMEOUT = 30.0

#: How long to wait for the broker to report every publish.
PUBLISH_SETTLE_TIMEOUT = 60.0


def _publish(params: pika.ConnectionParameters, queue: str, count: int,
             body_size: int) -> None:
    """
    Fill the queue, and wait for the broker to report all of it before returning.

    `basic_publish` here is fire-and-forget, so the depth is polled rather than read once: a single
    read returns a depth that is still short, which then reads as a message lost before the run
    began.

    :param params: Where to connect
    :param queue: Queue to publish to, deleted first
    :param count: How many messages to publish
    :param body_size: Bytes per message body
    """
    conn = Connection(params)
    try:
        channel = conn.channel()
        channel.queue_delete(queue)
        channel.queue_declare(queue, durable=True)
        body = b'x' * body_size
        for _ in range(count):
            channel.basic_publish('', queue, body)
        deadline = time.monotonic() + PUBLISH_SETTLE_TIMEOUT
        depth = _depth_on(channel, queue)
        while depth != count and time.monotonic() < deadline:
            time.sleep(0.1)
            depth = _depth_on(channel, queue)
        if depth != count:
            raise RuntimeError(f'published {count} but the queue holds {depth}')
    finally:
        conn.close(timeout=None)


def _depth_on(channel: Channel, queue: str) -> int:
    """
    Report the queue's depth over an existing channel.

    :param channel: An open channel
    :param queue: Queue to measure
    """
    return channel.queue_declare(queue, durable=True,
                                 passive=True).method.message_count


def _depth(params: pika.ConnectionParameters, queue: str) -> int:
    """
    Report the queue's depth over a connection of its own.

    :param params: Where to connect
    :param queue: Queue to measure
    """
    conn = Connection(params)
    try:
        return _depth_on(conn.channel(), queue)
    finally:
        conn.close(timeout=None)


def _run_once(args: argparse.Namespace) -> dict[str, int]:
    """
    Consume until the close, then report what reached a callback and what the broker still holds.

    :param args: The parsed command line
    """
    params = pika.ConnectionParameters(args.host)
    _publish(params, args.queue, args.messages, args.body_size)

    seen: list[int] = []
    first = threading.Event()
    auto_ack = args.ack == 'auto'

    conn = Connection(params, work_queue_maxsize=args.queue_maxsize)
    try:
        channel = conn.channel()

        def on_message(ch, method, _properties, _body) -> None:
            seen.append(method.delivery_tag)
            first.set()
            time.sleep(args.callback_ms / 1000.0)
            if not auto_ack:
                ch.basic_ack(method.delivery_tag)

        channel.basic_consume(args.queue, on_message, auto_ack=auto_ack)
        if not first.wait(FIRST_DELIVERY_TIMEOUT):
            raise RuntimeError('no delivery arrived; is the queue being '
                               'consumed by something else?')
        time.sleep(args.settle)

        if args.close == 'connection':
            conn.close(timeout=args.timeout)
            at_close, remaining = len(seen), _depth(params, args.queue)
        else:
            channel.close(timeout=args.timeout)
            # Both readings come before the connection close, which is
            # unbounded: it would otherwise let the daemon worker finish what
            # this close abandoned, crediting the bounded close with those
            # callbacks and with the acknowledgements they issue.
            at_close, remaining = len(seen), _depth(params, args.queue)
    finally:
        # The IOLoop thread is a daemon, so an abandoned connection costs a
        # socket until the process exits rather than hanging it; closing here
        # keeps a failed run from leaving one behind for every --runs.
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
                        default=_parse_timeout('10'),
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
    parser.add_argument('--queue',
                        default=QUEUE,
                        help='two concurrent runs must not share one')
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
              f'median {statistics.median(totals):g}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
