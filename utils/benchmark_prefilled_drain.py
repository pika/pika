"""
Compare how two pika sources drain a prefilled queue, against a live broker.

Reproduces the "prefilled queue drain and consumer CPU" figures of #1853. Each run publishes with
confirms into a fresh durable queue, checks the queue depth, then consumes it with an
`AsyncioConnection` and `auto_ack`. Wall time and consumer CPU (`time.process_time`) span the
`basic_consume` request to the last delivery callback. A 1 ms asyncio timer runs throughout, and the
lateness of each firing is the loop lag, whose p99 is reported per run. Every run checks the
delivered count, the order and each body size, and fails if one is wrong.

A source is a directory holding a `pika/` package, or a git ref of this repository, which is
extracted to a temporary directory and removed afterwards. Each run is a subprocess that imports
`pika` from its own source. Baseline and candidate alternate, taking turns to go first, so a drift
in the broker or the machine does not favour one. Without `--baseline` only the candidate is
measured, and it defaults to the checkout this script is in. Needs a broker (`hatch run rabbitmq`),
and is not part of CI because the figures are a measurement rather than a verdict.

Plaintext, `main` against the working tree::

    python -m utils.benchmark_prefilled_drain --baseline main

TLS, where `tests/certs` holds the certificates for the broker of `hatch run rabbitmq-tls`, which
requires a client certificate::

    python -m utils.benchmark_prefilled_drain --tls --ca-certs ca.pem --client-cert c.pem --client-key k.pem

The default workloads are 20000 messages of 1 KiB and 64 of 1 MiB; `--workload COUNTxSIZE`, which can
be repeated, replaces them. `--json PATH` keeps the raw runs and lag samples.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import math
import os
import ssl
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pika
from pika.adapters.asyncio_connection import AsyncioConnection

#: What `--help` shows, rather than the module docstring.
DESCRIPTION = 'Compare how two pika sources drain a prefilled queue, against a live broker.'

REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_WORKLOADS = ((20000, 1024), (64, 1024 * 1024))

#: The sequence number is the first 8 bytes of every body.
SEQUENCE_BYTES = 8

TIMER_INTERVAL = 0.001

#: Seconds for one drain, or for the connection to close afterwards.
DRAIN_TIMEOUT = 300.0
CLOSE_TIMEOUT = 10.0

SIDES = ('baseline', 'candidate')


def make_body(sequence: int, size: int) -> bytes:
    """
    Build a body of `size` bytes that starts with its sequence number.

    :param sequence: Position of the message in the publish order
    :param size: Bytes in the body, at least `SEQUENCE_BYTES`
    """
    return sequence.to_bytes(SEQUENCE_BYTES,
                             'big') + b'x' * (size - SEQUENCE_BYTES)


def check_delivery(body: bytes, expected_sequence: int,
                   size: int) -> str | None:
    """
    Describe what is wrong with a delivery, or return None when it is the one expected.

    :param body: The delivered body
    :param expected_sequence: Sequence number the next delivery must carry
    :param size: Bytes every body must have
    """
    if len(body) != size:
        return f'delivery {expected_sequence} has {len(body)} bytes, expected {size}'
    sequence = int.from_bytes(body[:SEQUENCE_BYTES], 'big')
    if sequence != expected_sequence:
        return f'delivery out of order: got {sequence}, expected {expected_sequence}'
    return None


def percentile(samples: list[float], fraction: float) -> float:
    """
    Interpolate linearly between the closest ranks, as `numpy.percentile` does.

    :param samples: Values to rank, not empty
    :param fraction: Position between 0 and 1
    """
    ordered = sorted(samples)
    position = fraction * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (position - lower) * (ordered[upper] -
                                                  ordered[lower])


def percent_change(before: float, after: float) -> float:
    """
    Report how much `after` differs from `before`, in percent of `before`.

    :param before: The reference value
    :param after: The value compared with it
    """
    return (after / before - 1) * 100


def pair_order(pair: int) -> tuple[str, str]:
    """
    Pick which side runs first in a pair, alternating so neither always goes first.

    :param pair: Zero-based pair number
    """
    return SIDES if pair % 2 == 0 else SIDES[::-1]


def parse_workload(text: str) -> tuple[int, int]:
    """
    Read `COUNTxSIZE` into a message count and a body size in bytes.

    :param text: The argument as given
    """
    try:
        count, size = (int(part) for part in text.lower().split('x'))
    except ValueError:
        raise argparse.ArgumentTypeError(
            f'{text!r} is not COUNTxSIZE, such as 20000x1024') from None
    if count < 1 or size < SEQUENCE_BYTES:
        raise argparse.ArgumentTypeError(
            f'{text!r} needs a count of at least 1 and a size of at least {SEQUENCE_BYTES}'
        )
    return count, size


def summarize(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Reduce the paired runs of one workload to medians and median paired changes.

    Each pair maps a side to its run. Changes need both sides, so they are None for one side alone.
    A positive throughput change means the candidate drained faster; the message count is the same
    on both sides, so it is the ratio of the drain times. A run that ended before any timer fired
    has no lag samples, so the p99 lag is the median over the runs that have some, or None when none
    do.

    :param pairs: One dict per pair, mapping a side name to its run
    """
    summary: dict[str, Any] = {}
    for side in SIDES:
        runs = [pair[side] for pair in pairs if side in pair]
        if runs:
            lagged = [run['lag_s'] for run in runs if run['lag_s']]
            summary[side] = {
                'drain_ms':
                    statistics.median(run['drain_s'] for run in runs) * 1000,
                'cpu_ms':
                    statistics.median(run['cpu_s'] for run in runs) * 1000,
                'p99_lag_ms':
                    statistics.median(percentile(lag, 0.99) for lag in lagged) *
                    1000 if lagged else None,
            }
    both = [pair for pair in pairs if all(side in pair for side in SIDES)]
    if both:
        summary['throughput_change_pct'] = statistics.median(
            percent_change(pair['candidate']['drain_s'], pair['baseline']
                           ['drain_s']) for pair in both)
        summary['cpu_change_pct'] = statistics.median(
            percent_change(pair['baseline']['cpu_s'], pair['candidate']
                           ['cpu_s']) for pair in both)
    return summary


def _cell(value: float | None) -> str:
    return f'{"n/a" if value is None else format(value, ".2f"):>10}'


def format_summary(count: int, size: int, pairs: list[dict[str, Any]]) -> str:
    """
    Render the medians table of one workload.

    :param count: Messages per run
    :param size: Bytes per body
    :param pairs: One dict per pair, mapping a side name to its run
    """
    summary = summarize(pairs)
    lines = [f'{count} x {size} B, {len(pairs)} runs per side']
    rows = (('drain ms', 'drain_ms'), ('consumer CPU ms', 'cpu_ms'),
            ('p99 lag ms', 'p99_lag_ms'))
    if 'baseline' in summary and 'candidate' in summary:
        lines.append(f'{"median":<17}{"baseline":>10}    {"candidate":>10}')
        for label, key in rows:
            lines.append(f'{label:<17}{_cell(summary["baseline"][key])} -> '
                         f'{_cell(summary["candidate"][key])}')
        lines.append(
            f'median paired throughput change: {summary["throughput_change_pct"]:+.1f}%'
        )
        lines.append(
            f'median paired consumer CPU change: {summary["cpu_change_pct"]:+.1f}%'
        )
    else:
        side = summary['candidate']
        lines.append(f'{"median":<17}{"candidate":>10}')
        lines.extend(f'{label:<17}{_cell(side[key])}' for label, key in rows)
    return '\n'.join(lines)


def materialize(spec: str, stack: contextlib.ExitStack) -> Path:
    """
    Resolve a source to a directory that holds a `pika` package.

    A directory is used as given. Anything else is a git ref of this repository, whose `pika/` is
    extracted to a temporary directory that `stack` removes.

    :param spec: A directory or a git ref
    :param stack: Cleans up what a ref extracts
    """
    path = Path(spec)
    if path.is_dir():
        if not (path / 'pika' / '__init__.py').is_file():
            raise ValueError(f'{spec} is a directory without a pika package')
        return path.resolve()
    archive = subprocess.run(
        ['git', '-C',
         str(REPO_ROOT), 'archive', '--format=zip', spec, 'pika'],
        capture_output=True,
        check=False)
    if archive.returncode:
        raise ValueError(f'{spec} is neither a directory nor a git ref: '
                         f'{archive.stderr.decode(errors="replace").strip()}')
    directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
    with zipfile.ZipFile(io.BytesIO(archive.stdout)) as zipped:
        zipped.extractall(directory)
    return directory


def connection_parameters(config: dict[str, Any]) -> Any:
    """
    Build the `pika.ConnectionParameters` for a run.

    :param config: The `connection` entry of a worker config
    """
    options = None
    if config['tls']:
        context = ssl.create_default_context(cafile=config['ca_certs'])
        if config['client_cert']:
            context.load_cert_chain(config['client_cert'], config['client_key'])
        options = pika.SSLOptions(context,
                                  server_hostname=config['server_hostname'] or
                                  config['host'])
    return pika.ConnectionParameters(host=config['host'],
                                     port=config['port'],
                                     virtual_host=config['vhost'],
                                     credentials=pika.PlainCredentials(
                                         config['username'],
                                         config['password']),
                                     ssl_options=options)


class LagSampler:
    """Record how late each 1 ms timer fires, re-arming it after every firing."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._deadline = 0.0
        self._handle: asyncio.Handle | None = None
        self.lateness: list[float] = []

    def start(self) -> None:
        self._arm()

    def stop(self) -> None:
        if self._handle is not None:
            self._handle.cancel()

    def _arm(self) -> None:
        self._deadline = self._loop.time() + TIMER_INTERVAL
        self._handle = self._loop.call_at(self._deadline, self._fire)

    def _fire(self) -> None:
        self.lateness.append(max(0.0, self._loop.time() - self._deadline))
        self._arm()


def publish(params: Any, queue: str, count: int, size: int) -> None:
    """
    Fill a new queue with confirmed publishes and check that it holds exactly `count` messages.

    :param params: Where to connect
    :param queue: Queue to declare, durable
    :param count: Messages to publish
    :param size: Bytes per body
    """
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        channel.queue_declare(queue, durable=True)
        channel.confirm_delivery()
        for sequence in range(count):
            channel.basic_publish('', queue, make_body(sequence, size))
        depth = channel.queue_declare(queue, durable=True,
                                      passive=True).method.message_count
        if depth != count:
            raise RuntimeError(f'published {count} but the queue holds {depth}')
    finally:
        connection.close()


def delete_queue(params: Any, queue: str) -> None:
    """
    Delete the queue, whether or not it exists.

    :param params: Where to connect
    :param queue: Queue to delete
    """
    connection = pika.BlockingConnection(params)
    try:
        connection.channel().queue_delete(queue)
    finally:
        connection.close()


class Drain:
    """Consume `count` messages from `queue` with `auto_ack`, timing the span and checking each."""

    def __init__(self, loop: asyncio.AbstractEventLoop, params: Any, queue: str,
                 count: int, size: int) -> None:
        self.loop = loop
        self.params = params
        self.queue = queue
        self.count = count
        self.size = size
        self.done = loop.create_future()
        self.closed = loop.create_future()
        self.received = 0
        self.connection: Any = None
        self.sampler = LagSampler(loop)
        self.started = (0.0, 0.0)
        self.result: dict[str, Any] = {}

    def run(self) -> dict[str, Any]:
        """Drain the queue and return the run's wall time, CPU time and lag samples."""
        self.connection = AsyncioConnection(
            self.params,
            on_open_callback=self._on_open,
            on_open_error_callback=self._on_open_error,
            on_close_callback=self._on_close,
            custom_ioloop=self.loop)
        try:
            self.loop.run_until_complete(
                asyncio.wait_for(self.done, DRAIN_TIMEOUT))
        finally:
            self.sampler.stop()
            self._close()
        return self.result

    def _close(self) -> None:
        if self.connection.is_closed:
            return
        if self.connection.is_open:
            self.connection.close()
        self.loop.run_until_complete(
            asyncio.wait_for(self.closed, CLOSE_TIMEOUT))

    def _fail(self, error: BaseException) -> None:
        if not self.done.done():
            self.done.set_exception(error)

    def _on_open(self, connection: Any) -> None:
        connection.channel(on_open_callback=self._on_channel)

    def _on_open_error(self, _connection: Any, error: BaseException) -> None:
        self._fail(RuntimeError(f'could not open the connection: {error!r}'))

    def _on_close(self, _connection: Any, reason: BaseException) -> None:
        self._fail(RuntimeError(f'the connection closed: {reason!r}'))
        if not self.closed.done():
            self.closed.set_result(None)

    def _on_channel(self, channel: Any) -> None:
        channel.add_on_close_callback(self._on_channel_close)
        self.sampler.start()
        self.started = (time.perf_counter(), time.process_time())
        channel.basic_consume(self.queue, self._on_message, auto_ack=True)

    def _on_channel_close(self, _channel: Any, reason: BaseException) -> None:
        self._fail(RuntimeError(f'the channel closed: {reason!r}'))

    def _on_message(self, _channel: Any, _method: Any, _properties: Any,
                    body: bytes) -> None:
        problem = check_delivery(body, self.received, self.size)
        if problem:
            self._fail(RuntimeError(problem))
            return
        self.received += 1
        if self.received == self.count and not self.done.done():
            wall = time.perf_counter() - self.started[0]
            cpu = time.process_time() - self.started[1]
            self.result = {
                'drain_s': wall,
                'cpu_s': cpu,
                'lag_s': self.sampler.lateness
            }
            self.done.set_result(None)


def new_loop() -> asyncio.AbstractEventLoop:
    """Create a selector event loop, which is what `AsyncioConnection` needs on Windows."""
    if sys.platform == 'win32':
        return asyncio.SelectorEventLoop()
    return asyncio.new_event_loop()


def worker(config: dict[str, Any]) -> dict[str, Any]:
    """
    Run one publish and drain with the pika that `config['source']` provides.

    :param config: The worker config the parent sent
    """
    source = Path(config['source']).resolve()
    imported = Path(pika.__file__).resolve()
    if source not in imported.parents:
        raise RuntimeError(f'imported pika from {imported}, outside {source}')

    params = connection_parameters(config['connection'])
    queue = f'pika-prefilled-drain-{uuid.uuid4().hex}'
    loop = new_loop()
    try:
        publish(params, queue, config['count'], config['size'])
        result = Drain(loop, params, queue, config['count'],
                       config['size']).run()
    finally:
        loop.close()
        delete_queue(params, queue)
    result['pika_version'] = pika.__version__
    result['pika_file'] = str(imported)
    return result


def run_worker(config: dict[str, Any]) -> dict[str, Any]:
    """
    Run `worker` in a subprocess whose `sys.path` starts at the source under test.

    :param config: The worker config, which carries the credentials and so goes over stdin
    """
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(
        [config['source'], *filter(None, [env.get('PYTHONPATH')])])
    completed = subprocess.run(
        [sys.executable,
         str(Path(__file__).resolve()), '--worker'],
        input=json.dumps(config),
        capture_output=True,
        check=False,
        text=True,
        env=env)
    if completed.returncode:
        raise RuntimeError(
            f'the run against {config["source"]} failed:\n{completed.stderr}')
    return json.loads(completed.stdout)


def measure(args: argparse.Namespace,
            sources: dict[str, Path]) -> list[dict[str, Any]]:
    """
    Run every workload as paired runs and return the raw results.

    :param args: The parsed command line
    :param sources: Directory to import pika from, by side
    """
    connection = {
        key: getattr(args, key)
        for key in ('host', 'port', 'vhost', 'username', 'password', 'tls',
                    'ca_certs', 'client_cert', 'client_key', 'server_hostname')
    }
    results = []
    for count, size in args.workload:
        pairs: list[dict[str, Any]] = []
        for pair in range(args.pairs):
            runs: dict[str, Any] = {}
            for side in pair_order(pair):
                if side not in sources:
                    continue
                print(
                    f'{count} x {size} B, pair {pair + 1}/{args.pairs}: {side}',
                    file=sys.stderr)
                runs[side] = run_worker({
                    'source': str(sources[side]),
                    'connection': connection,
                    'count': count,
                    'size': size
                })
            pairs.append(runs)
        results.append({'count': count, 'size': size, 'pairs': pairs})
    return results


def build_parser() -> argparse.ArgumentParser:
    """Define the command line."""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '--baseline',
        help='directory or git ref to compare against; omit to measure '
        'the candidate alone')
    parser.add_argument(
        '--candidate',
        default=str(REPO_ROOT),
        help='directory or git ref to measure (default: this checkout)')
    parser.add_argument(
        '--pairs',
        type=int,
        default=6,
        help=
        'runs per side and workload; an even count balances which side goes first '
        '(default: %(default)s)')
    parser.add_argument(
        '--workload',
        type=parse_workload,
        action='append',
        metavar='COUNTxSIZE',
        help='messages and body bytes, repeatable (default: 20000x1024 and '
        '64x1048576)')
    parser.add_argument('--json',
                        metavar='PATH',
                        help='write the raw runs here')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port',
                        type=int,
                        help='default: 5672, or 5671 with --tls')
    parser.add_argument('--vhost', default='/')
    parser.add_argument('--username', default='guest')
    parser.add_argument('--password', default='guest')
    parser.add_argument('--tls', action='store_true')
    parser.add_argument('--ca-certs',
                        help='CA bundle that signed the broker certificate')
    parser.add_argument('--client-cert',
                        help='for a broker that requires a client certificate')
    parser.add_argument('--client-key')
    parser.add_argument('--server-hostname',
                        help='name to verify, if it is not --host')
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse and complete the command line.

    :param argv: Command line, or None to read `sys.argv`
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.pairs < 1:
        parser.error('--pairs must be at least 1')
    if args.client_cert and not args.tls:
        parser.error('--client-cert needs --tls')
    if args.client_cert and not args.client_key:
        parser.error('--client-cert needs --client-key')
    if args.port is None:
        args.port = 5671 if args.tls else 5672
    if args.workload is None:
        args.workload = list(DEFAULT_WORKLOADS)
    return args


def main(argv: list[str] | None = None) -> int:
    """
    Measure the sources, print a medians table per workload and optionally save the raw runs.

    :param argv: Command line, or None to read `sys.argv`
    """
    if argv is None:
        argv = sys.argv[1:]
    if argv == ['--worker']:
        print(json.dumps(worker(json.load(sys.stdin))))
        return 0

    args = parse_args(argv)
    with contextlib.ExitStack() as stack:
        sources = {'candidate': materialize(args.candidate, stack)}
        if args.baseline:
            sources['baseline'] = materialize(args.baseline, stack)
        results = measure(args, sources)

    if args.json:
        saved = {
            key: value
            for key, value in vars(args).items()
            if key not in ('password', 'workload')
        }
        Path(args.json).write_text(json.dumps(
            {
                'args': saved,
                'workloads': results
            }, indent=2),
                                   encoding='utf-8')
    for result in results:
        print(format_summary(result['count'], result['size'], result['pairs']))
        print()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
