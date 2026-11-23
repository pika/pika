"""
Measure asyncio timer lateness during RabbitMQ publish and consume workloads.

Run `python -m utils.benchmark_asyncio_loop_lag --workload all` against a local plaintext broker.
For TLS, pass `--tls` and `--port 5671`. Add `--ca-certs ca.pem` for a private CA, and pass
`--client-cert client.pem` with `--client-key client.key` when the broker requires mutual TLS. Each
run declares a private, auto-deleted queue with a unique name. Timing thresholds do not cause the
command to fail. The defaults compare bursts of 1 and 50 for 500 1-KiB messages at 50 msg/s over 10
seconds; requested messages are capped by the measurement window. To compare 500-message and
50-message bursts at 25,000 msg/s, set `--workload small`, `--messages 2500`, `--duration 0.1`,
`--rate 25000`, and `--small-bursts 500,50`. Peak queued bytes measure Pika's local asyncio
transport write buffer, sampled after each publish. Publish and delivery rates divide in-window
counts by the fixed measurement window; the final delivery count also includes any drain after that
window.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import ssl
import sys
import time
import uuid
from typing import Any, Iterator

import pika
from pika.adapters.asyncio_connection import AsyncioConnection

#: What `--help` shows, rather than the module docstring.
DESCRIPTION = (
    'Measure asyncio timer lateness during RabbitMQ publish and consume '
    'workloads.')

DEFAULT_MESSAGES = 500
DEFAULT_DURATION = 10.0
DEFAULT_RATE = 50.0
DEFAULT_SMALL_BURSTS = (1, 50)


class LoopLagSampler:
    """Collect one lateness value for each scheduled timer deadline."""

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 interval: float) -> None:
        self._loop = loop
        self._interval = interval
        self._deadline = 0.0
        self._handle = None
        self.lateness: list[float] = []
        self.missed_deadlines = 0

    def start(self, end: float) -> None:
        now = self._loop.time()
        self._deadline = now + self._interval
        if self._deadline <= now:
            raise ValueError(
                'timer interval is too small to advance the event loop clock')
        self._end = end
        if self._deadline < end:
            self._handle = self._loop.call_at(self._deadline, self._sample)

    def stop(self) -> None:
        if self._handle is not None:
            self._handle.cancel()

    def _sample(self) -> None:
        now = self._loop.time()
        while self._deadline < self._end and self._deadline <= now:
            lateness = max(0.0, now - self._deadline)
            self.lateness.append(lateness)
            if now >= self._deadline + self._interval:
                self.missed_deadlines += 1
            self._deadline += self._interval
        if self._deadline < self._end:
            self._handle = self._loop.call_at(self._deadline, self._sample)


def _percentile(samples: list[float], percentile: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    position = percentile * (len(ordered) - 1)
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    fraction = position - lower_index
    return ordered[lower_index] + fraction * (ordered[upper_index] -
                                              ordered[lower_index])


def _report(samples: list[float], workload: str, requested_messages: int,
            published: int, delivered: int, delivered_in_window: int,
            duration: float, peak_queued_bytes: int, within_window: bool,
            body_size: int, rate: float, burst_size: int, timer_interval: float,
            sync_work_ms: float, missed_deadlines: int) -> dict[str, Any]:
    return {
        'workload':
            workload,
        'body_size_bytes':
            body_size,
        'offered_rate_msg_per_s':
            rate,
        'burst_size':
            burst_size,
        'measurement_window_s':
            duration,
        'timer_interval_ms':
            round(timer_interval * 1000, 3),
        'consumer_sync_work_ms':
            sync_work_ms,
        'samples':
            len(samples),
        'missed_deadlines':
            missed_deadlines,
        'p99_ms':
            round(_percentile(samples, 0.99) * 1000, 3),
        'p99_9_ms':
            round(_percentile(samples, 0.999) * 1000, 3),
        'max_ms':
            round(max(samples, default=0.0) * 1000, 3),
        'requested_messages':
            requested_messages,
        'published':
            published,
        'delivered':
            delivered,
        'delivered_during_window':
            delivered_in_window,
        'publish_throughput_msg_per_s':
            round(published / duration, 2),
        'delivery_throughput_msg_per_s':
            round(delivered_in_window / duration, 2),
        'peak_queued_bytes':
            peak_queued_bytes,
        'deliveries_finished_within_window':
            within_window,
    }


def _publish_schedule(messages: int, burst_size: int, rate: float,
                      duration: float) -> Iterator[tuple[float, int]]:
    """Yield batches at the requested rate, offering the same volume for every burst size."""
    budget = min(messages, math.floor(round(rate * duration, 9)))
    published = 0
    while published < budget:
        offset = published / rate
        batch = min(burst_size, budget - published)
        yield offset, batch
        published += batch


def _transport_write_buffer_size(connection: Any) -> int:
    """Read Pika's asyncio transport's buffered outbound byte count."""
    return connection._transport.get_write_buffer_size()


class _Run:

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 params: pika.ConnectionParameters, workload: str,
                 messages: int, body_size: int, rate: float, burst_size: int,
                 duration: float, timer_interval: float,
                 sync_work_ms: float) -> None:
        self.loop = loop
        self.params = params
        self.workload = workload
        self.messages = messages
        self.body = b'x' * body_size
        self.rate = rate
        self.burst_size = burst_size
        self.duration = duration
        self.timer_interval = timer_interval
        self.sync_work_ms = sync_work_ms
        self.ready = loop.create_future()
        self.failure = loop.create_future()
        self.closed = loop.create_future()
        self.deliveries_done = loop.create_future()
        self.channel = None
        self.connection = None
        self.queue = 'pika-loop-lag-' + uuid.uuid4().hex
        self.published = 0
        self.delivered = 0
        self.delivered_in_window = 0
        self.peak_queued_bytes = 0
        self.publisher_task = None
        self.publisher_finished = False
        self.closing = False
        self.was_opened = False
        self.sampler = LoopLagSampler(loop, timer_interval)
        self.started = 0.0
        self.window_end = 0.0

    async def run(self) -> dict[str, Any]:
        self.connection = AsyncioConnection(
            self.params,
            on_open_callback=self._on_open,
            on_open_error_callback=self._on_error,
            on_close_callback=self._on_connection_close,
            custom_ioloop=self.loop)
        try:
            await self._wait_or_fail(self.ready)
            self.started = self.loop.time()
            self.window_end = self.started + self.duration
            if self.window_end <= self.started:
                raise ValueError(
                    'measurement duration is too small for the event loop clock'
                )
            self.sampler.start(end=self.window_end)
            self.publisher_task = self.loop.create_task(self._publish())
            await self._wait_or_fail(asyncio.sleep(self.duration))
            self.sampler.stop()
            if not self.sampler.lateness:
                raise RuntimeError('no timer samples were collected')
            await self._wait_or_fail(self.publisher_task)
            if self.published:
                await self._wait_or_fail(self.deliveries_done, timeout=30)
            within_window = self.delivered_in_window == self.published
            return _report(samples=self.sampler.lateness,
                           workload=self.workload,
                           requested_messages=self.messages,
                           published=self.published,
                           delivered=self.delivered,
                           delivered_in_window=self.delivered_in_window,
                           duration=self.duration,
                           peak_queued_bytes=self.peak_queued_bytes,
                           within_window=within_window,
                           body_size=len(self.body),
                           rate=self.rate,
                           burst_size=self.burst_size,
                           timer_interval=self.timer_interval,
                           sync_work_ms=self.sync_work_ms,
                           missed_deadlines=self.sampler.missed_deadlines)
        finally:
            self.sampler.stop()
            if self.publisher_task is not None and not self.publisher_task.done(
            ):
                self.publisher_task.cancel()
                await asyncio.gather(self.publisher_task,
                                     return_exceptions=True)
            await self._close_connection()

    async def _wait_or_fail(self,
                            awaitable: Any,
                            timeout: float | None = None) -> Any:
        task = asyncio.ensure_future(awaitable)
        completed, _pending = await asyncio.wait(
            (task, self.failure),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED)
        if self.failure in completed:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise self.failure.result()
        if task not in completed:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return None
        return await task

    def _fail(self, error: BaseException) -> None:
        if not self.failure.done():
            self.failure.set_result(error)

    async def _close_connection(self) -> None:
        if self.connection is None or not self.was_opened:
            return
        self.closing = True
        if self.connection.is_open:
            self.connection.close()
        if not self.closed.done():
            try:
                await asyncio.wait_for(asyncio.shield(self.closed), timeout=5)
            except asyncio.TimeoutError as error:
                transport = self.connection._transport
                if transport is not None:
                    transport.abort()
                try:
                    await asyncio.wait_for(asyncio.shield(self.closed),
                                           timeout=1)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        'asyncio connection did not close') from error

    def _on_open(self, connection: Any) -> None:
        self.connection = connection
        self.was_opened = True
        try:
            connection.channel(on_open_callback=self._on_channel)
        except Exception as error:
            self._fail(error)

    def _on_error(self, _connection: Any, error: BaseException) -> None:
        self._fail(error)

    def _on_connection_close(self, _connection: Any,
                             reason: BaseException) -> None:
        if not self.closed.done():
            self.closed.set_result(None)
        if not self.closing:
            self._fail(reason)

    def _on_channel(self, channel: Any) -> None:
        self.channel = channel
        channel.add_on_close_callback(self._on_channel_close)
        try:
            channel.queue_declare(queue=self.queue,
                                  exclusive=True,
                                  auto_delete=True,
                                  callback=self._on_queue)
        except Exception as error:
            self._fail(error)

    def _on_channel_close(self, _channel: Any, reason: BaseException) -> None:
        if not self.closing:
            self._fail(reason)

    def _on_queue(self, _frame: Any) -> None:
        try:
            self.channel.basic_consume(self.queue,
                                       self._on_message,
                                       auto_ack=True,
                                       callback=self._on_consume_ok)
        except Exception as error:
            self._fail(error)

    def _on_consume_ok(self, _frame: Any) -> None:
        if not self.ready.done():
            self.ready.set_result(None)

    def _on_message(self, _channel: Any, _method: Any, _properties: Any,
                    _body: bytes) -> None:
        if self.sync_work_ms:
            end = time.perf_counter() + self.sync_work_ms / 1000.0
            while time.perf_counter() < end:
                pass
        self.delivered += 1
        if self.loop.time() < self.window_end:
            self.delivered_in_window += 1
        self._check_deliveries_done()

    def _check_deliveries_done(self) -> None:
        if (self.publisher_finished and self.delivered == self.published and
                not self.deliveries_done.done()):
            self.deliveries_done.set_result(None)

    async def _publish(self) -> None:
        for offset, batch in _publish_schedule(self.messages, self.burst_size,
                                               self.rate, self.duration):
            delay = self.started + offset - self.loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            if self.loop.time() >= self.window_end:
                break
            for _ in range(batch):
                if self.loop.time() >= self.window_end:
                    break
                self.channel.basic_publish('', self.queue, self.body)
                self.published += 1
                self.peak_queued_bytes = max(
                    self.peak_queued_bytes,
                    _transport_write_buffer_size(self.connection))
        self.publisher_finished = True
        self._check_deliveries_done()


def _params(args: argparse.Namespace) -> pika.ConnectionParameters:
    options = None
    if args.tls:
        context = ssl.create_default_context(cafile=args.ca_certs)
        if args.client_cert:
            context.load_cert_chain(args.client_cert, args.client_key)
        options = pika.SSLOptions(context,
                                  server_hostname=args.server_hostname or
                                  args.host)
    return pika.ConnectionParameters(host=args.host,
                                     port=args.port,
                                     ssl_options=options,
                                     credentials=pika.PlainCredentials(
                                         args.username, args.password))


def _validate_args(args: argparse.Namespace) -> None:
    positive = (args.duration, args.rate, args.timer_interval)
    nonnegative = (args.sync_work_ms,)
    if (not all(math.isfinite(value) for value in positive + nonnegative) or
            any(value <= 0 for value in positive) or
            any(value < 0 for value in nonnegative) or args.messages < 1 or
            args.small_body_size < 1 or args.large_body_size < 1 or
            args.large_burst_size < 1 or args.burst_size < 1 or
            not args.small_bursts or
            any(burst < 1 for burst in args.small_bursts)):
        raise ValueError(
            'counts, sizes, rates, and intervals must be positive; '
            'sync work must be finite and non-negative')
    if args.timer_interval >= args.duration:
        raise ValueError(
            'timer interval must be shorter than the measurement duration')
    now = time.monotonic()
    if now + args.duration <= now:
        raise ValueError(
            'measurement duration is too small to advance the event loop clock')
    if now + args.timer_interval <= now:
        raise ValueError(
            'timer interval is too small to advance the event loop clock')


async def _run_workload(args: argparse.Namespace,
                        name: str,
                        burst: int,
                        body_size: int,
                        sync_ms: float,
                        messages: int | None = None) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    run = _Run(loop, _params(args), name,
               args.messages if messages is None else messages, body_size,
               args.rate, burst, args.duration, args.timer_interval, sync_ms)
    return await run.run()


async def _main_async(args: argparse.Namespace) -> list[dict[str, Any]]:
    results = []
    if args.workload in ('all', 'idle'):
        results.append(await _run_workload(args, 'idle', 1, 0, 0, messages=0))
    if args.workload in ('all', 'small'):
        results.extend([
            await _run_workload(args, f'small-burst-{burst}', burst,
                                args.small_body_size, 0)
            for burst in args.small_bursts
        ])
    if args.workload in ('all', 'large-single'):
        results.append(await _run_workload(args, 'large-single', 1,
                                           args.large_body_size, 0))
    if args.workload in ('all', 'large-burst'):
        results.append(await
                       _run_workload(args, 'large-burst', args.large_burst_size,
                                     args.large_body_size, 0))
    if args.workload in ('all', 'sync-consumer'):
        results.append(await
                       _run_workload(args, 'sync-consumer', args.burst_size,
                                     args.small_body_size, args.sync_work_ms))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5672)
    parser.add_argument('--username', default='guest')
    parser.add_argument('--password', default='guest')
    parser.add_argument('--tls', action='store_true')
    parser.add_argument('--ca-certs')
    parser.add_argument('--client-cert')
    parser.add_argument('--client-key')
    parser.add_argument('--server-hostname')
    parser.add_argument('--workload',
                        choices=('all', 'idle', 'small', 'large-single',
                                 'large-burst', 'sync-consumer'),
                        default='all')
    parser.add_argument('--messages', type=int, default=DEFAULT_MESSAGES)
    parser.add_argument('--duration', type=float, default=DEFAULT_DURATION)
    parser.add_argument('--rate', type=float, default=DEFAULT_RATE)
    parser.add_argument('--small-bursts',
                        type=lambda v: [int(x) for x in v.split(',')],
                        default=list(DEFAULT_SMALL_BURSTS))
    parser.add_argument('--small-body-size', type=int, default=1024)
    parser.add_argument('--large-body-size', type=int, default=64 * 1024)
    parser.add_argument('--large-burst-size', type=int, default=10)
    parser.add_argument('--burst-size', type=int, default=50)
    parser.add_argument('--sync-work-ms', type=float, default=1.0)
    parser.add_argument('--timer-interval', type=float, default=0.001)
    args = parser.parse_args(argv)
    if args.tls and args.client_cert and not args.client_key:
        parser.error('--client-key is required with --client-cert')
    try:
        _validate_args(args)
    except ValueError as error:
        parser.error(str(error))
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    results = asyncio.run(_main_async(args))
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(
        item['published'] == item['delivered'] for item in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
