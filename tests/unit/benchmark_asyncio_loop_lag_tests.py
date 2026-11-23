"""Tests for the loop lag measurement seam."""

import asyncio
import importlib.util
import time
from pathlib import Path

import pytest

_SCRIPT = Path(
    __file__).resolve().parents[2] / 'utils' / 'benchmark_asyncio_loop_lag.py'
_SPEC = importlib.util.spec_from_file_location('benchmark_asyncio_loop_lag',
                                               _SCRIPT)
_BENCHMARK = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_BENCHMARK)


class _Handle:

    def cancel(self):
        pass


class _Future:

    def __init__(self):
        self._done = False
        self._result = None

    def done(self):
        return self._done

    def set_result(self, result):
        self._result = result
        self._done = True

    def result(self):
        return self._result


class _Loop:

    def __init__(self, now=0.0):
        self.now = now
        self.callbacks = []

    def time(self):
        return self.now

    def call_at(self, deadline, callback):
        self.callbacks.append((deadline, callback))
        return _Handle()

    def create_future(self):
        return _Future()


class _Channel:

    def __init__(self):
        self.consume_callback = None
        self.close_callback = None

    def basic_consume(self, _queue, _callback, auto_ack, callback):
        assert auto_ack
        self.consume_callback = callback

    def add_on_close_callback(self, callback):
        self.close_callback = callback


def test_loop_lag_sampler_excludes_the_window_end_and_counts_missed_deadlines():
    loop = _Loop()
    sampler = _BENCHMARK.LoopLagSampler(loop, 0.01)
    sampler.start(end=0.03)
    loop.now = 0.03
    loop.callbacks.pop(0)[1]()

    assert len(sampler.lateness) == 2
    assert sampler.missed_deadlines == 2
    assert loop.callbacks == []


def test_timer_interval_must_advance_the_loop_clock():
    sampler = _BENCHMARK.LoopLagSampler(_Loop(now=1.0), 1e-300)

    with pytest.raises(ValueError, match='advance'):
        sampler.start(end=2.0)


def test_report_uses_interpolated_quantiles_and_window_throughput():
    result = _BENCHMARK._report(samples=[0.001, 0.002, 0.003],
                                workload='sample',
                                requested_messages=500,
                                published=4,
                                delivered=3,
                                delivered_in_window=2,
                                duration=2.0,
                                peak_queued_bytes=1024,
                                within_window=False,
                                body_size=10,
                                rate=50.0,
                                burst_size=5,
                                timer_interval=0.001,
                                sync_work_ms=1.0,
                                missed_deadlines=1)

    assert result['requested_messages'] == 500
    assert result['published'] == 4
    assert result['delivered'] == 3
    assert result['delivered_during_window'] == 2
    assert result['p99_ms'] == 2.98
    assert result['p99_9_ms'] == 2.998
    assert result['max_ms'] == 3.0
    assert result['publish_throughput_msg_per_s'] == 2.0
    assert result['delivery_throughput_msg_per_s'] == 1.0
    assert result['peak_queued_bytes'] == 1024
    assert result['deliveries_finished_within_window'] is False


def test_default_burst_schedules_offer_equal_volume_and_rate_throughout_window(
):
    single = list(
        _BENCHMARK._publish_schedule(_BENCHMARK.DEFAULT_MESSAGES,
                                     _BENCHMARK.DEFAULT_SMALL_BURSTS[0],
                                     _BENCHMARK.DEFAULT_RATE,
                                     _BENCHMARK.DEFAULT_DURATION))
    burst = list(
        _BENCHMARK._publish_schedule(_BENCHMARK.DEFAULT_MESSAGES,
                                     _BENCHMARK.DEFAULT_SMALL_BURSTS[1],
                                     _BENCHMARK.DEFAULT_RATE,
                                     _BENCHMARK.DEFAULT_DURATION))

    assert sum(
        batch for _offset, batch in single) == _BENCHMARK.DEFAULT_MESSAGES
    assert sum(batch for _offset, batch in burst) == _BENCHMARK.DEFAULT_MESSAGES
    assert single[0][0] == 0
    assert single[-1][0] < _BENCHMARK.DEFAULT_DURATION
    assert burst[0][0] == 0
    assert burst[-1][0] < _BENCHMARK.DEFAULT_DURATION
    assert len(single) == _BENCHMARK.DEFAULT_MESSAGES
    assert len(burst) == 10
    assert single[1][0] == pytest.approx(1 / _BENCHMARK.DEFAULT_RATE)
    assert burst[1][0] == pytest.approx(1.0)


@pytest.mark.parametrize('rate, duration, expected',
                         ((50.0, 0.03, 1), (50.0, 10.0, 500), (100.0, 0.29, 29),
                          (25000.0, 0.1, 2500)))
def test_every_burst_size_offers_the_same_volume_in_short_windows(
        rate, duration, expected):
    totals = [
        sum(batch
            for _offset, batch in _BENCHMARK._publish_schedule(
                _BENCHMARK.DEFAULT_MESSAGES * 10, burst, rate, duration))
        for burst in (1, 50, 500)
    ]

    assert totals == [expected] * 3


def test_consumer_readiness_waits_for_consume_ok():
    loop = _Loop()
    benchmark = _BENCHMARK._Run(loop, _BENCHMARK.pika.ConnectionParameters(),
                                'idle', 0, 0, 50.0, 1, 10.0, 0.01, 0)
    channel = _Channel()
    benchmark.channel = channel

    benchmark._on_queue(None)

    assert not benchmark.ready.done()
    channel.consume_callback(None)
    assert benchmark.ready.done()
    assert benchmark.ready.result() is None


def test_channel_close_before_consume_ok_fails_setup():
    loop = _Loop()
    benchmark = _BENCHMARK._Run(loop, _BENCHMARK.pika.ConnectionParameters(),
                                'idle', 0, 0, 50.0, 1, 10.0, 0.01, 0)
    error = RuntimeError('queue declaration was refused')

    benchmark._on_channel_close(None, error)

    assert benchmark.failure.done()
    assert benchmark.failure.result() is error


class _FakeTransport:

    def get_write_buffer_size(self):
        return 8192


def _install_connection(monkeypatch, on_publish):
    """Replace `AsyncioConnection` with a stub whose channel calls `on_publish(consumer)`."""
    connections = []

    class Channel:

        def __init__(self):
            self.consumer = None

        def add_on_close_callback(self, _callback):
            pass

        def queue_declare(self, **kwargs):
            asyncio.get_running_loop().call_soon(kwargs['callback'], None)

        def basic_consume(self, _queue, consumer, auto_ack, callback):
            assert auto_ack
            self.consumer = consumer
            asyncio.get_running_loop().call_soon(callback, None)

        def basic_publish(self, _exchange, _queue, _body):
            on_publish(self.consumer)

    class Connection:

        def __init__(self, _params, on_open_callback, on_close_callback,
                     **_kwargs):
            self.is_open = True
            self._transport = _FakeTransport()
            self.channel_instance = Channel()
            self.on_close_callback = on_close_callback
            self.loop = asyncio.get_running_loop()
            self.loop.call_soon(on_open_callback, self)
            connections.append(self)

        def channel(self, on_open_callback):
            self.loop.call_soon(on_open_callback, self.channel_instance)

        def close(self):
            self.is_open = False
            self.loop.call_soon(self.on_close_callback, self,
                                RuntimeError('closed'))

    monkeypatch.setattr(_BENCHMARK, 'AsyncioConnection', Connection)
    return connections


def _run_benchmark(messages, rate, duration, timer_interval):

    async def run():
        loop = asyncio.get_running_loop()
        benchmark = _BENCHMARK._Run(loop,
                                    _BENCHMARK.pika.ConnectionParameters(),
                                    'sample', messages, 32, rate, 1, duration,
                                    timer_interval, 0)
        return await benchmark.run()

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(run())
    finally:
        loop.close()


def _publish_failure(_consumer):
    raise RuntimeError('publish failed')


def test_run_surfaces_publish_failure_and_closes_connection(monkeypatch):
    connections = _install_connection(monkeypatch, _publish_failure)

    with pytest.raises(RuntimeError, match='publish failed'):
        _run_benchmark(1, 100.0, 0.1, 0.02)

    assert connections[0].is_open is False


def test_delivery_before_the_cutoff_finishes_within_the_window_when_the_publisher_wakes_late(
        monkeypatch):

    def deliver_then_overrun_the_window(consumer):
        consumer(None, None, None, b'')
        time.sleep(0.15)

    connections = _install_connection(monkeypatch,
                                      deliver_then_overrun_the_window)

    result = _run_benchmark(2, 100.0, 0.1, 0.02)

    assert result['published'] == 1
    assert result['delivered_during_window'] == 1
    assert result['deliveries_finished_within_window'] is True
    assert result['peak_queued_bytes'] == 8192
    assert connections[0].is_open is False


def test_delivery_after_the_cutoff_does_not_finish_within_the_window(
        monkeypatch):

    def deliver_after_the_window(consumer):
        asyncio.get_running_loop().call_later(0.2, consumer, None, None, None,
                                              b'')

    _install_connection(monkeypatch, deliver_after_the_window)

    result = _run_benchmark(1, 100.0, 0.1, 0.02)

    assert result['published'] == 1
    assert result['delivered'] == 1
    assert result['delivered_during_window'] == 0
    assert result['deliveries_finished_within_window'] is False


def test_run_without_timer_samples_fails_and_closes_connection(monkeypatch):
    connections = _install_connection(monkeypatch, lambda _consumer: None)

    with pytest.raises(RuntimeError, match='no timer samples'):
        _run_benchmark(0, 100.0, 0.01, 0.01)

    assert connections[0].is_open is False


def test_validation_rejects_non_finite_timing_and_work_values():
    args = _BENCHMARK.argparse.Namespace(duration=float('inf'),
                                         rate=50.0,
                                         timer_interval=0.001,
                                         sync_work_ms=1.0,
                                         messages=500,
                                         small_body_size=1024,
                                         large_body_size=65536,
                                         large_burst_size=10,
                                         burst_size=50,
                                         small_bursts=[1, 50])
    with pytest.raises(ValueError, match='finite'):
        _BENCHMARK._validate_args(args)

    args.duration = 10.0
    args.sync_work_ms = float('inf')
    with pytest.raises(ValueError, match='finite'):
        _BENCHMARK._validate_args(args)

    args.sync_work_ms = 1.0
    args.timer_interval = 1e-300
    with pytest.raises(ValueError, match='advance'):
        _BENCHMARK._validate_args(args)


@pytest.mark.parametrize('timer_interval', (0.02, 0.03))
def test_validation_rejects_timer_intervals_that_cannot_sample_the_window(
        timer_interval):
    args = _BENCHMARK.argparse.Namespace(duration=0.02,
                                         rate=50.0,
                                         timer_interval=timer_interval,
                                         sync_work_ms=1.0,
                                         messages=500,
                                         small_body_size=1024,
                                         large_body_size=65536,
                                         large_burst_size=10,
                                         burst_size=50,
                                         small_bursts=[1, 50])

    with pytest.raises(ValueError, match='shorter than the measurement'):
        _BENCHMARK._validate_args(args)
