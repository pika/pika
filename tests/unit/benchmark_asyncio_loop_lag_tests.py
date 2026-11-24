"""Tests for the loop lag measurement seam."""

import asyncio
import contextlib
import importlib.util
import io
import time
import types
import unittest
from pathlib import Path
from unittest import mock

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


class _FakeTransport:

    def get_write_buffer_size(self):
        return 8192


def _valid_args(**overrides):
    args = _BENCHMARK.argparse.Namespace(duration=10.0,
                                         rate=50.0,
                                         timer_interval=0.001,
                                         sync_work_ms=1.0,
                                         messages=500,
                                         small_body_size=1024,
                                         large_body_size=65536,
                                         large_burst_size=10,
                                         burst_size=50,
                                         small_bursts=[1, 50])
    for name, value in overrides.items():
        setattr(args, name, value)
    return args


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


class SamplerAndReportTests(unittest.TestCase):

    def test_loop_lag_sampler_excludes_the_window_end_and_counts_missed_deadlines(
            self):
        loop = _Loop()
        sampler = _BENCHMARK.LoopLagSampler(loop, 0.01)
        sampler.start(end=0.03)
        loop.now = 0.03
        loop.callbacks.pop(0)[1]()

        self.assertEqual(len(sampler.lateness), 2)
        self.assertEqual(sampler.missed_deadlines, 2)
        self.assertEqual(loop.callbacks, [])

    def test_timer_interval_must_advance_the_loop_clock(self):
        sampler = _BENCHMARK.LoopLagSampler(_Loop(now=1.0), 1e-300)

        with self.assertRaisesRegex(ValueError, 'advance'):
            sampler.start(end=2.0)

    def test_report_uses_interpolated_quantiles_and_window_throughput(self):
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

        self.assertEqual(result['requested_messages'], 500)
        self.assertEqual(result['published'], 4)
        self.assertEqual(result['delivered'], 3)
        self.assertEqual(result['delivered_during_window'], 2)
        self.assertEqual(result['p99_ms'], 2.98)
        self.assertEqual(result['p99_9_ms'], 2.998)
        self.assertEqual(result['max_ms'], 3.0)
        self.assertEqual(result['publish_throughput_msg_per_s'], 2.0)
        self.assertEqual(result['delivery_throughput_msg_per_s'], 1.0)
        self.assertEqual(result['peak_queued_bytes'], 1024)
        self.assertIs(result['deliveries_finished_within_window'], False)


class PublishScheduleTests(unittest.TestCase):

    def test_default_burst_schedules_offer_equal_volume_and_rate_throughout_window(
            self):
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

        self.assertEqual(sum(batch for _offset, batch in single),
                         _BENCHMARK.DEFAULT_MESSAGES)
        self.assertEqual(sum(batch for _offset, batch in burst),
                         _BENCHMARK.DEFAULT_MESSAGES)
        self.assertEqual(single[0][0], 0)
        self.assertLess(single[-1][0], _BENCHMARK.DEFAULT_DURATION)
        self.assertEqual(burst[0][0], 0)
        self.assertLess(burst[-1][0], _BENCHMARK.DEFAULT_DURATION)
        self.assertEqual(len(single), _BENCHMARK.DEFAULT_MESSAGES)
        self.assertEqual(len(burst), 10)
        self.assertAlmostEqual(single[1][0], 1 / _BENCHMARK.DEFAULT_RATE)
        self.assertAlmostEqual(burst[1][0], 1.0)

    def test_every_burst_size_offers_the_same_volume_in_short_windows(self):
        for rate, duration, expected in ((50.0, 0.03, 1), (50.0, 10.0, 500),
                                         (100.0, 0.29, 29), (25000.0, 0.1,
                                                             2500)):
            with self.subTest(rate=rate, duration=duration):
                totals = [
                    sum(batch
                        for _offset, batch in _BENCHMARK._publish_schedule(
                            _BENCHMARK.DEFAULT_MESSAGES *
                            10, burst, rate, duration))
                    for burst in (1, 50, 500)
                ]

                self.assertEqual(totals, [expected] * 3)


class SetupTests(unittest.TestCase):

    def test_consumer_readiness_waits_for_consume_ok(self):
        loop = _Loop()
        benchmark = _BENCHMARK._Run(loop,
                                    _BENCHMARK.pika.ConnectionParameters(),
                                    'idle', 0, 0, 50.0, 1, 10.0, 0.01, 0)
        channel = _Channel()
        benchmark.channel = channel

        benchmark._on_queue(None)

        self.assertFalse(benchmark.ready.done())
        channel.consume_callback(None)
        self.assertTrue(benchmark.ready.done())
        self.assertIsNone(benchmark.ready.result())

    def test_channel_close_before_consume_ok_fails_setup(self):
        loop = _Loop()
        benchmark = _BENCHMARK._Run(loop,
                                    _BENCHMARK.pika.ConnectionParameters(),
                                    'idle', 0, 0, 50.0, 1, 10.0, 0.01, 0)
        error = RuntimeError('queue declaration was refused')

        benchmark._on_channel_close(None, error)

        self.assertTrue(benchmark.failure.done())
        self.assertIs(benchmark.failure.result(), error)


class RunTests(unittest.TestCase):

    def _install_connection(self, on_publish, declare_error=None):
        """
        Replace `AsyncioConnection` with a stub whose channel calls `on_publish(consumer)`.

        With `declare_error`, the channel closes with it instead of declaring the queue.
        """
        connections = []

        class Channel:

            def __init__(self):
                self.consumer = None
                self.close_callback = None

            def add_on_close_callback(self, callback):
                self.close_callback = callback

            def queue_declare(self, **kwargs):
                loop = asyncio.get_running_loop()
                if declare_error is None:
                    loop.call_soon(kwargs['callback'], None)
                else:
                    loop.call_soon(self.close_callback, self, declare_error)

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

        patcher = mock.patch.object(_BENCHMARK, 'AsyncioConnection', Connection)
        patcher.start()
        self.addCleanup(patcher.stop)
        return connections

    def test_run_surfaces_publish_failure_and_closes_connection(self):
        connections = self._install_connection(_publish_failure)

        with self.assertRaisesRegex(RuntimeError, 'publish failed'):
            _run_benchmark(1, 100.0, 0.1, 0.02)

        self.assertIs(connections[0].is_open, False)

    def test_run_surfaces_a_broker_refusal_and_closes_connection(self):
        refusal = RuntimeError('queue declaration was refused')
        connections = self._install_connection(_publish_failure,
                                               declare_error=refusal)

        with self.assertRaises(RuntimeError) as raised:
            _run_benchmark(1, 100.0, 0.1, 0.02)

        self.assertIs(raised.exception, refusal)
        self.assertIs(connections[0].is_open, False)

    def test_delivery_before_the_cutoff_finishes_within_the_window_when_the_publisher_wakes_late(
            self):

        def deliver_then_overrun_the_window(consumer):
            consumer(None, None, None, b'')
            time.sleep(0.15)

        connections = self._install_connection(deliver_then_overrun_the_window)

        result = _run_benchmark(2, 100.0, 0.1, 0.02)

        self.assertEqual(result['published'], 1)
        self.assertEqual(result['delivered_during_window'], 1)
        self.assertIs(result['deliveries_finished_within_window'], True)
        self.assertEqual(result['peak_queued_bytes'], 8192)
        self.assertIs(connections[0].is_open, False)

    def test_delivery_after_the_cutoff_does_not_finish_within_the_window(self):

        def deliver_after_the_window(consumer):
            asyncio.get_running_loop().call_later(0.2, consumer, None, None,
                                                  None, b'')

        self._install_connection(deliver_after_the_window)

        result = _run_benchmark(1, 100.0, 0.1, 0.02)

        self.assertEqual(result['published'], 1)
        self.assertEqual(result['delivered'], 1)
        self.assertEqual(result['delivered_during_window'], 0)
        self.assertIs(result['deliveries_finished_within_window'], False)

    def test_run_without_timer_samples_fails_and_closes_connection(self):
        connections = self._install_connection(lambda _consumer: None)

        with self.assertRaisesRegex(RuntimeError, 'no timer samples'):
            _run_benchmark(0, 100.0, 0.01, 0.01)

        self.assertIs(connections[0].is_open, False)


class ValidationTests(unittest.TestCase):

    def test_validation_rejects_non_finite_timing_and_work_values(self):
        args = _valid_args(duration=float('inf'))
        with self.assertRaisesRegex(ValueError, 'finite'):
            _BENCHMARK._validate_args(args)

        args.duration = 10.0
        args.sync_work_ms = float('inf')
        with self.assertRaisesRegex(ValueError, 'finite'):
            _BENCHMARK._validate_args(args)

        args.sync_work_ms = 1.0
        args.timer_interval = 1e-300
        with self.assertRaisesRegex(ValueError, 'advance'):
            _BENCHMARK._validate_args(args)

    def test_validation_rejects_timer_intervals_that_cannot_sample_the_window(
            self):
        for timer_interval in (0.02, 0.03):
            with self.subTest(timer_interval=timer_interval):
                args = _valid_args(duration=0.02, timer_interval=timer_interval)

                with self.assertRaisesRegex(ValueError,
                                            'shorter than the measurement'):
                    _BENCHMARK._validate_args(args)

    def test_validation_rejects_a_timer_interval_the_clock_cannot_resolve(self):
        coarse = _BENCHMARK.time.get_clock_info('monotonic')
        coarse.resolution = 0.015625
        with mock.patch.object(_BENCHMARK.time,
                               'get_clock_info',
                               return_value=coarse):
            with self.assertRaisesRegex(ValueError, 'clock resolution'):
                _BENCHMARK._validate_args(_valid_args(timer_interval=0.001))
            _BENCHMARK._validate_args(_valid_args(timer_interval=0.2))


class MainTests(unittest.TestCase):

    def _main(self, argv, platform=None):
        """Run `main` without a broker; return its exit code and stderr."""
        stderr = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(contextlib.redirect_stderr(stderr))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            # The default `--timer-interval` is below ten ticks of Windows'
            # clock before Python 3.13, so pin a fine clock for these tests.
            stack.enter_context(
                mock.patch.object(
                    _BENCHMARK.time,
                    'get_clock_info',
                    return_value=types.SimpleNamespace(resolution=1e-09)))
            stack.enter_context(
                mock.patch.object(_BENCHMARK, '_main_async', mock.Mock()))
            self.run = stack.enter_context(
                mock.patch.object(_BENCHMARK.asyncio, 'run', return_value=[]))
            self.set_policy = stack.enter_context(
                mock.patch.object(_BENCHMARK.asyncio, 'set_event_loop_policy'))
            if platform is not None:
                stack.enter_context(
                    mock.patch.object(
                        _BENCHMARK, 'sys',
                        types.SimpleNamespace(platform=platform[0],
                                              version_info=platform[1])))
                stack.enter_context(
                    mock.patch.object(_BENCHMARK.asyncio,
                                      'WindowsSelectorEventLoopPolicy',
                                      create=True))
            try:
                code = _BENCHMARK.main(argv)
            except SystemExit as error:
                code = error.code
        return code, stderr.getvalue()

    def test_tls_options_without_tls_are_rejected(self):
        code, stderr = self._main(['--ca-certs', 'ca.pem', '--client-key', 'k'])

        self.assertEqual(code, 2)
        self.assertIn('--ca-certs, --client-key requires --tls', stderr)

    def test_tls_options_with_tls_are_accepted(self):
        code, stderr = self._main(['--tls', '--ca-certs', 'ca.pem'])

        self.assertEqual(code, 0)
        self.assertEqual(stderr, '')

    def test_windows_runs_a_selector_loop_without_a_policy_from_3_12(self):
        self._main(['--workload', 'idle'], platform=('win32', (3, 12)))

        self.assertIs(self.run.call_args[1]['loop_factory'],
                      _BENCHMARK.asyncio.SelectorEventLoop)
        self.assertFalse(self.set_policy.called)

    def test_windows_before_3_12_selects_the_loop_with_a_policy(self):
        self._main(['--workload', 'idle'], platform=('win32', (3, 11)))

        self.assertNotIn('loop_factory', self.run.call_args[1])
        self.assertTrue(self.set_policy.called)
