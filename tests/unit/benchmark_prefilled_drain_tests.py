"""Tests for utils/benchmark_prefilled_drain.py, which need no broker."""

import argparse
import asyncio
import contextlib
import io
import ssl
import tempfile
import unittest
from pathlib import Path

from tests.base.asyncio_loop import new_pika_asyncio_loop
from utils import benchmark_prefilled_drain as bench

CERTS = Path(__file__).resolve().parents[1] / 'certs'


def run(drain_s, cpu_s, lag_s=(0.001,)):
    return {'drain_s': drain_s, 'cpu_s': cpu_s, 'lag_s': list(lag_s)}


class DeliveryTests(unittest.TestCase):

    def test_a_body_carries_its_sequence_and_size(self):
        body = bench.make_body(7, 1024)
        self.assertEqual(len(body), 1024)
        self.assertIsNone(bench.check_delivery(body, 7, 1024))

    def test_a_body_of_the_wrong_size_is_reported(self):
        problem = bench.check_delivery(bench.make_body(3, 100), 3, 200)
        self.assertIn('100 bytes, expected 200', problem)

    def test_a_delivery_out_of_order_is_reported(self):
        problem = bench.check_delivery(bench.make_body(5, 64), 4, 64)
        self.assertIn('got 5, expected 4', problem)


class MathTests(unittest.TestCase):

    def test_percentile_interpolates_between_ranks(self):
        samples = [4.0, 1.0, 3.0, 2.0]
        self.assertEqual(bench.percentile(samples, 0), 1.0)
        self.assertEqual(bench.percentile(samples, 0.5), 2.5)
        self.assertEqual(bench.percentile(samples, 1), 4.0)
        self.assertAlmostEqual(bench.percentile(samples, 0.99), 3.97)

    def test_percentile_of_one_sample_is_that_sample(self):
        self.assertEqual(bench.percentile([0.25], 0.99), 0.25)

    def test_percent_change_is_relative_to_before(self):
        self.assertEqual(bench.percent_change(200, 150), -25)
        self.assertEqual(bench.percent_change(100, 150), 50)

    def test_sides_take_turns_going_first(self):
        self.assertEqual([bench.pair_order(pair)[0] for pair in range(4)],
                         ['baseline', 'candidate', 'baseline', 'candidate'])
        self.assertEqual(sorted(bench.pair_order(1)), sorted(bench.SIDES))


PAIRS = [
    {
        'baseline': run(1.0, 0.8, [0.001, 0.003]),
        'candidate': run(0.5, 0.4)
    },
    {
        'baseline': run(2.0, 1.0),
        'candidate': run(1.0, 0.9)
    },
    {
        'baseline': run(3.0, 1.2),
        'candidate': run(2.4, 1.2)
    },
]


class SummaryTests(unittest.TestCase):

    def test_medians_are_per_side(self):
        summary = bench.summarize(PAIRS)
        self.assertEqual(summary['baseline']['drain_ms'], 2000)
        self.assertEqual(summary['candidate']['drain_ms'], 1000)
        self.assertEqual(summary['baseline']['cpu_ms'], 1000)
        self.assertAlmostEqual(summary['candidate']['p99_lag_ms'], 1)

    def test_changes_are_the_median_of_the_pairwise_changes(self):
        summary = bench.summarize(PAIRS)
        # Drain ratios 2, 2 and 1.25 give +100, +100 and +25 percent throughput.
        self.assertAlmostEqual(summary['throughput_change_pct'], 100)
        # CPU changes are -50, -10 and 0 percent.
        self.assertAlmostEqual(summary['cpu_change_pct'], -10)

    def test_runs_without_a_timer_firing_leave_the_lag_unreported(self):
        pairs = [{
            'baseline': run(0.001, 0.001, []),
            'candidate': run(0.001, 0.001, [])
        }, {
            'baseline': run(0.002, 0.001, [0.004]),
            'candidate': run(0.001, 0.001, [])
        }]
        summary = bench.summarize(pairs)
        self.assertEqual(summary['baseline']['p99_lag_ms'], 4)
        self.assertIsNone(summary['candidate']['p99_lag_ms'])
        text = bench.format_summary(10, 64, pairs)
        self.assertIn('n/a', text)
        self.assertIn('4.00', text)

    def test_one_side_alone_has_no_changes(self):
        summary = bench.summarize([{'candidate': run(1.0, 0.5)}])
        self.assertNotIn('throughput_change_pct', summary)
        self.assertNotIn('baseline', summary)
        text = bench.format_summary(10, 64, [{'candidate': run(1.0, 0.5)}])
        self.assertIn('10 x 64 B', text)
        self.assertNotIn('change', text)

    def test_the_table_shows_before_and_after_with_the_changes(self):
        text = bench.format_summary(20000, 1024, PAIRS)
        self.assertIn('20000 x 1024 B, 3 runs per side', text)
        self.assertIn('drain ms', text)
        self.assertIn('2000.00 ->    1000.00', text)
        self.assertIn('median paired throughput change: +100.0%', text)
        self.assertIn('median paired consumer CPU change: -10.0%', text)


class WorkloadTests(unittest.TestCase):

    def test_count_and_size_are_read(self):
        self.assertEqual(bench.parse_workload('64x1048576'), (64, 1048576))
        self.assertEqual(bench.parse_workload('5X100'), (5, 100))

    def test_malformed_and_too_small_workloads_are_refused(self):
        for text in ('64', 'axb', '0x100', '5x7', '1x2x3'):
            with self.subTest(text=text), self.assertRaises(
                    argparse.ArgumentTypeError):
                bench.parse_workload(text)


class ParseArgsTests(unittest.TestCase):

    def test_defaults_measure_this_checkout_on_the_plaintext_port(self):
        args = bench.parse_args([])
        self.assertIsNone(args.baseline)
        self.assertEqual(args.candidate, str(bench.REPO_ROOT))
        self.assertEqual(args.port, 5672)
        self.assertEqual(args.pairs, 6)
        self.assertEqual(args.workload, [(20000, 1024), (64, 1048576)])

    def test_tls_moves_the_default_port_and_an_explicit_port_wins(self):
        self.assertEqual(bench.parse_args(['--tls']).port, 5671)
        self.assertEqual(
            bench.parse_args(['--tls', '--port', '5674']).port, 5674)

    def test_workloads_replace_the_defaults(self):
        args = bench.parse_args(['--workload', '10x64', '--workload', '2x1024'])
        self.assertEqual(args.workload, [(10, 64), (2, 1024)])

    def test_inconsistent_options_exit_with_a_usage_error(self):
        for argv in (['--pairs',
                      '0'], ['--client-cert',
                             'c.pem'], ['--tls', '--client-cert', 'c.pem']):
            with self.subTest(argv=argv), contextlib.redirect_stderr(
                    io.StringIO()), self.assertRaises(SystemExit):
                bench.parse_args(argv)


class MaterializeTests(unittest.TestCase):

    def test_a_directory_with_pika_is_used_as_given(self):
        with contextlib.ExitStack() as stack:
            self.assertEqual(bench.materialize(str(bench.REPO_ROOT), stack),
                             bench.REPO_ROOT)

    def test_a_directory_without_pika_is_refused(self):
        with contextlib.ExitStack() as stack, self.assertRaisesRegex(
                ValueError, 'without a pika package'):
            bench.materialize(str(CERTS), stack)

    def test_a_ref_is_extracted_and_removed_with_the_stack(self):
        with contextlib.ExitStack() as stack:
            directory = bench.materialize('HEAD', stack)
            self.assertTrue((directory / 'pika' / '__init__.py').is_file())
        self.assertFalse(directory.exists())

    def test_an_unknown_ref_is_refused(self):
        with contextlib.ExitStack() as stack, self.assertRaisesRegex(
                ValueError, 'neither a directory'):
            bench.materialize('no-such-ref-for-benchmark', stack)


CONFIG = {
    'host': 'broker.example',
    'port': 5671,
    'vhost': 'bench',
    'username': 'u',
    'password': 'p',
    'tls': False,
    'ca_certs': None,
    'client_cert': None,
    'client_key': None,
    'server_hostname': None,
}


class ConnectionParametersTests(unittest.TestCase):

    def test_plaintext_has_no_ssl_options(self):
        params = bench.connection_parameters(CONFIG)
        self.assertEqual((params.host, params.port, params.virtual_host),
                         ('broker.example', 5671, 'bench'))
        self.assertEqual(params.credentials.username, 'u')
        self.assertIsNone(params.ssl_options)

    def test_tls_verifies_the_host_unless_told_a_name(self):
        config = dict(CONFIG,
                      tls=True,
                      ca_certs=str(CERTS / 'ca_certificate.pem'),
                      client_cert=str(CERTS / 'client_certificate.pem'),
                      client_key=str(CERTS / 'client_key.pem'))
        options = bench.connection_parameters(config).ssl_options
        self.assertEqual(options.server_hostname, 'broker.example')
        self.assertEqual(options.context.verify_mode, ssl.CERT_REQUIRED)
        named = dict(config, server_hostname='localhost')
        self.assertEqual(
            bench.connection_parameters(named).ssl_options.server_hostname,
            'localhost')


class WorkerTests(unittest.TestCase):

    def test_a_source_that_is_not_the_imported_pika_fails_before_connecting(
            self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(
                RuntimeError, 'outside'):
            bench.worker({'source': directory})


class DrainTests(unittest.TestCase):
    """Feed deliveries to `Drain` directly, since a broker is the only other source."""

    def setUp(self):
        self.loop = new_pika_asyncio_loop()
        self.addCleanup(self.loop.close)

    def drain(self, count, size=64):
        return bench.Drain(self.loop, None, 'queue', count, size)

    def deliver(self, drain, sequences, size=64):
        for sequence in sequences:
            drain._on_message(None, None, None, bench.make_body(sequence, size))

    def test_the_last_expected_delivery_completes_the_run(self):
        drain = self.drain(3)
        drain.started = (0.0, 0.0)
        self.deliver(drain, range(3))
        self.assertTrue(drain.done.done())
        self.assertIsNone(drain.done.result())
        self.assertEqual(set(drain.result), {'drain_s', 'cpu_s', 'lag_s'})
        self.assertEqual(drain.received, 3)

    def test_a_missing_delivery_fails_the_run(self):
        drain = self.drain(3)
        self.deliver(drain, [0, 2])
        with self.assertRaisesRegex(RuntimeError, 'got 2, expected 1'):
            drain.done.result()

    def test_a_wrong_body_size_fails_the_run(self):
        drain = self.drain(2, size=64)
        self.deliver(drain, [0], size=32)
        with self.assertRaisesRegex(RuntimeError, '32 bytes, expected 64'):
            drain.done.result()

    def test_an_unexpected_close_fails_the_run(self):
        drain = self.drain(2)
        drain._on_close(None, Exception('boom'))
        with self.assertRaisesRegex(RuntimeError, 'connection closed'):
            drain.done.result()


class LagSamplerTests(unittest.TestCase):

    def test_samples_are_non_negative_and_stop_with_the_sampler(self):
        loop = new_pika_asyncio_loop()
        self.addCleanup(loop.close)
        sampler = bench.LagSampler(loop)
        sampler.start()
        loop.run_until_complete(asyncio.sleep(0.05))
        sampler.stop()
        taken = len(sampler.lateness)
        loop.run_until_complete(asyncio.sleep(0.02))
        self.assertGreater(taken, 0)
        self.assertEqual(len(sampler.lateness), taken)
        self.assertTrue(all(late >= 0 for late in sampler.lateness))


if __name__ == '__main__':
    unittest.main()
