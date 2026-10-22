"""`--use-tls` has to reach the process that runs the tests, and the broker over TLS."""

import logging

import pytest

import pika
from tests.base.async_test_base import AsyncTestCase, enable_tls


def test_use_tls_reaches_the_test_process(pytestconfig):
    """
    Fails if the option is lost on the way to an xdist worker.

    `enable_tls` once read `sys.argv`, which a worker does not share with the controller, so every
    `tls=true` leg in CI silently ran the plaintext suite (#1782).
    """
    if not pytestconfig.getoption('--use-tls'):
        pytest.skip('only meaningful with --use-tls')
    assert enable_tls()


def test_tls_connection_parameters_open_a_connection(pytestconfig):
    """The parameters a TLS run uses reach a working listener, rather than only being chosen."""
    if not pytestconfig.getoption('--use-tls'):
        pytest.skip('only meaningful with --use-tls')
    case = AsyncTestCase('run')
    case.logger = logging.getLogger(__name__)  # normally set by setUp
    params = case.new_connection_params()
    assert params.port == 5671
    assert params.ssl_options is not None
    conn = pika.Connection(params)
    try:
        assert conn.is_open
    finally:
        conn.close(timeout=10)
