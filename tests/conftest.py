import os

#: Set by `pytest_configure` and read by `tests.base.async_test_base.enable_tls`.
#: An environment variable rather than `sys.argv`, because pytest-xdist runs
#: the tests in worker processes whose argv carries none of the options (#1782).
USE_TLS_ENV = 'PIKA_TEST_USE_TLS'


def pytest_addoption(parser):
    parser.addoption('--use-tls',
                     action='store_true',
                     default=False,
                     help='Use TLS for acceptance tests.')


def pytest_configure(config):
    if config.getoption('--use-tls'):
        os.environ[USE_TLS_ENV] = '1'
