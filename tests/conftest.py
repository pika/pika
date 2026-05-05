def pytest_addoption(parser):
    parser.addoption('--use-tls',
                     action='store_true',
                     default=False,
                     help='Use TLS for acceptance tests.')
