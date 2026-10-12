"""
Putting `ssl_options` into an AMQP URL rather than building an `SSLOptions` by hand.

`URLParameters` reads the `ssl_options` query argument with `ast.literal_eval`, so the value is a
Python dict literal and `urlencode` produces an acceptable encoding of one. Most of the keys name an
argument of an `ssl.SSLContext` method: `protocol` for the constructor, `ca_certs`/`cafile`,
`ca_path`/`capath` and `ca_data`/`cadata` for `load_verify_locations`, `certfile`, `keyfile` and
`password` for `load_cert_chain`, `ciphers` for `set_ciphers`. `server_hostname` is the exception,
and goes to `pika.SSLOptions` rather than to the context. See `_set_url_ssl_options` in
`pika/connection.py` for the whole list, which is the only place it is written down.
"""

from pathlib import Path
from urllib.parse import urlencode

import pika

_certs = Path(__file__).resolve().parent.parent / 'tests' / 'certs'

query = urlencode({
    'ssl_options': {
        'ca_certs': str(_certs / 'ca_certificate.pem'),
        'certfile': str(_certs / 'client_certificate.pem'),
        'keyfile': str(_certs / 'client_key.pem'),
        'server_hostname': 'localhost',
    }
})

url = f'amqps://guest:guest@localhost:5671/%2F?{query}'
print(url)

# The scheme decides TLS; the query argument configures it.
parameters = pika.URLParameters(url)
print(f'port={parameters.port} '
      f'server_hostname={parameters.ssl_options.server_hostname}')

with pika.BlockingConnection(parameters) as connection:
    print(f'connected: {connection.is_open}')
