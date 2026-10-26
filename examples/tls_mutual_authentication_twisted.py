"""
Mutual TLS with the Twisted adapter, authenticating by client certificate.

`ExternalCredentials` asks the broker for SASL EXTERNAL, which means the broker takes the identity
from the TLS client certificate rather than from a username and password. That needs broker-side
setup beyond a TLS listener:

- the `rabbitmq_auth_mechanism_ssl` plugin enabled
- `EXTERNAL` among `auth_mechanisms`
- a user named for the identity the broker extracts from the certificate. It uses the whole
  distinguished name by default, so for `tests/certs/client_certificate.pem` that user is
  `O=client,CN=shostakovich`; set `ssl_cert_login_from = common_name` to have it use `shostakovich`
  instead

Without those the connection is refused during authentication. See
https://www.rabbitmq.com/docs/access-control#with-client-certificate-authentication
"""

import sys
from pathlib import Path

from twisted.internet import defer, protocol, reactor, ssl

from pika import ConnectionParameters
from pika.adapters import twisted_connection
from pika.credentials import ExternalCredentials


@defer.inlineCallbacks
def publish(connection):
    channel = yield connection.channel()
    yield channel.basic_publish(
        exchange='amq.topic',
        routing_key='hello.world',
        body='Hello World!',
    )
    print('published')


def connection_ready(conn):
    conn.ready.addCallback(lambda _: conn)
    return conn.ready


def done(_result):
    reactor.stop()


failures = []


def failed(failure):
    # Without this the reactor reports 'Unhandled error in Deferred' and the
    # process sits there, which says nothing about the broker refusing the
    # certificate.
    print(f'failed: {failure.type.__name__}: {failure.getErrorMessage()}')
    failures.append(failure)
    reactor.stop()


_certs = Path(__file__).resolve().parent.parent / 'tests' / 'certs'
# Load the CA certificate to validate the server's identity
with _certs.joinpath('ca_certificate.pem').open() as fd:
    ca_cert = ssl.Certificate.loadPEM(fd.read())

# Load the client certificate and key to authenticate with the server
with _certs.joinpath('client_key.pem').open() as fd:
    client_key = fd.read()
with _certs.joinpath('client_certificate.pem').open() as fd:
    client_cert = fd.read()
client_keypair = ssl.PrivateCertificate.loadPEM(client_key + client_cert)

context_factory = ssl.optionsForClientTLS(
    'localhost',
    trustRoot=ca_cert,
    clientCertificate=client_keypair,
)
params = ConnectionParameters(credentials=ExternalCredentials())
cc = protocol.ClientCreator(reactor,
                            twisted_connection.TwistedProtocolConnection,
                            params)
deferred = cc.connectSSL('localhost', 5671, context_factory)
deferred.addCallback(connection_ready)
deferred.addCallback(publish)
deferred.addCallbacks(done, failed)
reactor.run()
sys.exit(1 if failures else 0)
