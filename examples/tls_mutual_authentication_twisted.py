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
    print("published")


def connection_ready(conn):
    conn.ready.addCallback(lambda _: conn)
    return conn.ready


_certs = Path('PIKA_DIR/testdata/certs')
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
    "localhost",
    trustRoot=ca_cert,
    clientCertificate=client_keypair,
)
params = ConnectionParameters(credentials=ExternalCredentials())
cc = protocol.ClientCreator(reactor,
                            twisted_connection.TwistedProtocolConnection,
                            params)
deferred = cc.connectSSL("localhost", 5671, context_factory)
deferred.addCallback(connection_ready)
deferred.addCallback(publish)
reactor.run()
