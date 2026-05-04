from pika import ConnectionParameters
from pika.adapters import twisted_connection
from pika.credentials import ExternalCredentials

from twisted.internet import defer, protocol, ssl, reactor

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
    conn.ready.addCallback(lambda _ :conn)
    return conn.ready

# Load the CA certificate to validate the server's identity
with open("PIKA_DIR/testdata/certs/ca_certificate.pem") as fd:
    ca_cert = ssl.Certificate.loadPEM(fd.read())

# Load the client certificate and key to authenticate with the server
with open("PIKA_DIR/testdata/certs/client_key.pem") as fd:
    client_key = fd.read()
with open("PIKA_DIR/testdata/certs/client_certificate.pem") as fd:
    client_cert = fd.read()
client_keypair = ssl.PrivateCertificate.loadPEM(client_key + client_cert)

context_factory = ssl.optionsForClientTLS(
    "localhost",
    trustRoot=ca_cert,
    clientCertificate=client_keypair,
)
params = ConnectionParameters(credentials=ExternalCredentials())
cc = protocol.ClientCreator(
    reactor, twisted_connection.TwistedProtocolConnection, params)
deferred = cc.connectSSL("localhost", 5671, context_factory)
deferred.addCallback(connection_ready)
deferred.addCallback(publish)
reactor.run()
