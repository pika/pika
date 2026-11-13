"""Reopen Twisted channels through close notifications against a real broker."""

import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from twisted.internet import defer, endpoints, task

from pika import ConnectionParameters
from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.exceptions import ChannelClosedByBroker, ChannelClosedByClient
from tests.base.async_test_base import enable_tls


@pytest.mark.parametrize('notification', ['client', 'broker', 'rpc_errback'])
def test_reopen_from_close_notification(notification):
    if enable_tls():
        pytest.importorskip('OpenSSL')
    # A stopped Twisted reactor cannot restart; isolate each broker connection.
    result = subprocess.run([
        sys.executable, '-m', 'tests.acceptance.channel_reopen_twisted_test',
        notification
    ],
                            capture_output=True,
                            text=True,
                            cwd=Path(__file__).resolve().parents[2],
                            timeout=30,
                            check=False)
    assert result.returncode == 0, result.stdout + result.stderr


def _endpoint(reactor):
    if not enable_tls():
        return endpoints.TCP4ClientEndpoint(reactor,
                                            '127.0.0.1',
                                            5672,
                                            timeout=5)

    from twisted.internet import ssl

    certs = Path(__file__).resolve().parents[1] / 'certs'
    ca = ssl.Certificate.loadPEM((certs / 'ca_certificate.pem').read_bytes())
    client = ssl.PrivateCertificate.loadPEM(
        (certs / 'client_key.pem').read_bytes() +
        (certs / 'client_certificate.pem').read_bytes())
    context = ssl.optionsForClientTLS('localhost',
                                      trustRoot=ca,
                                      clientCertificate=client)
    return endpoints.SSL4ClientEndpoint(reactor,
                                        'localhost',
                                        5671,
                                        context,
                                        timeout=5)


@defer.inlineCallbacks
def _reopen(reactor, notification):
    connection = TwistedProtocolConnection(ConnectionParameters(),
                                           custom_reactor=reactor)
    ready = connection.ready
    yield endpoints.connectProtocol(_endpoint(reactor), connection)
    yield ready
    try:
        old = yield connection.channel(5)

        def reopen(reason):
            assert old.is_closed
            assert connection.is_open
            expected = (ChannelClosedByClient
                        if notification == 'client' else ChannelClosedByBroker)
            assert isinstance(reason, expected), reason
            return connection.channel(old.channel_number)

        if notification == 'rpc_errback':
            reopened = old.queue_declare(queue='missing-' + uuid.uuid4().hex,
                                         passive=True)
            reopened.addErrback(lambda failure: reopen(failure.value))
        else:
            reopened = old.on_closed.addCallback(reopen)
            if notification == 'client':
                old.close()
            else:
                failed = old.queue_declare(queue='missing-' + uuid.uuid4().hex,
                                           passive=True)
                failed.addErrback(
                    lambda failure: failure.trap(ChannelClosedByBroker))

        replacement = yield reopened
        assert replacement is not old
        assert replacement.channel_number == old.channel_number
        assert replacement.is_open
        assert connection._impl._channels[5] is replacement._channel
        with pytest.raises(ValueError, match='already in use'):
            connection.channel(5)

        declared = yield replacement.queue_declare('', exclusive=True)
        queue = declared.method.queue
        yield replacement.confirm_delivery()
        yield replacement.basic_publish('', queue, b'reopened', mandatory=True)
        message = yield replacement.basic_get(queue, auto_ack=True)
        assert message.body == b'reopened'
        assert message.channel is replacement

        closed = replacement.on_closed
        replacement.close()
        yield closed
        # Cleanup follows the final close notification when nothing reopens.
        yield task.deferLater(reactor, 0, lambda: None)
        assert connection.is_open
        assert not connection._impl._channels
    finally:
        yield connection.close()


if __name__ == '__main__':
    task.react(
        lambda reactor: _reopen(reactor, sys.argv[1]).addTimeout(15, reactor))
