"""
Post-release smoke test for the published pika wheel.

Run against a live broker after installing pika from PyPI to prove the distributed package works.
Exits 0 on success, non-zero on any failure.

The `pika.Connection` check is deliberately not a minimal one, because what packaging or a Python
version is most likely to break is what makes this connection different from the old one:

- **Separate connections for publishing and consuming**, which is the documented practice: a
  blocked publisher must not stall the consumer's connection.
- **Several threads publishing on one channel at once**, which is the point of the class.
- **Publisher confirms handled rather than merely enabled**: every delivery tag is tracked until
  the broker accounts for it, `multiple` is honoured, and a `Basic.Nack` fails the run.
- **Events and semaphores rather than sleeps**, so the test runs at the broker's pace and a failure
  reports what never arrived instead of what a sleep happened to miss.

`BlockingConnection` is deprecated but still shipped, so it is checked too, minimally, and its
`DeprecationWarning` is filtered so the output says what failed rather than burying it.

This lives under .ci/ rather than examples/ or tests/ so it is not picked up by pytest, yapf, or
ruff globs, and so it exercises the *installed* package rather than the source tree.

Usage:
    python .ci/smoke_test.py [amqp_url]

The broker URL is taken from the first argument, then the PIKA_SMOKE_AMQP_URL environment
variable, then a localhost default.
"""

import os
import sys
import threading
import warnings

import pika

DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2F'

#: One queue per check. The `pika.Connection` queue is shared by two
#: connections, so it cannot be exclusive, and RabbitMQ 4.3 and later refuse a
#: queue that is neither durable nor exclusive; hence durable plus an explicit
#: delete. The `BlockingConnection` queue serves one connection and can be
#: exclusive, which cleans itself up.
CONNECTION_QUEUE = 'pika-smoke-test-connection'
BLOCKING_QUEUE = 'pika-smoke-test-blocking'

BODY = b'pika smoke test'

#: Publishers, and messages each one publishes. Small enough to stay quick,
#: more than one thread so the channel is genuinely shared.
PUBLISHER_THREADS = 4
PER_THREAD = 25
TOTAL = PUBLISHER_THREADS * PER_THREAD

#: Seconds to wait for the broker. Generous: this bounds a failure rather than
#: describing a pace the test expects to approach.
TIMEOUT = 30.0


class _Confirms:
    """
    Tracks published delivery tags until the broker accounts for each one.

    `basic_publish`'s `on_publish` runs on the IOLoop thread as the frame is written, and the
    confirm listener runs on the channel's worker thread, so every field here is touched from more
    than one thread and guarded by the one lock.
    """

    def __init__(self, expected: int) -> None:
        """
        :param expected: How many publishes to account for.
        """
        self._expected = expected
        self._lock = threading.Lock()
        self._outstanding: set = set()
        self._published = 0
        self._confirmed = 0
        self._nacked: list = []
        self.done = threading.Event()

    def on_publish(self, delivery_tag: int) -> None:
        """
        Record a tag the broker owes a confirm for.

        :param delivery_tag: Tag the broker assigned to this publish.
        """
        with self._lock:
            self._outstanding.add(delivery_tag)
            self._published += 1
            self._signal_if_settled()

    def on_confirm(self, method_frame) -> None:
        """
        Retire the tags a Basic.Ack covers, or record a Basic.Nack.

        :param method_frame: One Basic.Ack or Basic.Nack from the broker.
        """
        method = method_frame.method
        tag = method.delivery_tag
        with self._lock:
            # `multiple` covers every earlier tag too, so retiring only `tag`
            # would leave this waiting for confirms already given.
            covered = ({t for t in self._outstanding if t <= tag}
                       if method.multiple else {tag} & self._outstanding)
            self._outstanding -= covered
            if isinstance(method, pika.spec.Basic.Nack):
                self._nacked.extend(sorted(covered))
            else:
                self._confirmed += len(covered)
            self._signal_if_settled()

    def _signal_if_settled(self) -> None:
        """
        Signal once every publish is accounted for.

        The caller holds the lock.
        """
        if self._published >= self._expected and not self._outstanding:
            self.done.set()

    def failure(self) -> str:
        """
        Describe the shortfall, or the empty string when every publish was acknowledged.

        :returns: A message naming what is missing.
        """
        with self._lock:
            if self._nacked:
                return (f'broker nacked {len(self._nacked)} publish(es), tags '
                        f'{self._nacked[:5]}')
            if self._published < self._expected:
                return (f'only {self._published} of {self._expected} publishes '
                        f'reached the broker')
            if self._outstanding:
                return (f'{len(self._outstanding)} publish(es) were never '
                        f'confirmed, tags {sorted(self._outstanding)[:5]}')
            if self._confirmed != self._expected:
                return (f'confirmed {self._confirmed}, expected '
                        f'{self._expected}')
        return ''


def _ready_count(url: str, queue: str) -> int:
    """
    Report how many messages the broker holds ready, over a connection of its own.

    :param url: Broker URL.
    :param queue: Queue to measure.
    :returns: The queue's message count.
    """
    connection = pika.Connection(pika.URLParameters(url))
    try:
        frame = connection.channel().queue_declare(queue=queue,
                                                   durable=True,
                                                   passive=True)
        count = frame.method.message_count
        # Optional in `pika.spec`, which is annotated as of 1.5.0; the broker
        # always sets it on a Queue.DeclareOk.
        assert count is not None
        return count
    finally:
        connection.close(timeout=TIMEOUT)


def _delete_queue(url: str, queue: str) -> None:
    """
    Remove a queue over a connection of its own.

    The publishing and consuming connections may be closing or already gone by the time this runs,
    and a cleanup failure must not mask the result it is cleaning up after.

    :param url: Broker URL.
    :param queue: Queue to delete.
    """
    try:
        connection = pika.Connection(pika.URLParameters(url))
        try:
            connection.channel().queue_delete(queue=queue)
        finally:
            connection.close(timeout=TIMEOUT)
    except Exception as exc:
        print(f'warning: could not delete {queue}: {exc!r}')


def _check_connection(url: str) -> int:
    """
    Publish from several threads with confirms, and consume on a second connection.

    :param url: Broker URL.
    :returns: 0 on success.
    """
    publisher = pika.Connection(pika.URLParameters(url))
    consumer = pika.Connection(pika.URLParameters(url))
    consumed = threading.Semaphore(0)
    bodies: list = []
    bodies_lock = threading.Lock()
    try:
        pub_ch = publisher.channel()
        pub_ch.queue_declare(queue=CONNECTION_QUEUE, durable=True)
        pub_ch.queue_purge(queue=CONNECTION_QUEUE)

        confirms = _Confirms(TOTAL)
        pub_ch.confirm_delivery(confirms.on_confirm)

        def publish_batch() -> None:
            """Publish this thread's share on the channel every thread shares."""
            for _ in range(PER_THREAD):
                pub_ch.basic_publish(exchange='',
                                     routing_key=CONNECTION_QUEUE,
                                     body=BODY,
                                     on_publish=confirms.on_publish)

        threads = [
            threading.Thread(target=publish_batch, name=f'smoke-pub-{i}')
            for i in range(PUBLISHER_THREADS)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=TIMEOUT)
            if thread.is_alive():
                print(f'smoke test FAILED: {thread.name} did not finish '
                      f'publishing within {TIMEOUT:g}s')
                return 1

        if not confirms.done.wait(timeout=TIMEOUT):
            print(f'smoke test FAILED: {confirms.failure()}')
            return 1
        shortfall = confirms.failure()
        if shortfall:
            print(f'smoke test FAILED: {shortfall}')
            return 1

        con_ch = consumer.channel()

        def on_message(channel, method, _properties, body) -> None:
            """Acknowledge on the consumer's own connection and release the waiter."""
            with bodies_lock:
                bodies.append(body)
            channel.basic_ack(method.delivery_tag)
            consumed.release()

        con_ch.basic_consume(queue=CONNECTION_QUEUE,
                             on_message_callback=on_message)
        for i in range(TOTAL):
            if not consumed.acquire(timeout=TIMEOUT):
                print(f'smoke test FAILED: {i} of {TOTAL} messages consumed '
                      f'within {TIMEOUT:g}s')
                return 1

        with bodies_lock:
            wrong = [body for body in bodies if body != BODY]
        if wrong:
            print(f'smoke test FAILED: {len(wrong)} message(s) had an '
                  f'unexpected body, first {wrong[0]!r}')
            return 1

        # Closing the consumer is what makes the acknowledgements checkable.
        # An unacknowledged message is not counted by `message_count`, so a
        # consumer that received everything and acknowledged nothing looks
        # identical here until its connection goes and the broker requeues.
        consumer.close(timeout=TIMEOUT)
        remaining = _ready_count(url, CONNECTION_QUEUE)
        if remaining != 0:
            print(f'smoke test FAILED: {remaining} message(s) returned to the '
                  f'queue, so they were consumed but never acknowledged')
            return 1
    finally:
        for connection in (publisher, consumer):
            if connection.is_open:
                connection.close(timeout=TIMEOUT)
        # Not exclusive, so it outlives both connections above.
        _delete_queue(url, CONNECTION_QUEUE)

    print(f'  Connection: ok ({TOTAL} published from {PUBLISHER_THREADS} '
          f'threads, all confirmed, all consumed and acknowledged)')
    return 0


def _check_blocking_connection(url: str) -> int:
    """
    Publish and read back over the deprecated `BlockingConnection`.

    :param url: Broker URL.
    :returns: 0 on success.
    """
    with warnings.catch_warnings():
        # Deprecated since 1.5.0 and still shipped, so still worth proving
        # installable; the notice is not news here.
        warnings.filterwarnings('ignore',
                                message='.*will be removed in Pika 2\\.0',
                                category=DeprecationWarning)
        connection = pika.BlockingConnection(pika.URLParameters(url))
    try:
        channel = connection.channel()
        # Exclusive so the queue is auto-deleted when this connection closes
        # (no cleanup, no leftover state between repeated test runs) and to
        # avoid the transient non-exclusive queue combination, which modern
        # RabbitMQ rejects by default.
        channel.queue_declare(queue=BLOCKING_QUEUE, exclusive=True)
        channel.basic_publish(exchange='',
                              routing_key=BLOCKING_QUEUE,
                              body=BODY)

        method, _properties, body = channel.basic_get(queue=BLOCKING_QUEUE,
                                                      auto_ack=True)
        if method is None:
            print('smoke test FAILED: BlockingConnection basic_get returned '
                  'no message')
            return 1
        if body != BODY:
            print(f'smoke test FAILED: BlockingConnection expected {BODY!r}, '
                  f'got {body!r}')
            return 1
    finally:
        # The broker may have already closed the connection (e.g. on a
        # rejected declare); closing an already-closed connection raises and
        # would mask the original error.
        if connection.is_open:
            connection.close()
    print('  BlockingConnection: ok')
    return 0


def main() -> int:
    """
    Check both shipped connections against a live broker.

    :returns: Process exit status.
    """
    url = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
        'PIKA_SMOKE_AMQP_URL', DEFAULT_URL))
    print(f'pika {pika.__version__}: connecting to {url}')

    for check in (_check_connection, _check_blocking_connection):
        status = check(url)
        if status:
            return status

    print('smoke test PASSED')
    return 0


if __name__ == '__main__':
    sys.exit(main())
