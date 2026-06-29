"""
Example: publisher confirms with ThreadSafeConnection.

This is the ThreadSafeConnection counterpart to
examples/blocking_delivery_confirmations.py.  Where the BlockingConnection
version calls confirm_delivery() with no arguments and relies on
basic_publish raising UnroutableError synchronously, ThreadSafeChannel is
fully asynchronous: confirm_delivery() takes an ack_nack_callback that the
broker's Basic.Ack / Basic.Nack frames are delivered to.

That callback is dispatched on the channel's per-channel worker thread - the
same thread that delivery callbacks use - not the IOLoop thread.  A slow
confirm handler therefore never stalls heartbeats, and the callback may
safely call any ThreadSafeChannel method.

Each publish is tagged with a monotonically increasing delivery tag, exposed
via the on_publish callback and the next_publish_seq_no property, matching
the behaviour of the RabbitMQ Java and .NET clients.  We track the
outstanding tags and block until the broker has confirmed every one.

Pairs with examples/basic_consumer_threaded.py if you want to consume the
messages afterwards.
"""

import logging
import threading

import pika
from pika import spec
from pika.adapters.thread_safe_connection import ThreadSafeConnection
from pika.exchange_type import ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

EXCHANGE_NAME = 'test_exchange'
QUEUE_NAME = 'standard'
ROUTING_KEY = 'standard_key'
MESSAGE_COUNT = 10


def main():
    parameters = pika.ConnectionParameters(
        credentials=pika.PlainCredentials('guest', 'guest'))

    LOGGER.info('Connecting ...')
    conn = ThreadSafeConnection(parameters)
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE_NAME,
                        exchange_type=ExchangeType.direct,
                        durable=True)
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.queue_bind(queue=QUEUE_NAME,
                  exchange=EXCHANGE_NAME,
                  routing_key=ROUTING_KEY)

    # Outstanding delivery tags awaiting a Basic.Ack / Basic.Nack from the
    # broker.  Touched from two threads - the IOLoop thread (on_publish) and
    # the worker thread (on_confirm) - so guard it with a condition and
    # signal completion once the set drains.
    pending = set()
    all_confirmed = threading.Condition()

    def on_confirm(method_frame):
        # Runs on the per-channel worker thread, NOT the IOLoop thread.
        tag = method_frame.method.delivery_tag
        multiple = method_frame.method.multiple
        acked = isinstance(method_frame.method, spec.Basic.Ack)
        LOGGER.info('Worker thread %s: %s delivery_tag=%s multiple=%s',
                    threading.get_ident(), 'ack' if acked else 'NACK', tag,
                    multiple)
        with all_confirmed:
            if multiple:
                # A multiple ack/nack confirms every outstanding tag up to
                # and including `tag`.
                confirmed = {t for t in pending if t <= tag}
            else:
                confirmed = pending & {tag}
            pending.difference_update(confirmed)
            if not pending:
                all_confirmed.notify_all()

    # confirm_delivery blocks until Confirm.SelectOk arrives and arms the
    # delivery-tag counter (next_publish_seq_no starts at 1).
    ch.confirm_delivery(on_confirm)

    def remember(tag):
        # on_publish runs on the IOLoop thread immediately after the publish
        # frame is written, with the tag the broker will confirm.
        with all_confirmed:
            pending.add(tag)

    LOGGER.info('Publishing %d messages with publisher confirms', MESSAGE_COUNT)
    for i in range(1, MESSAGE_COUNT + 1):
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=f'message {i}'.encode(),
            properties=pika.BasicProperties(
                content_type='text/plain',
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
            on_publish=remember,
        )

    # Block until every publish has been confirmed by the broker.
    with all_confirmed:
        if all_confirmed.wait_for(lambda: not pending, timeout=30):
            LOGGER.info('All %d messages confirmed by the broker',
                        MESSAGE_COUNT)
        else:
            LOGGER.warning('Timed out waiting for confirms; still pending: %s',
                           sorted(pending))

    conn.close()
    LOGGER.info('Done')


if __name__ == '__main__':
    main()
