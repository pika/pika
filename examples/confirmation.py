import logging

import pika
from pika import DeliveryMode, spec
from pika.exchange_type import ExchangeType

ITERATIONS = 100
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

confirmed = 0
errors = 0
published = 0


def on_open(conn):
    conn.channel(on_open_callback=on_channel_open)


def on_channel_open(channel):
    # Declare the exchange before publishing to it. Without this the broker
    # answers the first publish with a 404, closes the channel, and no
    # confirmation ever arrives.
    channel.exchange_declare(
        exchange='test',
        exchange_type=ExchangeType.direct,
        callback=lambda _frame: on_exchange_declared(channel))


def on_exchange_declared(channel):
    global published
    channel.confirm_delivery(ack_nack_callback=on_delivery_confirmation)
    for _iteration in range(ITERATIONS):
        channel.basic_publish(
            'test', 'test.confirm', 'message body value',
            pika.BasicProperties(content_type='text/plain',
                                 delivery_mode=DeliveryMode.Transient))
        published += 1


def on_delivery_confirmation(frame):
    global confirmed, errors
    if isinstance(frame.method, spec.Basic.Ack):
        confirmed += 1
        LOGGER.info('Received confirmation: %r', frame.method)
    else:
        LOGGER.error('Received negative confirmation: %r', frame.method)
        errors += 1
    if (confirmed + errors) == ITERATIONS:
        LOGGER.info(
            'All confirmations received, published %i, confirmed %i with %i errors',
            published, confirmed, errors)
        connection.close()


def on_close(_connection, _exception):
    # Invoked once the connection has finished closing. Stopping the IOLoop
    # here is what lets the process exit; closing the connection does not stop
    # it on its own.
    connection.ioloop.stop()


parameters = pika.URLParameters(
    'amqp://guest:guest@localhost:5672/%2F?connection_attempts=50')
connection = pika.SelectConnection(parameters=parameters,
                                   on_open_callback=on_open,
                                   on_close_callback=on_close)

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    # close() raises ConnectionWrongStateError unless the connection is open,
    # and by this point it may have closed itself, so ask first. Note this
    # also skips an interrupt arriving mid-connect, which drops the socket.
    if connection.is_open:
        connection.close()
        connection.ioloop.start()
