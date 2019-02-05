# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205,W0603

import logging
import pika
from pika import spec

ITERATIONS = 100

logging.basicConfig(level=logging.INFO)

confirmed = 0
errors = 0
published = 0


def on_open(conn):
    conn.channel(on_open_callback=on_channel_open)


def on_channel_open(channel):
    global published
    channel.confirm_delivery(ack_nack_callback=on_delivery_confirmation)
    for _iteration in range(0, ITERATIONS):
        channel.basic_publish(
            'test', 'test.confirm', 'message body value',
            pika.BasicProperties(content_type='text/plain', delivery_mode=1))
        published += 1


def on_delivery_confirmation(frame):
    global confirmed, errors
    if isinstance(frame.method, spec.Basic.Ack):
        confirmed += 1
        logging.info('Received confirmation: %r', frame.method)
    else:
        logging.error('Received negative confirmation: %r', frame.method)
        errors += 1
    if (confirmed + errors) == ITERATIONS:
        logging.info(
            'All confirmations received, published %i, confirmed %i with %i errors',
            published, confirmed, errors)
        connection.close()


parameters = pika.URLParameters(
    'amqp://guest:guest@localhost:5672/%2F?connection_attempts=50')
connection = pika.SelectConnection(
    parameters=parameters, on_open_callback=on_open)

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    connection.close()
    connection.ioloop.start()
