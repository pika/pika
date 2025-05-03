# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import random
import pika
from pika.exchange_type import ExchangeType

"""
This module implements a client that connects to multiple RabbitMQ brokers
distributed across different ports (5672, 5673, 5674) and consumes messages
from a shared queue.

The process follows these main steps:

1. It randomly selects one of the available RabbitMQ brokers (on ports 5672, 5673, and 5674)
  to establish a connection.
2. Declares a `direct` type exchange called 'test_exchange' and a queue named 'standard'.
3. The 'standard' queue is then bound to the exchange using the routing key 'standard_key'.
4. Configures the `on_message` function as a callback to process messages received from the queue.
5. Starts consuming messages, processing them, and acknowledges receipt via `basic_ack`.
6. If a connection error occurs, the system will attempt to reconnect automatically, except in cases
  of connection closure by the broker or channel errors.


The name of the module, `blocking_consume_recover_multiple_hosts.py`, reflects its key functionalities:
- "blocking_consume": The client uses a blocking connection to RabbitMQ and consumes messages synchronously.
- "recover": The module is designed to recover from connection errors by attempting to reconnect.
- "multiple_hosts": It connects to multiple RabbitMQ brokers distributed across different ports to ensure availability and redundancy.
"""


def on_message(ch, method_frame, _header_frame, body, userdata=None):
    print('Userdata: {} Message body: {}'.format(userdata, body))
    ch.basic_ack(delivery_tag=method_frame.delivery_tag)


credentials = pika.PlainCredentials('guest', 'guest')

params1 = pika.ConnectionParameters(
    'localhost', port=5672, credentials=credentials)
params2 = pika.ConnectionParameters(
    'localhost', port=5673, credentials=credentials)
params3 = pika.ConnectionParameters(
    'localhost', port=5674, credentials=credentials)
params_all = [params1, params2, params3]

# Infinite loop
while True:
    try:
        random.shuffle(params_all)
        connection = pika.BlockingConnection(params_all)
        channel = connection.channel()
        channel.exchange_declare(
            exchange='test_exchange',
            exchange_type=ExchangeType.direct,
            passive=False,
            durable=True,
            auto_delete=False)
        channel.queue_declare(queue='standard', auto_delete=True)
        channel.queue_bind(
            queue='standard',
            exchange='test_exchange',
            routing_key='standard_key')
        channel.basic_qos(prefetch_count=1)

        on_message_callback = functools.partial(
            on_message, userdata='on_message_userdata')
        channel.basic_consume('standard', on_message_callback)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        connection.close()
        break
    # Do not recover if connection was closed by broker
    except pika.exceptions.ConnectionClosedByBroker:
        break
    # Do not recover on channel errors
    except pika.exceptions.AMQPChannelError:
        break
    # Recover on all other connection errors
    except pika.exceptions.AMQPConnectionError:
        continue
