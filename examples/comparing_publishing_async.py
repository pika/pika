import pika
from pika.exchange_type import ExchangeType


# Step #3
def on_open(connection):

    connection.channel(on_open_callback=on_channel_open)


# Step #4
def on_channel_open(channel):

    # Declare the exchange we publish to. Without this the broker answers the
    # publish with a 404 and closes the channel. Everything is a callback here,
    # so the publish waits for the declare to be confirmed.
    channel.exchange_declare(
        exchange='test_exchange',
        exchange_type=ExchangeType.direct,
        callback=lambda _frame: on_exchange_declared(channel))


# Step #5
def on_exchange_declared(channel):

    channel.basic_publish(
        'test_exchange', 'test_routing_key', 'message body value',
        pika.BasicProperties(content_type='text/plain',
                             delivery_mode=pika.DeliveryMode.Transient))

    connection.close()


def on_close(_connection, _exception):

    # Invoked once the connection has finished closing, whether we asked for it
    # or the broker did. Stopping the IOLoop here is what lets the process exit.
    connection.ioloop.stop()


# Step #1: Connect to RabbitMQ
parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')

connection = pika.SelectConnection(parameters=parameters,
                                   on_open_callback=on_open,
                                   on_close_callback=on_close)

try:

    # Step #2 - Block on the IOLoop
    connection.ioloop.start()

# Catch a Keyboard Interrupt to make sure that the connection is closed cleanly
except KeyboardInterrupt:

    # close() raises ConnectionWrongStateError unless the connection is open,
    # and by this point it may have closed itself, so ask first. Note this
    # also skips an interrupt arriving mid-connect, which drops the socket.
    if connection.is_open:
        connection.close()

        # Start the IOLoop again so Pika can communicate. on_close stops it.
        connection.ioloop.start()
