import pika
from pika.exchange_type import ExchangeType


# Step #3
def on_open(connection):
    connection.channel(on_open_callback=on_channel_open)


# Step #4
def on_channel_open(channel):
    # Declare the exchange before publishing to it. Without this the broker
    # answers the publish with a 404 and closes the channel.
    channel.exchange_declare(
        exchange='exchange_name',
        exchange_type=ExchangeType.direct,
        callback=lambda _frame: on_exchange_declared(channel))


# Step #5
def on_exchange_declared(channel):
    channel.basic_publish(
        'exchange_name', 'routing_key', 'Test Message',
        pika.BasicProperties(content_type='text/plain', type='example'))

    # Nothing left to do, so close and let on_close stop the IOLoop.
    connection.close()


def on_close(connection, exception):
    # Invoked when the connection is closed
    connection.ioloop.stop()


# Step #1: Connect to RabbitMQ
connection = pika.SelectConnection(on_open_callback=on_open,
                                   on_close_callback=on_close)

try:
    # Step #2 - Start and block on the IOLoop
    connection.ioloop.start()

# Catch a Keyboard Interrupt to make sure that the connection is closed cleanly
except KeyboardInterrupt:
    # close() raises ConnectionWrongStateError unless the connection is open,
    # and by this point it may have closed itself, so ask first. Note this
    # also skips an interrupt arriving mid-connect, which drops the socket.
    if connection.is_open:
        connection.close()
        # Start the IOLoop again so Pika can communicate,
        # it will stop when on_close is called
        connection.ioloop.start()
