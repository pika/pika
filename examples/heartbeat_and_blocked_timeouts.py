"""
This example demonstrates explicit setting of heartbeat and blocked connection timeouts.

Starting with RabbitMQ 3.5.5, the broker's default heartbeat timeout decreased from 580 seconds to
60 seconds. As a result, applications that perform lengthy processing in the same thread that also
runs their Pika connection may experience unexpected dropped connections due to heartbeat timeout.
Here, we propose an explicit heartbeat timeout lower than the broker's default.

When RabbitMQ broker is running out of certain resources, such as memory and disk space, it may
block connections that are performing resource-consuming operations, such as publishing messages.
Once a connection is blocked, RabbitMQ stops reading from that connection's socket, so no commands
from the client will get through to the broker on that connection until the broker unblocks it. A
blocked connection may last for an indefinite period of time, stalling the connection and possibly
resulting in a hang (e.g., in BlockingConnection) until the connection is unblocked. Blocked
Connection Timeout is intended to interrupt (i.e., drop) a connection that has been blocked longer
than the given timeout value.
"""

import pika


def main():

    # NOTE: These parameters work with all Pika connection types.
    # Per AMQP 0-9-1 the client may only reduce the broker's proposed
    # heartbeat timeout: with the broker's default of 60 seconds, proposing
    # 30 seconds yields an effective timeout of 30, while a value above the
    # broker's proposal (such as 600) would simply be negotiated down to 60.
    params = pika.ConnectionParameters(heartbeat=30,
                                       blocked_connection_timeout=300)

    conn = pika.BlockingConnection(params)

    chan = conn.channel()

    chan.basic_publish('', 'my-alphabet-queue', b'abc')

    # If publish causes the connection to become blocked, then this conn.close()
    # would hang until the connection is unblocked, if ever. However, the
    # blocked_connection_timeout connection parameter would interrupt the wait,
    # resulting in ConnectionClosed exception from BlockingConnection (or the
    # on_connection_closed callback call in an asynchronous adapter)
    conn.close()


if __name__ == '__main__':
    main()
