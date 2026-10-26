import random

from retry import retry

import pika


def on_message(channel, method_frame, header_frame, body):
    print(method_frame.delivery_tag)
    print(body)
    print()
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


## Three endpoints to recover across. They are ports on localhost so the
## example runs against a single broker; point them at your own nodes to see
## recovery move between them.
node1 = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
node2 = pika.URLParameters('amqp://guest:guest@localhost:5673/%2F')
node3 = pika.URLParameters('amqp://guest:guest@localhost:5674/%2F')
all_endpoints = [node1, node2, node3]


@retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
def consume():
    random.shuffle(all_endpoints)
    connection = pika.BlockingConnection(all_endpoints)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    ## The queue stays non-durable on purpose, which is the point of the
    ## example: a durable queue whose home node is down stays unavailable until
    ## that node returns, and the redeclare below would fail rather than move to
    ## a surviving endpoint. `exclusive=True` keeps it non-durable while staying
    ## permitted on RabbitMQ 4.3, which refuses transient *non-exclusive*
    ## queues by default. See examples/README.md.
    channel.queue_declare('recovery-example',
                          durable=False,
                          exclusive=True,
                          auto_delete=True)
    channel.basic_consume('recovery-example', on_message)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
    except pika.exceptions.ConnectionClosedByBroker:
        # Uncomment this to make the example not attempt recovery
        # from server-initiated connection closure, including
        # when the node is stopped cleanly
        # except pika.exceptions.ConnectionClosedByBroker:
        #     pass
        pass


try:
    consume()
except KeyboardInterrupt:
    # Ctrl-C during a retry delay, rather than while consuming.
    pass
