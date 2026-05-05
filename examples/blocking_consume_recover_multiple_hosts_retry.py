import pika
import random
from retry import retry


def on_message(channel, method_frame, header_frame, body):
    print(method_frame.delivery_tag)
    print(body)
    print()
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


## Assuming there are three hosts: host1, host2, and host3
node1 = pika.URLParameters('amqp://node1')
node2 = pika.URLParameters('amqp://node2')
node3 = pika.URLParameters('amqp://node3')
all_endpoints = [node1, node2, node3]


@retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
def consume():
    random.shuffle(all_endpoints)
    connection = pika.BlockingConnection(all_endpoints)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    ## This queue is intentionally non-durable. See http://www.rabbitmq.com/ha.html#non-mirrored-queue-behavior-on-node-failure
    ## to learn more.
    channel.queue_declare('recovery-example', durable=False, auto_delete=True)
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


consume()
