Using the Blocking Connection with connection recovery with multiple hosts
==========================================================================

.. _example_blocking_basic_consume_recover_multiple_hosts:

RabbitMQ nodes can be `clustered <http://www.rabbitmq.com/clustering.html>`_.
In the absence of failure clients can connect to any node and perform any operation.
In case a node fails, stops, or becomes unavailable, clients should be able to
connect to another node and continue.

To simplify reconnection to a different node, connection recovery mechanism
should be combined with connection configuration that specifies multiple hosts.

The BlockingConnection adapter relies on exception handling to check for
connection errors::

    import pika
    import random

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

    while(True):
        try:
            print("Connecting...")
            ## Shuffle the hosts list before reconnecting.
            ## This can help balance connections.
            random.shuffle(all_endpoints)
            connection = pika.BlockingConnection(all_endpoints)
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            ## This queue is intentionally non-durable. See http://www.rabbitmq.com/ha.html#non-mirrored-queue-behavior-on-node-failure
            ## to learn more.
            channel.queue_declare('recovery-example', durable = False, auto_delete = True)
            channel.basic_consume('recovery-example', on_message)
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
                break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment this to make the example not attempt recovery
            # from server-initiated connection closure, including
            # when the node is stopped cleanly
            #
            # break
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            print("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying...")
            continue

Generic operation retry libraries such as `retry <https://github.com/invl/retry>`_
can prove useful.

To run the following example, install the library first with `pip install retry`.

In this example the `retry` decorator is used to set up recovery with delay::

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
        channel.queue_declare('recovery-example', durable = False, auto_delete = True)
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
            continue

    consume()
