Using the Blocking Connection with connection recovery with multiple hosts
==========================================================================

.. _example_blocking_basic_consume_recover_multiple_hosts:

RabbitMQ broker can be clustered and provide multiple hosts to connect to.
In case a node fails, stops, or becomes unavailable, clients should be able to
connect to another node to continue.

To do that, connection recovery mechanism may be combined with multiple
hosts connection configuration.

For BlockingConnection adapter exception handling can be used to check for
connection errors.

The example below shows how it can be done with exception handling recovery::

    import pika
    import random

    def on_message(channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)
        print()
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    ## Assuming there are three hosts: host1, host2, and host3
    params1 = pika.URLParameters('amqp://host1')
    params2 = pika.URLParameters('amqp://host2')
    params3 = pika.URLParameters('amqp://host3')
    params_all = [params1, params2, params3]

    while(True):
        try:
            ## Shuffle the hosts list before reconnecting.
            ## This can help balance connections.
            random.shuffle(params_all)
            connection = pika.BlockingConnection(params_all)
            channel = connection.channel()
            channel.basic_consume('test', on_message)
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


You can use decorators from libraries like `retry <https://github.com/invl/retry>`_
to set up recovery behaviour.

You can install the library with `pip install retry`

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
    params1 = pika.URLParameters('amqp://host1')
    params2 = pika.URLParameters('amqp://host2')
    params3 = pika.URLParameters('amqp://host3')
    params_all = [params1, params2, params3]

    @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
    def consume():
        random.shuffle(params_all)
        connection = pika.BlockingConnection(params_all)
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume('test', on_message)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
            connection.close()
        # Do not recover connections closed by server
        except pika.exceptions.ConnectionClosedByBroker:
            pass

    consume()

