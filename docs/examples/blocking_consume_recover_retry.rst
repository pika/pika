Using the Blocking Connection with connection recovery decorator
================================================================

.. _example_blocking_basic_consume_recover_retry:

Connection recovery is mostly valuable in case of consumers, because they are
long-running tasks doing smaller pieces of work on each message, hence they
can be restarted on errors.

For BlockingConnection adapter exception handling can be used to check for
connection errors.

You can use decorators from libraries like `retry` to set up recovery behaviour.

In this eample the `retry` decorator is used to set up recovery with delay::

    import pika
    from retry import retry


    def on_message(channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)
        print()
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
    def consume():
        connection = pika.BlockingConnection()
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
