Using the Blocking Connection with connection recovery
======================================================

.. _example_blocking_basic_consume_recover:

Connection recovery is mostly valuable in case of consumers, because they are
long-running tasks doing smaller pieces of work on each message, hence they
can be restarted on errors.

For BlockingConnection adapter exception handling can be used to check for
connection errors.

Example of consuming messages with connection recovery using exception handling::

    import pika

    def on_message(channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)
        print()
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    while(True):
        try:
            connection = pika.BlockingConnection()
            channel = connection.channel()
            channel.basic_consume('test', on_message)
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()
            connection.close()
        # Do not recover if connection was closed by broker
        except pika.exceptions.ConnectionClosedByBroker:
            break
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError:
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            continue

