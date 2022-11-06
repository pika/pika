Direct reply-to example
==============================
The following example demonstrates the use of the RabbitMQ "Direct reply-to" feature via `pika.BlockingConnection`. See https://www.rabbitmq.com/direct-reply-to.html for more info about this feature.

direct_reply_to.py::

    # -*- coding: utf-8 -*-

    """
    This example demonstrates the RabbitMQ "Direct reply-to" usage via
    `pika.BlockingConnection`. See https://www.rabbitmq.com/direct-reply-to.html
    for more info about this feature.
    """
    import pika


    SERVER_QUEUE = 'rpc.server.queue'


    def main():
        """ Here, Client sends "Marco" to RPC Server, and RPC Server replies with
        "Polo".

        NOTE Normally, the server would be running separately from the client, but
        in this very simple example both are running in the same thread and sharing
        connection and channel.

        """
        with pika.BlockingConnection() as conn:
            channel = conn.channel()

            # Set up server

            channel.queue_declare(queue=SERVER_QUEUE,
                                  exclusive=True,
                                  auto_delete=True)
            channel.basic_consume(SERVER_QUEUE, on_server_rx_rpc_request)


            # Set up client

            # NOTE Client must create its consumer and publish RPC requests on the
            # same channel to enable the RabbitMQ broker to make the necessary
            # associations.
            #
            # Also, client must create the consumer *before* starting to publish the
            # RPC requests.
            #
            # Client must create its consumer with auto_ack=True, because the reply-to
            # queue isn't real.

            channel.basic_consume('amq.rabbitmq.reply-to',
                                  on_client_rx_reply_from_server,
                                  auto_ack=True)
            channel.basic_publish(
                exchange='',
                routing_key=SERVER_QUEUE,
                body='Marco',
                properties=pika.BasicProperties(reply_to='amq.rabbitmq.reply-to'))

            channel.start_consuming()


    def on_server_rx_rpc_request(ch, method_frame, properties, body):
        print('RPC Server got request: %s' % body)

        ch.basic_publish('', routing_key=properties.reply_to, body='Polo')

        ch.basic_ack(delivery_tag=method_frame.delivery_tag)

        print('RPC Server says good bye')


    def on_client_rx_reply_from_server(ch, method_frame, properties, body):
        print('RPC Client got reply: %s' % body)

        # NOTE A real client might want to make additional RPC requests, but in this
        # simple example we're closing the channel after getting our first reply
        # to force control to return from channel.start_consuming()
        print('RPC Client says bye')
        ch.close()
