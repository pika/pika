Asynchronous publisher example
==============================
The following example implements a publisher that will respond to RPC commands sent from RabbitMQ and uses delivery confirmations. It will reconnect if RabbitMQ closes the connection and will shutdown if RabbitMQ closes the channel. While it may look intimidating, each method is very short and represents a individual actions that a publisher can do.

publisher.py::

        import logging
        import pika
        import time

        LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                      '-35s %(lineno) -5d: %(message)s')
        LOGGER = logging.getLogger(__name__)


        class ExamplePublisher(object):
            """This is an example publisher that will handle unexpected interactions
            with RabbitMQ such as channel and connection closures.

            If RabbitMQ closes the connection, it will reopen it. You should
            look at the output, as there are limited reasons why the connection may
            be closed, which usually are tied to permission related issues or
            socket timeouts.

            It uses delivery confirmations and illustrates one way to keep track of
            messages that have been sent and if they've been confirmed by RabbitMQ.

            """
            EXCHANGE = 'message'
            EXCHANGE_TYPE = 'topic'
            PUBLISH_INTERVAL = 1
            QUEUE = 'text'
            ROUTING_KEY = 'example.text'

            def __init__(self, amqp_url):
                """Setup the example publisher object, passing in the URL we will use
                to connect to RabbitMQ.

                :param str amqp_url: The URL for connecting to RabbitMQ

                """
                self._connection = None
                self._channel = None
                self._deliveries = []
                self._acked = 0
                self._nacked = 0
                self._message_number = 0
                self._stopping = False
                self._url = amqp_url

            def connect(self):
                """This method connects to RabbitMQ, returning the connection handle.
                When the connection is established, the on_connection_open method
                will be invoked by pika.

                :rtype: pika.SelectConnection

                """
                LOGGER.info('Connecting to %s', self._url)
                return pika.SelectConnection(pika.URLParameters(self._url),
                                             self.on_connection_open)

            def close_connection(self):
                """This method closes the connection to RabbitMQ."""
                LOGGER.info('Closing connection')
                self._connection.close()

            def add_on_connection_close_callback(self):
                """This method adds an on close callback that will be invoked by pika
                when RabbitMQ closes the connection to the publisher unexpectedly.

                """
                LOGGER.info('Adding connection close callback')
                self._connection.add_on_close_callback(self.on_connection_closed)

            def on_connection_closed(self, method_frame):
                """This method is invoked by pika when the connection to RabbitMQ is
                closed unexpectedly. Since it is unexpected, we will reconnect to
                RabbitMQ if it disconnects.

                :param pika.frame.Method method_frame: The method frame from RabbitMQ

                """
                LOGGER.warning('Server closed connection, reopening: (%s) %s',
                               method_frame.method.reply_code,
                               method_frame.method.reply_text)
                self._channel = None
                self._connection = self.connect()

            def on_connection_open(self, unused_connection):
                """This method is called by pika once the connection to RabbitMQ has
                been established. It passes the handle to the connection object in
                case we need it, but in this case, we'll just mark it unused.

                :type unused_connection: pika.SelectConnection

                """
                LOGGER.info('Connection opened')
                self.add_on_connection_close_callback()
                self.open_channel()

            def add_on_channel_close_callback(self):
                """This method tells pika to call the on_channel_closed method if
                RabbitMQ unexpectedly closes the channel.

                """
                LOGGER.info('Adding channel close callback')
                self._channel.add_on_close_callback(self.on_channel_closed)

            def on_channel_closed(self, method_frame):
                """Invoked by pika when RabbitMQ unexpectedly closes the channel.
                Channels are usually closed if you attempt to do something that
                violates the protocol, such as redeclare an exchange or queue with
                different paramters. In this case, we'll close the connection
                to shutdown the object.

                :param pika.frame.Method method_frame: The Channel.Close method frame

                """
                LOGGER.warning('Channel was closed: (%s) %s',
                               method_frame.method.reply_code,
                               method_frame.method.reply_text)
                self._connection.close()

            def on_channel_open(self, channel):
                """This method is invoked by pika when the channel has been opened.
                The channel object is passed in so we can make use of it.

                Since the channel is now open, we'll declare the exchange to use.

                :param pika.channel.Channel channel: The channel object

                """
                LOGGER.info('Channel opened')
                self._channel = channel
                self.add_on_channel_close_callback()
                self.setup_exchange(self.EXCHANGE)

            def setup_exchange(self, exchange_name):
                """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
                command. When it is complete, the on_exchange_declareok method will
                be invoked by pika.

                :param str|unicode exchange_name: The name of the exchange to declare

                """
                LOGGER.info('Declaring exchange %s', exchange_name)
                self._channel.exchange_declare(self.on_exchange_declareok,
                                               exchange_name,
                                               self.EXCHANGE_TYPE)

            def on_exchange_declareok(self, unused_frame):
                """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
                command.

                :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

                """
                LOGGER.info('Exchange declared')
                self.setup_queue(self.QUEUE)

            def setup_queue(self, queue_name):
                """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
                command. When it is complete, the on_queue_declareok method will
                be invoked by pika.

                :param str|unicode queue_name: The name of the queue to declare.

                """
                LOGGER.info('Declaring queue %s', queue_name)
                self._channel.queue_declare(self.on_queue_declareok, queue_name)

            def on_queue_declareok(self, method_frame):
                """Method invoked by pika when the Queue.Declare RPC call made in
                setup_queue has completed. In this method we will bind the queue
                and exchange together with the routing key by issuing the Queue.Bind
                RPC command. When this command is complete, the on_bindok method will
                be invoked by pika.

                :param pika.frame.Method method_frame: The Queue.DeclareOk frame

                """
                LOGGER.info('Binding %s to %s with %s',
                            self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
                self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                         self.EXCHANGE, self.ROUTING_KEY)

            def on_delivery_confirmation(self, method_frame):
                """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
                command, passing in either a Basic.Ack or Basic.Nack frame with
                the delivery tag of the message that was published. The delivery tag
                is an integer counter indicating the message number that was sent
                on the channel via Basic.Publish. Here we're just doing house keeping
                to keep track of stats and remove message numbers that we expect
                a delivery confirmation of from the list used to keep track of messages
                that are pending confirmation.

                :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

                """
                confirmation_type = method_frame.method.NAME.split('.')[1].lower()
                LOGGER.info('Received %s for delivery tag: %i',
                            confirmation_type,
                            method_frame.method.delivery_tag)
                if confirmation_type == 'ack':
                    self._acked += 1
                elif confirmation_type == 'nack':
                    self._nacked += 1
                self._deliveries.remove(method_frame.method.delivery_tag)
                LOGGER.info('Published %i messages, %i have yet to be confirmed, '
                            '%i were acked and %i were nacked',
                            self._message_number, len(self._deliveries),
                            self._acked, self._nacked)

            def enable_delivery_confirmations(self):
                """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
                confirmations on the channel. The only way to turn this off is to close
                the channel and create a new one.

                When the message is confirmed from RabbitMQ, the
                on_delivery_confirmation method will be invoked passing in a Basic.Ack
                or Basic.Nack method from RabbitMQ that will indicate which messages it
                is confirming or rejecting.

                """
                LOGGER.info('Issuing Confirm.Select RPC command')
                self._channel.confirm_delivery(self.on_delivery_confirmation)

            def publish_message(self):
                """If the class is not stopping, publish a message to RabbitMQ,
                appending a list of deliveries with the message number that was sent.
                This list will be used to check for delivery confirmations in the
                on_delivery_confirmations method.

                Once the message has been sent, schedule another message to be sent.
                The main reason I put scheduling in was just so you can get a good idea
                of how the process is flowing by slowing down and speeding up the
                delivery intervals by changing the PUBLISH_INTERVAL constant in the
                class.

                """
                if self._stopping:
                    return

                message = 'The current epoch value is %i' % time.time()
                properties = pika.BasicProperties(app_id='example-publisher',
                                                  content_type='text/plain')

                self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                            message, properties)
                self._message_number += 1
                self._deliveries.append(self._message_number)
                LOGGER.info('Published message # %i', self._message_number)
                self.schedule_next_message()

            def schedule_next_message(self):
                """If we are not closing our connection to RabbitMQ, schedule another
                message to be delivered in PUBLISH_INTERVAL seconds.

                """
                if self._stopping:
                    return
                LOGGER.info('Scheduling next message for %0.1f seconds',
                            self.PUBLISH_INTERVAL)
                self._connection.add_timeout(self.PUBLISH_INTERVAL,
                                             self.publish_message)

            def start_publishing(self):
                """This method will enable delivery confirmations and schedule the
                first message to be sent to RabbitMQ

                """
                LOGGER.info('Issuing consumer related RPC commands')
                self.enable_delivery_confirmations()
                self.schedule_next_message()

            def on_bindok(self, unused_frame):
                """This method is invoked by pika when it receives the Queue.BindOk
                response from RabbitMQ. Since we know we're now setup and bound, it's
                time to start publishing."""
                LOGGER.info('Queue bound')
                self.start_publishing()

            def close_channel(self):
                """Invoke this command to close the channel with RabbitMQ by sending
                the Channel.Close RPC command.

                """
                LOGGER.info('Closing the channel')
                self._channel.close()

            def open_channel(self):
                """This method will open a new channel with RabbitMQ by issuing the
                Channel.Open RPC command. When RabbitMQ confirms the channel is open
                by sending the Channel.OpenOK RPC reply, the on_channel_open method
                will be invoked.

                """
                LOGGER.info('Creating a new channel')
                self._connection.channel(on_open_callback=self.on_channel_open)

            def run(self):
                """Run the example code by connecting and then starting the IOLoop.

                """
                self._connection = self.connect()
                self._connection.ioloop.start()

            def stop(self):
                """Stop the example by closing the channel and connection. We
                set a flag here so that we stop scheduling new messages to be
                published. The IOLoop is started because this method is
                invoked by the Try/Catch below when KeyboardInterrupt is caught.
                Starting the IOLoop again will allow the publisher to cleanly
                disconnect from RabbitMQ.

                """
                LOGGER.info('Stopping')
                self._stopping = True
                self.close_channel()
                self.close_connection()
                self._connection.ioloop.start()

        def main():
            logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

            # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
            example = ExamplePublisher('amqp://guest:guest@localhost:5672/%2F')
            try:
                example.run()
            except KeyboardInterrupt:
                example.stop()

        if __name__ == '__main__':
            main()
