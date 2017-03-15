#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A version of the NYTimes nyt⨍aбrik rabbit_helpers utility framework.
This version uses only libev_connection.

"""
import sys
import calendar
import logging
import traceback
from itertools import count
from datetime import datetime

import pika
import pyev


def isots_to_datetime(isotimestamp):
    try:
        datetime_utc = datetime.strptime(
            isotimestamp, '%Y-%m-%dT%H:%M:%S.%fZ'
        )
    except ValueError:
        datetime_utc = datetime.strptime(
            isotimestamp, '%Y-%m-%dT%H:%M:%SZ'
        )

    return datetime_utc


def isots_to_amqpts(isotimestamp):
    datetime_utc = isots_to_datetime(isotimestamp)
    return calendar.timegm(datetime_utc.timetuple())


class RabbitConnection(object):

    """
    Set up the connection to rabbitmq and the glue methods.

    """

    def __init__(
        self,
        virtual_host='/',
        user_id='guest',
        password='guest',
        amqp_host='localhost',
        amqp_port=5672,
        on_signal_callback=None,  # propagate SIGHUP and SIGINT
        custom_ioloop=None,  # mix with other libev modules like Cassandra
        stop_ioloop_on_close=True,  # stop the loop and bail out
        rabbit_connection_callback=None,
        rabbit_connection_error_callback=None,
        stop_method=None,  # graceful shutdown
        logger_name=__name__
    ):
        self._virtual_host = virtual_host
        self._user_id = user_id
        self._password = password
        self._amqp_host = amqp_host
        self._amqp_port = amqp_port
        self._on_signal_callback = on_signal_callback
        self._custom_ioloop = custom_ioloop
        self._stop_ioloop_on_close = stop_ioloop_on_close
        self._rabbit_connection_callback = rabbit_connection_callback
        self._on_open_error_callback = rabbit_connection_error_callback
        self._stop_method = stop_method
        self._logger = logging.getLogger(logger_name)

        self._conn_info = ''
        self._closing = False

        # start here
        self._connection = self._connect()

    def send_async(self):  # wake up ioloop from another thread
        self._logger.debug('')
        self._connection.async.send()

    def add_timeout(
            self,
            deadline,
            callback_method,
            callback_timeout=False,
            **kwargs
    ):
        self._logger.debug('')

        return self._connection.add_timeout(
            deadline,
            callback_method,
            callback_timeout=callback_timeout,
            **kwargs
        )

    def remove_timeout(self, timeout):
        self._logger.debug('')
        self._connection.remove_timeout(timeout)

    def ioloop_start(self):
        self._logger.debug('')
        self._connection.ioloop.start()

    def ioloop_stop(self):
        self._logger.debug('')
        self._connection.ioloop.stop()

    def close(self):
        self._logger.debug('')
        self._closing = True  # stop handling any normal stuff

        # cancel consumers, close channels, disconnect, stop io_loop
        if self._connection:
            self._connection.close()

    def _connection_on_close_callback(
            self,
            connection,
            reply_code,
            reply_text
    ):
        self._logger.debug('')

        self._logger.warning(
            "{0}: connection closed: ({1}) {2}".format(
                self._conn_info, reply_code, reply_text
            )
        )

        self.close()  # shut everything down

    def _connection_on_open_error_callback(self, *args, **kwargs):
        self._logger.debug('')

        if self._on_open_error_callback:
            self._on_open_error_callback(*args, **kwargs)
        else:
            if self._stop_method:
                self._stop_method()
            else:
                self.close()

            raise Exception(*args, **kwargs)

    def _connection_on_open_callback(self, connection):
        self._logger.debug('')

        if self._rabbit_connection_callback:
            try:
                self._rabbit_connection_callback()
            except Exception as e:
                error_msg = traceback.format_exc(e)
                self._logger.error("Exception raised: {}".format(error_msg))

                if self._stop_method:
                    self._stop_method()
                else:
                    self.close()

                raise

    def _connect(self):
        """
        Perform the actual connection.

        """
        self._logger.debug('')
        self._logger.info("Attempting to connect using libev event_loop...")

        self._conn_info = "{0}@{1}:{2}{3}".format(
            self._user_id,
            self._amqp_host,
            int(self._amqp_port),
            self._virtual_host
        )

        parameters = pika.ConnectionParameters(
            virtual_host=self._virtual_host,
            credentials=pika.PlainCredentials(self._user_id, self._password),
            host=self._amqp_host,
            port=self._amqp_port,
            socket_timeout=30.0
        )

        connection_adapter = pika.LibevConnection

        kwargs = {
            'on_open_callback': self._connection_on_open_callback,
            'on_open_error_callback': self._connection_on_open_error_callback,
            'on_close_callback': self._connection_on_close_callback,
            'stop_ioloop_on_close': self._stop_ioloop_on_close,
            'on_signal_callback': self._on_signal_callback,
            'custom_ioloop': self._custom_ioloop
        }

        connection = connection_adapter(parameters, **kwargs)
        return connection


class MessagePublish(object):
    BASIC_PROPERTIES = set([  # string properties only - see put_message
        'content_type',
        'content_encoding'
        'correlation_id'
        'reply_to',
        'message_id',
        'type',
        'app_id'
    ])

    ACK = pika.spec.Basic.Ack

    def __init__(
        self,
        rabbit_connection,
        exchange='',
        queue='',
        queue_args={},
        on_open_callback=None,
        on_close_callback=None,
        on_confirm_callback=None
    ):
        self._logger = rabbit_connection._logger
        self._rabbit_connection = rabbit_connection
        self._user_id = rabbit_connection._user_id
        self._exchange = exchange
        self._queue = queue
        self._queue_args = queue_args
        self._on_open_callback = on_open_callback
        self._on_close_callback = on_close_callback
        self._on_confirm_callback = on_confirm_callback

        self.delivery_tag = count(1)
        self.queue_message_count = 0
        self.queue_consumer_count = 0
        self._channel = None
        self._closing = False

        # start here
        self._rabbit_connection._connection.channel(
            self._channel_on_open_callback)

    def put_message(self, message, routing_key='', **kwargs):
        # kwargs should be valid keys for BasicProperties
        self._logger.debug('')

        if self._rabbit_connection._closing:
            return

        body = message.pop('body', '')

        if body:
            message['content_length'] = len(body)

        basic_properties = {
            bp: str(
                message[bp]
            ) for bp in self.BASIC_PROPERTIES if bp in message
        }

        if 'delivery_mode' in message:
            basic_properties['delivery_mode'] = int(message['delivery_mode'])

        if 'priority' in message:
            basic_properties['priority'] = int(message['priority'])

        if 'timestamp' in message:
            basic_properties['timestamp'] = isots_to_amqpts(
                message['timestamp']
            )

        basic_properties.update(kwargs)

        if not 'headers' in basic_properties:
            basic_properties['headers'] = {}

        basic_properties['headers']['metadata'] = message
        basic_properties['user_id'] = self._user_id
        properties = pika.BasicProperties(**basic_properties)

        if self._queue:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue,
                body=body,
                properties=properties
            )
        else:
            self._channel.basic_publish(
                exchange=self._exchange,
                routing_key=routing_key,
                body=body,
                properties=properties
            )

        return self.delivery_tag.next()

    def close(self):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._closing = True
        self._channel.close()

    def _channel_on_close_callback(self, channel, reply_code, reply_text):
        self._logger.debug('')

        if self._rabbit_connection._closing:
            return

        if self._on_close_callback:
            self._on_close_callback(reply_code, reply_text)
        else:
            if self._closing:
                return

            if self._rabbit_connection._stop_method:
                self._rabbit_connection._stop_method()
            else:
                self._rabbit_connection.close()

            raise Exception(reply_code, reply_text)

    def _queue_declare_ok(self, method_frame):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self.queue_message_count = method_frame.method.message_count
        self.queue_consumer_count = method_frame.method.consumer_count

        if self._on_open_callback:
            self._on_open_callback()

    def _channel_confirm_callback(self, method_frame):
        self._logger.debug('')

        if isinstance(method_frame.method, self.ACK):
            self._on_confirm_callback(
                delivery_tag=method_frame.method.delivery_tag)
        else:
            self._logger.error("Negative publisher confirm")

            if self._rabbit_connection._stop_method:
                self._rabbit_connection._stop_method()
            else:
                self._rabbit_connection.close()

            raise Exception("Negative publisher confirm")

    def _channel_on_open_callback(self, channel):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._channel = channel
        self._channel.add_on_close_callback(self._channel_on_close_callback)

        if self._on_confirm_callback:
            self._channel.confirm_delivery(self._channel_confirm_callback)

        if self._queue:
            self._channel.queue_declare(
                self._queue_declare_ok, self._queue, arguments=self._queue_args
            )
        else:
            if self._on_open_callback:
                self._on_open_callback()


class MessageSubscribe(object):

    def __init__(
        self,
        rabbit_connection,
        queue='',
        exclusive=False,
        exchange='',
        routing_key='',
        prefetch_count=1,
        failed_message_action='reject',
        message_callback=None,
        on_consuming_callback=None,
        on_close_callback=None
    ):
        self._logger = rabbit_connection._logger
        self._rabbit_connection = rabbit_connection
        self._queue = queue
        self._exclusive = exclusive
        self._exchange = exchange
        self._routing_key = routing_key
        self._prefetch_count = prefetch_count
        self._failed_message_action = failed_message_action
        self._message_callback = message_callback
        self._on_consuming_callback = on_consuming_callback
        self._on_close_callback = on_close_callback

        self._channel = None
        self._consumer_tag = None
        self._closing = False
        self._canceling = False

        self.queue_message_count = 0
        self.queue_consumer_count = 0

        # start here
        self._rabbit_connection._connection.channel(
            self._channel_on_open_callback)

    def cancel(self, callback=None):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._canceling = True
        nowait = False if callback else True

        self._channel.basic_cancel(
            callback=callback, consumer_tag=self._consumer_tag, nowait=nowait
        )

    def reconsume(self):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._queue_status()

    def close(self):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._closing = True
        self._channel.close()

    def message_handled(self, delivery_tag):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing or self._canceling:
            return

        self._channel.basic_ack(delivery_tag)

    def message_not_handled(self, delivery_tag):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing or self._canceling:
            return

        self._channel.basic_nack(delivery_tag)

    def message_rejected(self, delivery_tag):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing or self._canceling:
            return

        self._channel.basic_nack(delivery_tag, requeue=False)

    def _handle_message(self, method_frame, header_frame, message):
        self._logger.debug('')

        if self._message_callback:
            self._message_callback(
                message,
                method_frame,
                properties=header_frame
            )
        else:
            self._logger.warning("No callback.")
            self._channel.basic_ack(method_frame.delivery_tag)

    def _build_message(self, method_frame, header_frame, body):
        self._logger.debug('')
        message = header_frame.headers.get('metadata')

        if not message:
            raise Exception('header_frame is missing metadata')

        if body:
            message['body'] = body

        return message

    def _channel_consumer_callback(
            self,
            channel,
            method_frame,
            header_frame,
            body
    ):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing or self._canceling:
            return

        try:
            message = self._build_message(method_frame, header_frame, body)
            self._handle_message(method_frame, header_frame, message)
        except Exception as e:
            error_msg = "Unable to process message: {}".format(
                traceback.format_exc(e)
            )

            if self._failed_message_action is 'nack':
                self._logger.warning("Message nacked: {}".format(error_msg))
                self.message_not_handled(method_frame.delivery_tag)
            elif self._failed_message_action is 'reject':
                self._logger.warning("Message rejected: {}".format(error_msg))
                self.message_rejected(method_frame.delivery_tag)
            elif self._failed_message_action is 'ack':
                self._logger.warning("Message acked: {}".format(error_msg))
                self.message_handled(method_frame.delivery_tag)
            else:
                self._logger.error("Exception raised: {}".format(error_msg))

                if self._rabbit_connection._stop_method:
                    self._rabbit_connection._stop_method()
                else:
                    self._rabbit_connection.close()

                raise

    def _channel_basic_consume(self):
        self._logger.debug('')
        self._canceling = False
        self._delivery_tag_cache = set()

        self._consumer_tag = self._channel.basic_consume(
            self._channel_consumer_callback,
            queue=self._queue,
            exclusive=self._exclusive
        )

        if self._on_consuming_callback:
            self._on_consuming_callback()

    def _queue_status_ok(self, method_frame):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self.queue_message_count = method_frame.method.message_count
        self.queue_consumer_count = method_frame.method.consumer_count
        self._channel_basic_consume()

    def _queue_status(self):
        self._logger.debug('')

        self._channel.queue_declare(
            self._queue_status_ok,
            self._queue,
            passive=True
        )

    def _queue_bind_ok(self, method_frame):
        self._logger.debug('_queue_bind_ok')

        if self._rabbit_connection._closing or self._closing:
            return

        self._queue_status()

    def _queue_declare_ok(self, method_frame):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._queue = method_frame.method.queue

        if self._exchange and self._routing_key:
            self._channel.queue_bind(
                self._queue_bind_ok,
                self._queue,
                self._exchange,
                routing_key=self._routing_key
            )
        else:
            self._queue_status()

    def _channel_on_cancel_callback(self, method_frame):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._logger.warning(
            "Basic cancel from server for queue {0}".format(
                self._queue
            )
        )

        self._canceling = True
        self._queue_status

    def _channel_on_close_callback(self, channel, reply_code, reply_text):
        self._logger.debug('')

        if self._rabbit_connection._closing:
            return

        if self._on_close_callback:
            self._on_close_callback(reply_code, reply_text)
        else:
            if self._closing:
                return

            if self._rabbit_connection._stop_method:
                self._rabbit_connection._stop_method()
            else:
                self._rabbit_connection.close()

            raise Exception(reply_code, reply_text)

    def _channel_basic_qos_ok(self, method_frame):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._channel.add_on_close_callback(self._channel_on_close_callback)
        self._channel.add_on_cancel_callback(self._channel_on_cancel_callback)

        if self._queue:
            self._queue_status()
        else:
            self._channel.queue_declare(
                self._queue_declare_ok,
                '',
                exclusive=True
            )

    def _channel_on_open_callback(self, channel):
        self._logger.debug('')

        if self._rabbit_connection._closing or self._closing:
            return

        self._channel = channel

        self._channel.basic_qos(
            prefetch_count=int(self._prefetch_count),
            callback=self._channel_basic_qos_ok
        )

if __name__ == "__main__":
    sys.exit()
