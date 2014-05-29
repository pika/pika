#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
push

This example subscribes to a queue and pushes the messages into Cassandra.

It operates asynchronously both with RabbitMQ and Cassandra.

It uses the prefetch_count to pace its pushes, always up to that many
of them 'in flight' until all messages are pushed.

Message bodies are mapped to Cassandra blobs; metadata to a json string.

Install Cassandra and run setup.cql before running this app.

"""
import sys
import traceback
import json
import uuid
from time import time

import pyev
from cassandra.cluster import Cluster

import logging

LOG_FORMAT = (
    '%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
    '-35s %(lineno) -5d: %(message)s'
)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

from rabbit_helpers import RabbitConnection, MessageSubscribe

PREFETCH_COUNT = 10
IDLE_TIMEOUT_SECS = 5


class CachePushService(object):
    JSON_DUMPS_ARGS = {'ensure_ascii': False}

    def __init__(self):
        self._connected = False
        self._rabbit_connection = None
        self._subscribe = None

        self._cql_cluster = None
        self._cql_session = None
        self._stopped = False

        self._start_time = 0
        self._stop_time = 0
        self._message_count = 0
        self._idle_timeout = None

        self._query = None

        self._stmt = u"""
            UPDATE data
            SET metadata = ?, body = ?
            WHERE hash_key = ? AND message_id = ?
        """

    def query_callback(self, rows, delivery_tag=None):
        """
        A query has completed - acknowledge and wake up RabbitMQ.

        Note: the acknowledgement clears the way for another query to be
        submitted for processing.

        """
        try:
            self._subscribe.message_handled(delivery_tag)

            if IDLE_TIMEOUT_SECS:
                if self._idle_timeout:
                    self._rabbit_connection.remove_timeout(self._idle_timeout)

                self._idle_timeout = self._rabbit_connection.add_timeout(
                    IDLE_TIMEOUT_SECS, self.stop
                )

            # wake up the rabbit in the main thread
            self._rabbit_connection.send_async()
            self._stop_time = time()
            self._message_count += 1
        except Exception as e:
            self.stop_and_raise_exception(e)

    def query_errback(self, e):
        self.stop_and_raise_exception(e)

    def stop_and_raise_exception(self, e):
        """
        Something bad happened - bail out!

        """
        error_msg = 'traceback: {}'.format(traceback.format_exc(e))
        logger.error(error_msg)
        self.stop()
        raise e

    def submit_query(self, message, delivery_tag):
        """
        Set the substitution args and asynchronously submit the query.

        Add callbacks to handle success and failure.

        """
        if not self._start_time:
            self._start_time = time()

        body = message.pop('body')

        substitution_args = (
            json.dumps(message, **self.JSON_DUMPS_ARGS),
            body,
            message['hash_key'],
            uuid.UUID(message['message_id'])
        )

        try:
            future = self._cql_session.execute_async(
                self._query, substitution_args
            )

            future.add_callback(
                self.query_callback, delivery_tag=delivery_tag
            )

            future.add_errback(self.query_errback)
        except Exception as e:
            self.stop_and_raise_exception(e)

    def handle_message(self, message, delivery_info, properties):
        """
        Grab the message and the delivery_tag.

        """
        self.submit_query(message, delivery_info.delivery_tag)

    def init_subscribe(self):
        """
        Subscribe to the data queue setting prefetch_count.

        """
        self._subscribe = MessageSubscribe(
            self._rabbit_connection,
            queue='data',
            prefetch_count=PREFETCH_COUNT,
            message_callback=self.handle_message,
            failed_message_action='raise_exception'
        )

    def stop(self):
        """
        Called when we are done.

        Also called if provided to RabbitConnection whenever an exception
        is encountered in a callback. Typically, such exceptions are rare
        so just terminate the process.

        Monit or supervisord will restart it.

        """
        if self._stopped:
            return

        print("PREFETCH_COUNT: {} msgs".format(PREFETCH_COUNT))
        print("MESSAGE_COUNT: {} msgs".format(self._message_count))

        if self._message_count:
            elapsed = self._stop_time - self._start_time
            print("elapsed time: {} secs".format(elapsed))
            print("msgs per sec: {}".format(self._message_count // elapsed))

        self._stopped = True
        raise_exception = False
        logger.info("Stopping service.")

        if self._connected:
            try:
                self._rabbit_connection.close()
            except Exception as e:
                error_msg = traceback.format_exc(e)

                logger.info(
                    "Exception on rabbit_connection.close(): {0}".format(
                        error_msg
                    )
                )

                raise_exception = True

        if self._cql_cluster:
            try:
                self._cql_cluster.shutdown()
            except Exception as e:
                error_msg = traceback.format_exc(e)

                logger.info(
                    "Exception on cql_cluster.shutdown(): {0}".format(
                        error_msg
                    )
                )

                raise_exception = True

        logger.info("Stopped.")

        if raise_exception:
            raise Exception('Exception encountered shutting down services')

    def halt(self, signal='nada'):
        """
        Usually called only when SIGINT or SIGHUP are received.

        """
        logger.info("Halting service - received {0}".format(signal))
        self.stop()

    def connected(self, connection=None):
        logger.info('Connected to RabbitMQ')
        self._connected = True
        self.init_subscribe()

    def connection(self):
        try:
            self._cql_cluster = Cluster()
            self._cql_session = self._cql_cluster.connect('test')
            self._query = self._cql_session.prepare(self._stmt)
        except Exception as e:
            error_msg = 'Cassandra init error; traceback: {}'.format(
                traceback.format_exc(e)
            )

            logger.error(error_msg)
            self.stop()
            raise e

        logger.info('Connected to Cassandra')

        self._rabbit_connection = RabbitConnection(
            rabbit_connection_callback=self.connected,
            custom_ioloop=pyev.Loop(),
            on_signal_callback=self.halt,
            stop_method=self.stop,
            logger_name=logger.name
        )

    def run(self):
        """
        Initiate the connection then start the ioloop.

        """
        logger.info("Starting service.")
        self.connection()
        self._rabbit_connection.ioloop_start()
        logger.info("Ending service.")


def main(argv=None):
    """
    Start up. Catch interrupts and exceptions.

    """
    if argv is None:
        argv = sys.argv

    logger.info("Initializing...")

    try:
        service = CachePushService()
        service.run()
    except KeyboardInterrupt:
        logger.info("Service terminated by SIGINT.")
    except Exception as e:
        error_msg = traceback.format_exc(e)
        logger.error("Runtime exception: {0}".format(error_msg))

    service.stop()
    logger.info("Halted")

if __name__ == "__main__":
    sys.exit(main())
