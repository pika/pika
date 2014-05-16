#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import traceback
import json
import uuid
from time import time
from datetime import datetime

import pyev
import cassandra
from cassandra.cluster import Cluster

import logging

LOG_FORMAT = (
    '%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
    '-35s %(lineno) -5d: %(message)s'
)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

from rabbit_helpers import RabbitConnection, MessageSubscribe, MessagePublish

PREFETCH_COUNT = 10


class CachePushService(object):
    JSON_DUMPS_ARGS = {'ensure_ascii': False}

    def __init__(self):
        self._connected = False
        self._rabbit_connection = None
        self._subscribe = None
        self._publish = None
        self._journal = None

        self._cql_cluster = None
        self._cql_session = None
        self._stopped = False

        self._app_id = os.path.basename(sys.argv[0]).replace(".py", "")

        self._query = None

        self._stmt = u"""
            SELECT metadata, body
            FROM data
            WHERE hash_key = ?
            LIMIT 10000
        """

    def send_response(self, rows, query):
        """
        Make up the response and send it.

        Create an array of messages and marshal as json.

        Route to the caller and to the journal with one 'put'.

        """
        body = []
        for row in rows:
            message = json.loads(row.metadata)
            message['body'] = row.body
            body.append(message)

        count = len(body)
        body_json = json.dumps(body, self.JSON_DUMPS_ARGS)

        response = {
            "correlation_id": query['correlation_id'],
            "count": count,
            "app_id": self._app_id,
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "content_type": "application/json",
            "body": body_json
        }

        # route back to the caller AND to the journal
        self._publish.put_message(
            response,
            routing_key=query['reply_to'],
            headers={"CC": ["journal"]}
        )

    def query_callback(
        self, rows, message=None, delivery_tag=None, start_time=0
    ):
        """
        Handle the response and print some stuff.

        """
        try:
            self.send_response(rows, message)
            self._subscribe.message_handled(delivery_tag)
            self._rabbit_connection.send_async()
            elapsed_ms = (time() - start_time) * 1000.0

            print(
                "reply_to: {reply_to}; elapsed_ms:{elapsed_ms:7.1f}".format(
                    elapsed_ms=elapsed_ms, **message
                )
            )
        except Exception as e:
            self.stop_and_raise_exception(e)

    def query_errback(self, e):
        self.stop_and_raise_exception(e)

    def stop_and_raise_exception(self, e):
        """
        Bail out.

        """
        error_msg = 'traceback: {}'.format(traceback.format_exc(e))
        logger.error(error_msg)
        self.stop()
        raise e

    def submit_query(self, message, delivery_tag):
        """
        Submit the query asynchronously, substituting the hash_key.

        Set callbacks.

        """
        substitution_args = (message['hash_key'],) # tuple

        try:
            future = self._cql_session.execute_async(
                self._query, substitution_args
            )

            future.add_callback(
                self.query_callback,
                message=message,
                delivery_tag=delivery_tag,
                start_time=time()
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
        Get queries from the queue setting prefetch_count.

        """
        self._subscribe = MessageSubscribe(
            self._rabbit_connection,
            queue='queries',
            prefetch_count=PREFETCH_COUNT,
            message_callback=self.handle_message,
            failed_message_action='exception'
        )

    def cleanup_journal(self):
        """
        The journal queue is ready so we can close this publisher.

        """
        self._journal.close()
        self.init_subscribe()

    def create_journal(self):
        """
        Create a journal queue with a limited length.

        """
        self._journal = MessagePublish(
            self._rabbit_connection,
            queue="journal",
            queue_args={"x-max-length": 100},
            on_open_callback=self.cleanup_journal
        )

    def init_publish(self):
        """
        Get a generic publisher so we can publish directly to the queue
        dynamically requested in the query's 'reply_to' property.

        """
        self._publish = MessagePublish(
            self._rabbit_connection,
            on_open_callback=self.create_journal
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
            raise

        logger.info('Connected to Cassandra')

        self._rabbit_connection = RabbitConnection(
            rabbit_connection_callback=self.connected,
            custom_ioloop=pyev.Loop(),
            on_signal_callback=self.halt,
            stop_method=self.stop,
            logger_name=logger.name
        )

    def connected(self, connection=None):
        logger.info('Connected to RabbitMQ')
        self._connected = True
        self.init_publish()

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
