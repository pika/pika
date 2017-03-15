#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
query

This example uses asynchronous RPC to send/receive queries.

it publishes queries at a set interval which you can adjust.

"""

import os
import sys
import traceback
import uuid
import string
import random
from time import time
from datetime import datetime

import logging

LOG_FORMAT = (
    '%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
    '-35s %(lineno) -5d: %(message)s'
)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

from rabbit_helpers import RabbitConnection, MessagePublish, MessageSubscribe

MSG_INTERVAL_MS = 1000

HASH_RANGE = 4  # the range of the random hash_key generated for each query


class QueryService(object):

    def __init__(self):
        self._rabbit_connection = None
        self._connected = False
        self._stopped = False

        self._publish = None
        self._published_count = 0
        self._subscribe = None
        self._correlation_cache = {}

        self._start_time = 0

        self._app_id = os.path.basename(sys.argv[0]).replace(".py", "")

    def handle_response(self, response, delivery_info, properties):
        """
        Match up using correlation_id and print some of the response.

        """
        hash_key = self._correlation_cache.pop(response['correlation_id'])

        print(
            "Rcvd: cid: {correlation_id}; " +
            "hk: {hash_key}; #: {count}; cache: {clen}".format(
                hash_key=hash_key,
                clen=len(self._correlation_cache),
                **response
            )
        )

        self._subscribe.message_handled(delivery_info.delivery_tag)

    def publish_query(self):
        """
        Create a message with a unique id, a random hask_key, and other
        attributes.

        The correlation_id will be used to match the query with the response.

        """
        message_id = str(uuid.uuid1())  # UUID version 1

        hash_key = \
            ''.join(random.choice(string.digits) for i in range(HASH_RANGE))

        query = {
            "message_id": message_id,
            "hash_key": hash_key,
            "app_id": self._app_id,
            "timestamp": datetime.utcnow().isoformat() + 'Z',  # UTC/isoformat
            "reply_to": self._subscribe._queue,
            "correlation_id": message_id
        }

        self._correlation_cache[message_id] = hash_key
        self._publish.put_message(query)
        print("Sent: cid: {correlation_id}; hk: {hash_key}".format(**query))

    def publish_periodic_messages(self):
        """
        Loop until there is an interrupt.

        """
        self.publish_query()
        self._published_count += 1

        self._rabbit_connection.add_timeout(
            MSG_INTERVAL_MS / 1000.0,
            self.publish_periodic_messages
        )

    def init_publish(self):
        """
        Use the RabbitConnection to set up a MessagePublish instance.
        Publish to queue 'queries' via the built-in exchange.
        (The queue will be created if necessary)

        """
        self._publish = MessagePublish(
            self._rabbit_connection,
            queue="queries",
            on_open_callback=self.publish_periodic_messages
        )

        self._start_time = time()

    def init_subscribe(self):
        self._subscribe = MessageSubscribe(
            self._rabbit_connection,
            on_consuming_callback=self.init_publish,
            message_callback=self.handle_response,
            failed_message_action='exception'
        )

    def connected(self, connection=None):
        """
        Connected - the connection attempt succeeded.

        """
        logger.info('Connected to rabbit')
        self._connected = True
        self.init_subscribe()

    def connection(self):
        """
        Provide a callback for success - failure will abort.
        For libev: provide a callback for SIGINT and SIGHUP.
        This is because libev will not propagate them on its own.
        Also provide a stop_method since libev will not propagate
        exceptions through its ioloop, requiring some special handling.

        """
        self._rabbit_connection = RabbitConnection(
            rabbit_connection_callback=self.connected,
            on_signal_callback=self.halt,
            stop_method=self.stop,
            logger_name=logger.name
        )

    def halt(self, signal='nada'):
        """
        Usually called only when SIGINT or SIGHUP are received.

        """
        logger.info("Halting service - received {0}".format(signal))
        self.stop()

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

        print("MSG_INTERVAL_MS: {}".format(MSG_INTERVAL_MS))
        print("self._published_count: {} msgs".format(self._published_count))
        elapsed = time() - self._start_time
        print("elapsed time: {} secs".format(elapsed))

        self._stopped = True
        logger.info("Stopping service")

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

                raise

        logger.info("Stopped")

    def run(self):
        """
        Initiate the connection then start the ioloop.

        """
        logger.info("Starting service")
        self.connection()
        self._rabbit_connection.ioloop_start()
        logger.info("Ending service")


def main(argv=None):
    """
    Start up. Catch interrupts and exceptions.

    """
    if argv is None:
        argv = sys.argv

    logger.info("Initializing...")

    try:
        service = QueryService()
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
