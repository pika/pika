#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
publish

This example generates and publishes messages asynchronously using the libev
event loop.

It uses 'publisher confirms' to pace its publishing, always trying to keep a
certain number of them 'in flight' until all messages are published.
Then it stops.

It incorporates the NYTimes nyt⨍aбrik 'rabbit_helpers' framework to handle
the lower level logic.

Each message is represented as a dict.
The 'body' attribute value is mapped to the rabbitmq 'payload' frame.
The other attributes are mapped to the 'metadata' attribute of the
'headers' atribute in the rabbitmq 'properties' frame.
(These mappings are done by 'rabbit_helpers'.)
Metadata attributes that match properties are propagatedto those properties
by 'rabbit_helpers', e.g. 'message_id'.

Be sure to view the results in the rabbitmq management UI.

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

from rabbit_helpers import RabbitConnection, MessagePublish

# play with these numbers and see how the results differ
MESSAGE_COUNT = 10000
CONFIRM_COUNT = 100
BODY_LENGTH = 1000

HASH_RANGE = 4  # the range of the random hash_key generated for each message


class PublishService(object):

    def __init__(self):
        self._rabbit_connection = None
        self._connected = False
        self._stopped = False

        self._publish = None
        self._published_count = 0

        self._start_time = None

        self._app_id = os.path.basename(sys.argv[0]).replace(".py", "")

        self._body = \
            "".join(random.choice(string.letters) for i in xrange(BODY_LENGTH))

    def publish_message(self):
        """
        Create a message and publish it to a queue via the built-in direct
        exchange.

        """
        hash_key = \
            "".join(random.choice(string.digits) for i in xrange(HASH_RANGE))

        message = {
            "message_id": str(uuid.uuid1()),  # UUID version 1
            "hash_key": hash_key,
            "app_id": self._app_id,
            "timestamp": datetime.utcnow().isoformat() + 'Z',  # UTC/isoformat
            "content_type": "text/plain",
            "body": self._body
        }

        # the delivery tag could be used to track messages individually
        delivery_tag = self._publish.put_message(message)

    def wrap_up(self):
        """
        Print some stuff and stop.

        """
        print("MESSAGE_COUNT: {} msgs".format(MESSAGE_COUNT))
        print("CONFIRM_COUNT: {} msgs".format(CONFIRM_COUNT))
        print("BODY_LENGTH: {} bytes".format(BODY_LENGTH))
        elapsed = time() - self._start_time
        print("elapsed time: {} secs".format(elapsed))
        print("msgs per sec: {}".format(MESSAGE_COUNT // elapsed))
        self.stop()  # done

    def handle_messages(self, delivery_tag=0):
        """
        Check to see if we have reached MESSAGE_COUNT and are done.
        Publish enough messages such that CONFIRM_COUNT are outstanding...
        but do not publish more than MESSAGE_COUNT.
        Note that delivery_tags may increase by any integral amount;
        they are not necessarily consecutive integers.
        On the first invocation, publish_count becomes CONFIRM_COUNT.

        """
        if delivery_tag >= MESSAGE_COUNT:
            self.wrap_up()

        # for this cycle, publish CONFIRM_COUNT less outstanding messages
        publish_count = CONFIRM_COUNT - (self._published_count - delivery_tag)

        # but do not exceed MESSAGE_COUNT
        publish_count = min(
            publish_count, MESSAGE_COUNT - self._published_count
        )

        for i in xrange(0, publish_count):
            self.publish_message()

        self._published_count += publish_count

    def init_publish(self):
        """
        Use the RabbitConnection to set up a MessagePublish.

        We are publishing to queue 'data' via the built-in exchange.
        (The queue will be created if necessary.)
        On open, libev will callback to 'handle_messages'.

        Enable 'confirm' for the channel.
        On confirm, libev will callback to 'handle_messages' with a
        delivery_tag.

        """
        self._publish = MessagePublish(
            self._rabbit_connection,
            queue="data",
            on_open_callback=self.handle_messages,  # first invocation
            on_confirm_callback=self.handle_messages
        )

        self._start_time = time()

    def stop(self):
        """
        Called when we are done.

        Also called if provided to RabbitConnection whenever an exception
        is encountered in a callback. Typically, such exceptions are rare
        so just terminate the process.

        """
        if self._stopped:
            return

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

    def halt(self, signal='nada'):
        """
        Usually called only when SIGINT or SIGHUP are received.

        """
        logger.info("Halting service - received {0}".format(signal))
        self.stop()

    def connected(self, connection=None):
        """
        Connected - the connection attempt succeeded.

        """
        logger.info('Connected to rabbit')
        self._connected = True
        self.init_publish()

    def connection(self):
        """
        Connect to rabbitmq.

        Provide a callback for success - failure will abort.
        Provide a callback for SIGINT and SIGHUP.
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
        service = PublishService()
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
