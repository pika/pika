#!/usr/bin/env python
# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
#
# ***** END LICENSE BLOCK *****

'''
Example of simple consumer. Acks each message as it arrives.
'''

import logging
import multiprocessing
import os
import pika
import pika.adapters
import time


logging.basicConfig(level=logging.INFO)

RECEIVERS = 25
SENDERS = 30
MAX_MESSAGES_PER_SEND=100
NO_ACK = False

HOST = 'localhost'

class PikaClient(multiprocessing.Process):

    def close(self):
        logging.info("%i:%s:close" % (self.pid, self.__class__.__name__))
        self.connection.close()

    def run(self):
        logging.info("%i:%s:run" % (self.pid, self.__class__.__name__))
        self.channel = None
        self.connection = None
        self._count = 0
        self._last = 0
        self._last_count = 0
        self.ioloop = pika.adapters.IOLoop.instance()
        self.parameters = pika.ConnectionParameters(HOST)
        self.connection = pika.adapters.SelectConnection(self.parameters,
                                                         self.on_connected)
        self.connection.add_timeout(5, self.report_stats)
        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            self._close()
            self.ioloop.start()

    def _close(self):
        for consumer_tag in self.channel.get_consumer_tags():
            self.channel.basic_cancel(consumer_tag, self.on_basic_cancel)

        self.connection.close()

    def on_basic_cancel(self):
        logging.info("%i:%s:on_basic_cancel" % (self.pid,
                                                self.__class__.__name__))
        self.connection.close()

    def on_connected(self, connection):
        logging.info("%i:%s:on_connected" % (self.pid,
                                             self.__class__.__name__))
        self.channel = self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        logging.info("%i:%s:on_channel_open" % (self.pid,
                                                self.__class__.__name__))
        self.channel.queue_declare(queue="test", durable=True,
                                   exclusive=False, auto_delete=False,
                                   callback=self.on_queue_declared)

    def on_queue_declared(self):
        logging.info("%i:%s:on_queue_declared" % (self.pid,
                                                  self.__class__.__name__))
        self._on_ready()

    def report_stats(self):

        if self._count:
            duration = time.time() - self._last
            sent = self._count - self._last_count
            mps = float(sent) / float(duration)
        else:
            sent = 0
            mps = 0

        self._last = time.time()
        self._last_count = self._count

        logging.info("%i:%s:report_stats: %i messages, %.4f messages per sec" \
                     % (self.pid, self.__class__.__name__, sent, mps))

        self.connection.add_timeout(5, self.report_stats)

    def _on_ready(self):
        pass

class Receiver(PikaClient):

    def _on_ready(self):
        logging.info("%i:%s:_on_ready" % (self.pid, self.__class__.__name__))
        self.channel.basic_consume(self.handle_delivery,
                                   queue='test',
                                   consumer_tag='ctag0',
                                   no_ack=NO_ACK)

    def handle_delivery(self, channel, method, header, body):
        if not NO_ACK:
            channel.basic_ack(delivery_tag=method.delivery_tag)
        self._count += 1

class Sender(PikaClient):

    def _on_ready(self):
        logging.info("%i:%s:_on_ready" % (self.pid, self.__class__.__name__))
        self.connection.add_timeout(0.01, self._send_message)

    def _send_message(self):

        if not self.connection.is_open():
            return

        for x in xrange(0, MAX_MESSAGES_PER_SEND):
            message = "%i:%i:%.8f" % (self.pid, x, time.time())
            self.channel.basic_publish(exchange='',
                                       routing_key="test",
                                       body=message,
                                       properties=pika.BasicProperties(
                                          content_type="text/plain",
                                          delivery_mode=1))
            self.connection.flush_outbound()
            self._count += 1
        self.connection.add_timeout(0.01, self._send_message)


if __name__ == '__main__':

    host = 'localhost'

    processes = []
    for x in xrange(0, RECEIVERS):
        process = Receiver()
        processes.append(process)
        process.start()

    for x in xrange(0, SENDERS):
        process = Sender()
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
