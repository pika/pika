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
App that uses multiprocessing to send and receive messages across multiple
processes, keeping track of how many messages per second it is sending
per process.

PikaClient is extended by both the Sender and the Receiver.

Call --help to change the default behaviors
'''

import logging
import multiprocessing
import optparse
import pika
import pika.adapters
import signal
import time

LOG_FORMAT = "%(levelname)s:%(process)d:%(module)s:%(message)s"
QUEUE_NAME = 'speed-test'

# Defaults
DURATION = 120
NO_ACK = False
MAX_MESSAGES_PER_INTERVAL = 250
RECEIVERS = 25
REPORT_INTERVAL = 20
SENDERS = 25


class PikaClient(multiprocessing.Process):

    def __init__(self, adapter, host, port, max_messages_per_interval,
                 no_ack, report_interval, throughput_data, process_ready,
                 parent_ready, select_poller, *args, **kwargs):

        multiprocessing.Process.__init__(self, *args, **kwargs)

        self.adapter = adapter
        self.channel = None
        self.connection = None
        self.host = host
        self.port = port
        self.max_messages_per_interval = max_messages_per_interval
        self.no_ack = no_ack
        self.report_interval = report_interval
        self.open = True
        self.parent_ready = parent_ready
        self.select_poller = select_poller

        # Counter data
        self._count = 0
        self._last_count = 0
        self._last = 0
        self.start_time = 0

        # Multiprocessing Queue
        self.throughput_data = throughput_data

        # Multiprocessing Manager List, append when were ready to start
        self.process_ready = process_ready

        # Set sigterm to call our close
        signal.signal(signal.SIGTERM, self.stop)

    def run(self):
        logging.debug("%i:%s:run" % (self.pid, self.__class__.__name__))

        # If we asked for a specific poller and we're using Select, set it
        if self.adapter.__module__ == 'pika.adapters.select_connection' and \
           self.select_poller:
            pika.adapters.select_connection.SELECT_TYPE = self.select_poller

        # Set our connection parameters
        self.parameters = pika.ConnectionParameters(self.host, self.port)

        # Create our connection
        self.connection = self.adapter(self.parameters, self.on_connected)

        # Loop while we run our tests
        self.connection.ioloop.start()

    def stop(self, signalnum, frame):
        logging.debug("%i:%s:_close" % (self.pid, self.__class__.__name__))

        # Don't let our functions do anything else
        self.open = False

        # Append the data to the queue
        throughput_data.put({self.name:
                                {'count': self._count,
                                 'duration': time.time() - self.start_time}})

        # Wait until we're done collecting queue data
        self.parent_ready.wait()

        # Tell the connection to close
        self.connection.close()

        # Block until we're closed, app will stop then
        while self.connection.is_open:
            time.sleep(1)

        self.connection.ioloop.stop()

    def on_connected(self, connection):
        logging.debug("%i:%s:on_connected" % (self.pid,
                                              self.__class__.__name__))
        self.open = True
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        logging.debug("%i:%s:on_channel_open" % (self.pid,
                                                 self.__class__.__name__))

        self.channel = channel
        self.channel.queue_declare(queue=QUEUE_NAME, durable=True,
                                   exclusive=False, auto_delete=False,
                                   callback=self.on_queue_declared)

    def on_queue_declared(self, frame):
        logging.debug("%i:%s:on_queue_declared" % (self.pid,
                                                   self.__class__.__name__))

        # Let our parent know we're ready
        self.process_ready.append(self.pid)

        # Loop until our parent sets this value to true
        self.parent_ready.wait()

        # Call _on_ready
        self._on_ready()

    def report_stats(self):

        # Dont report data when we're closing down
        if not self.open:
            return

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

        self.connection.add_timeout(self.report_interval, self.report_stats)

    def _on_ready(self):
        pass


class Receiver(PikaClient):

    def _on_ready(self):
        logging.debug("%i:%s:_on_ready" % (self.pid, self.__class__.__name__))

        # Set our start time and last sample time
        self.start_time = self._last = time.time()

        self.channel.basic_consume(self.handle_delivery,
                                   queue=QUEUE_NAME,
                                   consumer_tag='ctag-%i' % self.pid,
                                   no_ack=self.no_ack)

        # Start our first report interval timer
        self.connection.add_timeout(self.report_interval, self.report_stats)

    def handle_delivery(self, channel, method, header, body):

        if not self.open:
            return

        if not self.no_ack:
            channel.basic_ack(delivery_tag=method.delivery_tag)
        self._count += 1


class Sender(PikaClient):

    def _on_ready(self):
        logging.debug("%i:%s:_on_ready" % (self.pid, self.__class__.__name__))

        # Set the start and last sample time
        self.start_time = self._last = time.time()

        # Start our first report interval timer
        self.connection.add_timeout(self.report_interval, self.report_stats)

        # Send our first set of messages
        self._send_message()

    def _send_message(self):

        if not self.open:
            return

        for x in xrange(0, self.max_messages_per_interval):
            message = "%i:%i:%.8f" % (self.pid, x, time.time())
            self.channel.basic_publish(exchange='',
                                       routing_key=QUEUE_NAME,
                                       body=message,
                                       properties=pika.BasicProperties(
                                          content_type="text/plain",
                                          delivery_mode=1))
            self.connection.flush_outbound()
            self._count += 1

        # Dont soak up 100% if this process, let the ioloop work too
        self.connection.add_timeout(1, self._send_message)


if __name__ == '__main__':

    usage = "usage: %prog [options]"
    description = "Pika + RabbitMQ Speed/Throughput Measurement Demo"

    # Create our parser and setup our command line options
    parser = optparse.OptionParser(usage=usage, description=description)

    parser.add_option("-a", "--adapter",
                        action="store", dest="adapter", type="choice",
                        choices=['AsyncoreConnection',
                                 'SelectConnection',
                                 'TornadoConnection'],
                        default='SelectConnection',
                        help="Specify the adapter to use. One of: \
                              AsynccoreConnection, SelectConnection, or \
                              TornadoConnection. Default: SelectConnection")

    parser.add_option("-d", "--duration",
                        action="store", dest="duration", type="int",
                        default=DURATION,
                        help="Specify the duration of the test to run in \
                              seconds. Default: %i" % DURATION)

    parser.add_option("--host",
                        action="store", dest="host",
                        default='localhost',
                        help="Specify the RabbitMQ hostname or ip address \
                              to connect to. Default: localhost")

    parser.add_option("-i", "--interval",
                        action="store", dest="interval", type="int",
                        default=REPORT_INTERVAL,
                        help="The log reporting interval. Default %i" % \
                              REPORT_INTERVAL)

    parser.add_option("-l", "--log-level",
                        action="store", dest="level", type="choice",
                        choices=['debug', 'info', 'warning'],
                        default='info',
                        help="Specify the logging verbosity. One of: debug, \
                              info, warning. Default: info.")

    parser.add_option("-f", "--log-file", action="store", dest="filename",
                        default='speed-test.log',
                        help="File to send log data to. \
                              Default: speed-test.log")

    parser.add_option("-m", "--max",
                        action="store", dest="max_messages", type="int",
                        default=MAX_MESSAGES_PER_INTERVAL,
                        help="Specify the maximum number of messages to send \
                              until pausing for the adapter to send content. \
                              Default: %i" % MAX_MESSAGES_PER_INTERVAL)

    parser.add_option("-n", "--no-ack",
                        action="store_true", dest="no_ack",
                        default=NO_ACK,
                        help="Turn off message acknowledgement in receivers. \
                              Default: %s" % NO_ACK)

    parser.add_option("-p", "--port",
                        action="store", dest="port", type="int",
                        default=5672,
                        help="Specify the RabbitMQ Port to connect to. \
                              Default: 5672")

    parser.add_option("-r", "--receivers",
                        action="store", dest="receivers", type="int",
                        default=RECEIVERS,
                        help="Specify the number of receiving processes. \
                              Default: %i" % RECEIVERS)

    parser.add_option("--select",
                        action="store", dest="select_poller", type="choice",
                        choices=[None, 'epoll', 'poll', 'kqueue', 'select'],
                        default=None,
                        help="Specify the select poller to use. Leave blank \
                              to have adapter select for you. One of: \
                              epoll, poll, kqueue, or select. Default: None")

    parser.add_option("-s", "--senders",
                        action="store", dest="senders", type="int",
                        default=SENDERS,
                        help="Specify the number of sending processes. \
                              Default: %i" % SENDERS)

    # Parse our options and arguments
    options, args = parser.parse_args()

    # Only show the info level of content, debug is too verbose
    if options.level == 'info':
        level = logging.INFO
    elif options.level == 'debug':
        level = logging.DEBUG
    elif options.level == 'warning':
        level = logging.WARN

    # Specify the logging data
    logging.basicConfig(level=level,
                        format=LOG_FORMAT,
                        filename=options.filename)

    # Choose the adapter
    if options.adapter == 'AsyncoreConnection':
        adapter = pika.adapters.AsyncoreConnection
    elif options.adapter == 'SelectConnection':
        adapter = pika.adapters.SelectConnection
    elif options.adapter == 'TornadoConnection':
        import pika.adapters.tornado_connection
        adapter = pika.adapters.tornado_connection.TornadoConnection

    # Let the user know what parameters we are using
    print "speed_test.py Startup Parameters:"
    print
    print "       RabbitMQ Server: %s:%i" % (options.host, options.port)
    print "               Adapter: %s" % options.adapter
    if options.select_poller:
        print "         Select Poller: %s" % options.select_poller
    print " Max Messages per loop: %i" % options.max_messages
    print "                No Ack: %s" % options.no_ack
    print "    Receiver Processes: %i" % options.receivers
    print "              Log file: %s" % options.filename
    print "    Reporting Interval: %i" % options.interval
    print "      Sender Processes: %i" % options.senders
    print "         Test Duration: %i" % options.duration
    print
    print "Starting test processes"

    # Multiprocessing data objects for cross process communication
    manager = multiprocessing.Manager()
    lock = multiprocessing.Lock()
    parent_ready = multiprocessing.Event()
    processes_ready = manager.list()
    throughput_data = multiprocessing.Queue()

    # Spawn our processes
    processes = list()

    # First receivers
    for x in xrange(0, options.receivers):
        process = Receiver(adapter, options.host, options.port,
                           options.max_messages, options.no_ack,
                           options.interval, throughput_data,
                           processes_ready, parent_ready,
                           options.select_poller)
        processes.append(process)
        process.start()

    # Then Senders
    for x in xrange(0, options.senders):
        process = Sender(adapter, options.host, options.port,
                         options.max_messages, options.no_ack,
                         options.interval, throughput_data,
                         processes_ready, parent_ready,
                         options.select_poller)
        processes.append(process)
        process.start()

    # Wait until all processes are started up
    pre_join = time.time()

    # Wait for all of our processes to tell us they're connected
    # and have a channel
    while len(processes_ready) < len(processes):
        time.sleep(1)

    print "Signaling test processes to start activity"

    # Send the event to let the children know to start testing
    parent_ready.set()

    # Loop while we're running
    start_time = time.time()
    end_time = time.time() + options.duration
    while end_time >= time.time():
        print "Waiting for test to complete, complete in %i seconds" % \
                     (end_time - time.time())
        time.sleep(1)

    # Clear the state flag so the children wait for us again
    parent_ready.clear()

    # Stop the processes
    for process in processes:
        process.terminate()

    # Get our time for calculation
    total_time = time.time() - start_time

    print "Waiting for data from test processes"
    test_data = dict()
    while len(test_data) < len(processes):
        data = throughput_data.get()
        if data:
            key = data.keys()[0]
            test_data[key] = data[key]

    # Let the children know they can continue shutting down
    parent_ready.set()

    print "Waiting for shutdown of test processes"
    while processes:
        for process in processes:
            if not process.is_alive():
                processes.remove(process)

    # Get our data
    messages_sent = list()
    messages_received = list()
    duration_sending = list()
    duration_receiving = list()

    # Put all our values into lists for each type of client
    for key in test_data.keys():
        if key[0:8] == 'Receiver':
            messages_received.append(test_data[key]['count'])
            duration_receiving.append(test_data[key]['duration'])

        elif key[0:6] == 'Sender':
            messages_sent.append(test_data[key]['count'])
            duration_sending.append(test_data[key]['duration'])

    print
    print "speed_test.py Test Results:"
    print
    print "              RabbitMQ Server: %s:%i" % (options.host, options.port)
    print "                      Adapter: %s" % options.adapter
    if options.select_poller:
        print "                Select Poller: %s" % options.select_poller
    print "        Max Messages per loop: %i" % options.max_messages
    print "                       No Ack: %s" % options.no_ack
    print
    print "                Test Duration: %.4f" % total_time
    print
    print "           Receiver Processes: %i" % options.receivers
    print "            Messages Received: %i" % sum(messages_received)
    print "           Duration Receiving: %.4f" % sum(duration_receiving)
    print " Received Messages per Second: %.4f" % \
          (sum(messages_received) / total_time)
    print
    print "             Sender Processes: %i" % options.senders

    print "           Messages Delivered: %i" % sum(messages_sent)
    print "          Duration Delivering: %.4f" % sum(duration_sending)
    print "Delivered Messages per Second: %.4f" % \
          (sum(messages_sent) / total_time)
    print
