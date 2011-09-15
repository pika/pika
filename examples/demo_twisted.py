# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Example of using Pika with Twisted. Gives na example of connecting in two ways:
using the normal Pika interfaces, which blocks and using Twisted, which is
asynchronous.
"""
import sys

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

from pika import log as pika_log
from pika.adapters.twisted_connection import TwistedConnection
from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.connection import ConnectionParameters

from twisted.internet import task, protocol, reactor
from twisted.python import log as twisted_log


class TwistedHandler(object):
    """
    And example class that declares an anonymous queue and prints messages
    delivered to it, until it receives a 'close-please' message, which makes it
    delete the queue from the server.
    """
    def __init__(self):
        self.pings = task.LoopingCall(self.ping)
        self.pings.start(3)

    def ping(self):
        pika_log.info("demo_twisted: Still alive")

    def on_connected(self, connection):
        pika_log.info("demo_twisted: Connected to RabbitMQ")
        d = connection.channel()
        d.addCallback(self.got_channel)
        d.addCallback(self.declare_exchange)
        d.addCallback(self.declare_queue)
        d.addCallback(self.bind_queue)
        d.addCallback(self.start_consuming)
        d.addCallback(self.handle_deliveries)
        d.addErrback(twisted_log.err)

    def got_channel(self, channel):
        pika_log.info("demo_twisted: Got the channel")
        self.channel = channel

    def declare_exchange(self, _):
        pika_log.info("demo_twisted: Declaring the exchange")
        return self.channel.exchange_declare(exchange="twisted", durable=True,
                                             type="direct", auto_delete=True)

    def declare_queue(self, _):
        pika_log.info("demo_twisted: Declaring an anonymous queue")
        return self.channel.queue_declare(queue='', auto_delete=True,
                                          durable=False, exclusive=False)

    def bind_queue(self, frame):
        self.queue_name = frame.method.queue
        pika_log.info("demo_twisted: Queue %s declared" % self.queue_name)
        return self.channel.queue_bind(queue=self.queue_name,
                                       exchange='twisted',
                                       routing_key='1')

    def start_consuming(self, _):
        pika_log.info("demo_twisted: Queue bound")
        return self.channel.basic_consume(queue=self.queue_name, no_ack=True)

    def handle_deliveries(self, queue_and_consumer_tag):
        pika_log.info("demo_twisted: Consuming started")
        queue, consumer_tag = queue_and_consumer_tag
        self.lc = task.LoopingCall(self.consume_from_queue, queue)
        return self.lc.start(0)

    def consume_from_queue(self, queue):
        d = queue.get()
        return d.addCallback(lambda ret: self.handle_payload(*ret))

    def handle_payload(self, channel, method_frame, header_frame, body):
        pika_log.info("demo_twisted: Message received: %s" % body)
        if body == 'stop-please':
            self.stop_consuming()

    def stop_consuming(self):
        self.lc.stop()
        d = self.channel.queue_delete(queue=self.queue_name)
        d.addCallbacks(self.stopped_consuming, twisted_log.err)

    def stopped_consuming(self, _):
        pika_log.info("demo_twisted: Consuming stopped, queue deleted")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >>sys.stderr, ("usage: %s [p|t] (use Pika or Twisted "
                             "for connecting" % sys.argv[0])
        sys.exit(1)

    handler = TwistedHandler()
    parameters = ConnectionParameters()
    pika_log.setup(color=True)

    if sys.argv[1] == 'p':
        TwistedConnection(parameters, handler.on_connected)
    elif sys.argv[1] == 't':
        cc = protocol.ClientCreator(reactor,
                                    TwistedProtocolConnection, parameters)
        d = cc.connectTCP('localhost', 5672)
        d.addCallback(lambda protocol: protocol.ready)
        d.addCallback(handler.on_connected)
        d.addErrback(twisted_log.err)
    else:
        print >>sys.stderr, ("usage: %s [p|t] (use Pika or Twisted "
                             "for connecting" % sys.argv[0])
        sys.exit(1)

    reactor.run()
