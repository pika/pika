# -*- coding:utf-8 -*-
# based on:
#  - txamqp-helpers by Dan Siemon <dan@coverfire.com> (March 2010)
#    http://git.coverfire.com/?p=txamqp-twistd.git;a=tree
#  - Post by Brian Chandler
#    https://groups.google.com/forum/#!topic/pika-python/o_deVmGondk
#  - Pika Documentation
#    http://pika.readthedocs.org/en/latest/examples/twisted_example.html

import pika
from pika import spec
from pika import exceptions
from pika.adapters import twisted_connection

from twisted.internet import protocol
from twisted.application import internet
from twisted.application import service
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

PREFETCH_COUNT = 2

class PikaService(service.MultiService):
    name = 'amqp'

    def __init__(self, parameter):
        service.MultiService.__init__(self)
        self.parameters = parameter

    def startService(self):
        self.connect()
        service.MultiService.startService(self)

    def getFactory(self):
        if len(self.services) > 0:
            return self.services[0].factory

    def connect(self):
        f = PikaFactory(self.parameters)
        if self.parameters.ssl:
            s = ssl.ClientContextFactory()
            serv = internet.SSLClient(host=self.parameters.host, port=self.parameters.port, factory=f, contextFactory=s)
        else:
            serv = internet.TCPClient(host=self.parameters.host, port=self.parameters.port, factory=f)
        serv.factory = f
        f.service = serv
        name = '%s%s:%d' % ('ssl:' if self.parameters.ssl else '', self.parameters.host, self.parameters.port)
        serv.__repr__ = lambda : '<AMQP Connection to %s>' % name
        serv.setName(name)
        serv.parent = self
        self.addService(serv)


class PikaProtocol(twisted_connection.TwistedProtocolConnection):
    connected = False
    name = 'AMQP:Protocol'

    @inlineCallbacks
    def connected(self, connection):
        self.channel = yield connection.channel()
        yield self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        self.connected = True
        for (exchange, routing_key, callback,) in self.factory.read_list:
            self.setup_read(exchange, routing_key, callback)

        self.send()

    def read(self, exchange, routing_key, callback):
        """Add an exchange to the list of exchanges to read from."""
        if self.connected:
            self.setup_read(exchange, routing_key, callback)

    @inlineCallbacks
    def setup_read(self, exchange, routing_key, callback):
        """This function does the work to read from an exchange."""
        if not exchange == '':
            yield self.channel.exchange_declare(exchange=exchange, type='topic', durable=True, auto_delete=False)
        if not exchange == '':
            queue = yield self.channel.queue_declare(durable=False, exclusive=True, auto_delete=True)
            queue_name = queue.method.queue
            yield self.channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=routing_key)
        else:
            queue = yield self.channel.queue_declare(queue=routing_key, durable=False, exclusive=True, auto_delete=True)
            queue_name = queue.fields[0]
        (queue, consumer_tag,) = yield self.channel.basic_consume(queue=queue_name, no_ack=True)
        d = queue.get()
        d.addCallback(self._read_item, queue, callback)
        d.addErrback(self._read_item_err)

    def _read_item(self, item, queue, callback):
        """Callback function which is called when an item is read."""
        d = queue.get()
        d.addCallback(self._read_item, queue, callback)
        d.addErrback(self._read_item_err)
        (channel, deliver, props, msg,) = item
        log.msg('%s (%s): %s' % (deliver.exchange, deliver.routing_key, repr(msg)), system='Pika:<=')
        callback(item)

    def _read_item_err(self, error):
        print error

    def send(self):
        """If connected, send all waiting messages."""
        if self.connected:
            while len(self.factory.queued_messages) > 0:
                (exchange, r_key, message,) = self.factory.queued_messages.pop(0)
                self.send_message(exchange, r_key, message)

    @inlineCallbacks
    def send_message(self, exchange, routing_key, msg):
        """Send a single message."""
        log.msg('%s (%s): %s' % (exchange, routing_key, repr(msg)), system='Pika:=>')
        yield self.channel.exchange_declare(exchange=exchange, type='topic', durable=True, auto_delete=False)
        prop = spec.BasicProperties(delivery_mode=2)
        try:
            yield self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg, properties=prop)
        except Exception as error:
            log.msg('Error while sending message: %s' % error, system=self.name)


class PikaFactory(protocol.ReconnectingClientFactory):
    name = 'AMQP:Factory'

    def __init__(self, parameters):
        self.parameters = parameters
        self.client = None
        self.queued_messages = []
        self.read_list = []

    def startedConnecting(self, connector):
        log.msg('Started to connect.', system=self.name)

    def buildProtocol(self, addr):
        self.resetDelay()
        log.msg('Connected', system=self.name)
        self.client = PikaProtocol(self.parameters)
        self.client.factory = self
        self.client.ready.addCallback(self.client.connected)
        return self.client

    def clientConnectionLost(self, connector, reason):
        log.msg('Lost connection.  Reason: %s' % reason, system=self.name)
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        log.msg('Connection failed. Reason: %s' % reason, system=self.name)
        protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def send_message(self, exchange = None, routing_key = None, message = None):
        self.queued_messages.append((exchange, routing_key, message))
        if self.client is not None:
            self.client.send()

    def read_messages(self, exchange, routing_key, callback):
        """Configure an exchange to be read from."""
        self.read_list.append((exchange, routing_key, callback))
        if self.client is not None:
            self.client.read(exchange, routing_key, callback)

## Fire up test application with
# twistd -ny twisted_service.py
## application will answer to requests to exchange "foobar" and routing_key "request"
## with messages to the same exchange but with routing_key "response"

application = service.Application("pikaapplication")

ps = PikaService(pika.ConnectionParameters(host="localhost", virtual_host="/", credentials=pika.PlainCredentials("guest", "guest")))
ps.setServiceParent(application)

class TestService(service.Service):

    def respond(self, msg):
        self.amqp.send_message('foobar', 'response', msg[3])

    def startService(self):
        self.amqp = self.parent.getServiceNamed("amqp").getFactory()
        self.amqp.read_messages("foobar", "request", self.respond)

ts = TestService()
ts.setServiceParent(application)
