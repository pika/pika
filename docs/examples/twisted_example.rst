Twisted Consumer Example
========================

.. _example_twisted_consumer:

Example of writing a consumer using :py:class:`TwistedConnection <TwistedConnection>`::

    # -*- coding:utf-8 -*-

    import pika
    from pika import exceptions
    from pika.adapters import twisted_connection
    from twisted.internet import defer, reactor, protocol,task


    @defer.inlineCallbacks
    def run(connection):

        channel = yield connection.channel()

        exchange = yield channel.exchange_declare(exchange='topic_link',type='topic')

        queue = yield channel.queue_declare(queue='hello', auto_delete=False, exclusive=False)

        yield channel.queue_bind(exchange='topic_link',queue='hello',routing_key='hello.world')

        yield channel.basic_qos(prefetch_count=1)

        queue_object, consumer_tag = yield channel.basic_consume(queue='hello',no_ack=False)

        l = task.LoopingCall(read, queue_object)

        l.start(0.01)


    @defer.inlineCallbacks
    def read(queue_object):

        ch,method,properties,body = yield queue_object.get()

        if body:
            print body

        yield ch.basic_ack(delivery_tag=method.delivery_tag)


    parameters = pika.ConnectionParameters()
    cc = protocol.ClientCreator(reactor, twisted_connection.TwistedProtocolConnection, parameters)
    d = cc.connectTCP('hostname', 5672)
    d.addCallback(lambda protocol: protocol.ready)
    d.addCallback(run)
    reactor.run()
