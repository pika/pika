# -*- coding=UTF-8 -*-
# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""Test Unicode Support in Pika"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__date__ = '2011-04-15'

import nose
import support
import support.tools
from pika.adapters import SelectConnection
from pika.spec import BasicProperties


class TestUnicode(support.tools.AsyncPattern):

    def __init__(self):
        # UTf-8 Values to test with
        self.exchange = "أرنب"
        self.queue = "ճագար"
        self.routing_key = "兔"
        self.app_id = "კურდღლების"
        self.values = ['κουνέλι',
                       'ארנב',
                       'ख़रगोश',
                       'ウサギ',
                       '토끼',
                       'зајакот',
                       'кролик',
                       'กระต่าย',
                       'tavşan',
                       'зец']
        self.received = []
        self.properties = BasicProperties(app_id=self.app_id,
                                          content_type="text/plain",
                                          delivery_mode=1)

    def test_unicode(self):
        # Start the test connection
        support.tools.AsyncPattern.__init__(self)
        self.confirmed = False
        self.connection = self._connect(SelectConnection, support.PARAMETERS)
        self.connection.ioloop.start()

        # We're here when test starts
        if self._timeout:
            assert False, "Test timed out"

    def _on_channel(self, channel):
        self.channel = channel
        # Test the exchange declare
        self.test_exchange_declare()

    @support.tools.timeout
    @nose.tools.nottest
    def test_exchange_declare(self):
        self.channel.exchange_declare(callback=self.test_queue_declare,
                                      exchange=self.exchange,
                                      auto_delete=True)

    @support.tools.timeout
    @nose.tools.nottest
    def test_queue_declare(self, frame):
        if frame.method.NAME != 'Exchange.DeclareOk':
            assert False, \
                    "Exchange did not declare correctly: %s" % self.exchange
        self.channel.queue_declare(callback=self.test_queue_bind,
                                   queue=self.queue,
                                   auto_delete=True)

    @support.tools.timeout
    @nose.tools.nottest
    def test_queue_bind(self, frame):
        if frame.method.queue != self.queue:
            assert False, \
                "Queue did not declare correctly: %s" % self.queue

        self.channel.queue_bind(exchange=self.exchange,
                                queue=self.queue,
                                routing_key=self.routing_key,
                                callback=self.test_publishing)

    @support.tools.timeout
    @nose.tools.nottest
    def test_publishing(self, frame):
        if frame.method.NAME != 'Queue.BindOk':
            assert False, \
                    "Queue did not bind correctly: %s" % self.routing_key

        for word in self.values:
            self.channel.basic_publish(exchange=self.exchange,
                                       routing_key=self.routing_key,
                                       body=word,
                                       properties=self.properties)

        self.channel.basic_consume(self.on_message, self.queue)

    @nose.tools.nottest
    def on_message(self, channel, method_frame, header_frame, body):
        if header_frame.app_id != self.app_id:
            assert False, "Application ID didn't match in Basic.Deliver header"

        if body in self.values:
            self.received.append(body)

        if len(self.received) == len(self.values):
            self.connection.add_on_close_callback(self._on_closed)
            self.connection.close()
