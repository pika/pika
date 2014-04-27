import time

import async_test_base

from pika import adapters
from pika import spec


class AsyncTestCase(async_test_base.AsyncTestCase):
    ADAPTER = adapters.AsyncoreConnection


class BoundQueueTestCase(async_test_base.BoundQueueTestCase):
    ADAPTER = adapters.AsyncoreConnection


class TestA_Connect(AsyncTestCase):

    ADAPTER = adapters.AsyncoreConnection

    def begin(self, channel):
        self.stop()

    def start_test(self):
        """AsyncoreConnection should connect, open channel and disconnect"""
        self.start()


class TestConfirmSelect(AsyncTestCase):

    def begin(self, channel):
        channel._on_selectok = self.on_complete
        channel.confirm_delivery()

    def on_complete(self, frame):
        self.assertIsInstance(frame.method, spec.Confirm.SelectOk)
        self.stop()

    def start_test(self):
        """AsyncoreConnection should receive confirmation of Confirm.Select"""
        self.start()


class TestExchangeDeclareAndDelete(AsyncTestCase):

    X_TYPE = 'direct'

    def begin(self, channel):
        self.name = self.__class__.__name__ + ':' + str(id(self))
        channel.exchange_declare(self.on_exchange_declared,
                                 self.name,
                                 exchange_type=self.X_TYPE,
                                 passive=False,
                                 durable=False,
                                 auto_delete=True)

    def on_exchange_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Exchange.DeclareOk)
        self.channel.exchange_delete(self.on_exchange_delete, self.name)

    def on_exchange_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Exchange.DeleteOk)
        self.stop()

    def start_test(self):
        """TornadoConnection should create and delete an exchange"""
        self.start()


class TestExchangeRedeclareWithDifferentValues(AsyncTestCase):

    X_TYPE1 = 'direct'
    X_TYPE2 = 'topic'

    def begin(self, channel):
        self.name = self.__class__.__name__ + ':' + str(id(self))
        self.channel.add_on_close_callback(self.on_channel_closed)
        channel.exchange_declare(self.on_exchange_declared,
                                 self.name,
                                 exchange_type=self.X_TYPE1,
                                 passive=False,
                                 durable=False,
                                 auto_delete=True)


    def on_cleanup_channel(self, channel):
        channel.exchange_delete(None, self.name, nowait=True)
        self.stop()

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.connection.channel(self.on_cleanup_channel)

    def on_exchange_declared(self, frame):
        self.channel.exchange_declare(self.on_exchange_declared,
                                      self.name,
                                      exchange_type=self.X_TYPE2,
                                      passive=False,
                                      durable=False,
                                      auto_delete=True)

    def on_bad_result(self, frame):
        self.channel.exchange_delete(None, self.name, nowait=True)
        raise AssertionError("Should not have received a Queue.DeclareOk")

    def start_test(self):
        """TornadoConnection should close chan: re-declared exchange w/ diff params

        """
        self.start()


class TestQueueDeclareAndDelete(AsyncTestCase):

    def begin(self, channel):
        channel.queue_declare(self.on_queue_declared,
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=False,
                              nowait=False,
                              arguments={'x-expires': self.TIMEOUT})

    def on_queue_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeclareOk)
        self.channel.queue_delete(self.on_queue_delete, frame.method.queue)

    def on_queue_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeleteOk)
        self.stop()

    def start_test(self):
        """AsyncoreConnection should create and delete a queue"""
        self.start()


class TestQueueNameDeclareAndDelete(AsyncTestCase):

    def begin(self, channel):
        channel.queue_declare(self.on_queue_declared, str(id(self)),
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=True,
                              nowait=False,
                              arguments={'x-expires': self.TIMEOUT})

    def on_queue_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeclareOk)
        self.assertEqual(frame.method.queue, str(id(self)))
        self.channel.queue_delete(self.on_queue_delete, frame.method.queue)

    def on_queue_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeleteOk)
        self.stop()

    def start_test(self):
        """AsyncoreConnection should create and delete a named queue"""
        self.start()


class TestQueueRedeclareWithDifferentValues(AsyncTestCase):

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        channel.queue_declare(self.on_queue_declared,
                              str(id(self)),
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=True,
                              nowait=False,
                              arguments={'x-expires': self.TIMEOUT})

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_queue_declared(self, frame):
        self.channel.queue_declare(self.on_bad_result,
                                   str(id(self)),
                                   passive=False,
                                   durable=True,
                                   exclusive=False,
                                   auto_delete=True,
                                   nowait=False,
                                   arguments={'x-expires': self.TIMEOUT})

    def on_bad_result(self, frame):
        self.channel.queue_delete(None, str(id(self)), nowait=True)
        raise AssertionError("Should not have received a Queue.DeclareOk")

    def start_test(self):
        """AsyncoreConnection should close chan: re-declared queue w/ diff params

        """
        self.start()


class TestTX1_Select(AsyncTestCase):

    def begin(self, channel):
        channel.tx_select(self.on_complete)

    def on_complete(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.stop()

    def test_confirm_select(self):
        """AsyncoreConnection should receive confirmation of Tx.Select"""
        self.start()


class TestTX2_Commit(AsyncTestCase):

    def begin(self, channel):
        channel.tx_select(self.on_selectok)

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.channel.tx_commit(self.on_commitok)

    def on_commitok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.CommitOk)
        self.stop()

    def start_test(self):
        """AsyncoreConnection should start a transaction, then commit it back"""
        self.start()


class TestTX2_CommitFailure(AsyncTestCase):

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.tx_commit(self.on_commitok)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)

    def on_commitok(self, frame):
        raise AssertionError("Should not have received a Tx.CommitOk")

    def start_test(self):
        """AsyncoreConnection should close the channel: commit without a TX"""
        self.start()


class TestTX3_Rollback(AsyncTestCase):

    def begin(self, channel):
        channel.tx_select(self.on_selectok)

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.channel.tx_rollback(self.on_rollbackok)

    def on_rollbackok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.RollbackOk)
        self.stop()

    def start_test(self):
        """AsyncoreConnection should start a transaction, then roll it back"""
        self.start()


class TestTX3_RollbackFailure(AsyncTestCase):

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.tx_rollback(self.on_commitok)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_commitok(self, frame):
        raise AssertionError("Should not have received a Tx.RollbackOk")

    def start_test(self):
        """AsyncoreConnection should close the channel: rollback without a TX"""
        self.start()


class TestZ_PublishAndConsume(BoundQueueTestCase):

    def on_ready(self, frame):
        self.ctag = self.channel.basic_consume(self.on_message, self.queue)
        self.msg_body = "%s: %i" % (self.__class__.__name__, time.time())
        self.channel.basic_publish(self.exchange,
                                   self.routing_key,
                                   self.msg_body)

    def on_cancelled(self, frame):
        self.assertIsInstance(frame.method, spec.Basic.CancelOk)
        self.stop()

    def on_message(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.Deliver)
        self.assertEqual(body, self.msg_body)
        self.channel.basic_ack(method.delivery_tag)
        self.channel.basic_cancel(self.on_cancelled, self.ctag)

    def start_test(self):
        """AsyncoreConnection should publish a message and consume it"""
        self.start()


class TestZ_PublishAndConsumeBig(BoundQueueTestCase):

    def _get_msg_body(self):
        return '\n'.join(["%s" % i for i in range(0, 2097152)])

    def on_ready(self, frame):
        self.ctag = self.channel.basic_consume(self.on_message, self.queue)
        self.msg_body = self._get_msg_body()
        self.channel.basic_publish(self.exchange,
                                   self.routing_key,
                                   self.msg_body)

    def on_cancelled(self, frame):
        self.assertIsInstance(frame.method, spec.Basic.CancelOk)
        self.stop()

    def on_message(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.Deliver)
        self.assertEqual(body, self.msg_body)
        self.channel.basic_ack(method.delivery_tag)
        self.channel.basic_cancel(self.on_cancelled, self.ctag)

    def start_test(self):
        """AsyncoreConnection should publish a big message and consume it"""
        self.start()



class TestZ_PublishAndGet(BoundQueueTestCase):

    def on_ready(self, frame):
        self.msg_body = "%s: %i" % (self.__class__.__name__, time.time())
        self.channel.basic_publish(self.exchange,
                                   self.routing_key,
                                   self.msg_body)
        self.channel.basic_get(self.on_get, self.queue)

    def on_get(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.GetOk)
        self.assertEqual(body, self.msg_body)
        self.channel.basic_ack(method.delivery_tag)
        self.stop()

    def start_test(self):
        """AsyncoreConnection should publish a message and get it"""
        self.start()

