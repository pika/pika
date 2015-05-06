import time
import uuid
import select
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import platform
target = platform.python_implementation()

from async_test_base import (AsyncTestCase, BoundQueueTestCase)

from pika import adapters
from pika.adapters import select_connection
from pika import spec


class AllAdapters(object):
    
    def select1_test(self):
        "SelectConnection:DefaultPoller"
        self.start(adapters.SelectConnection)
 
    def select2_test(self):
        "SelectConnection:select"
        select_connection.POLLER_TYPE='select'
        self.start(adapters.SelectConnection)

    @unittest.skipIf(not hasattr(select, 'poll') 
        or not hasattr(select.poll(), 'modify'), "poll not supported")
    def select3_test(self):
        "SelectConnection:poll"
        select_connection.POLLER_TYPE='poll'
        self.start(adapters.SelectConnection)
    
    @unittest.skipIf(not hasattr(select, 'epoll'), "epoll not supported")
    def select4_test(self):
        "SelectConnection:epoll"
        select_connection.POLLER_TYPE='epoll'
        self.start(adapters.SelectConnection)
    
    @unittest.skipIf(not hasattr(select, 'kqueue'), "kqueue not supported")
    def select5_test(self):
        "SelectConnection:kqueue"
        select_connection.POLLER_TYPE='kqueue'
        self.start(adapters.SelectConnection)

    def tornado_test(self):
        "TornadoConnection"
        self.start(adapters.TornadoConnection)

    def asyncore_test(self):
        "AsyncoreConnection"
        self.start(adapters.AsyncoreConnection)

    @unittest.skipIf(target == 'PyPy', 'PyPy is not supported')
    @unittest.skipIf(adapters.LibevConnection is None, 'pyev is not installed')
    def libev_test(self):
        "LibevConnection"
        self.start(adapters.LibevConnection)


class TestA_Connect(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Connect, open channel and disconnect"
    
    def begin(self, channel):
        self.stop()


class TestConfirmSelect(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Receive confirmation of Confirm.Select"

    def begin(self, channel):
        channel._on_selectok = self.on_complete
        channel.confirm_delivery()

    def on_complete(self, frame):
        self.assertIsInstance(frame.method, spec.Confirm.SelectOk)
        self.stop()


class TestConsumeCancel(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Consume and cancel"

    def begin(self, channel):
        self.queue_name = str(uuid.uuid4())
        channel.queue_declare(self.on_queue_declared, queue=self.queue_name)

    def on_queue_declared(self, frame):
        for i in range(0, 100):
            msg_body = '{0}:{1}:{2}'.format(self.__class__.__name__, i,
                                            time.time())
            self.channel.basic_publish('', self.queue_name, msg_body)
        self.ctag = self.channel.basic_consume(self.on_message,
                                               queue=self.queue_name,
                                               no_ack=True)

    def on_message(self, _channel, _frame, _header, body):
        self.channel.basic_cancel(self.on_cancel, self.ctag)

    def on_cancel(self, _frame):
        self.channel.queue_delete(self.on_deleted, self.queue_name)

    def on_deleted(self, _frame):
        self.stop()


class TestExchangeDeclareAndDelete(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Create and delete and exchange"

    X_TYPE = 'direct'

    def begin(self, channel):
        self.name = self.__class__.__name__ + ':' + str(id(self))
        channel.exchange_declare(self.on_exchange_declared, self.name,
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


class TestExchangeRedeclareWithDifferentValues(AsyncTestCase, AllAdapters):
    DESCRIPTION = "should close chan: re-declared queue w/ diff params"

    X_TYPE1 = 'direct'
    X_TYPE2 = 'topic'

    def begin(self, channel):
        self.name = self.__class__.__name__ + ':' + str(id(self))
        self.channel.add_on_close_callback(self.on_channel_closed)
        channel.exchange_declare(self.on_exchange_declared, self.name,
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
        self.channel.exchange_declare(self.on_exchange_declared, self.name,
                                      exchange_type=self.X_TYPE2,
                                      passive=False,
                                      durable=False,
                                      auto_delete=True)

    def on_bad_result(self, frame):
        self.channel.exchange_delete(None, self.name, nowait=True)
        raise AssertionError("Should not have received a Queue.DeclareOk")


class TestQueueDeclareAndDelete(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Create and delete a queue"

    def begin(self, channel):
        channel.queue_declare(self.on_queue_declared,
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=False,
                              nowait=False,
                              arguments={'x-expires': self.TIMEOUT * 1000})

    def on_queue_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeclareOk)
        self.channel.queue_delete(self.on_queue_delete, frame.method.queue)

    def on_queue_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeleteOk)
        self.stop()



class TestQueueNameDeclareAndDelete(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Create and delete a named queue"

    def begin(self, channel):
        channel.queue_declare(self.on_queue_declared, str(id(self)),
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=True,
                              nowait=False,
                              arguments={'x-expires': self.TIMEOUT * 1000})

    def on_queue_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeclareOk)
        self.assertEqual(frame.method.queue, str(id(self)))
        self.channel.queue_delete(self.on_queue_delete, frame.method.queue)

    def on_queue_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeleteOk)
        self.stop()



class TestQueueRedeclareWithDifferentValues(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Should close chan: re-declared queue w/ diff params"

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        channel.queue_declare(self.on_queue_declared, str(id(self)),
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=True,
                              nowait=False,
                              arguments={'x-expires': self.TIMEOUT * 1000})

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_queue_declared(self, frame):
        self.channel.queue_declare(self.on_bad_result, str(id(self)),
                                   passive=False,
                                   durable=True,
                                   exclusive=False,
                                   auto_delete=True,
                                   nowait=False,
                                   arguments={'x-expires': self.TIMEOUT * 1000})

    def on_bad_result(self, frame):
        self.channel.queue_delete(None, str(id(self)), nowait=True)
        raise AssertionError("Should not have received a Queue.DeclareOk")



class TestTX1_Select(AsyncTestCase, AllAdapters):
    DESCRIPTION="Receive confirmation of Tx.Select"

    def begin(self, channel):
        channel.tx_select(self.on_complete)

    def on_complete(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.stop()



class TestTX2_Commit(AsyncTestCase, AllAdapters):
    DESCRIPTION="Start a transaction, and commit it"

    def begin(self, channel):
        channel.tx_select(self.on_selectok)

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.channel.tx_commit(self.on_commitok)

    def on_commitok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.CommitOk)
        self.stop()


class TestTX2_CommitFailure(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Close the channel: commit without a TX"

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.tx_commit(self.on_commitok)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)

    def on_commitok(self, frame):
        raise AssertionError("Should not have received a Tx.CommitOk")


class TestTX3_Rollback(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Start a transaction, then rollback"

    def begin(self, channel):
        channel.tx_select(self.on_selectok)

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.channel.tx_rollback(self.on_rollbackok)

    def on_rollbackok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.RollbackOk)
        self.stop()



class TestTX3_RollbackFailure(AsyncTestCase, AllAdapters):
    DESCRIPTION = "Close the channel: rollback without a TX"

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.tx_rollback(self.on_commitok)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_commitok(self, frame):
        raise AssertionError("Should not have received a Tx.RollbackOk")



class TestZ_PublishAndConsume(BoundQueueTestCase, AllAdapters):
    DESCRIPTION = "Publish a message and consume it"

    def on_ready(self, frame):
        self.ctag = self.channel.basic_consume(self.on_message, self.queue)
        self.msg_body = "%s: %i" % (self.__class__.__name__, time.time())
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   self.msg_body)

    def on_cancelled(self, frame):
        self.assertIsInstance(frame.method, spec.Basic.CancelOk)
        self.stop()

    def on_message(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.Deliver)
        self.assertEqual(body, self.msg_body)
        self.channel.basic_ack(method.delivery_tag)
        self.channel.basic_cancel(self.on_cancelled, self.ctag)



class TestZ_PublishAndConsumeBig(BoundQueueTestCase, AllAdapters):
    DESCRIPTION = "Publish a big message and consume it"

    def _get_msg_body(self):
        return '\n'.join(["%s" % i for i in range(0, 2097152)])

    def on_ready(self, frame):
        self.ctag = self.channel.basic_consume(self.on_message, self.queue)
        self.msg_body = self._get_msg_body()
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   self.msg_body)

    def on_cancelled(self, frame):
        self.assertIsInstance(frame.method, spec.Basic.CancelOk)
        self.stop()

    def on_message(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.Deliver)
        self.assertEqual(body, self.msg_body)
        self.channel.basic_ack(method.delivery_tag)
        self.channel.basic_cancel(self.on_cancelled, self.ctag)



class TestZ_PublishAndGet(BoundQueueTestCase, AllAdapters):
    DESCRIPTION = "Publish a message and get it"

    def on_ready(self, frame):
        self.msg_body = "%s: %i" % (self.__class__.__name__, time.time())
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   self.msg_body)
        self.channel.basic_get(self.on_get, self.queue)

    def on_get(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.GetOk)
        self.assertEqual(body, self.msg_body)
        self.channel.basic_ack(method.delivery_tag)
        self.stop()
