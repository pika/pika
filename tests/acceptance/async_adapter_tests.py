# Suppress pylint messages concerning missing class and method docstrings
# pylint: disable=C0111

# Suppress pylint warning about attribute defined outside __init__
# pylint: disable=W0201

# Suppress pylint warning about access to protected member
# pylint: disable=W0212

# Suppress pylint warning about unused argument
# pylint: disable=W0613

import time
import uuid

from pika import spec
from pika.compat import as_bytes
import pika.connection
import pika.frame
import pika.spec

from async_test_base import (AsyncTestCase, BoundQueueTestCase, AsyncAdapters)


class TestA_Connect(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Connect, open channel and disconnect"

    def begin(self, channel):
        self.stop()


class TestConfirmSelect(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Receive confirmation of Confirm.Select"

    def begin(self, channel):
        channel.confirm_delivery(ack_nack_callback=self.ack_nack_callback,
                                 callback=self.on_complete)

    def ack_nack_callback(frame):
        pass

    def on_complete(self, frame):
        self.assertIsInstance(frame.method, spec.Confirm.SelectOk)
        self.stop()


class TestBlockingNonBlockingBlockingRPCWontStall(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = ("Verify that a sequence of blocking, non-blocking, blocking "
                   "RPC requests won't stall")

    def begin(self, channel):
        # Queue declaration params table: queue name, nowait value
        self._expected_queue_params = (
            ("blocking-non-blocking-stall-check-" + uuid.uuid1().hex, False),
            ("blocking-non-blocking-stall-check-" + uuid.uuid1().hex, True),
            ("blocking-non-blocking-stall-check-" + uuid.uuid1().hex, False)
        )

        self._declared_queue_names = []

        for queue, nowait in self._expected_queue_params:
            cb = self._queue_declare_ok_cb if not nowait else None
            channel.queue_declare(queue=queue,
                                  auto_delete=True,
                                  arguments={'x-expires': self.TIMEOUT * 1000},
                                  callback=cb)

    def _queue_declare_ok_cb(self, declare_ok_frame):
        self._declared_queue_names.append(declare_ok_frame.method.queue)

        if len(self._declared_queue_names) == 2:
            # Initiate check for creation of queue declared with nowait=True
            self.channel.queue_declare(queue=self._expected_queue_params[1][0],
                                       passive=True,
                                       callback=self._queue_declare_ok_cb)
        elif len(self._declared_queue_names) == 3:
            self.assertSequenceEqual(
                sorted(self._declared_queue_names),
                sorted(item[0] for item in self._expected_queue_params))
            self.stop()


class TestConsumeCancel(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Consume and cancel"

    def begin(self, channel):
        self.queue_name = self.__class__.__name__ + ':' + uuid.uuid1().hex
        channel.queue_declare(self.queue_name, callback=self.on_queue_declared)

    def on_queue_declared(self, frame):
        for i in range(0, 100):
            msg_body = '{}:{}:{}'.format(self.__class__.__name__, i,
                                         time.time())
            self.channel.basic_publish('', self.queue_name, msg_body)
        self.ctag = self.channel.basic_consume(self.queue_name,
                                               self.on_message,
                                               no_ack=True)

    def on_message(self, _channel, _frame, _header, body):
        self.channel.basic_cancel(self.ctag, callback=self.on_cancel)

    def on_cancel(self, _frame):
        self.channel.queue_delete(self.queue_name, callback=self.on_deleted)

    def on_deleted(self, _frame):
        self.stop()


class TestExchangeDeclareAndDelete(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Create and delete and exchange"

    X_TYPE = 'direct'

    def begin(self, channel):
        self.name = self.__class__.__name__ + ':' + uuid.uuid1().hex
        channel.exchange_declare(self.name,
                                 exchange_type=self.X_TYPE,
                                 passive=False,
                                 durable=False,
                                 auto_delete=True,
                                 callback=self.on_exchange_declared)

    def on_exchange_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Exchange.DeclareOk)
        self.channel.exchange_delete(self.name, callback=self.on_exchange_delete)

    def on_exchange_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Exchange.DeleteOk)
        self.stop()


class TestExchangeRedeclareWithDifferentValues(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "should close chan: re-declared exchange w/ diff params"

    X_TYPE1 = 'direct'
    X_TYPE2 = 'topic'

    def begin(self, channel):
        self.name = self.__class__.__name__ + ':' + uuid.uuid1().hex
        self.channel.add_on_close_callback(self.on_channel_closed)
        channel.exchange_declare(self.name,
                                 exchange_type=self.X_TYPE1,
                                 passive=False,
                                 durable=False,
                                 auto_delete=True,
                                 callback=self.on_exchange_declared)

    def on_cleanup_channel(self, channel):
        channel.exchange_delete(self.name)
        self.stop()

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.connection.channel(on_open_callback=self.on_cleanup_channel)

    def on_exchange_declared(self, frame):
        self.channel.exchange_declare(self.name,
                                      exchange_type=self.X_TYPE2,
                                      passive=False,
                                      durable=False,
                                      auto_delete=True,
                                      callback=self.on_bad_result)

    def on_bad_result(self, frame):
        self.channel.exchange_delete(self.name)
        raise AssertionError("Should not have received a Queue.DeclareOk")


class TestQueueDeclareAndDelete(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Create and delete a queue"

    def begin(self, channel):
        channel.queue_declare(queue='',
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=False,
                              arguments={'x-expires': self.TIMEOUT * 1000},
                              callback=self.on_queue_declared)

    def on_queue_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeclareOk)
        self.channel.queue_delete(frame.method.queue, callback=self.on_queue_delete)

    def on_queue_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeleteOk)
        self.stop()



class TestQueueNameDeclareAndDelete(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Create and delete a named queue"

    def begin(self, channel):
        self._q_name = self.__class__.__name__ + ':' + uuid.uuid1().hex
        channel.queue_declare(self._q_name,
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=True,
                              arguments={'x-expires': self.TIMEOUT * 1000},
                              callback=self.on_queue_declared)

    def on_queue_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeclareOk)
        # Frame's method's queue is encoded (impl detail)
        self.assertEqual(frame.method.queue, self._q_name)
        self.channel.queue_delete(frame.method.queue, callback=self.on_queue_delete)

    def on_queue_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeleteOk)
        self.stop()



class TestQueueRedeclareWithDifferentValues(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Should close chan: re-declared queue w/ diff params"

    def begin(self, channel):
        self._q_name = self.__class__.__name__ + ':' + uuid.uuid1().hex
        self.channel.add_on_close_callback(self.on_channel_closed)
        channel.queue_declare(self._q_name,
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=True,
                              arguments={'x-expires': self.TIMEOUT * 1000},
                              callback=self.on_queue_declared)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_queue_declared(self, frame):
        self.channel.queue_declare(self._q_name,
                                   passive=False,
                                   durable=True,
                                   exclusive=False,
                                   auto_delete=True,
                                   arguments={'x-expires': self.TIMEOUT * 1000},
                                   callback=self.on_bad_result)

    def on_bad_result(self, frame):
        self.channel.queue_delete(self._q_name)
        raise AssertionError("Should not have received a Queue.DeclareOk")



class TestTX1_Select(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Receive confirmation of Tx.Select"

    def begin(self, channel):
        channel.tx_select(callback=self.on_complete)

    def on_complete(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.stop()



class TestTX2_Commit(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Start a transaction, and commit it"

    def begin(self, channel):
        channel.tx_select(callback=self.on_selectok)

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.channel.tx_commit(callback=self.on_commitok)

    def on_commitok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.CommitOk)
        self.stop()


class TestTX2_CommitFailure(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Close the channel: commit without a TX"

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.tx_commit(callback=self.on_commitok)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)

    @staticmethod
    def on_commitok(frame):
        raise AssertionError("Should not have received a Tx.CommitOk")


class TestTX3_Rollback(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Start a transaction, then rollback"

    def begin(self, channel):
        channel.tx_select(callback=self.on_selectok)

    def on_selectok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.SelectOk)
        self.channel.tx_rollback(callback=self.on_rollbackok)

    def on_rollbackok(self, frame):
        self.assertIsInstance(frame.method, spec.Tx.RollbackOk)
        self.stop()



class TestTX3_RollbackFailure(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Close the channel: rollback without a TX"

    def begin(self, channel):
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.tx_rollback(callback=self.on_commitok)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.stop()

    @staticmethod
    def on_commitok(frame):
        raise AssertionError("Should not have received a Tx.RollbackOk")


class TestZ_PublishAndConsume(BoundQueueTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Publish a message and consume it"

    def on_ready(self, frame):
        self.ctag = self.channel.basic_consume(self.queue, self.on_message)
        self.msg_body = "%s: %i" % (self.__class__.__name__, time.time())
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   self.msg_body)

    def on_cancelled(self, frame):
        self.assertIsInstance(frame.method, spec.Basic.CancelOk)
        self.stop()

    def on_message(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.Deliver)
        self.assertEqual(body, as_bytes(self.msg_body))
        self.channel.basic_ack(method.delivery_tag)
        self.channel.basic_cancel(self.ctag, callback=self.on_cancelled)



class TestZ_PublishAndConsumeBig(BoundQueueTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Publish a big message and consume it"

    @staticmethod
    def _get_msg_body():
        return '\n'.join(["%s" % i for i in range(0, 2097152)])

    def on_ready(self, frame):
        self.ctag = self.channel.basic_consume(self.queue, self.on_message)
        self.msg_body = self._get_msg_body()
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   self.msg_body)

    def on_cancelled(self, frame):
        self.assertIsInstance(frame.method, spec.Basic.CancelOk)
        self.stop()

    def on_message(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.Deliver)
        self.assertEqual(body, as_bytes(self.msg_body))
        self.channel.basic_ack(method.delivery_tag)
        self.channel.basic_cancel(self.ctag, callback=self.on_cancelled)


class TestZ_PublishAndGet(BoundQueueTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Publish a message and get it"

    def on_ready(self, frame):
        self.msg_body = "%s: %i" % (self.__class__.__name__, time.time())
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   self.msg_body)
        self.channel.basic_get(self.queue, self.on_get)

    def on_get(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.GetOk)
        self.assertEqual(body, as_bytes(self.msg_body))
        self.channel.basic_ack(method.delivery_tag)
        self.stop()


class TestZ_AccessDenied(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Verify that access denied invokes on open error callback"

    def start(self, *args, **kwargs):
        self.parameters.virtual_host = str(uuid.uuid4())
        self.error_captured = False
        super(TestZ_AccessDenied, self).start(*args, **kwargs)
        self.assertTrue(self.error_captured)

    def on_open_error(self, connection, error):
        self.error_captured = True
        self.stop()

    def on_open(self, connection):
        super(TestZ_AccessDenied, self).on_open(connection)
        self.stop()


class TestBlockedConnectionTimesOut(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Verify that blocked connection terminates on timeout"

    def start(self, *args, **kwargs):
        self.parameters.blocked_connection_timeout = 0.001
        self.on_closed_pair = None
        super(TestBlockedConnectionTimesOut, self).start(*args, **kwargs)
        self.assertEqual(
            self.on_closed_pair,
            (pika.connection.InternalCloseReasons.BLOCKED_CONNECTION_TIMEOUT,
             'Blocked connection timeout expired'))

    def begin(self, channel):

        # Simulate Connection.Blocked
        channel.connection._on_connection_blocked(pika.frame.Method(
            0,
            pika.spec.Connection.Blocked('Testing blocked connection timeout')))

    def on_closed(self, connection, reply_code, reply_text):
        """called when the connection has finished closing"""
        self.on_closed_pair = (reply_code, reply_text)
        super(TestBlockedConnectionTimesOut, self).on_closed(connection,
                                                             reply_code,
                                                             reply_text)


class TestBlockedConnectionUnblocks(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Verify that blocked-unblocked connection closes normally"

    def start(self, *args, **kwargs):
        self.parameters.blocked_connection_timeout = 0.001
        self.on_closed_pair = None
        super(TestBlockedConnectionUnblocks, self).start(*args, **kwargs)
        self.assertEqual(
            self.on_closed_pair,
            (200, 'Normal shutdown'))

    def begin(self, channel):

        # Simulate Connection.Blocked
        channel.connection._on_connection_blocked(pika.frame.Method(
            0,
            pika.spec.Connection.Blocked(
                'Testing blocked connection unblocks')))

        # Simulate Connection.Unblocked
        channel.connection._on_connection_unblocked(pika.frame.Method(
            0,
            pika.spec.Connection.Unblocked()))

        # Schedule shutdown after blocked connection timeout would expire
        channel.connection.add_timeout(0.005, self.on_cleanup_timer)

    def on_cleanup_timer(self):
        self.stop()

    def on_closed(self, connection, reply_code, reply_text):
        """called when the connection has finished closing"""
        self.on_closed_pair = (reply_code, reply_text)
        super(TestBlockedConnectionUnblocks, self).on_closed(connection,
                                                             reply_code,
                                                             reply_text)
