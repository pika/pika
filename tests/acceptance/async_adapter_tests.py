
# too-many-lines
# pylint: disable=C0302

# Suppress pylint messages concerning missing class and method docstrings
# pylint: disable=C0111

# Suppress pylint warning about attribute defined outside __init__
# pylint: disable=W0201

# Suppress pylint warning about access to protected member
# pylint: disable=W0212

# Suppress pylint warning about unused argument
# pylint: disable=W0613

# invalid-name
# pylint: disable=C0103


import functools
import socket
import threading
import uuid

import pika
from pika.adapters.utils import connection_workflow
from pika import spec
from pika.compat import as_bytes, time_now
import pika.connection
import pika.exceptions
import pika.frame

from . import async_test_base
from .async_test_base import (AsyncTestCase, BoundQueueTestCase, AsyncAdapters)


class TestA_Connect(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Connect, open channel and disconnect"

    def begin(self, channel):
        self.stop()


class TestConstructAndImmediatelyCloseConnection(AsyncTestCase,
                                                 AsyncAdapters):
    DESCRIPTION = "Construct and immediately close connection."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        connection_class = self.connection.__class__

        params = self.new_connection_params()

        @async_test_base.make_stop_on_error_with_self(self)
        def on_opened(connection):
            self.fail('Connection should have aborted, but got '
                      'on_opened({!r})'.format(connection))

        @async_test_base.make_stop_on_error_with_self(self)
        def on_open_error(connection, error):
            self.assertIsInstance(error,
                                  pika.exceptions.ConnectionOpenAborted)
            self.stop()

        conn = connection_class(params,
                                on_open_callback=on_opened,
                                on_open_error_callback=on_open_error,
                                custom_ioloop=self.connection.ioloop)
        conn.close()


class TestCloseConnectionDuringAMQPHandshake(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Close connection during AMQP handshake."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        base_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        params = self.new_connection_params()


        class MyConnectionClass(base_class):
            # Cause an exception if _on_stream_connected doesn't exist
            base_class._on_stream_connected  # pylint: disable=W0104

            @async_test_base.make_stop_on_error_with_self(self)
            def _on_stream_connected(self, *args, **kwargs):
                # Now that AMQP handshake has begun, schedule imminent closing
                # of the connection
                self._nbio.add_callback_threadsafe(self.close)

                return super(MyConnectionClass, self)._on_stream_connected(
                    *args,
                    **kwargs)


        @async_test_base.make_stop_on_error_with_self(self)
        def on_opened(connection):
            self.fail('Connection should have aborted, but got '
                      'on_opened({!r})'.format(connection))

        @async_test_base.make_stop_on_error_with_self(self)
        def on_open_error(connection, error):
            self.assertIsInstance(error, pika.exceptions.ConnectionOpenAborted)
            self.stop()

        conn = MyConnectionClass(params,
                                 on_open_callback=on_opened,
                                 on_open_error_callback=on_open_error,
                                 custom_ioloop=self.connection.ioloop)
        conn.close()


class TestSocketConnectTimeoutWithTinySocketTimeout(AsyncTestCase,
                                                    AsyncAdapters):
    DESCRIPTION = "Force socket.connect() timeout with very tiny socket_timeout."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        connection_class = self.connection.__class__

        params = self.new_connection_params()

        # socket_timeout expects something > 0
        params.socket_timeout = 0.0000000000000000001

        @async_test_base.make_stop_on_error_with_self(self)
        def on_opened(connection):
            self.fail('Socket connection should have timed out, but got '
                      'on_opened({!r})'.format(connection))

        @async_test_base.make_stop_on_error_with_self(self)
        def on_open_error(connection, error):
            self.assertIsInstance(error,
                                  pika.exceptions.AMQPConnectionError)
            self.stop()

        connection_class(
            params,
            on_open_callback=on_opened,
            on_open_error_callback=on_open_error,
            custom_ioloop=self.connection.ioloop)


class TestStackConnectionTimeoutWithTinyStackTimeout(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Force stack bring-up timeout with very tiny stack_timeout."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        connection_class = self.connection.__class__

        params = self.new_connection_params()

        # stack_timeout expects something > 0
        params.stack_timeout = 0.0000000000000000001

        @async_test_base.make_stop_on_error_with_self(self)
        def on_opened(connection):
            self.fail('Stack connection should have timed out, but got '
                      'on_opened({!r})'.format(connection))

        def on_open_error(connection, exception):
            error = None
            if not isinstance(exception, pika.exceptions.AMQPConnectionError):
                error = AssertionError(
                    'Expected AMQPConnectionError, but got {!r}'.format(
                        exception))
            self.stop(error)

        connection_class(
            params,
            on_open_callback=on_opened,
            on_open_error_callback=on_open_error,
            custom_ioloop=self.connection.ioloop)


class TestCreateConnectionViaDefaultConnectionWorkflow(AsyncTestCase,
                                                       AsyncAdapters):
    DESCRIPTION = "Connect via adapter's create_connection() method with single config."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        configs = [self.parameters]
        connection_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(conn):
            self.assertIsInstance(conn, connection_class)
            conn.add_on_close_callback(on_my_connection_closed)
            conn.close()

        @async_test_base.make_stop_on_error_with_self(self)
        def on_my_connection_closed(_conn, error):
            self.assertIsInstance(error,
                                  pika.exceptions.ConnectionClosedByClient)
            self.stop()

        workflow = connection_class.create_connection(configs,
                                                      on_done,
                                                      self.connection.ioloop)
        self.assertIsInstance(
            workflow,
            connection_workflow.AbstractAMQPConnectionWorkflow)


class TestCreateConnectionViaCustomConnectionWorkflow(AsyncTestCase,
                                                      AsyncAdapters):
    DESCRIPTION = "Connect via adapter's create_connection() method using custom workflow."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        configs = [self.parameters]
        connection_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(conn):
            self.assertIsInstance(conn, connection_class)
            self.assertIs(conn.i_was_here, MyWorkflow)

            conn.add_on_close_callback(on_my_connection_closed)
            conn.close()

        @async_test_base.make_stop_on_error_with_self(self)
        def on_my_connection_closed(_conn, error):
            self.assertIsInstance(error,
                                  pika.exceptions.ConnectionClosedByClient)
            self.stop()

        class MyWorkflow(connection_workflow.AMQPConnectionWorkflow):
            if not hasattr(connection_workflow.AMQPConnectionWorkflow,
                           '_report_completion_and_cleanup'):
                raise AssertionError('_report_completion_and_cleanup not in '
                                     'AMQPConnectionWorkflow.')

            def _report_completion_and_cleanup(self, result):
                """Override implementation to tag the presumed connection"""
                result.i_was_here = MyWorkflow
                super(MyWorkflow, self)._report_completion_and_cleanup(result)


        original_workflow = MyWorkflow()
        workflow = connection_class.create_connection(
            configs,
            on_done,
            self.connection.ioloop,
            workflow=original_workflow)

        self.assertIs(workflow, original_workflow)


class TestCreateConnectionMultipleConfigsDefaultConnectionWorkflow(
        AsyncTestCase,
        AsyncAdapters):
    DESCRIPTION = "Connect via adapter's create_connection() method with multiple configs."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        good_params = self.parameters
        connection_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        sock = socket.socket()
        self.addCleanup(sock.close)
        sock.bind(('127.0.0.1', 0))
        bad_host, bad_port = sock.getsockname()
        sock.close()  # so that attempt to connect will fail immediately

        bad_params = pika.ConnectionParameters(host=bad_host, port=bad_port)

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(conn):
            self.assertIsInstance(conn, connection_class)
            self.assertEqual(conn.params.host, good_params.host)
            self.assertEqual(conn.params.port, good_params.port)
            self.assertNotEqual((conn.params.host, conn.params.port),
                                (bad_host, bad_port))

            conn.add_on_close_callback(on_my_connection_closed)
            conn.close()

        @async_test_base.make_stop_on_error_with_self(self)
        def on_my_connection_closed(_conn, error):
            self.assertIsInstance(error,
                                  pika.exceptions.ConnectionClosedByClient)
            self.stop()

        workflow = connection_class.create_connection([bad_params, good_params],
                                                      on_done,
                                                      self.connection.ioloop)
        self.assertIsInstance(
            workflow,
            connection_workflow.AbstractAMQPConnectionWorkflow)


class TestCreateConnectionRetriesWithDefaultConnectionWorkflow(
        AsyncTestCase,
        AsyncAdapters):
    DESCRIPTION = "Connect via adapter's create_connection() method with multiple retries."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        base_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        first_config = self.parameters

        second_config = self.new_connection_params()

        # Configure retries (default connection workflow keys off the last
        # config in the sequence)
        second_config.retry_delay = 0.001
        second_config.connection_attempts = 2

        # MyConnectionClass will use connection_attempts to distinguish between
        # first and second configs
        self.assertNotEqual(first_config.connection_attempts,
                            second_config.connection_attempts)

        logger = self.logger

        class MyConnectionClass(base_class):

            got_second_config = False

            def __init__(self, parameters, *args, **kwargs):
                logger.info('Entered MyConnectionClass constructor: %s',
                            parameters)

                if (parameters.connection_attempts ==
                        second_config.connection_attempts):
                    MyConnectionClass.got_second_config = True
                    logger.info('Got second config.')
                    raise Exception('Reject second config.')

                if not MyConnectionClass.got_second_config:
                    logger.info('Still on first attempt with first config.')
                    raise Exception('Still on first attempt with first config.')

                logger.info('Start of retry cycle detected.')

                super(MyConnectionClass, self).__init__(parameters,
                                                        *args,
                                                        **kwargs)

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(conn):
            self.assertIsInstance(conn, MyConnectionClass)
            self.assertEqual(conn.params.connection_attempts,
                             first_config.connection_attempts)

            conn.add_on_close_callback(on_my_connection_closed)
            conn.close()

        @async_test_base.make_stop_on_error_with_self(self)
        def on_my_connection_closed(_conn, error):
            self.assertIsInstance(error,
                                  pika.exceptions.ConnectionClosedByClient)
            self.stop()

        MyConnectionClass.create_connection([first_config, second_config],
                                            on_done,
                                            self.connection.ioloop)


class TestCreateConnectionConnectionWorkflowSocketConnectionFailure(
        AsyncTestCase,
        AsyncAdapters):
    DESCRIPTION = "Connect via adapter's create_connection() fails to connect socket."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        connection_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        sock = socket.socket()
        self.addCleanup(sock.close)
        sock.bind(('127.0.0.1', 0))
        bad_host, bad_port = sock.getsockname()
        sock.close()  # so that attempt to connect will fail immediately

        bad_params = pika.ConnectionParameters(host=bad_host, port=bad_port)

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(exc):
            self.assertIsInstance(
                exc,
                connection_workflow.AMQPConnectionWorkflowFailed)
            self.assertIsInstance(
                exc.exceptions[-1],
                connection_workflow.AMQPConnectorSocketConnectError)
            self.stop()

        connection_class.create_connection([bad_params,],
                                           on_done,
                                           self.connection.ioloop)


class TestCreateConnectionAMQPHandshakeTimesOutDefaultWorkflow(AsyncTestCase,
                                                               AsyncAdapters):
    DESCRIPTION = "AMQP handshake timeout handling in adapter's create_connection()."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        base_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        params = self.parameters

        workflow = None  # type: connection_workflow.AMQPConnectionWorkflow

        class MyConnectionClass(base_class):
            # Cause an exception if _on_stream_connected doesn't exist
            base_class._on_stream_connected  # pylint: disable=W0104

            @async_test_base.make_stop_on_error_with_self(self)
            def _on_stream_connected(self, *args, **kwargs):
                # Now that AMQP handshake has begun, simulate imminent stack
                # timeout in AMQPConnector
                connector = workflow._connector  # type: connection_workflow.AMQPConnector
                connector._stack_timeout_ref.cancel()
                connector._stack_timeout_ref = connector._nbio.call_later(
                    0,
                    connector._on_overall_timeout)

                return super(MyConnectionClass, self)._on_stream_connected(
                    *args,
                    **kwargs)

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(error):
            self.assertIsInstance(
                error,
                connection_workflow.AMQPConnectionWorkflowFailed)
            self.assertIsInstance(
                error.exceptions[-1],
                connection_workflow.AMQPConnectorAMQPHandshakeError)
            self.assertIsInstance(
                error.exceptions[-1].exception,
                connection_workflow.AMQPConnectorStackTimeout)

            self.stop()

        workflow = MyConnectionClass.create_connection([params],
                                                       on_done,
                                                       self.connection.ioloop)


class TestCreateConnectionAndImmediatelyAbortDefaultConnectionWorkflow(
        AsyncTestCase,
        AsyncAdapters):
    DESCRIPTION = "Immediately abort workflow initiated via adapter's create_connection()."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        configs = [self.parameters]
        connection_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(exc):
            self.assertIsInstance(
                exc,
                connection_workflow.AMQPConnectionWorkflowAborted)
            self.stop()

        workflow = connection_class.create_connection(configs,
                                                      on_done,
                                                      self.connection.ioloop)
        workflow.abort()


class TestCreateConnectionAndAsynchronouslyAbortDefaultConnectionWorkflow(
        AsyncTestCase,
        AsyncAdapters):
    DESCRIPTION = "Asyncrhonously abort workflow initiated via adapter's create_connection()."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        configs = [self.parameters]
        connection_class = self.connection.__class__  # type: pika.adapters.BaseConnection

        @async_test_base.make_stop_on_error_with_self(self)
        def on_done(exc):
            self.assertIsInstance(
                exc,
                connection_workflow.AMQPConnectionWorkflowAborted)
            self.stop()

        workflow = connection_class.create_connection(configs,
                                                      on_done,
                                                      self.connection.ioloop)
        self.connection._nbio.add_callback_threadsafe(workflow.abort)


class TestConfirmSelect(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Receive confirmation of Confirm.Select"

    def begin(self, channel):
        channel.confirm_delivery(ack_nack_callback=self.ack_nack_callback,
                                 callback=self.on_complete)

    @staticmethod
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
                                         time_now())
            self.channel.basic_publish('', self.queue_name, msg_body)
        self.ctag = self.channel.basic_consume(self.queue_name,
                                               self.on_message,
                                               auto_ack=True)

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

    def on_channel_closed(self, _channel, _reason):
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
        raise AssertionError("Should not have received an Exchange.DeclareOk")


class TestNoDeadlockWhenClosingChannelWithPendingBlockedRequestsAndConcurrentChannelCloseFromBroker(
        AsyncTestCase, AsyncAdapters):
    DESCRIPTION = ("No deadlock when closing a channel with pending blocked "
                   "requests and concurrent Channel.Close from broker.")

    # To observe the behavior that this is testing, comment out this line
    # in pika/channel.py - _on_close:
    #
    # self._drain_blocked_methods_on_remote_close()
    #
    # With the above line commented out, this test will hang

    def begin(self, channel):
        base_exch_name = self.__class__.__name__ + ':' + uuid.uuid1().hex
        self.channel.add_on_close_callback(self.on_channel_closed)
        for i in range(0, 99):
            # Passively declare a non-existent exchange to force Channel.Close
            # from broker
            exch_name = base_exch_name + ':' + str(i)
            cb = functools.partial(self.on_bad_result, exch_name)
            channel.exchange_declare(exch_name,
                                     exchange_type='direct',
                                     passive=True,
                                     callback=cb)
        channel.close()

    def on_channel_closed(self, _channel, _reason):
        # The close is expected because the requested exchange doesn't exist
        self.stop()

    def on_bad_result(self, exch_name, frame):
        self.fail("Should not have received an Exchange.DeclareOk")


class TestClosingAChannelPermitsBlockedRequestToComplete(AsyncTestCase,
                                                         AsyncAdapters):
    DESCRIPTION = "Closing a channel permits blocked requests to complete."

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        self._queue_deleted = False

        channel.add_on_close_callback(self.on_channel_closed)

        q_name = self.__class__.__name__ + ':' + uuid.uuid1().hex
        # NOTE we pass callback to make it a blocking request
        channel.queue_declare(q_name,
                              exclusive=True,
                              callback=lambda _frame: None)

        self.assertIsNotNone(channel._blocking)

        # The Queue.Delete should block on completion of Queue.Declare
        channel.queue_delete(q_name, callback=self.on_queue_deleted)
        self.assertTrue(channel._blocked)

        # This Channel.Close should allow the blocked Queue.Delete to complete
        # Before closing the channel
        channel.close()

    def on_queue_deleted(self, _frame):
        # Getting this callback shows that the blocked request was processed
        self._queue_deleted = True

    @async_test_base.stop_on_error_in_async_test_case_method
    def on_channel_closed(self, _channel, _reason):
        self.assertTrue(self._queue_deleted)
        self.stop()


class TestQueueUnnamedDeclareAndDelete(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Create and delete an unnamed queue"

    @async_test_base.stop_on_error_in_async_test_case_method
    def begin(self, channel):
        channel.queue_declare(queue='',
                              passive=False,
                              durable=False,
                              exclusive=True,
                              auto_delete=False,
                              arguments={'x-expires': self.TIMEOUT * 1000},
                              callback=self.on_queue_declared)

    @async_test_base.stop_on_error_in_async_test_case_method
    def on_queue_declared(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeclareOk)
        self.channel.queue_delete(frame.method.queue, callback=self.on_queue_delete)

    @async_test_base.stop_on_error_in_async_test_case_method
    def on_queue_delete(self, frame):
        self.assertIsInstance(frame.method, spec.Queue.DeleteOk)
        self.stop()


class TestQueueNamedDeclareAndDelete(AsyncTestCase, AsyncAdapters):
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

    def on_channel_closed(self, _channel, _reason):
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

    def on_channel_closed(self, _channel, _reason):
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

    def on_channel_closed(self, _channel, _reason):
        self.stop()

    @staticmethod
    def on_commitok(frame):
        raise AssertionError("Should not have received a Tx.RollbackOk")


class TestZ_PublishAndConsume(BoundQueueTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Publish a message and consume it"

    def on_ready(self, frame):
        self.ctag = self.channel.basic_consume(self.queue, self.on_message)
        self.msg_body = "%s: %i" % (self.__class__.__name__, time_now())
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
        self.msg_body = "%s: %i" % (self.__class__.__name__, time_now())
        self.channel.basic_publish(self.exchange, self.routing_key,
                                   self.msg_body)
        self.channel.basic_get(self.queue, self.on_get)

    def on_get(self, channel, method, header, body):
        self.assertIsInstance(method, spec.Basic.GetOk)
        self.assertEqual(body, as_bytes(self.msg_body))
        self.channel.basic_ack(method.delivery_tag)
        self.stop()


class TestZ_AccessDenied(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Unknown vhost results in ProbableAccessDeniedError."

    def start(self, *args, **kwargs):  # pylint: disable=W0221
        self.parameters.virtual_host = str(uuid.uuid4())
        self.error_captured = None
        super(TestZ_AccessDenied, self).start(*args, **kwargs)
        self.assertIsInstance(self.error_captured,
                              pika.exceptions.ProbableAccessDeniedError)

    def on_open_error(self, connection, error):
        self.error_captured = error
        self.stop()

    def on_open(self, connection):
        super(TestZ_AccessDenied, self).on_open(connection)
        self.stop()


class TestBlockedConnectionTimesOut(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Verify that blocked connection terminates on timeout"

    def start(self, *args, **kwargs):  # pylint: disable=W0221
        self.parameters.blocked_connection_timeout = 0.001
        self.on_closed_error = None
        super(TestBlockedConnectionTimesOut, self).start(*args, **kwargs)
        self.assertIsInstance(self.on_closed_error,
                              pika.exceptions.ConnectionBlockedTimeout)

    def begin(self, channel):

        # Simulate Connection.Blocked
        channel.connection._on_connection_blocked(
            channel.connection,
            pika.frame.Method(0,
                              spec.Connection.Blocked(
                                  'Testing blocked connection timeout')))

    def on_closed(self, connection, error):
        """called when the connection has finished closing"""
        self.on_closed_error = error
        self.stop()  # acknowledge that closed connection is expected
        super(TestBlockedConnectionTimesOut, self).on_closed(connection, error)


class TestBlockedConnectionUnblocks(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Verify that blocked-unblocked connection closes normally"

    def start(self, *args, **kwargs):  # pylint: disable=W0221
        self.parameters.blocked_connection_timeout = 0.001
        self.on_closed_error = None
        super(TestBlockedConnectionUnblocks, self).start(*args, **kwargs)
        self.assertIsInstance(self.on_closed_error,
                              pika.exceptions.ConnectionClosedByClient)
        self.assertEqual(
            (self.on_closed_error.reply_code, self.on_closed_error.reply_text),
            (200, 'Normal shutdown'))

    def begin(self, channel):

        # Simulate Connection.Blocked
        channel.connection._on_connection_blocked(
            channel.connection,
            pika.frame.Method(0,
                              spec.Connection.Blocked(
                                  'Testing blocked connection unblocks')))

        # Simulate Connection.Unblocked
        channel.connection._on_connection_unblocked(
            channel.connection,
            pika.frame.Method(0, spec.Connection.Unblocked()))

        # Schedule shutdown after blocked connection timeout would expire
        channel.connection._adapter_call_later(0.005, self.on_cleanup_timer)

    def on_cleanup_timer(self):
        self.stop()

    def on_closed(self, connection, error):
        """called when the connection has finished closing"""
        self.on_closed_error = error
        super(TestBlockedConnectionUnblocks, self).on_closed(connection, error)


class TestAddCallbackThreadsafeRequestBeforeIOLoopStarts(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = (
        "Test _adapter_add_callback_threadsafe request before ioloop starts.")

    def _run_ioloop(self, *args, **kwargs):  # pylint: disable=W0221
        """We intercept this method from AsyncTestCase in order to call
        _adapter_add_callback_threadsafe before AsyncTestCase starts the ioloop.

        """
        self.my_start_time = time_now()
        # Request a callback from our current (ioloop's) thread
        self.connection._adapter_add_callback_threadsafe(
            self.on_requested_callback)

        return super(
            TestAddCallbackThreadsafeRequestBeforeIOLoopStarts, self)._run_ioloop(
                *args, **kwargs)

    def start(self, *args, **kwargs):  # pylint: disable=W0221
        self.loop_thread_ident = threading.current_thread().ident
        self.my_start_time = None
        self.got_callback = False
        super(TestAddCallbackThreadsafeRequestBeforeIOLoopStarts, self).start(*args, **kwargs)
        self.assertTrue(self.got_callback)

    def begin(self, channel):
        self.stop()

    def on_requested_callback(self):
        self.assertEqual(threading.current_thread().ident,
                         self.loop_thread_ident)
        self.assertLess(time_now() - self.my_start_time, 0.25)
        self.got_callback = True


class TestAddCallbackThreadsafeFromIOLoopThread(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = (
        "Test _adapter_add_callback_threadsafe request from same thread.")

    def start(self, *args, **kwargs):  # pylint: disable=W0221
        self.loop_thread_ident = threading.current_thread().ident
        self.my_start_time = None
        self.got_callback = False
        super(TestAddCallbackThreadsafeFromIOLoopThread, self).start(*args, **kwargs)
        self.assertTrue(self.got_callback)

    def begin(self, channel):
        self.my_start_time = time_now()
        # Request a callback from our current (ioloop's) thread
        channel.connection._adapter_add_callback_threadsafe(
            self.on_requested_callback)

    def on_requested_callback(self):
        self.assertEqual(threading.current_thread().ident,
                         self.loop_thread_ident)
        self.assertLess(time_now() - self.my_start_time, 0.25)
        self.got_callback = True
        self.stop()


class TestAddCallbackThreadsafeFromAnotherThread(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = (
        "Test _adapter_add_callback_threadsafe request from another thread.")

    def start(self, *args, **kwargs):  # pylint: disable=W0221
        self.loop_thread_ident = threading.current_thread().ident
        self.my_start_time = None
        self.got_callback = False
        super(TestAddCallbackThreadsafeFromAnotherThread, self).start(*args, **kwargs)
        self.assertTrue(self.got_callback)

    def begin(self, channel):
        self.my_start_time = time_now()
        # Request a callback from ioloop while executing in another thread
        timer = threading.Timer(
            0,
            lambda: channel.connection._adapter_add_callback_threadsafe(
                self.on_requested_callback))
        self.addCleanup(timer.cancel)
        timer.start()

    def on_requested_callback(self):
        self.assertEqual(threading.current_thread().ident,
                         self.loop_thread_ident)
        self.assertLess(time_now() - self.my_start_time, 0.25)
        self.got_callback = True
        self.stop()


class TestIOLoopStopBeforeIOLoopStarts(AsyncTestCase, AsyncAdapters):
    DESCRIPTION = "Test ioloop.stop() before ioloop starts causes ioloop to exit quickly."

    def _run_ioloop(self, *args, **kwargs):  # pylint: disable=W0221
        """We intercept this method from AsyncTestCase in order to call
        ioloop.stop() before AsyncTestCase starts the ioloop.
        """
        # Request ioloop to stop before it starts
        my_start_time = time_now()
        self.stop_ioloop_only()

        super(
            TestIOLoopStopBeforeIOLoopStarts, self)._run_ioloop(*args, **kwargs)

        self.assertLess(time_now() - my_start_time, 0.25)

        # Resume I/O loop to facilitate normal course of events and closure
        # of connection in order to prevent reporting of a socket resource leak
        # from an unclosed connection.
        super(
            TestIOLoopStopBeforeIOLoopStarts, self)._run_ioloop(*args, **kwargs)

    def begin(self, channel):
        self.stop()


class TestViabilityOfMultipleTimeoutsWithSameDeadlineAndCallback(AsyncTestCase, AsyncAdapters):  # pylint: disable=C0103
    DESCRIPTION = "Test viability of multiple timeouts with same deadline and callback"

    def begin(self, channel):
        timer1 = channel.connection._adapter_call_later(0, self.on_my_timer)
        timer2 = channel.connection._adapter_call_later(0, self.on_my_timer)

        self.assertIsNot(timer1, timer2)

        channel.connection._adapter_remove_timeout(timer1)

        # Wait for timer2 to fire

    def on_my_timer(self):
        self.stop()
