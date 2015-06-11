"""blocking adapter test"""
from datetime import datetime
import logging
import socket
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import uuid

from forward_server import ForwardServer

import pika
from pika.adapters import blocking_connection
import pika.connection
import pika.exceptions

# Disable warning about access to protected member
# pylint: disable=W0212

# Disable warning Attribute defined outside __init__
# pylint: disable=W0201

# Disable warning Missing docstring
# pylint: disable=C0111

# Disable warning Too many public methods
# pylint: disable=R0904

# Disable warning Invalid variable name
# pylint: disable=C0103

LOGGER = logging.getLogger(__name__)
PARAMS_URL_TEMPLATE = (
    'amqp://guest:guest@127.0.0.1:%(port)s/%%2f?socket_timeout=1')
DEFAULT_URL = PARAMS_URL_TEMPLATE % {'port': 5672}
DEFAULT_PARAMS = pika.URLParameters(DEFAULT_URL)
DEFAULT_TIMEOUT = 15


def _p3_as_bytes(value):
    if pika.compat.PY3:
        return pika.compat.as_bytes(value)
    else:
        return value


class BlockingTestCaseBase(unittest.TestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def _connect(self,
                 url=DEFAULT_URL,
                 connection_class=pika.BlockingConnection,
                 impl_class=None):
        parameters = pika.URLParameters(url)
        connection = connection_class(parameters, _impl_class=impl_class)
        self.addCleanup(lambda: connection.close()
                        if connection.is_open else None)

        connection._impl.add_timeout(
            self.TIMEOUT, # pylint: disable=E1101
            self._on_test_timeout)

        return connection

    def _on_test_timeout(self):
        """Called when test times out"""
        LOGGER.info('%s TIMED OUT (%s)', datetime.utcnow(), self)
        self.fail('Test timed out')


class TestCreateAndCloseConnection(BlockingTestCaseBase):

    def test(self):
        """Create connection, channel, and consumer, then close connection"""
        connection = self._connect()
        self.assertIsInstance(connection, pika.BlockingConnection)
        self.assertTrue(connection.is_open)
        self.assertFalse(connection.is_closed)
        self.assertFalse(connection.is_closing)

        connection.close()
        self.assertTrue(connection.is_closed)
        self.assertFalse(connection.is_open)
        self.assertFalse(connection.is_closing)


class TestInvalidExchangeTypeRaisesConnectionClosed(BlockingTestCaseBase):
    def test(self):
        """ConnectionClosed raised when creating exchange with invalid type"""
        # This test exploits behavior specific to RabbitMQ whereby the broker
        # closes the connection if an attempt is made to declare an exchange
        # with an invalid exchange type
        connection = self._connect()
        ch = connection.channel()

        exg_name = ("TestInvalidExchangeTypeRaisesConnectionClosed_" +
                    uuid.uuid1().hex)

        with self.assertRaises(pika.exceptions.ConnectionClosed) as ex_cm:
            # Attempt to create an exchange with invalid exchange type
            ch.exchange_declare(exg_name, exchange_type='ZZwwInvalid')

        self.assertEqual(ex_cm.exception.args[0], 503)


class TestCreateAndCloseConnectionWithChannelAndConsumer(BlockingTestCaseBase):

    def test(self):
        """Create and close connection"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestCreateAndCloseConnectionWithChannelAndConsumer_q' +
            uuid.uuid1().hex)

        body1 = _p3_as_bytes('a' * 1024)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Publish the message to the queue by way of default exchange
        ch.publish(exchange='', routing_key=q_name, body=body1)

        # Create a non-ackable consumer
        ch.basic_consume(lambda *x: None, q_name, no_ack=True,
                         exclusive=False, arguments=None)

        connection.close()
        self.assertTrue(connection.is_closed)
        self.assertFalse(connection.is_open)
        self.assertFalse(connection.is_closing)

        self.assertFalse(connection._impl._channels)

        self.assertFalse(ch._consumer_infos)
        self.assertFalse(ch._impl._consumers)


class TestSuddenBrokerDisconnectBeforeChannel(BlockingTestCaseBase):

    def test(self):
        """BlockingConnection resets properly on TCP/IP drop during channel()
        """
        with ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port)) as fwd:
            self.connection = self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection._impl.socket)


class TestNoAccessToFileDescriptorAfterConnectionClosed(BlockingTestCaseBase):

    def test(self):
        """BlockingConnection no access file descriptor after ConnectionClosed
        """
        with ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port)) as fwd:
            self.connection = self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection._impl.socket)

        # Attempt to operate on the connection once again after ConnectionClosed
        self.assertIsNone(self.connection._impl.socket)
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.connection.channel()


class TestConnectWithDownedBroker(BlockingTestCaseBase):

    def test(self):
        """ BlockingConnection to downed broker results in AMQPConnectionError

        """
        # Reserve a port for use in connect
        sock = socket.socket()
        self.addCleanup(sock.close)

        sock.bind(("127.0.0.1", 0))

        port = sock.getsockname()[1]

        sock.close()

        with self.assertRaises(pika.exceptions.AMQPConnectionError):
            self.connection = self._connect(
                PARAMS_URL_TEMPLATE % {"port": port})


class TestDisconnectDuringConnectionStart(BlockingTestCaseBase):

    def test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_START
        """
        fwd = ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connection_start')

            def _on_connection_start(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connection_start(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.ProbableAuthenticationError):
            self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
                impl_class=MySelectConnection)


class TestDisconnectDuringConnectionTune(BlockingTestCaseBase):

    def test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_TUNE
        """
        fwd = ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connection_tune')

            def _on_connection_tune(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connection_tune(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.ProbableAccessDeniedError):
            self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
                impl_class=MySelectConnection)


class TestDisconnectDuringConnectionProtocol(BlockingTestCaseBase):

    def test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_PROTOCOL
        """
        fwd = ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connected')

            def _on_connected(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connected(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.IncompatibleProtocolError):
            self._connect(PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
                          impl_class=MySelectConnection)


class TestProcessDataEvents(BlockingTestCaseBase):

    def test(self):
        """BlockingConnection.process_data_events"""
        connection = self._connect()

        # Try with time_limit=0
        start_time = time.time()
        connection.process_data_events(time_limit=0)
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)

        # Try with time_limit=0.005
        start_time = time.time()
        connection.process_data_events(time_limit=0.005)
        elapsed = time.time() - start_time
        self.assertGreaterEqual(elapsed, 0.005)
        self.assertLess(elapsed, 0.25)


class TestAddTimeoutRemoveTimeout(BlockingTestCaseBase):

    def test(self):
        """BlockingConnection.add_timeout and remove_timeout"""
        connection = self._connect()

        # Test timer completion
        start_time = time.time()
        rx_callback = []
        timer_id = connection.add_timeout(
            0.005,
            lambda: rx_callback.append(time.time()))
        while not rx_callback:
            connection.process_data_events(time_limit=None)

        self.assertEqual(len(rx_callback), 1)
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)

        # Test removing triggered timeout
        connection.remove_timeout(timer_id)


        # Test aborted timer
        rx_callback = []
        timer_id = connection.add_timeout(
            0.001,
            lambda: rx_callback.append(time.time()))
        connection.remove_timeout(timer_id)
        connection.process_data_events(time_limit=0.1)
        self.assertFalse(rx_callback)


class TestRemoveTimeoutFromTimeoutCallback(BlockingTestCaseBase):

    def test(self):
        """BlockingConnection.remove_timeout from timeout callback"""
        connection = self._connect()

        # Test timer completion
        timer_id1 = connection.add_timeout(5, lambda: 0/0)

        rx_timer2 = []
        def on_timer2():
            connection.remove_timeout(timer_id1)
            connection.remove_timeout(timer_id2)
            rx_timer2.append(1)

        timer_id2 = connection.add_timeout(0, on_timer2)

        while not rx_timer2:
            connection.process_data_events(time_limit=None)

        self.assertNotIn(timer_id1, connection._impl.ioloop._timeouts)
        self.assertFalse(connection._pending_timers)


class TestSleep(BlockingTestCaseBase):

    def test(self):
        """BlockingConnection.sleep"""
        connection = self._connect()

        # Try with duration=0
        start_time = time.time()
        connection.sleep(duration=0)
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)

        # Try with duration=0.005
        start_time = time.time()
        connection.sleep(duration=0.005)
        elapsed = time.time() - start_time
        self.assertGreaterEqual(elapsed, 0.005)
        self.assertLess(elapsed, 0.25)


class TestConnectionProperties(BlockingTestCaseBase):

    def test(self):
        """Test BlockingConnection properties"""
        connection = self._connect()

        self.assertTrue(connection.is_open)
        self.assertFalse(connection.is_closing)
        self.assertFalse(connection.is_closed)

        self.assertTrue(connection.basic_nack_supported)
        self.assertTrue(connection.consumer_cancel_notify_supported)
        self.assertTrue(connection.exchange_exchange_bindings_supported)
        self.assertTrue(connection.publisher_confirms_supported)

        connection.close()
        self.assertFalse(connection.is_open)
        self.assertFalse(connection.is_closing)
        self.assertTrue(connection.is_closed)



class TestCreateAndCloseChannel(BlockingTestCaseBase):

    def test(self):
        """Create and close channel"""
        connection = self._connect()

        ch = connection.channel()
        self.assertIsInstance(ch, blocking_connection.BlockingChannel)
        self.assertTrue(ch.is_open)
        self.assertFalse(ch.is_closed)
        self.assertFalse(ch.is_closing)
        self.assertIs(ch.connection, connection)

        ch.close()
        self.assertTrue(ch.is_closed)
        self.assertFalse(ch.is_open)
        self.assertFalse(ch.is_closing)


class TestExchangeDeclareAndDelete(BlockingTestCaseBase):

    def test(self):
        """Test exchange_declare and exchange_delete"""
        connection = self._connect()

        ch = connection.channel()

        name = "TestExchangeDeclareAndDelete_" + uuid.uuid1().hex

        # Declare a new exchange
        frame = ch.exchange_declare(name, exchange_type='direct')
        self.addCleanup(connection.channel().exchange_delete, name)

        self.assertIsInstance(frame.method, pika.spec.Exchange.DeclareOk)

        # Check if it exists by declaring it passively
        frame = ch.exchange_declare(name, passive=True)
        self.assertIsInstance(frame.method, pika.spec.Exchange.DeclareOk)

        # Delete the exchange
        frame = ch.exchange_delete(name)
        self.assertIsInstance(frame.method, pika.spec.Exchange.DeleteOk)

        # Verify that it's been deleted
        with self.assertRaises(pika.exceptions.ChannelClosed) as cm:
            ch.exchange_declare(name, passive=True)

        self.assertEqual(cm.exception.args[0], 404)


class TestQueueDeclareAndDelete(BlockingTestCaseBase):

    def test(self):
        """Test queue_declare and queue_delete"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestQueueDeclareAndDelete_' + uuid.uuid1().hex)

        # Declare a new queue
        frame = ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        self.assertIsInstance(frame.method, pika.spec.Queue.DeclareOk)

        # Check if it exists by declaring it passively
        frame = ch.queue_declare(q_name, passive=True)
        self.assertIsInstance(frame.method, pika.spec.Queue.DeclareOk)

        # Delete the queue
        frame = ch.queue_delete(q_name)
        self.assertIsInstance(frame.method, pika.spec.Queue.DeleteOk)

        # Verify that it's been deleted
        with self.assertRaises(pika.exceptions.ChannelClosed) as cm:
            ch.queue_declare(q_name, passive=True)

        self.assertEqual(cm.exception.args[0], 404)


class TestPassiveQueueDeclareOfUnknownQueueRaisesChannelClosed(
        BlockingTestCaseBase):
    def test(self):
        """ChannelClosed raised when passive-declaring unknown queue"""
        connection = self._connect()
        ch = connection.channel()

        q_name = ("TestPassiveQueueDeclareOfUnknownQueueRaisesChannelClosed_q_"
                  + uuid.uuid1().hex)

        with self.assertRaises(pika.exceptions.ChannelClosed) as ex_cm:
            ch.queue_declare(q_name, passive=True)

        self.assertEqual(ex_cm.exception.args[0], 404)


class TestQueueBindAndUnbindAndPurge(BlockingTestCaseBase):

    def test(self):
        """Test queue_bind and queue_unbind"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestQueueBindAndUnbindAndPurge_q' + uuid.uuid1().hex)
        exg_name = _p3_as_bytes(
            'TestQueueBindAndUnbindAndPurge_exg_' + uuid.uuid1().hex)
        routing_key = _p3_as_bytes('TestQueueBindAndUnbindAndPurge')

        # Place channel in publisher-acknowledgments mode so that we may test
        # whether the queue is reachable by publishing with mandatory=True
        res = ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCleanup(connection.channel().exchange_delete, exg_name)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Bind the queue to the exchange using routing key
        frame = ch.queue_bind(q_name, exchange=exg_name,
                              routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Queue.BindOk)

        # Check that the queue is empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Deposit a message in the queue
        ch.publish(exg_name, routing_key, body='TestQueueBindAndUnbindAndPurge',
                   mandatory=True)

        # Check that the queue now has one message
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 1)

        # Unbind the queue
        frame = ch.queue_unbind(queue=q_name, exchange=exg_name,
                                routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Queue.UnbindOk)

        # Verify that the queue is now unreachable via that binding
        with self.assertRaises(pika.exceptions.UnroutableError):
            ch.publish(exg_name, routing_key,
                       body='TestQueueBindAndUnbindAndPurge-2',
                       mandatory=True)

        # Purge the queue and verify that 1 message was purged
        frame = ch.queue_purge(q_name)
        self.assertIsInstance(frame.method, pika.spec.Queue.PurgeOk)
        self.assertEqual(frame.method.message_count, 1)

        # Verify that the queue is now empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicGet(BlockingTestCaseBase):

    def tearDown(self):
        LOGGER.info('%s TEARING DOWN (%s)', datetime.utcnow(), self)

    def test(self):
        """BlockingChannel.basic_get"""
        LOGGER.info('%s STARTED (%s)', datetime.utcnow(), self)

        connection = self._connect()
        LOGGER.info('%s CONNECTED (%s)', datetime.utcnow(), self)

        ch = connection.channel()
        LOGGER.info('%s CREATED CHANNEL (%s)', datetime.utcnow(), self)

        q_name = _p3_as_bytes('TestBasicGet_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()
        LOGGER.info('%s ENABLED PUB-ACKS (%s)', datetime.utcnow(), self)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)
        LOGGER.info('%s DECLARED QUEUE (%s)', datetime.utcnow(), self)

        # Verify result of getting a message from an empty queue
        msg = ch.basic_get(q_name, no_ack=False)
        self.assertTupleEqual(msg, (None, None, None))
        LOGGER.info('%s GOT FROM EMPTY QUEUE (%s)', datetime.utcnow(), self)

        # Deposit a message in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicGet'),
                   mandatory=True)
        LOGGER.info('%s PUBLISHED (%s)', datetime.utcnow(), self)

        # Get the message
        (method, properties, body) = ch.basic_get(q_name, no_ack=False)
        LOGGER.info('%s GOT FROM NON-EMPTY QUEUE (%s)', datetime.utcnow(), self)
        self.assertIsInstance(method, pika.spec.Basic.GetOk)
        self.assertEqual(method.delivery_tag, 1)
        self.assertFalse(method.redelivered)
        self.assertEqual(method.exchange, _p3_as_bytes(''))
        self.assertEqual(method.routing_key, q_name)
        self.assertEqual(method.message_count, 0)

        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertIsNone(properties.headers)
        self.assertEqual(body, _p3_as_bytes('TestBasicGet'))

        # Ack it
        ch.basic_ack(delivery_tag=method.delivery_tag)
        LOGGER.info('%s ACKED (%s)', datetime.utcnow(), self)

        # Verify that the queue is now empty
        frame = ch.queue_declare(q_name, passive=True)
        LOGGER.info('%s DECLARE PASSIVE QUEUE DONE (%s)',
                    datetime.utcnow(), self)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicReject(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.basic_reject"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestBasicReject_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Deposit two messages in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicReject1'),
                   mandatory=True)
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicReject2'),
                   mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body, pika.compat.as_bytes('TestBasicReject1'))

        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body, pika.compat.as_bytes('TestBasicReject2'))

        # Nack the second message
        ch.basic_reject(rx_method.delivery_tag, requeue=True)

        # Verify that exactly one message is present in the queue, namely the
        # second one
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 1)
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body, pika.compat.as_bytes('TestBasicReject2'))


class TestBasicRejectNoRequeue(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.basic_reject with requeue=False"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestBasicRejectNoRequeue_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Deposit two messages in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicRejectNoRequeue1'),
                   mandatory=True)
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicRejectNoRequeue2'),
                   mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicRejectNoRequeue1'))

        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicRejectNoRequeue2'))

        # Nack the second message
        ch.basic_reject(rx_method.delivery_tag, requeue=False)

        # Verify that no messages are present in the queue
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicNack(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.basic_nack single message"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestBasicNack_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Deposit two messages in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicNack1'),
                   mandatory=True)
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicNack2'),
                   mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body, pika.compat.as_bytes('TestBasicNack1'))

        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body, pika.compat.as_bytes('TestBasicNack2'))

        # Nack the second message
        ch.basic_nack(rx_method.delivery_tag, multiple=False, requeue=True)

        # Verify that exactly one message is present in the queue, namely the
        # second one
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 1)
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body, pika.compat.as_bytes('TestBasicNack2'))


class TestBasicNackNoRequeue(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.basic_nack with requeue=False"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestBasicNackNoRequeue_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Deposit two messages in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicNackNoRequeue1'),
                   mandatory=True)
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicNackNoRequeue2'),
                   mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicNackNoRequeue1'))

        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicNackNoRequeue2'))

        # Nack the second message
        ch.basic_nack(rx_method.delivery_tag, requeue=False)

        # Verify that no messages are present in the queue
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicNackMultiple(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.basic_nack multiple messages"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestBasicNackMultiple_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Deposit two messages in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicNackMultiple1'),
                   mandatory=True)
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicNackMultiple2'),
                   mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicNackMultiple1'))

        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicNackMultiple2'))

        # Nack both messages via the "multiple" option
        ch.basic_nack(rx_method.delivery_tag, multiple=True, requeue=True)

        # Verify that both messages are present in the queue
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 2)
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicNackMultiple1'))
        (rx_method, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicNackMultiple2'))


class TestBasicRecoverWithRequeue(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.basic_recover with requeue=True.

        NOTE: the requeue=False option is not supported by RabbitMQ broker as
        of this writing (using RabbitMQ 3.5.1)
        """
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestBasicRecoverWithRequeue_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Deposit two messages in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicRecoverWithRequeue1'),
                   mandatory=True)
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestBasicRecoverWithRequeue2'),
                   mandatory=True)

        rx_messages = []
        num_messages = 0
        for msg in ch.consume(q_name, no_ack=False):
            num_messages += 1

            if num_messages == 2:
                ch.basic_recover(requeue=True)

            if num_messages > 2:
                rx_messages.append(msg)

            if num_messages == 4:
                break
        else:
            self.fail('consumer aborted prematurely')

        # Get the messages
        (_, _, rx_body) = rx_messages[0]
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicRecoverWithRequeue1'))

        (_, _, rx_body) = rx_messages[1]
        self.assertEqual(rx_body,
                         pika.compat.as_bytes('TestBasicRecoverWithRequeue2'))


class TestTxCommit(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.tx_commit"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestTxCommit_q' + uuid.uuid1().hex)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Select standard transaction mode
        frame = ch.tx_select()
        self.assertIsInstance(frame.method, pika.spec.Tx.SelectOk)

        # Deposit a message in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestTxCommit1'),
                   mandatory=True)

        # Verify that queue is still empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Commit the transaction
        ch.tx_commit()

        # Verify that the queue has the expected message
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 1)

        (_, _, rx_body) = ch.basic_get(q_name, no_ack=False)
        self.assertEqual(rx_body, pika.compat.as_bytes('TestTxCommit1'))


class TestTxRollback(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.tx_commit"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes('TestTxRollback_q' + uuid.uuid1().hex)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Select standard transaction mode
        frame = ch.tx_select()
        self.assertIsInstance(frame.method, pika.spec.Tx.SelectOk)

        # Deposit a message in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name,
                   body=_p3_as_bytes('TestTxRollback1'),
                   mandatory=True)

        # Verify that queue is still empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Roll back the transaction
        ch.tx_rollback()

        # Verify that the queue continues to be empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicConsumeFromUnknownQueueRaisesChannelClosed(BlockingTestCaseBase):
    def test(self):
        """ChannelClosed raised when consuming from unknown queue"""
        connection = self._connect()
        ch = connection.channel()

        q_name = ("TestBasicConsumeFromUnknownQueueRaisesChannelClosed_q_" +
                  uuid.uuid1().hex)

        with self.assertRaises(pika.exceptions.ChannelClosed) as ex_cm:
            ch.basic_consume(lambda *args: None, q_name)

        self.assertEqual(ex_cm.exception.args[0], 404)


class TestPublishAndConsumeWithPubacksAndQosOfOne(BlockingTestCaseBase):

    def test(self):  # pylint: disable=R0914
        """BlockingChannel.basic_publish, publish, basic_consume, QoS, \
        Basic.Cancel from broker
        """
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestPublishAndConsumeAndQos_q' + uuid.uuid1().hex)
        exg_name = _p3_as_bytes(
            'TestPublishAndConsumeAndQos_exg_' + uuid.uuid1().hex)
        routing_key = _p3_as_bytes('TestPublishAndConsumeAndQos')

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        res = ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCleanup(connection.channel().exchange_delete, exg_name)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Verify unroutable message handling using basic_publish
        res = ch.basic_publish(exg_name, routing_key=routing_key, body='',
                               mandatory=True)
        self.assertEqual(res, False)

        # Verify unroutable message handling using publish
        with self.assertRaises(pika.exceptions.UnroutableError) as cm:
            ch.publish(exg_name, routing_key=routing_key, body='',
                       mandatory=True)
        (msg,) = cm.exception.messages
        self.assertIsInstance(msg, blocking_connection.ReturnedMessage)
        self.assertIsInstance(msg.method, pika.spec.Basic.Return)
        self.assertEqual(msg.method.reply_code, 312)
        self.assertEqual(msg.method.exchange, exg_name)
        self.assertEqual(msg.method.routing_key, routing_key)
        self.assertIsInstance(msg.properties, pika.BasicProperties)
        self.assertEqual(msg.body, _p3_as_bytes(''))

        # Bind the queue to the exchange using routing key
        frame = ch.queue_bind(q_name, exchange=exg_name,
                              routing_key=routing_key)

        # Deposit a message in the queue via basic_publish
        res = ch.basic_publish(exg_name, routing_key=routing_key,
                               body='via-basic_publish',
                               mandatory=True)
        self.assertEqual(res, True)

        # Deposit another message in the queue via publish
        ch.publish(exg_name, routing_key, body='via-publish',
                   mandatory=True)

        # Check that the queue now has two messages
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 2)

        # Configure QoS for one message
        ch.basic_qos(prefetch_size=0, prefetch_count=1, all_channels=False)

        # Create a consumer
        rx_messages = []
        consumer_tag = ch.basic_consume(
            lambda *args: rx_messages.append(args),
            q_name,
            no_ack=False,
            exclusive=False,
            arguments=None)

        # Wait for first message to arrive
        while not rx_messages:
            connection.process_data_events(time_limit=None)

        self.assertEqual(len(rx_messages), 1)

        # Check the first message
        msg = rx_messages[0]
        self.assertIsInstance(msg, tuple)
        rx_ch, rx_method, rx_properties, rx_body = msg
        self.assertIs(rx_ch, ch)
        self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
        self.assertEqual(rx_method.consumer_tag, consumer_tag)
        self.assertEqual(rx_method.delivery_tag, 1)
        self.assertFalse(rx_method.redelivered)
        self.assertEqual(rx_method.exchange, exg_name)
        self.assertEqual(rx_method.routing_key, routing_key)

        self.assertIsInstance(rx_properties, pika.BasicProperties)
        self.assertEqual(rx_body, _p3_as_bytes('via-basic_publish'))

        # There shouldn't be any more events now
        self.assertFalse(ch._pending_events)

        # Ack the mesage so that the next one can arrive (we configured QoS with
        # prefetch_count=1)
        ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

        # Get the second message
        while len(rx_messages) < 2:
            connection.process_data_events(time_limit=None)

        self.assertEqual(len(rx_messages), 2)

        msg = rx_messages[1]
        self.assertIsInstance(msg, tuple)
        rx_ch, rx_method, rx_properties, rx_body = msg
        self.assertIs(rx_ch, ch)
        self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
        self.assertEqual(rx_method.consumer_tag, consumer_tag)
        self.assertEqual(rx_method.delivery_tag, 2)
        self.assertFalse(rx_method.redelivered)
        self.assertEqual(rx_method.exchange, exg_name)
        self.assertEqual(rx_method.routing_key, routing_key)

        self.assertIsInstance(rx_properties, pika.BasicProperties)
        self.assertEqual(rx_body, _p3_as_bytes('via-publish'))

        # There shouldn't be any more events now
        self.assertFalse(ch._pending_events)

        ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

        # Verify that the queue is now empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Attempt to cosume again with a short timeout
        connection.process_data_events(time_limit=0.005)
        self.assertEqual(len(rx_messages), 2)

        # Delete the queue and wait for consumer cancellation
        rx_cancellations = []
        ch.add_on_cancel_callback(lambda frame: rx_cancellations.append(frame))
        ch.queue_delete(q_name)
        ch.start_consuming()

        self.assertEqual(len(rx_cancellations), 1)
        frame, = rx_cancellations
        self.assertEqual(frame.method.consumer_tag, consumer_tag)


class TestPublishFromBasicConsumeCallback(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.basic_publish from basic_consume callback
        """
        connection = self._connect()

        ch = connection.channel()

        src_q_name = _p3_as_bytes(
            'TestPublishFromBasicConsumeCallback_src_q' + uuid.uuid1().hex)
        dest_q_name = _p3_as_bytes(
            'TestPublishFromBasicConsumeCallback_dest_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        ch.confirm_delivery()

        # Declare source and destination queues
        ch.queue_declare(src_q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, src_q_name)
        ch.queue_declare(dest_q_name, auto_delete=True)
        self.addCleanup(self._connect().channel().queue_delete, dest_q_name)

        # Deposit a message in the source queue
        ch.publish(_p3_as_bytes(''),
                   routing_key=src_q_name,
                   body='via-publish',
                   mandatory=True)

        # Create a consumer
        def on_consume(channel, method, props, body):
            channel.publish(
                _p3_as_bytes(''), routing_key=dest_q_name, body=body,
                properties=props, mandatory=True)
            channel.basic_ack(method.delivery_tag)

        ch.basic_consume(on_consume,
                         src_q_name,
                         no_ack=False,
                         exclusive=False,
                         arguments=None)

        # Consume from destination queue
        for _, _, rx_body in ch.consume(dest_q_name,
                                                       no_ack=True):
            self.assertEqual(rx_body, pika.compat.as_bytes('via-publish'))
            break
        else:
            self.fail('failed to consume a messages from destination q')


class TestStopConsumingFromBasicConsumeCallback(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.stop_consuming from basic_consume callback
        """
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestStopConsumingFromBasicConsumeCallback_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        ch.confirm_delivery()

        # Declare the queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(connection.channel().queue_delete, q_name)

        # Deposit two messages in the queue
        ch.publish(_p3_as_bytes(''),
                   routing_key=q_name,
                   body='via-publish1',
                   mandatory=True)

        ch.publish(_p3_as_bytes(''),
                   routing_key=q_name,
                   body='via-publish2',
                   mandatory=True)

        # Create a consumer
        def on_consume(channel, method, props, body):  # pylint: disable=W0613
            channel.stop_consuming()
            channel.basic_ack(method.delivery_tag)

        ch.basic_consume(on_consume,
                         q_name,
                         no_ack=False,
                         exclusive=False,
                         arguments=None)

        ch.start_consuming()

        ch.close()

        ch = connection.channel()

        # Verify that only the second message is present in the queue
        _, _, rx_body = ch.basic_get(q_name)
        self.assertEqual(rx_body, pika.compat.as_bytes('via-publish2'))

        msg = ch.basic_get(q_name)
        self.assertTupleEqual(msg, (None, None, None))


class TestCloseChannelFromBasicConsumeCallback(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel.close from basic_consume callback
        """
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestCloseChannelFromBasicConsumeCallback_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        ch.confirm_delivery()

        # Declare the queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(connection.channel().queue_delete, q_name)

        # Deposit two messages in the queue
        ch.publish(_p3_as_bytes(''),
                   routing_key=q_name,
                   body='via-publish1',
                   mandatory=True)

        ch.publish(_p3_as_bytes(''),
                   routing_key=q_name,
                   body='via-publish2',
                   mandatory=True)

        # Create a consumer
        def on_consume(channel, method, props, body):  # pylint: disable=W0613
            channel.close()

        ch.basic_consume(on_consume,
                         q_name,
                         no_ack=False,
                         exclusive=False,
                         arguments=None)

        ch.start_consuming()

        self.assertTrue(ch.is_closed)


        # Verify that both messages are present in the queue
        ch = connection.channel()
        _, _, rx_body = ch.basic_get(q_name)
        self.assertEqual(rx_body, pika.compat.as_bytes('via-publish1'))
        _, _, rx_body = ch.basic_get(q_name)
        self.assertEqual(rx_body, pika.compat.as_bytes('via-publish2'))


class TestCloseConnectionFromBasicConsumeCallback(BlockingTestCaseBase):

    def test(self):
        """BlockingConnection.close from basic_consume callback
        """
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestCloseConnectionFromBasicConsumeCallback_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        ch.confirm_delivery()

        # Declare the queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Deposit two messages in the queue
        ch.publish(_p3_as_bytes(''),
                   routing_key=q_name,
                   body='via-publish1',
                   mandatory=True)

        ch.publish(_p3_as_bytes(''),
                   routing_key=q_name,
                   body='via-publish2',
                   mandatory=True)

        # Create a consumer
        def on_consume(channel, method, props, body):  # pylint: disable=W0613
            connection.close()

        ch.basic_consume(on_consume,
                         q_name,
                         no_ack=False,
                         exclusive=False,
                         arguments=None)

        ch.start_consuming()

        self.assertTrue(ch.is_closed)
        self.assertTrue(connection.is_closed)


        # Verify that both messages are present in the queue
        ch = self._connect().channel()
        _, _, rx_body = ch.basic_get(q_name)
        self.assertEqual(rx_body, pika.compat.as_bytes('via-publish1'))
        _, _, rx_body = ch.basic_get(q_name)
        self.assertEqual(rx_body, pika.compat.as_bytes('via-publish2'))


class TestNonPubAckPublishAndConsumeHugeMessage(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel publish/consume huge message"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestPublishAndConsumeHugeMessage_q' + uuid.uuid1().hex)

        body = _p3_as_bytes('a' * 1000000)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Publish a message to the queue by way of default exchange
        ch.publish(exchange='', routing_key=q_name, body=body)
        LOGGER.info('Published message body size=%s', len(body))

        # Consume the message
        for rx_method, rx_props, rx_body in ch.consume(q_name, no_ack=False,
                                                       exclusive=False,
                                                       arguments=None):
            self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
            self.assertEqual(rx_method.delivery_tag, 1)
            self.assertFalse(rx_method.redelivered)
            self.assertEqual(rx_method.exchange, _p3_as_bytes(''))
            self.assertEqual(rx_method.routing_key, q_name)

            self.assertIsInstance(rx_props, pika.BasicProperties)
            self.assertEqual(rx_body, body)

            # Ack the mesage
            ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

            break

        # There shouldn't be any more events now
        self.assertFalse(ch._queue_consumer_generator.pending_events)

        # Verify that the queue is now empty
        ch.close()
        ch = connection.channel()
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestNonPubackPublishAndConsumeManyMessages(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel non-pub-ack publish/consume many messages"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestNonPubackPublishAndConsumeManyMessages_q' + uuid.uuid1().hex)

        body = _p3_as_bytes('b' * 1024)

        num_messages_to_publish = 500

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        for _ in pika.compat.xrange(num_messages_to_publish):
            # Publish a message to the queue by way of default exchange
            ch.publish(exchange='', routing_key=q_name, body=body)

        # Consume the messages
        num_consumed = 0
        for rx_method, rx_props, rx_body in ch.consume(q_name,
                                                       no_ack=False,
                                                       exclusive=False,
                                                       arguments=None):
            num_consumed += 1
            self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
            self.assertEqual(rx_method.delivery_tag, num_consumed)
            self.assertFalse(rx_method.redelivered)
            self.assertEqual(rx_method.exchange, _p3_as_bytes(''))
            self.assertEqual(rx_method.routing_key, q_name)

            self.assertIsInstance(rx_props, pika.BasicProperties)
            self.assertEqual(rx_body, body)

            # Ack the mesage
            ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

            if num_consumed >= num_messages_to_publish:
                break

        # There shouldn't be any more events now
        self.assertFalse(ch._queue_consumer_generator.pending_events)

        ch.close()

        self.assertIsNone(ch._queue_consumer_generator)

        # Verify that the queue is now empty
        ch = connection.channel()
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicCancelWithNonAckableConsumer(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel user cancels non-ackable consumer via basic_cancel"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestBasicCancelWithNonAckableConsumer_q' + uuid.uuid1().hex)

        body1 = _p3_as_bytes('a' * 1024)
        body2 = _p3_as_bytes('b' * 2048)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Publish two messages to the queue by way of default exchange
        ch.publish(exchange='', routing_key=q_name, body=body1)
        ch.publish(exchange='', routing_key=q_name, body=body2)

        # Create a non-ackable consumer
        consumer_tag = ch.basic_consume(lambda *x: None, q_name, no_ack=True,
                                        exclusive=False, arguments=None)

        # Cancel the consumer
        messages = ch.basic_cancel(consumer_tag)

        # Both messages should have been on their way when we cancelled
        self.assertEqual(len(messages), 2)

        _, _, rx_body1 = messages[0]
        self.assertEqual(rx_body1, body1)

        _, _, rx_body2 = messages[1]
        self.assertEqual(rx_body2, body2)

        ch.close()

        ch = connection.channel()

        # Verify that the queue is now empty; this validates the multi-ack
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicCancelWithAckableConsumer(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel user cancels ackable consumer via basic_cancel"""
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestBasicCancelWithAckableConsumer_q' + uuid.uuid1().hex)

        body1 = _p3_as_bytes('a' * 1024)
        body2 = _p3_as_bytes('b' * 2048)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Publish two messages to the queue by way of default exchange
        ch.publish(exchange='', routing_key=q_name, body=body1)
        ch.publish(exchange='', routing_key=q_name, body=body2)

        # Create an ackable consumer
        consumer_tag = ch.basic_consume(lambda *x: None, q_name, no_ack=False,
                                        exclusive=False, arguments=None)

        # Cancel the consumer
        messages = ch.basic_cancel(consumer_tag)

        # Both messages should have been on their way when we cancelled
        self.assertEqual(len(messages), 0)

        ch.close()

        ch = connection.channel()

        # Verify that the queue is now empty; this validates the multi-ack
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 2)


class TestUnackedMessageAutoRestoredToQueueOnChannelClose(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel unacked message restored to q on channel close """
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestUnackedMessageAutoRestoredToQueueOnChannelClose_q' +
            uuid.uuid1().hex)

        body1 = 'a' * 1024
        body2 = 'b' * 2048

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Publish two messages to the queue by way of default exchange
        ch.publish(exchange='', routing_key=q_name, body=body1)
        ch.publish(exchange='', routing_key=q_name, body=body2)

        # Consume the events, but don't ack
        rx_messages = []
        ch.basic_consume(lambda *args: rx_messages.append(args),
                         q_name,
                         no_ack=False,
                         exclusive=False,
                         arguments=None)
        while len(rx_messages) != 2:
            connection.process_data_events(time_limit=None)

        self.assertEqual(rx_messages[0][1].delivery_tag, 1)
        self.assertEqual(rx_messages[1][1].delivery_tag, 2)

        # Verify no more ready messages in queue
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Closing channel should restore messages back to queue
        ch.close()

        # Verify that there are two messages in q now
        ch = connection.channel()

        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 2)


class TestNoAckMessageNotRestoredToQueueOnChannelClose(BlockingTestCaseBase):

    def test(self):
        """BlockingChannel unacked message restored to q on channel close """
        connection = self._connect()

        ch = connection.channel()

        q_name = _p3_as_bytes(
            'TestNoAckMessageNotRestoredToQueueOnChannelClose_q' +
            uuid.uuid1().hex)

        body1 = 'a' * 1024
        body2 = 'b' * 2048

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=False)
        self.addCleanup(self._connect().channel().queue_delete, q_name)

        # Publish two messages to the queue by way of default exchange
        ch.publish(exchange='', routing_key=q_name, body=body1)
        ch.publish(exchange='', routing_key=q_name, body=body2)

        # Consume, but don't ack
        num_messages = 0
        for rx_method, _, _ in ch.consume(q_name,
                                                       no_ack=True,
                                                       exclusive=False):
            num_messages += 1

            self.assertEqual(rx_method.delivery_tag, num_messages)

            if num_messages == 2:
                break
        else:
            self.fail('expected 2 messages, but consumed %i' % (num_messages,))

        # Verify no more ready messages in queue
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Closing channel should not restore no-ack messages back to queue
        ch.close()

        # Verify that there are no messages in q now
        ch = connection.channel()
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


if __name__ == '__main__':
    unittest.main()
