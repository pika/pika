"""The Channel class provides a wrapper for interacting with RabbitMQ
implementing the methods and behaviors for an AMQP Channel.

"""
import collections
import logging
import warnings

import pika.frame as frame
import pika.exceptions as exceptions
import pika.spec as spec
from pika.utils import is_callable

LOGGER = logging.getLogger(__name__)
MAX_CHANNELS = 32768


class Channel(object):
    """A Channel is the primary communication method for interacting with
    RabbitMQ. It is recommended that you do not directly invoke
    the creation of a channel object in your application code but rather
    construct the a channel by calling the active connection's channel()
    method.

    """
    CLOSED = 0
    OPENING = 1
    OPEN = 2
    CLOSING = 3

    def __init__(self, connection, channel_number, on_open_callback=None, force_binary=False):
        """Create a new instance of the Channel

        :param pika.connection.Connection connection: The connection
        :param int channel_number: The channel number for this instance
        :param method on_open_callback: The method to call on channel open
        :param bool force_binary: Prevents channel from autodetecting unicode

        """
        if not isinstance(channel_number, int):
            raise exceptions.InvalidChannelNumber
        self.channel_number = channel_number
        self.callbacks = connection.callbacks
        self.connection = connection

        # The frame-handler changes depending on the type of frame processed
        self.frame_dispatcher = ContentFrameDispatcher(force_binary)

        self._blocked = collections.deque(list())
        self._blocking = None
        self._has_on_flow_callback = False
        self._cancelled = list()
        self._consumers = dict()
        self._on_flowok_callback = None
        self._on_getok_callback = None
        self._on_openok_callback = on_open_callback
        self._pending = dict()
        self._state = self.CLOSED

    def __int__(self):
        """Return the channel object as its channel number

        :rtype: int

        """
        return self.channel_number

    def add_callback(self, callback, replies, one_shot=True):
        """Pass in a callback handler and a list replies from the
        RabbitMQ broker which you'd like the callback notified of. Callbacks
        should allow for the frame parameter to be passed in.

        :param method callback: The method to call
        :param list replies: The replies to get a callback for
        :param bool one_shot: Only handle the first type callback

        """
        for reply in replies:
            self.callbacks.add(self.channel_number, reply, callback, one_shot)

    def add_on_cancel_callback(self, callback):
        """Pass a callback function that will be called when the basic_cancel
        is sent by the server. The callback function should receive a frame
        parameter.

        :param method callback: The method to call on callback

        """
        self.callbacks.add(self.channel_number, spec.Basic.Cancel, callback,
                           False)

    def add_on_close_callback(self, callback):
        """Pass a callback function that will be called when the channel is
        closed. The callback function will receive the channel, the
        reply_code (int) and the reply_text (int) sent by the server describing
        why the channel was closed.

        :param method callback: The method to call on callback

        """
        self.callbacks.add(self.channel_number, '_on_channel_close', callback,
                           False, self)

    def add_on_flow_callback(self, callback):
        """Pass a callback function that will be called when Channel.Flow is
        called by the remote server. Note that newer versions of RabbitMQ
        will not issue this but instead use TCP backpressure

        :param method callback: The method to call on callback

        """
        self._has_on_flow_callback = True
        self.callbacks.add(self.channel_number, spec.Channel.Flow, callback,
                           False)

    def add_on_return_callback(self, callback):
        """Pass a callback function that will be called when basic_publish as
        sent a message that has been rejected and returned by the server. The
        callback handler should receive a method, header and body frame. The
        base signature for the callback should be the same as the method
        signature one creates for a basic_consume callback.

        :param method callback: The method to call on callback

        """
        self.callbacks.add(self.channel_number, '_on_return', callback, False)

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Acknowledge one or more messages. When sent by the client, this
        method acknowledges one or more messages delivered via the Deliver or
        Get-Ok methods. When sent by server, this method acknowledges one or
        more messages published with the Publish method on a channel in
        confirm mode. The acknowledgement can be for a single message or a
        set of messages up to and including a specific message.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        """
        if not self.is_open:
            raise exceptions.ChannelClosed()
        return self._send_method(spec.Basic.Ack(delivery_tag, multiple))

    def basic_cancel(self, callback=None, consumer_tag='', nowait=False):
        """This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply. It may also be sent from the server to the client in
        the event of the consumer being unexpectedly cancelled (i.e. cancelled
        for any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of
        the loss of consumers due to events such as queue deletion.

        :param method callback: Method to call for a Basic.CancelOk response
        :param str consumer_tag: Identifier for the consumer
        :param bool nowait: Do not expect a Basic.CancelOk response
        :raises: ValueError

        """
        self._validate_channel_and_callback(callback)
        if consumer_tag not in self.consumer_tags:
            return
        if callback:
            if nowait is True:
                raise ValueError('Can not pass a callback if nowait is True')
            self.callbacks.add(self.channel_number,
                               spec.Basic.CancelOk,
                               callback)
        self._cancelled.append(consumer_tag)
        self._rpc(spec.Basic.Cancel(consumer_tag=consumer_tag,
                                    nowait=nowait),
                  self._on_cancelok,
                  [(spec.Basic.CancelOk,
                    {'consumer_tag': consumer_tag})] if nowait is False else [])

    def basic_consume(self, consumer_callback, queue='', no_ack=False,
                      exclusive=False, consumer_tag=None):
        """Sends the AMQP command Basic.Consume to the broker and binds messages
        for the consumer_tag to the consumer callback. If you do not pass in
        a consumer_tag, one will be automatically generated for you. Returns
        the consumer tag.

        For more information on basic_consume, see:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param method consumer_callback: The method to callback when consuming
        :param queue: The queue to consume from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a response
        :param bool exclusive: Don't allow other consumers on the queue
        :param consumer_tag: Specify your own consumer tag
        :type consumer_tag: str or unicode
        :rtype: str

        """
        self._validate_channel_and_callback(consumer_callback)

        # If a consumer tag was not passed, create one
        consumer_tag = consumer_tag or 'ctag%i.%i' % (self.channel_number,
                                                      len(self._consumers) +
                                                      len(self._cancelled))

        if consumer_tag in self._consumers or consumer_tag in self._cancelled:
            raise exceptions.DuplicateConsumerTag(consumer_tag)

        self._consumers[consumer_tag] = consumer_callback
        self._pending[consumer_tag] = list()
        self._rpc(spec.Basic.Consume(queue=queue,
                                     consumer_tag=consumer_tag,
                                     no_ack=no_ack,
                                     exclusive=exclusive),
                           self._on_eventok,
                           [(spec.Basic.ConsumeOk,
                             {'consumer_tag': consumer_tag})])

        return consumer_tag

    def basic_get(self, callback=None, queue='', no_ack=False):
        """Get a single message from the AMQP broker. The callback method
        signature should have 3 parameters: The method frame, header frame and
        the body, like the consumer callback for Basic.Consume. If you want to
        be notified of Basic.GetEmpty, use the Channel.add_callback method
        adding your Basic.GetEmpty callback which should expect only one
        parameter, frame. For more information on basic_get and its
        parameters, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get

        :param method callback: The method to callback with a message
        :param queue: The queue to get a message from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a reply

        """
        self._validate_channel_and_callback(callback)
        self._on_getok_callback = callback
        self._send_method(spec.Basic.Get(queue=queue,
                                         no_ack=no_ack))

    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        """This method allows a client to reject one or more incoming messages.
        It can be used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        if not self.is_open:
            raise exceptions.ChannelClosed()
        return self._send_method(spec.Basic.Nack(delivery_tag, multiple,
                                                 requeue))

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """Publish to the channel with the given exchange, routing key and body.
        For more information on basic_publish and what the parameters do, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body
        :type body: str or unicode
        :param pika.spec.Properties properties: Basic.properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        """
        if not self.is_open:
            raise exceptions.ChannelClosed()
        if immediate:
            LOGGER.warning('The immediate flag is deprecated in RabbitMQ')
        if isinstance(body, unicode):
            body = body.encode('utf-8')
        properties = properties or spec.BasicProperties()
        self._send_method(spec.Basic.Publish(exchange=exchange,
                                             routing_key=routing_key,
                                             mandatory=mandatory,
                                             immediate=immediate),
                          (properties, body))

    def basic_qos(self, callback=None, prefetch_size=0, prefetch_count=0,
                  all_channels=False):
        """Specify quality of service. This method requests a specific quality
        of service. The QoS can be specified for the current channel or for all
        channels on the connection. The client can request that messages be sent
        in advance so that when the client finishes processing a message, the
        following message is already held locally, rather than needing to be
        sent down the channel. Prefetching gives a performance improvement.

        :param method callback: The method to callback for Basic.QosOk response
        :param int prefetch_size:  This field specifies the prefetch window
                                   size. The server will send a message in
                                   advance if it is equal to or smaller in size
                                   than the available prefetch size (and also
                                   falls into other prefetch limits). May be set
                                   to zero, meaning "no specific limit",
                                   although other prefetch limits may still
                                   apply. The prefetch-size is ignored if the
                                   no-ack option is set.
        :param int prefetch_count: Specifies a prefetch window in terms of whole
                                   messages. This field may be used in
                                   combination with the prefetch-size field; a
                                   message will only be sent in advance if both
                                   prefetch windows (and those at the channel
                                   and connection level) allow it. The
                                   prefetch-count is ignored if the no-ack
                                   option is set.
        :param bool all_channels: Should the QoS apply to all channels

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Basic.Qos(prefetch_size, prefetch_count,
                                        all_channels),
                         callback,
                         [spec.Basic.QosOk])

    def basic_reject(self, delivery_tag=None, requeue=True):
        """Reject an incoming message. This method allows a client to reject a
        message. It can be used to interrupt and cancel large incoming messages,
        or return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        if not self.is_open:
            raise exceptions.ChannelClosed()
        return self._send_method(spec.Basic.Reject(delivery_tag, requeue))

    def basic_recover(self, callback=None, requeue=False):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered. This
        method replaces the asynchronous Recover.

        :param method callback: Method to call when receiving Basic.RecoverOk
        :param bool requeue: If False, the message will be redelivered to the
                             original recipient. If True, the server will
                             attempt to requeue the message, potentially then
                             delivering it to an alternative subscriber.

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Basic.Recover(requeue), callback,
                         [spec.Basic.RecoverOk])

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """
        if not self.is_open:
            raise exceptions.ChannelClosed()
        LOGGER.info('Channel.close(%s, %s)', reply_code, reply_text)
        if self._consumers:
            LOGGER.debug('Cancelling %i consumers', len(self._consumers))
            for consumer_tag in self._consumers.keys():
                self.basic_cancel(consumer_tag=consumer_tag)
        self._set_state(self.CLOSING)
        self._rpc(spec.Channel.Close(reply_code, reply_text, 0, 0),
                  self._on_closeok, [spec.Channel.CloseOk])

    def confirm_delivery(self, callback=None, nowait=False):
        """Turn on Confirm mode in the channel. Pass in a callback to be
        notified by the Broker when a message has been confirmed as received or
        rejected (Basic.Ack, Basic.Nack) from the broker to the publisher.

        For more information see:
            http://www.rabbitmq.com/extensions.html#confirms

        :param method callback: The callback for delivery confirmations
        :param bool nowait: Do not send a reply frame (Confirm.SelectOk)

        """
        self._validate_channel_and_callback(callback)
        if (self.connection.publisher_confirms is False or
            self.connection.basic_nack is False):
            raise exceptions.MethodNotImplemented('Not Supported on Server')

        # Add the ack and nack callbacks
        if callback is not None:
            self.callbacks.add(self.channel_number,
                               spec.Basic.Ack,
                               callback,
                               False)
            self.callbacks.add(self.channel_number,
                               spec.Basic.Nack,
                               callback,
                               False)

        # Send the RPC command
        self._rpc(spec.Confirm.Select(nowait),
                  self._on_selectok,
                  [spec.Confirm.SelectOk] if nowait is False else [])

    @property
    def consumer_tags(self):
        """Property method that returns a list of currently active consumers

        :rtype: list

        """
        return self._consumers.keys()

    def exchange_bind(self, callback=None, destination=None, source=None,
                      routing_key='', nowait=False, arguments=None):
        """Bind an exchange to another exchange.

        :param method callback: The method to call on Exchange.BindOk
        :param destination: The destination exchange to bind
        :type destination: str or unicode
        :param source: The source exchange to bind to
        :type source: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for an Exchange.BindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Exchange.Bind(0, destination, source,
                                            routing_key, nowait,
                                            arguments or dict()), callback,
                         [spec.Exchange.BindOk] if nowait is False else [])

    def exchange_declare(self, callback=None, exchange=None,
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False, nowait=False,
                         arguments=None, type=None):
        """This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        If passive set, the server will reply with Declare-Ok if the exchange
        already exists with the same name, and raise an error if not and if the
        exchange does not already exist, the server MUST raise a channel
        exception with reply code 404 (not found).

        :param method callback: Call this method on Exchange.DeclareOk
        :param exchange: The exchange name consists of a non-empty
        :type exchange: str or unicode
                                     sequence of these characters: letters,
                                     digits, hyphen, underscore, period, or
                                     colon.
        :param str exchange_type: The exchange type to use
        :param bool passive: Perform a declare or just check to see if it exists
        :param bool durable: Survive a reboot of RabbitMQ
        :param bool auto_delete: Remove when no more queues are bound to it
        :param bool internal: Can only be published to by other exchanges
        :param bool nowait: Do not expect an Exchange.DeclareOk response
        :param dict arguments: Custom key/value pair arguments for the exchange
        :param str type: The deprecated exchange type parameter

        """
        self._validate_channel_and_callback(callback)
        if type is not None:
            warnings.warn('type is deprecated, use exchange_type instead',
                          DeprecationWarning)
            if exchange_type == 'direct' and type != exchange_type:
                exchange_type = type
        return self._rpc(spec.Exchange.Declare(0, exchange, exchange_type,
                                               passive, durable, auto_delete,
                                               internal, nowait,
                                               arguments or dict()),
                         callback,
                         [spec.Exchange.DeclareOk] if nowait is False else [])

    def exchange_delete(self, callback=None, exchange=None, if_unused=False,
                        nowait=False):
        """Delete the exchange.

        :param method callback: The method to call on Exchange.DeleteOk
        :param exchange: The exchange name
        :type exchange: str or unicode
        :param bool if_unused: only delete if the exchange is unused
        :param bool nowait: Do not wait for an Exchange.DeleteOk

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Exchange.Delete(0, exchange, if_unused, nowait),
                         callback,
                         [spec.Exchange.DeleteOk] if nowait is False else [])

    def exchange_unbind(self, callback=None, destination=None, source=None,
                        routing_key='', nowait=False, arguments=None):
        """Unbind an exchange from another exchange.

        :param method callback: The method to call on Exchange.UnbindOk
        :param destination: The destination exchange to unbind
        :type destination: str or unicode
        :param source: The source exchange to unbind from
        :type source: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for an Exchange.UnbindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Exchange.Unbind(0, destination, source,
                                              routing_key, nowait, arguments),
                         callback,
                         [spec.Exchange.UnbindOk] if nowait is False else [])

    def flow(self, callback, active):
        """Turn Channel flow control off and on. Pass a callback to be notified
        of the response from the server. active is a bool. Callback should
        expect a bool in response indicating channel flow state. For more
        information, please reference:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#channel.flow

        :param method callback: The callback method
        :param bool active: Turn flow on or off

        """
        self._validate_channel_and_callback(callback)
        self._on_flowok_callback = callback
        self._rpc(spec.Channel.Flow(active),
                  self._on_flowok,
                  [spec.Channel.FlowOk])

    @property
    def is_closed(self):
        """Returns True if the channel is closed.

        :rtype: bool

        """
        return self._state == self.CLOSED

    @property
    def is_closing(self):
        """Returns True if the channel is closing.

        :rtype: bool

        """
        return self._state == self.CLOSING

    @property
    def is_open(self):
        """Returns True if the channel is open.

        :rtype: bool

        """
        return self._state == self.OPEN

    def open(self):
        """Open the channel"""
        self._set_state(self.OPENING)
        self._add_callbacks()
        self._rpc(spec.Channel.Open(), self._on_openok, [spec.Channel.OpenOk])

    def queue_bind(self, callback, queue, exchange, routing_key=None,
                   nowait=False, arguments=None):
        """Bind the queue to the specified exchange

        :param method callback: The method to call on Queue.BindOk
        :param queue: The queue to bind to the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for a Queue.BindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        self._validate_channel_and_callback(callback)
        replies = [spec.Queue.BindOk] if nowait is False else []
        if routing_key is None:
            routing_key = queue
        return self._rpc(spec.Queue.Bind(0, queue, exchange, routing_key,
                                         nowait, arguments or dict()), callback,
                         replies)

    def queue_declare(self, callback, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        """Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Leave the queue name empty for a auto-named queue in RabbitMQ

        :param method callback: The method to call on Queue.DeclareOk
        :param queue: The queue name
        :type queue: str or unicode
        :param bool passive: Only check to see if the queue exists
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param bool nowait: Do not wait for a Queue.DeclareOk
        :param dict arguments: Custom key/value arguments for the queue

        """
        condition = (spec.Queue.DeclareOk,
                     {'queue': queue}) if queue else spec.Queue.DeclareOk
        replies = [condition] if nowait is False else []
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Queue.Declare(0, queue, passive, durable,
                                            exclusive, auto_delete, nowait,
                                            arguments or dict()), callback,
                         replies)

    def queue_delete(self, callback=None, queue='', if_unused=False,
                     if_empty=False, nowait=False):
        """Delete a queue from the broker.

        :param method callback: The method to call on Queue.DeleteOk
        :param queue: The queue to delete
        :type queue: str or unicode
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty
        :param bool nowait: Do not wait for a Queue.DeleteOk

        """
        replies = [spec.Queue.DeleteOk] if nowait is False else []
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Queue.Delete(0, queue, if_unused, if_empty,
                                           nowait), callback,
                         replies)

    def queue_purge(self, callback=None, queue='', nowait=False):
        """Purge all of the messages from the specified queue

        :param method callback: The method to call on Queue.PurgeOk
        :param queue: The queue to purge
        :type queue: str or unicode
        :param bool nowait: Do not expect a Queue.PurgeOk response

        """
        replies = [spec.Queue.PurgeOk] if nowait is False else []
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Queue.Purge(0, queue, nowait), callback,
                         replies)

    def queue_unbind(self, callback=None, queue='', exchange=None,
                     routing_key=None, arguments=None):
        """Unbind a queue from an exchange.

        :param method callback: The method to call on Queue.UnbindOk
        :param queue: The queue to unbind from the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind from
        :type exchange: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        self._validate_channel_and_callback(callback)
        if routing_key is None:
            routing_key = queue
        return self._rpc(spec.Queue.Unbind(0, queue, exchange, routing_key,
                                           arguments or dict()), callback,
                         [spec.Queue.UnbindOk])

    def tx_commit(self, callback=None):
        """Commit a transaction

        :param method callback: The callback for delivery confirmations

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Tx.Commit(), callback, [spec.Tx.CommitOk])

    def tx_rollback(self, callback=None):
        """Rollback a transaction.

        :param method callback: The callback for delivery confirmations

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Tx.Rollback(), callback, [spec.Tx.RollbackOk])

    def tx_select(self, callback=None):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        :param method callback: The callback for delivery confirmations

        """
        self._validate_channel_and_callback(callback)
        return self._rpc(spec.Tx.Select(), callback, [spec.Tx.SelectOk])

    # Internal methods

    def _add_callbacks(self):
        """Callbacks that add the required behavior for a channel when
        connecting and connected to a server.

        """
        # Add a callback for Basic.GetEmpty
        self.callbacks.add(self.channel_number,
                           spec.Basic.GetEmpty,
                           self._on_getempty,
                           False)

        # Add a callback for Basic.Cancel
        self.callbacks.add(self.channel_number,
                           spec.Basic.Cancel,
                           self._on_cancel,
                           False)

        # Deprecated in newer versions of RabbitMQ but still register for it
        self.callbacks.add(self.channel_number,
                           spec.Channel.Flow,
                           self._on_flow,
                           False)

        # Add a callback for when the server closes our channel
        self.callbacks.add(self.channel_number,
                           spec.Channel.Close,
                           self._on_close,
                           True)

    def _add_pending_msg(self, consumer_tag, method_frame,  header_frame, body):
        """Add the received message to the pending message stack.

        :param str consumer_tag: The consumer tag for the message
        :param pika.frame.Method method_frame: The received method frame
        :param pika.frame.Header header_frame: The received header frame
        :param body: The message body
        :type body: str or unicode

        """
        self._pending[consumer_tag].append((self, method_frame.method,
                                            header_frame.properties, body))

    def _cleanup(self):
        """Remove all consumers and any callbacks for the channel."""
        self._consumers = dict()
        self.callbacks.cleanup(str(self.channel_number))

    def _get_pending_msg(self, consumer_tag):
        """Get a pending message for the consumer tag from the stack.

        :param str consumer_tag: The consumer tag to get a message from
        :rtype: tuple(pika.frame.Header, pika.frame.Method, str|unicode)

        """
        return self._pending[consumer_tag].pop(0)

    def _handle_content_frame(self, frame_value):
        """This is invoked by the connection when frames that are not registered
        with the CallbackManager have been found. This should only be the case
        when the frames are related to content delivery.

        The frame_dispatcher will be invoked which will return the fully formed
        message in three parts when all of the body frames have been received.

        :param pika.amqp_object.Frame frame_value: The frame to deliver

        """
        try:
            response = self.frame_dispatcher.process(frame_value)
        except exceptions.UnexpectedFrameError:
            return self._unexpected_frame(frame_value)

        if response:
            if isinstance(response[0].method, spec.Basic.Deliver):
                self._on_deliver(*response)
            elif isinstance(response[0].method, spec.Basic.GetOk):
                self._on_getok(*response)
            elif isinstance(response[0].method, spec.Basic.Return):
                self._on_return(*response)

    def _has_content(self, method_frame):
        """Return a bool if it's a content method as defined by the spec

        :param pika.amqp_object.Method method_frame: The method frame received

        """
        return spec.has_content(method_frame.INDEX)

    def _on_cancel(self, method_frame):
        """When the broker cancels a consumer, delete it from our internal
        dictionary.

        :param pika.frame.Method method_frame: The method frame received

        """
        self._cancelled.append(method_frame.method.consumer_tag)
        if method_frame.method.consumer_tag in self._consumers:
            del self._consumers[method_frame.method.consumer_tag]

    def _on_cancelok(self, method_frame):
        """Called in response to a frame from the Broker when the
         client sends Basic.Cancel

        :param pika.frame.Method method_frame: The method frame received

        """
        if method_frame.method.consumer_tag in self._consumers:
            del self._consumers[method_frame.method.consumer_tag]
        if method_frame.method.consumer_tag in self._pending:
            del self._pending[method_frame.method.consumer_tag]

    def _on_close(self, method_frame):
        """Handle the case where our channel has been closed for us

        :param pika.frame.Method method_frame: The close frame

        """
        LOGGER.info('%s', method_frame)
        LOGGER.warning('Received remote Channel.Close (%s): %s',
                       method_frame.method.reply_code,
                       method_frame.method.reply_text)
        if self.connection.is_open:
            self._send_method(spec.Channel.CloseOk())
        self._set_state(self.CLOSED)
        self.callbacks.process(self.channel_number,
                               '_on_channel_close',
                               self, self,
                               method_frame.method.reply_code,
                               method_frame.method.reply_text)
        self._cleanup()

    def _on_closeok(self, method_frame):
        """Invoked when RabbitMQ replies to a Channel.Close method

        :param pika.frame.Method method_frame: The CloseOk frame

        """
        LOGGER.warning('Received %s', method_frame.method)
        self._set_state(self.CLOSED)
        self.callbacks.process(self.channel_number,
                               '_on_channel_close',
                               self, self,
                               0, '')
        self._cleanup()

    def _on_deliver(self, method_frame, header_frame, body):
        """Cope with reentrancy. If a particular consumer is still active when
        another delivery appears for it, queue the deliveries up until it
        finally exits.

        :param pika.frame.Method method_frame: The method frame received
        :param pika.frame.Header header_frame: The header frame received
        :param body: The body received
        :type body: str or unicode

        """
        LOGGER.debug('Called with %r, %r, %r', method_frame, header_frame, body)
        consumer_tag = method_frame.method.consumer_tag
        if consumer_tag in self._cancelled:
            LOGGER.debug('Rejected message for cancelled consumer')
            return self.basic_reject(method_frame.method.delivery_tag)
        if consumer_tag not in self._consumers:
            return self._add_pending_msg(consumer_tag, method_frame,
                                         header_frame, body)
        while self._pending[consumer_tag]:
            self._consumers[consumer_tag](*self._get_pending_msg(consumer_tag))
        self._consumers[consumer_tag](self,
                                      method_frame.method,
                                      header_frame.properties,
                                      body)

    def _on_eventok(self, method_frame):
        """Generic events that returned ok that may have internal callbacks.
        We keep a list of what we've yet to implement so that we don't silently
        drain events that we don't support.

        :param pika.frame.Method method_frame: The method frame received

        """
        LOGGER.debug('Discarding frame %r', method_frame)

    def _on_flow(self, method_frame_unused):
        """Called if the server sends a Channel.Flow frame.

        :param pika.frame.Method method_frame_unused: The Channel.Flow frame

        """
        if self._has_on_flow_callback is False:
            LOGGER.warning('Channel.Flow received from server')

    def _on_flowok(self, method_frame):
        """Called in response to us asking the server to toggle on Channel.Flow

        :param pika.frame.Method method_frame: The method frame received

        """
        self.flow_active = method_frame.method.active
        if self._on_flowok_callback:
            self._on_flowok_callback(method_frame.method.active)
            self._on_flowok_callback = None
        else:
            LOGGER.warning('Channel.FlowOk received with no active callbacks')

    def _on_getempty(self, method_frame):
        """When we receive an empty reply do nothing but log it

        :param pika.frame.Method method_frame: The method frame received

        """
        LOGGER.debug('Received Basic.GetEmpty: %r', method_frame)

    def _on_getok(self, method_frame, header_frame, body):
        """Called in reply to a Basic.Get when there is a message.

        :param pika.frame.Method method_frame: The method frame received
        :param pika.frame.Header header_frame: The header frame received
        :param body: The body received
        :type body: str or unicode

        """
        if self._on_getok_callback is not None:
            self._on_getok_callback(self,
                                    method_frame.method,
                                    header_frame.properties,
                                    body)
            self._on_getok_callback = None
        else:
            LOGGER.error('Basic.GetOk received with no active callback')

    def _on_openok(self, frame_unused):
        """Called by our callback handler when we receive a Channel.OpenOk and
        subsequently calls our _on_openok_callback which was passed into the
        Channel constructor. The reason we do this is because we want to make
        sure that the on_open_callback parameter passed into the Channel
        constructor is not the first callback we make.

        :param pika.frame.Method frame_unused: Unused Channel.OpenOk frame

        """
        self._set_state(self.OPEN)
        if self._on_openok_callback is not None:
            self._on_openok_callback(self)

    def _on_return(self, method_frame, header_frame, body):
        """Called if the server sends a Basic.Return frame.

        :param pika.frame.Method method_frame: The Basic.Return frame
        :param pika.frame.Header header_frame: The content header frame
        :param body: The message body
        :type body: str or unicode

        """
        if not self.callbacks.process(self.channel_number, '_on_return', self,
                                      (self,
                                       method_frame.method,
                                       header_frame.properties,
                                       body)):
            LOGGER.warning('Basic.Return received from server (%r, %r)',
                           method_frame.method, header_frame.properties)

    def _on_selectok(self, method_frame):
        """Called when the broker sends a Confirm.SelectOk frame

        :param pika.frame.Method method_frame: The method frame received

        """
        LOGGER.debug("Confirm.SelectOk Received: %r", method_frame)

    def _on_synchronous_complete(self, method_frame_unused):
        """This is called when a synchronous command is completed. It will undo
        the blocking state and send all the frames that stacked up while we
        were in the blocking state.

        :param pika.frame.Method method_frame_unused: The method frame received

        """
        LOGGER.debug('%i blocked frames', len(self._blocked))
        self._blocking = None
        while len(self._blocked) > 0 and self._blocking is None:
            self._rpc(*self._blocked.popleft())

    def _rpc(self, method_frame, callback=None, acceptable_replies=None):
        """Shortcut wrapper to the Connection's rpc command using its callback
        stack, passing in our channel number.

        :param pika.amqp_object.Method method_frame: The method frame to call
        :param method callback: The callback for the RPC response
        :param list acceptable_replies: The replies this RPC call expects

        """
        # Make sure the channel is open
        if self.is_closed:
            raise exceptions.ChannelClosed

        # If the channel is blocking, add subsequent commands to our stack
        if self._blocking:
            return self._blocked.append([method_frame,
                                         callback,
                                         acceptable_replies])

        # Validate we got None or a list of acceptable_replies
        if acceptable_replies and not isinstance(acceptable_replies, list):
            raise TypeError("acceptable_replies should be list or None")

        # Validate the callback is callable
        if callback and not is_callable(callback):
            raise TypeError("callback should be None, a function or method.")

        # Block until a response frame is received for synchronous frames
        if method_frame.synchronous:
            self.blocking = method_frame.NAME

        # If acceptable replies are set, add callbacks
        if acceptable_replies:
            for reply in acceptable_replies or list():
                if isinstance(reply, tuple):
                    reply, arguments = reply
                else:
                    arguments = None
                LOGGER.debug('Adding in on_synchronous_complete callback')
                self.callbacks.add(self.channel_number, reply,
                                   self._on_synchronous_complete,
                                   arguments=arguments)
                if callback:
                    LOGGER.debug('Adding passed in callback')
                    self.callbacks.add(self.channel_number, reply, callback,
                                       arguments=arguments)

        self._send_method(method_frame)

    def _send_method(self, method_frame, content=None):
        """Shortcut wrapper to send a method through our connection, passing in
        the channel number

        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        self.connection._send_method(self.channel_number, method_frame, content)

    def _set_state(self, connection_state):
        """Set the channel connection state to the specified state value.

        :param int connection_state: The connection_state value

        """
        self._state = connection_state

    def _unexpected_frame(self, frame_value):
        """Invoked when a frame is received that is not setup to be processed.

        :param pika.frame.Frame frame_value: The frame received

        """
        LOGGER.warning('Unexpected frame: %r', frame_value)

    def _validate_channel_and_callback(self, callback):
        if not self.is_open:
            raise exceptions.ChannelClosed()
        if callback is not None and not is_callable(callback):
            raise ValueError('callback must be a function or method')



class ContentFrameDispatcher(object):
    """Handle content related frames, building a message and return the message
    back in three parts upon receipt.

    """
    def __init__(self, force_binary):
        """Create a new instance of the Dispatcher passing in the callback
        manager.

        """
        self._method_frame = None
        self._header_frame = None
        self._seen_so_far = 0
        self._body_fragments = list()
        self.force_binary = force_binary

    def process(self, frame_value):
        """Invoked by the Channel object when passed frames that are not
        setup in the rpc process and that don't have explicit reply types
        defined. This includes Basic.Publish, Basic.GetOk and Basic.Return

        :param Method|Header|Body frame_value: The frame to process

        """
        if (isinstance(frame_value, frame.Method) and
            spec.has_content(frame_value.method.INDEX)):
            self._method_frame = frame_value
        elif isinstance(frame_value, frame.Header):
            self._header_frame = frame_value
            if frame_value.body_size == 0:
                return self._finish()
        elif isinstance(frame_value, frame.Body):
            return self._handle_body_frame(frame_value)
        else:
            raise exceptions.UnexpectedFrameError(frame_value)

    def _finish(self):
        """Invoked when all of the message has been received

        :rtype: tuple(pika.frame.Method, pika.frame.Header, str|unicode)

        """
        value = None
        if self.force_binary:
            value = ''.join(self._body_fragments)
        else:
            try:
                value = ''.join(self._body_fragments).decode('utf-8')
                try:
                    value = str(value)
                except UnicodeEncodeError:
                    pass
            except UnicodeDecodeError:
                value = ''.join(self._body_fragments)

        content = (self._method_frame,
                   self._header_frame,
                   value)
        self._reset()
        return content

    def _handle_body_frame(self, body_frame):
        """Receive body frames and append them to the stack. When the body size
        matches, call the finish method.

        :param Body body_frame: The body frame
        :raises: pika.exceptions.BodyTooLongError
        :rtype: tuple(pika.frame.Method, pika.frame.Header, str|unicode)|None

        """
        self._seen_so_far += len(body_frame.fragment)
        self._body_fragments.append(body_frame.fragment)
        if self._seen_so_far == self._header_frame.body_size:
            return self._finish()
        elif self._seen_so_far > self._header_frame.body_size:
            raise exceptions.BodyTooLongError(self._seen_so_far,
                                              self._header_frame.body_size)
        return None

    def _reset(self):
        """Reset the values for processing frames"""
        self._method_frame = None
        self._header_frame = None
        self._seen_so_far = 0
        self._body_fragments = list()
