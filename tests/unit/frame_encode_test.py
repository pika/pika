# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Tests for creating raw frame data from pika.spec objects and validating that
they are being marshalled correctly.
"""
import support
import pika.frame


def encode_protocol_header_frame_test():
    frame_data = 'AMQP\x00\x00\t\x01'
    frame = pika.frame.ProtocolHeader()
    if frame.marshal() != frame_data:
        assert False, "ProtocolHeader frame did not match frame data sample"


def encode_access_request_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0c\x00\x1e\x00\n\x06Narnia\x1c\xce'
    kwargs = {'passive': 0, 'exclusive': False, 'realm': 'Narnia', 'active': True}
    frame = pika.frame.Method(0, pika.spec.Access.Request(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Access.Request frame did not match frame data sample"


def encode_access_request_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0c\x00\x1e\x00\n\x06Narnia\x1c\xce'
    kwargs = {'passive': 0, 'exclusive': False, 'realm': 'Narnia', 'active': True}
    frame = pika.frame.Method(1, pika.spec.Access.Request(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Access.Request frame did not match frame data sample"


def encode_access_requestok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x06\x00\x1e\x00\x0b\x00\x00\xce'
    kwargs = {'ticket': 0}
    frame = pika.frame.Method(1, pika.spec.Access.RequestOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Access.RequestOk frame did not match frame data sample"


def encode_basic_ack_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\r\x00<\x00P\x00\x00\x00\x00\x00\x00\x00\x01\x00\xce'
    kwargs = {'multiple': False, 'delivery_tag': 1}
    frame = pika.frame.Method(1, pika.spec.Basic.Ack(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Ack frame did not match frame data sample"


def encode_basic_cancel_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0b\x00<\x00\x1e\x05ctag0\x00\xce'
    kwargs = {'consumer_tag': 'ctag0', 'nowait': 0}
    frame = pika.frame.Method(1, pika.spec.Basic.Cancel(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Cancel frame did not match frame data sample"


def encode_basic_cancelok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\n\x00<\x00\x1f\x05ctag0\xce'
    kwargs = {'consumer_tag': 'ctag0'}
    frame = pika.frame.Method(1, pika.spec.Basic.CancelOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.CancelOk frame did not match frame data sample"


def encode_basic_consume_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x15\x00<\x00\x14\x00\x00\x03bar\x05ctag0\x00\x00\x00\x00\x00\xce'
    kwargs = {'exclusive': False, 'nowait': 0, 'no_local': 0, 'consumer_tag': 'ctag0', 'queue': 'bar', 'ticket': 0, 'no_ack': False}
    frame = pika.frame.Method(1, pika.spec.Basic.Consume(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Consume frame did not match frame data sample"


def encode_basic_consumeok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\n\x00<\x00\x15\x05ctag0\xce'
    kwargs = {'consumer_tag': 'ctag0'}
    frame = pika.frame.Method(1, pika.spec.Basic.ConsumeOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.ConsumeOk frame did not match frame data sample"


def encode_basic_deliver_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x1f\x00<\x00<\x05ctag0\x00\x00\x00\x00\x00\x00\x00\x01\x00\x03foo\x07foo.bar\xce'
    kwargs = {'consumer_tag': 'ctag0', 'redelivered': 0, 'routing_key': 'foo.bar', 'delivery_tag': 1, 'exchange': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Basic.Deliver(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Deliver frame did not match frame data sample"


def encode_basic_get_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0b\x00<\x00F\x00\x00\x03bar\x00\xce'
    kwargs = {'queue': 'bar', 'ticket': 0, 'no_ack': False}
    frame = pika.frame.Method(1, pika.spec.Basic.Get(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Get frame did not match frame data sample"


def encode_basic_getempty_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00<\x00H\x00\xce'
    frame = pika.frame.Method(1, pika.spec.Basic.GetEmpty())
    if frame.marshal() != frame_data:
        assert False, "Basic.GetEmpty frame did not match frame data sample"


def encode_basic_getok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x1d\x00<\x00G\x00\x00\x00\x00\x00\x00\x00\x01\x00\x03foo\x07foo.bar\x00\x00\x00\x01\xce'
    kwargs = {'message_count': 1, 'redelivered': 0, 'routing_key': 'foo.bar', 'delivery_tag': 1, 'exchange': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Basic.GetOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.GetOk frame did not match frame data sample"


def encode_basic_nack_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\r\x00<\x00x\x00\x00\x00\x00\x00\x00\x00\x01\x00\xce'
    kwargs = {'requeue': False, 'multiple': False, 'delivery_tag': 1}
    frame = pika.frame.Method(1, pika.spec.Basic.Nack(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Nack frame did not match frame data sample"


def encode_basic_publish_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x13\x00<\x00(\x00\x00\x03foo\x07foo.bar\x00\xce'
    kwargs = {'ticket': 0, 'mandatory': False, 'routing_key': 'foo.bar', 'immediate': False, 'exchange': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Basic.Publish(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Publish frame did not match frame data sample"


def encode_basic_qos_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0b\x00<\x00\n\x00\x00\x00\x00\x00\x00\x00\xce'
    kwargs = {'prefetch_count': 0, 'prefetch_size': 0}
    frame = pika.frame.Method(1, pika.spec.Basic.Qos(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Qos frame did not match frame data sample"


def encode_basic_qosok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00<\x00\x0b\xce'
    frame = pika.frame.Method(1, pika.spec.Basic.QosOk())
    if frame.marshal() != frame_data:
        assert False, "Basic.QosOk frame did not match frame data sample"


def encode_basic_recover_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00<\x00n\x00\xce'
    kwargs = {'requeue': False}
    frame = pika.frame.Method(1, pika.spec.Basic.Recover(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Recover frame did not match frame data sample"


def encode_basic_recoverasync_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00<\x00d\x00\xce'
    kwargs = {'requeue': False}
    frame = pika.frame.Method(1, pika.spec.Basic.RecoverAsync(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.RecoverAsync frame did not match frame data sample"


def encode_basic_recoverok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00<\x00o\xce'
    frame = pika.frame.Method(1, pika.spec.Basic.RecoverOk())
    if frame.marshal() != frame_data:
        assert False, "Basic.RecoverOk frame did not match frame data sample"


def encode_basic_reject_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\r\x00<\x00Z\x00\x00\x00\x00\x00\x00\x00\x01\x00\xce'
    kwargs = {'requeue': False, 'delivery_tag': 1}
    frame = pika.frame.Method(1, pika.spec.Basic.Reject(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Reject frame did not match frame data sample"


def encode_basic_return_test():
    frame_data = '\x01\x00\x01\x00\x00\x00"\x00<\x002\x00\xc8\x0fNormal shutdown\x03foo\x07foo.bar\xce'
    kwargs = {'reply_code': 200, 'reply_text': 'Normal shutdown', 'routing_key': 'foo.bar', 'exchange': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Basic.Return(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Basic.Return frame did not match frame data sample"


def encode_channel_close_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x1a\x00\x14\x00(\x00\xc8\x0fNormal shutdown\x00\n\x00\x01\xce'
    kwargs = {'class_id': 10, 'method_id': 1, 'reply_code': 200, 'reply_text': 'Normal shutdown'}
    frame = pika.frame.Method(1, pika.spec.Channel.Close(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Channel.Close frame did not match frame data sample"


def encode_channel_closeok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00\x14\x00)\xce'
    frame = pika.frame.Method(1, pika.spec.Channel.CloseOk())
    if frame.marshal() != frame_data:
        assert False, "Channel.CloseOk frame did not match frame data sample"


def encode_channel_flow_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00\x14\x00\x14\x01\xce'
    kwargs = {'active': True}
    frame = pika.frame.Method(1, pika.spec.Channel.Flow(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Channel.Flow frame did not match frame data sample"


def encode_channel_flowok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00\x14\x00\x15\x01\xce'
    kwargs = {'active': True}
    frame = pika.frame.Method(1, pika.spec.Channel.FlowOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Channel.FlowOk frame did not match frame data sample"


def encode_channel_open_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00\x14\x00\n\x00\xce'
    kwargs = {'out_of_band': ''}
    frame = pika.frame.Method(1, pika.spec.Channel.Open(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Channel.Open frame did not match frame data sample"


def encode_channel_openok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0b\x00\x14\x00\x0b\x00\x00\x00\x03foo\xce'
    kwargs = {'channel_id': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Channel.OpenOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Channel.OpenOk frame did not match frame data sample"


def encode_confirm_select_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00U\x00\n\x00\xce'
    kwargs = {'nowait': 0}
    frame = pika.frame.Method(1, pika.spec.Confirm.Select(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Confirm.Select frame did not match frame data sample"


def encode_confirm_selectok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00U\x00\x0b\xce'
    frame = pika.frame.Method(1, pika.spec.Confirm.SelectOk())
    if frame.marshal() != frame_data:
        assert False, "Confirm.SelectOk frame did not match frame data sample"


def encode_connection_close_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x1a\x00\n\x002\x00\xc8\x0fNormal shutdown\x00\n\x00\x01\xce'
    kwargs = {'class_id': 10, 'method_id': 1, 'reply_code': 200, 'reply_text': 'Normal shutdown'}
    frame = pika.frame.Method(1, pika.spec.Connection.Close(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.Close frame did not match frame data sample"


def encode_connection_closeok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00\n\x003\xce'
    frame = pika.frame.Method(1, pika.spec.Connection.CloseOk())
    if frame.marshal() != frame_data:
        assert False, "Connection.CloseOk frame did not match frame data sample"


def encode_connection_open_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\r\x00\n\x00(\x01/\x05PLAIN\x00\xce'
    kwargs = {'insist': False, 'capabilities': 'PLAIN', 'virtual_host': '/'}
    frame = pika.frame.Method(1, pika.spec.Connection.Open(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.Open frame did not match frame data sample"


def encode_connection_openok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x05\x00\n\x00)\x00\xce'
    kwargs = {'known_hosts': ''}
    frame = pika.frame.Method(1, pika.spec.Connection.OpenOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.OpenOk frame did not match frame data sample"


def encode_connection_secure_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x08\x00\n\x00\x14\x00\x00\x00\x00\xce'
    kwargs = {'challenge': ''}
    frame = pika.frame.Method(1, pika.spec.Connection.Secure(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.Secure frame did not match frame data sample"


def encode_connection_secureok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x08\x00\n\x00\x15\x00\x00\x00\x00\xce'
    kwargs = {'response': ''}
    frame = pika.frame.Method(1, pika.spec.Connection.SecureOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.SecureOk frame did not match frame data sample"


def encode_connection_start_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\xd8\x00\n\x00\n\x00\t\x00\x00\x00\xb8\x0binformationS\x00\x00\x005Licensed under the MPL.  See http://www.rabbitmq.com/\x07productS\x00\x00\x00\x08RabbitMQ\x07versionS\x00\x00\x00\x052.3.1\tcopyrightS\x00\x00\x00$Copyright (C) 2007-2011 VMware, Inc.\x08platformS\x00\x00\x00\nErlang/OTP\x00\x00\x00\x0ePLAIN AMQPLAIN\x00\x00\x00\x00\xce'
    kwargs = {'server_properties': {'information': 'Licensed under the MPL.  See http://www.rabbitmq.com/', 'product': 'RabbitMQ', 'version': '2.3.1', 'copyright': 'Copyright (C) 2007-2011 VMware, Inc.', 'platform': 'Erlang/OTP'}, 'version_minor': 9, 'mechanisms': 'PLAIN AMQPLAIN', 'locales': '', 'version_major': 0}
    frame = pika.frame.Method(1, pika.spec.Connection.Start(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.Start frame did not match frame data sample"


def encode_connection_startok_test():
    frame_data = '\x01\x00\x01\x00\x00\x003\x00\n\x00\x0b\x00\x00\x00\x1b\x07productS\x00\x00\x00\x0ePika Test Tool\x05PLAIN\x00\x00\x00\x00\x05en_US\xce'
    kwargs = {'locale': 'en_US', 'mechanism': 'PLAIN', 'client_properties': {'product': 'Pika Test Tool'}, 'response': ''}
    frame = pika.frame.Method(1, pika.spec.Connection.StartOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.StartOk frame did not match frame data sample"


def encode_connection_tune_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0c\x00\n\x00\x1e\x00\x00\x00\x02\x00\x00\x00\x00\xce'
    kwargs = {'frame_max': 131072, 'channel_max': 0, 'heartbeat': 0}
    frame = pika.frame.Method(1, pika.spec.Connection.Tune(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.Tune frame did not match frame data sample"


def encode_connection_tuneok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0c\x00\n\x00\x1f\x00\x00\x00\x02\x00\x00\x00\x00\xce'
    kwargs = {'frame_max': 131072, 'channel_max': 0, 'heartbeat': 0}
    frame = pika.frame.Method(1, pika.spec.Connection.TuneOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Connection.TuneOk frame did not match frame data sample"


def encode_exchange_bind_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x15\x00(\x00\x1e\x00\x00\x00\x00\x07foo.bar\x00\x00\x00\x00\x00\xce'
    kwargs = {'source': '', 'ticket': 0, 'destination': '', 'nowait': 0, 'routing_key': 'foo.bar'}
    frame = pika.frame.Method(1, pika.spec.Exchange.Bind(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Exchange.Bind frame did not match frame data sample"


def encode_exchange_bindok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00(\x00\x1f\xce'
    frame = pika.frame.Method(1, pika.spec.Exchange.BindOk())
    if frame.marshal() != frame_data:
        assert False, "Exchange.BindOk frame did not match frame data sample"


def encode_exchange_declare_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x16\x00(\x00\n\x00\x00\x03foo\x06direct\x00\x00\x00\x00\x00\xce'
    kwargs = {'nowait': 0, 'exchange': 'foo', 'durable': False, 'passive': 0, 'internal': False, 'ticket': 0, 'auto_delete': False}
    frame = pika.frame.Method(1, pika.spec.Exchange.Declare(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Exchange.Declare frame did not match frame data sample"


def encode_exchange_declareok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00(\x00\x0b\xce'
    frame = pika.frame.Method(1, pika.spec.Exchange.DeclareOk())
    if frame.marshal() != frame_data:
        assert False, "Exchange.DeclareOk frame did not match frame data sample"


def encode_exchange_delete_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0b\x00(\x00\x14\x00\x00\x03foo\x00\xce'
    kwargs = {'ticket': 0, 'if_unused': False, 'nowait': 0, 'exchange': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Exchange.Delete(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Exchange.Delete frame did not match frame data sample"


def encode_exchange_deleteok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00(\x00\x15\xce'
    frame = pika.frame.Method(1, pika.spec.Exchange.DeleteOk())
    if frame.marshal() != frame_data:
        assert False, "Exchange.DeleteOk frame did not match frame data sample"


def encode_exchange_unbind_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x15\x00(\x00(\x00\x00\x00\x00\x07foo.bar\x00\x00\x00\x00\x00\xce'
    kwargs = {'source': '', 'ticket': 0, 'destination': '', 'nowait': 0, 'routing_key': 'foo.bar'}
    frame = pika.frame.Method(1, pika.spec.Exchange.Unbind(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Exchange.Unbind frame did not match frame data sample"


def encode_exchange_unbindok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00(\x003\xce'
    frame = pika.frame.Method(1, pika.spec.Exchange.UnbindOk())
    if frame.marshal() != frame_data:
        assert False, "Exchange.UnbindOk frame did not match frame data sample"


def encode_queue_bind_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x1b\x002\x00\x14\x00\x00\x03bar\x03foo\x07foo.bar\x00\x00\x00\x00\x00\xce'
    kwargs = {'queue': 'bar', 'ticket': 0, 'nowait': 0, 'routing_key': 'foo.bar', 'exchange': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Queue.Bind(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.Bind frame did not match frame data sample"


def encode_queue_bindok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x002\x00\x15\xce'
    frame = pika.frame.Method(1, pika.spec.Queue.BindOk())
    if frame.marshal() != frame_data:
        assert False, "Queue.BindOk frame did not match frame data sample"


def encode_queue_declare_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0f\x002\x00\n\x00\x00\x03bar\x00\x00\x00\x00\x00\xce'
    kwargs = {'passive': 0, 'nowait': 0, 'exclusive': False, 'durable': False, 'queue': 'bar', 'ticket': 0, 'auto_delete': False}
    frame = pika.frame.Method(1, pika.spec.Queue.Declare(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.Declare frame did not match frame data sample"


def encode_queue_declareok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x10\x002\x00\x0b\x03bar\x00\x00\x00\x01\x00\x00\x00\x01\xce'
    kwargs = {'queue': 'bar', 'message_count': 1, 'consumer_count': 1}
    frame = pika.frame.Method(1, pika.spec.Queue.DeclareOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.DeclareOk frame did not match frame data sample"


def encode_queue_delete_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0b\x002\x00(\x00\x00\x03bar\x00\xce'
    kwargs = {'queue': 'bar', 'ticket': 0, 'if_empty': False, 'nowait': 0, 'if_unused': False}
    frame = pika.frame.Method(1, pika.spec.Queue.Delete(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.Delete frame did not match frame data sample"


def encode_queue_deleteok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x08\x002\x00)\x00\x00\x00\x01\xce'
    kwargs = {'message_count': 1}
    frame = pika.frame.Method(1, pika.spec.Queue.DeleteOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.DeleteOk frame did not match frame data sample"


def encode_queue_purge_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x0b\x002\x00\x1e\x00\x00\x03bar\x00\xce'
    kwargs = {'queue': 'bar', 'ticket': 0, 'nowait': 0}
    frame = pika.frame.Method(1, pika.spec.Queue.Purge(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.Purge frame did not match frame data sample"


def encode_queue_purgeok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x08\x002\x00\x1f\x00\x00\x00\x01\xce'
    kwargs = {'message_count': 1}
    frame = pika.frame.Method(1, pika.spec.Queue.PurgeOk(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.PurgeOk frame did not match frame data sample"


def encode_queue_unbind_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x1a\x002\x002\x00\x00\x03bar\x03foo\x07foo.bar\x00\x00\x00\x00\xce'
    kwargs = {'queue': 'bar', 'ticket': 0, 'routing_key': 'foo.bar', 'exchange': 'foo'}
    frame = pika.frame.Method(1, pika.spec.Queue.Unbind(**kwargs))
    if frame.marshal() != frame_data:
        assert False, "Queue.Unbind frame did not match frame data sample"


def encode_queue_unbindok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x002\x003\xce'
    frame = pika.frame.Method(1, pika.spec.Queue.UnbindOk())
    if frame.marshal() != frame_data:
        assert False, "Queue.UnbindOk frame did not match frame data sample"


def encode_tx_commit_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x14\xce'
    frame = pika.frame.Method(1, pika.spec.Tx.Commit())
    if frame.marshal() != frame_data:
        assert False, "Tx.Commit frame did not match frame data sample"


def encode_tx_commitok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x15\xce'
    frame = pika.frame.Method(1, pika.spec.Tx.CommitOk())
    if frame.marshal() != frame_data:
        assert False, "Tx.CommitOk frame did not match frame data sample"


def encode_tx_rollback_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x1e\xce'
    frame = pika.frame.Method(1, pika.spec.Tx.Rollback())
    if frame.marshal() != frame_data:
        assert False, "Tx.Rollback frame did not match frame data sample"


def encode_tx_rollbackok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x1f\xce'
    frame = pika.frame.Method(1, pika.spec.Tx.RollbackOk())
    if frame.marshal() != frame_data:
        assert False, "Tx.RollbackOk frame did not match frame data sample"


def encode_tx_select_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\n\xce'
    frame = pika.frame.Method(1, pika.spec.Tx.Select())
    if frame.marshal() != frame_data:
        assert False, "Tx.Select frame did not match frame data sample"


def encode_tx_selectok_test():
    frame_data = '\x01\x00\x01\x00\x00\x00\x04\x00Z\x00\x0b\xce'
    frame = pika.frame.Method(1, pika.spec.Tx.SelectOk())
    if frame.marshal() != frame_data:
        assert False, "Tx.SelectOk frame did not match frame data sample"

###########################################################################
## The following test cases were constructed following the discussion
## of GH issue 105, https://github.com/pika/pika/issues/105

def encode_empty_basic_properties_test():
    frame_data = ('02 0001 0000000e' + # frametype 2, channel 1, 14 bytes payload
                  '003c 0000 0000000000000080' + # basic class, "weight" (reserved), len 128
                  '0000' + # no property fields supplied
                  'ce' + # frame end byte
                  '').replace(' ', '').decode('hex')
    frame = pika.frame.Header(1, 128, pika.spec.BasicProperties())
    if frame.marshal() != frame_data:
        assert False, frame.marshal().encode('hex')

def encode_basic_properties_with_nonunicode_test():
    frame_data = ('02 0001 0000000f' + # frametype 2, channel 1, 15 bytes payload
                  '003c 0000 0000000000000080' + # basic class, "weight" (reserved), len 128
                  '0400 00' + # correlation_id supplied as the empty string
                  'ce' + # frame end byte
                  '').replace(' ', '').decode('hex')
    frame = pika.frame.Header(1, 128, pika.spec.BasicProperties(correlation_id = ''))
    if frame.marshal() != frame_data:
        assert False, frame.marshal().encode('hex')

## These next two are the most interesting tests. Issue #105 was the
## interaction between a unicode string being passed as a message
## property and the use of a length of 128 bytes. Even though the
## unicode string didn't have any characters needing encoding, the use
## of "''.join(pieces)" in the frame marshalling code caused the
## unicodeness to *contaminate* the other pieces, causing it to try to
## interpret the 0x80 length byte as a 7-bit ASCII character! The
## solution is to check for non-str arguments during encoding of
## properties and methods.

def encode_basic_properties_with_unicode_test():
    frame = pika.frame.Header(1, 128, pika.spec.BasicProperties(correlation_id = unicode('')))
    try:
        frame.marshal()
        assert False, "Expected a marshalling assertion failure"
    except AssertionError, e:
        expectedMessage = 'A non-bytestring value was supplied for self.correlation_id'
        assert e.message == expectedMessage, \
            "Expected a different message on assertion failure; got: " + repr(e.message)

def encode_basic_properties_with_encoded_unicode_test():
    frame_data = ('02 0001 0000000f' + # frametype 2, channel 1, 15 bytes payload
                  '003c 0000 0000000000000080' + # basic class, "weight" (reserved), len 128
                  '0400 00' + # correlation_id supplied as the empty string
                  'ce' + # frame end byte
                  '').replace(' ', '').decode('hex')
    frame = pika.frame.Header(1, 128, pika.spec.BasicProperties(correlation_id = \
                                                                    unicode('').encode('utf-8')))
    if frame.marshal() != frame_data:
        assert False, frame.marshal().encode('hex')

###########################################################################

if __name__ == "__main__":
    unittest.main()
