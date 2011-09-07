# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Tests for decoding raw frame data and validating the objects that is created
from the raw frame data.
"""

import support
import pika.frame
import pika.spec as spec


def decode_frame(frame_data):
    """
    Calls pika.frame.decode and validates the consumed amount is the same as
    the amount of data sent in
    """
    bytes_consumed, frame = pika.frame.decode_frame(frame_data)

    # Validate our length vs consumed amount
    if len(frame_data) != bytes_consumed:
        assert False, "frame_decode did not decode full frame_data,\
 sent %i bytes, used %i bytes" % (len(frame_data), bytes_consumed)

    # Only return the frame
    return frame


def validate_method_frame(frame, method):
    """
    Pass in a fully decoded frame object and it will validate that it is a
    method frame and return the frame.method object for additional test
    inspection.
    """

    # Make sure we have the right type of frame
    if frame.name != 'Method':
        assert False, "Invalid Frame Type: %s" % frame.name

    # Validate it's a method frame by frame_type
    if frame.frame_type != spec.FRAME_METHOD:
        assert False, "Frame type didn't match %i != %i" % (spec.FRAME_METHOD,
                                                            frame.frame_type)

    # Validate it has a frame method
    if not hasattr(frame, 'method'):
        assert False, "Did not find the method attribute in the frame"

    # Validate it has an integer channel number
    if not hasattr(frame, 'channel_number') or \
        not isinstance(frame.channel_number, int):
        assert False, "Missing or non-integer channel number"

    if not isinstance(frame.method, method):
        assert False, "Expected %s, received %s" % (method.NAME,
                                                    frame.method.name)

    # Return the frame method
    return frame.method


def validate_attribute(method, attribute, attribute_type, value='ignore'):
    """
    Validate that the given method object has the specified attribute of the
    specified attribute_type. If a value is passed in, validate that as well
    """
    if not hasattr(method, attribute):
        assert False, "%s missing %s attribute" % (method.NAME, attribute)

    if getattr(method, attribute) and \
       not isinstance(getattr(method, attribute), attribute_type):
        assert False, "%s.%s is not %s" % \
                      (method.NAME, attribute, attribute_type)

    if value != 'ignore' and value != getattr(method, attribute):
        assert False, "Expected a value of %r, received %r" % \
                      (value, getattr(method, attribute))


def decode_protocol_header_test():

    # Raw Frame Data
    frame_data = b'AMQP\x00\x00\t\x01'

    # Decode the frame and validate lengths
    frame = decode_frame(frame_data)

    if frame.name != 'ProtocolHeader':
        assert False, "Invalid Frame Type: %s" % frame.name

    if frame.frame_type != -1:
        assert False, "Frame type didn't match -1: %i" % frame.frame_type

    if (frame.major, frame.minor, frame.revision) != spec.PROTOCOL_VERSION:
        assert False, "Invalid Protocol Version: %i-%i-%i" % \
                      (frame.major, frame.minor, frame.revision)


def decode_header_frame_test():

    frame_data = b'\x02\x00\x01\x00\x00\x00\x1a\x00<\x00\x00\x00\x00\x00\x00\x00\x00\x00#\x90\x00\ntext/plain\x01\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)
    validate_attribute(frame, 'frame_type', int, 2)
    validate_attribute(frame, 'channel_number', int, 1)
    validate_attribute(frame, 'properties', spec.BasicProperties)
    validate_attribute(frame.properties, 'user_id', str)
    validate_attribute(frame.properties, 'timestamp', int)
    validate_attribute(frame.properties, 'delivery_mode', int, 1)
    validate_attribute(frame.properties, 'app_id', str)
    validate_attribute(frame.properties, 'priority', int)
    validate_attribute(frame.properties, 'headers', dict)
    validate_attribute(frame.properties, 'correlation_id', str)
    validate_attribute(frame.properties, 'cluster_id', str)
    validate_attribute(frame.properties, 'content_encoding', str)
    validate_attribute(frame.properties, 'content_type', str, 'text/plain')
    validate_attribute(frame.properties, 'reply_to', str, None)
    validate_attribute(frame.properties, 'type', int, None)
    validate_attribute(frame.properties, 'message_id', str, None)
    validate_attribute(frame.properties, 'expiration', str, None)


def decode_body_frame_test():

    frame_data = b'\x03\x00\x01\x00\x00\x00#Hello World #9: 1299445757.73953295\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    validate_attribute(frame, 'frame_type', int, 3)
    validate_attribute(frame, 'channel_number', int, 1)
    validate_attribute(frame, 'fragment', bytes, b'Hello World #9: 1299445757.73953295')


def decode_connection_start_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00\xdd\x00\n\x00\n\x00\t\x00\x00\x00\xb8\tcopyrightS\x00\x00\x00$Copyright (C) 2007-2011 VMware, Inc.\x0binformationS\x00\x00\x005Licensed under the MPL.  See http://www.rabbitmq.com/\x08platformS\x00\x00\x00\nErlang/OTP\x07productS\x00\x00\x00\x08RabbitMQ\x07versionS\x00\x00\x00\x052.3.1\x00\x00\x00\x0ePLAIN AMQPLAIN\x00\x00\x00\x05en_US\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Connection.Start)

    validate_attribute(method, 'server_properties', dict, {'information': 'Licensed under the MPL.  See http://www.rabbitmq.com/', 'product': 'RabbitMQ', 'version': '2.3.1', 'copyright': 'Copyright (C) 2007-2011 VMware, Inc.', 'platform': 'Erlang/OTP'})
    validate_attribute(method, 'version_minor', int, 9)
    validate_attribute(method, 'mechanisms', str, 'PLAIN AMQPLAIN')
    validate_attribute(method, 'locales', str, 'en_US')
    validate_attribute(method, 'version_major', int, 0)


def decode_connection_startok_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00P\x00\n\x00\x0b\x00\x00\x00,\x07productS\x00\x00\x00\x1fPika Python AMQP Client Library\x05PLAIN\x00\x00\x00\x0c\x00guest\x00guest\x05en_US\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Connection.StartOk)

    validate_attribute(method, 'locale', str, 'en_US')
    validate_attribute(method, 'mechanism', str, 'PLAIN')
    validate_attribute(method, 'client_properties', dict, {'product': 'Pika Python AMQP Client Library'})
    validate_attribute(method, 'response', str, '\x00guest\x00guest')


def decode_connection_tune_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00\x0c\x00\n\x00\x1e\x00\x00\x00\x02\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Connection.Tune)

    validate_attribute(method, 'frame_max', int, 131072)
    validate_attribute(method, 'channel_max', int, 0)
    validate_attribute(method, 'heartbeat', int, 0)


def decode_connection_tuneok_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00\x0c\x00\n\x00\x1f\x00\x00\x00\x02\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Connection.TuneOk)

    validate_attribute(method, 'frame_max', int, 131072)
    validate_attribute(method, 'channel_max', int, 0)
    validate_attribute(method, 'heartbeat', int, 0)


def decode_connection_open_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00\x08\x00\n\x00(\x01/\x00\x01\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Connection.Open)

    validate_attribute(method, 'insist', bool, True)
    validate_attribute(method, 'capabilities', str, '')
    validate_attribute(method, 'virtual_host', str, '/')


def decode_connection_openok_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00\x05\x00\n\x00)\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Connection.OpenOk)

    validate_attribute(method, 'known_hosts', str, '')


def decode_connection_close_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00\x1a\x00\n\x002\x00\xc8\x0fNormal shutdown\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Connection.Close)

    validate_attribute(method, 'class_id', int, 0)
    validate_attribute(method, 'method_id', int, 0)
    validate_attribute(method, 'reply_code', int, 200)
    validate_attribute(method, 'reply_text', str, 'Normal shutdown')


def decode_connection_closeok_test():

    frame_data = b'\x01\x00\x00\x00\x00\x00\x04\x00\n\x003\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    validate_method_frame(frame, spec.Connection.CloseOk)


def decode_channel_open_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x05\x00\x14\x00\n\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Channel.Open)

    validate_attribute(method, 'out_of_band', str, '')


def decode_channel_openok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x08\x00\x14\x00\x0b\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Channel.OpenOk)

    validate_attribute(method, 'channel_id', str, '')


def decode_queue_declare_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x10\x002\x00\n\x00\x00\x04test\x02\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Queue.Declare)

    validate_attribute(method, 'passive', bool, False)
    validate_attribute(method, 'nowait', bool, False)
    validate_attribute(method, 'exclusive', bool, False)
    validate_attribute(method, 'durable', bool, True)
    validate_attribute(method, 'queue', str, 'test')
    validate_attribute(method, 'arguments', dict, {})
    validate_attribute(method, 'ticket', int, 0)
    validate_attribute(method, 'auto_delete', bool, False)


def decode_queue_declareok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x11\x002\x00\x0b\x04test\x00\x00\x00\x00\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Queue.DeclareOk)

    validate_attribute(method, 'queue', str, 'test')
    validate_attribute(method, 'message_count', int, 0)
    validate_attribute(method, 'consumer_count', int, 0)


def decode_exchange_declare_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00/\x00(\x00\n\x00\x00\x1ctest-blocking_exchange-34219\x06direct\x04\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Exchange.Declare)

    validate_attribute(method, 'nowait', bool, False)
    validate_attribute(method, 'exchange', str, 'test-blocking_exchange-34219')
    validate_attribute(method, 'durable', bool, False)
    validate_attribute(method, 'passive', bool, False)
    validate_attribute(method, 'internal', bool, False)
    validate_attribute(method, 'arguments', dict, {})
    validate_attribute(method, 'ticket', int, 0)
    validate_attribute(method, 'type', str, 'direct')
    validate_attribute(method, 'auto_delete', bool, True)


def decode_exchange_declareok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x04\x00(\x00\x0b\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    validate_method_frame(frame, spec.Exchange.DeclareOk)


def decode_queue_bind_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00}\x002\x00\x14\x00\x00\x1btest-blocking_consume-34219\x1ctest-blocking_exchange-342198test-blocking_exchange-34219.test-blocking_consume-34219\x00\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Queue.Bind)

    validate_attribute(method, 'nowait', bool, False)
    validate_attribute(method, 'exchange', str, 'test-blocking_exchange-34219')
    validate_attribute(method, 'routing_key', str, 'test-blocking_exchange-34219.test-blocking_consume-34219')
    validate_attribute(method, 'queue', str, 'test-blocking_consume-34219')
    validate_attribute(method, 'arguments', dict, {})
    validate_attribute(method, 'ticket', int, 0)


def decode_queue_bindok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x04\x002\x00\x15\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    validate_method_frame(frame, spec.Queue.BindOk)


def decode_basic_publish_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00]\x00<\x00(\x00\x00\x1ctest-blocking_exchange-342198test-blocking_exchange-34219.test-blocking_consume-34219\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Basic.Publish)

    validate_attribute(method, 'ticket', int, 0)
    validate_attribute(method, 'mandatory', bool, False)
    validate_attribute(method, 'routing_key', str, 'test-blocking_exchange-34219.test-blocking_consume-34219')
    validate_attribute(method, 'immediate', bool, False)
    validate_attribute(method, 'exchange', str, 'test-blocking_exchange-34219')


def decode_basic_consume_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x16\x00<\x00\x14\x00\x00\x04test\x05ctag0\x00\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Basic.Consume)

    validate_attribute(method, 'exclusive', bool, False)
    validate_attribute(method, 'nowait', bool, False)
    validate_attribute(method, 'no_local', bool, False)
    validate_attribute(method, 'consumer_tag', str, 'ctag0')
    validate_attribute(method, 'queue', str, 'test')
    validate_attribute(method, 'arguments', dict, {})
    validate_attribute(method, 'ticket', int, 0)
    validate_attribute(method, 'no_ack', bool, False)


def decode_basic_consumeok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\n\x00<\x00\x15\x05ctag0\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Basic.ConsumeOk)

    validate_attribute(method, 'consumer_tag', str, 'ctag0')


def decode_basic_cancel_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x0b\x00<\x00\x1e\x05ctag0\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Basic.Cancel)

    validate_attribute(method, 'consumer_tag', str, 'ctag0')
    validate_attribute(method, 'nowait', bool, False)


def decode_basic_cancelok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\n\x00<\x00\x1f\x05ctag0\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Basic.CancelOk)

    validate_attribute(method, 'consumer_tag', str, 'ctag0')


def decode_channel_close_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x1a\x00\x14\x00(\x00\xc8\x0fNormal shutdown\x00\x00\x00\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Channel.Close)

    validate_attribute(method, 'class_id', int, 0)
    validate_attribute(method, 'method_id', int, 0)
    validate_attribute(method, 'reply_code', int, 200)
    validate_attribute(method, 'reply_text', str, 'Normal shutdown')


def decode_channel_closeok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x04\x00\x14\x00)\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    validate_method_frame(frame, spec.Channel.CloseOk)


def decode_basic_ack_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\r\x00<\x00P\x00\x00\x00\x00\x00\x00\x00\x01\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Basic.Ack)

    validate_attribute(method, 'delivery_tag', int, 1)
    validate_attribute(method, 'multiple', bool, False)


def decode_confirm_select_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x05\x00U\x00\n\x00\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    method = validate_method_frame(frame, spec.Confirm.Select)

    validate_attribute(method, 'nowait', bool, False)


def decode_confirm_selectok_test():

    frame_data = b'\x01\x00\x01\x00\x00\x00\x04\x00U\x00\x0b\xce'

    # Decode our frame data and validate lengths
    frame = decode_frame(frame_data)

    # Verify it is a method frame and return frame.method
    validate_method_frame(frame, spec.Confirm.SelectOk)

if __name__ == "__main__":
    unittest.main()
