"""
Tests for pika.frame

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import exceptions
from pika import frame
from pika import spec


class FrameTests(unittest.TestCase):

    BASIC_ACK = ('\x01\x00\x01\x00\x00\x00\r\x00<\x00P\x00\x00\x00\x00\x00\x00'
                 '\x00d\x00\xce')
    BODY_FRAME = '\x03\x00\x01\x00\x00\x00\x14I like it that sound\xce'
    BODY_FRAME_VALUE = 'I like it that sound'
    CONTENT_HEADER = ('\x02\x00\x01\x00\x00\x00\x0f\x00<\x00\x00\x00'
                      '\x00\x00\x00\x00\x00\x00d\x10\x00\x02\xce')
    HEARTBEAT = '\x08\x00\x00\x00\x00\x00\x00\xce'
    PROTOCOL_HEADER = 'AMQP\x00\x00\t\x01'

    def frame_marshal_not_implemented_test(self):
        frame_obj = frame.Frame(0x000A000B, 1)
        self.assertRaises(NotImplementedError, frame_obj.marshal)

    def frame_underscore_marshal_test(self):
        basic_ack = frame.Method(1, spec.Basic.Ack(100))
        self.assertEqual(basic_ack.marshal(), self.BASIC_ACK)

    def headers_marshal_test(self):
        header = frame.Header(1, 100,
                              spec.BasicProperties(delivery_mode=2))
        self.assertEqual(header.marshal(), self.CONTENT_HEADER)

    def body_marshal_test(self):
        body = frame.Body(1, 'I like it that sound')
        self.assertEqual(body.marshal(), self.BODY_FRAME)

    def heartbeat_marshal_test(self):
        heartbeat = frame.Heartbeat()
        self.assertEqual(heartbeat.marshal(), self.HEARTBEAT)

    def protocol_header_marshal_test(self):
        protocol_header = frame.ProtocolHeader()
        self.assertEqual(protocol_header.marshal(), self.PROTOCOL_HEADER)

    def decode_protocol_header_instance_test(self):
        self.assertIsInstance(frame.decode_frame(self.PROTOCOL_HEADER)[1],
                              frame.ProtocolHeader)

    def decode_protocol_header_bytes_test(self):
        self.assertEqual(frame.decode_frame(self.PROTOCOL_HEADER)[0], 8)

    def decode_method_frame_instance_test(self):
        self.assertIsInstance(frame.decode_frame(self.BASIC_ACK)[1],
                              frame.Method)

    def decode_protocol_header_failure_test(self):
        self.assertEqual(frame.decode_frame('AMQPa'), (0, None))

    def decode_method_frame_bytes_test(self):
        self.assertEqual(frame.decode_frame(self.BASIC_ACK)[0], 21)

    def decode_method_frame_method_test(self):
        self.assertIsInstance(frame.decode_frame(self.BASIC_ACK)[1].method,
                              spec.Basic.Ack)

    def decode_header_frame_instance_test(self):
        self.assertIsInstance(frame.decode_frame(self.CONTENT_HEADER)[1],
                              frame.Header)

    def decode_header_frame_bytes_test(self):
        self.assertEqual(frame.decode_frame(self.CONTENT_HEADER)[0], 23)

    def decode_header_frame_properties_test(self):
        frame_value = frame.decode_frame(self.CONTENT_HEADER)[1]
        self.assertIsInstance(frame_value.properties, spec.BasicProperties)

    def decode_frame_decoding_failure_test(self):
        self.assertEqual(frame.decode_frame('\x01\x00\x01\x00\x00\xce'),
                         (0, None))

    def decode_frame_decoding_no_end_byte_test(self):
        self.assertEqual(frame.decode_frame(self.BASIC_ACK[:-1]), (0, None))

    def decode_frame_decoding_wrong_end_byte_test(self):
        self.assertRaises(exceptions.InvalidFrameError,
                          frame.decode_frame,
                          self.BASIC_ACK[:-1] + 'A')

    def decode_body_frame_instance_test(self):
        self.assertIsInstance(frame.decode_frame(self.BODY_FRAME)[1],
                              frame.Body)

    def decode_body_frame_fragment_test(self):
        self.assertEqual(frame.decode_frame(self.BODY_FRAME)[1].fragment,
                         self.BODY_FRAME_VALUE)

    def decode_body_frame_fragment_consumed_bytes_test(self):
        self.assertEqual(frame.decode_frame(self.BODY_FRAME)[0], 28)

    def decode_heartbeat_frame_test(self):
        self.assertIsInstance(frame.decode_frame(self.HEARTBEAT)[1],
                              frame.Heartbeat)

    def decode_heartbeat_frame_bytes_consumed_test(self):
        self.assertEqual(frame.decode_frame(self.HEARTBEAT)[0], 8)

    def decode_frame_invalid_frame_type_test(self):
        self.assertRaises(exceptions.InvalidFrameError,
                          frame.decode_frame,
                          '\x09\x00\x00\x00\x00\x00\x00\xce')
