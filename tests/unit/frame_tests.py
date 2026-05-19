"""
Tests for pika.frame

"""
import unittest

from pika import DeliveryMode, exceptions, frame, spec


class FrameTests(unittest.TestCase):

    BASIC_ACK = (b'\x01\x00\x01\x00\x00\x00\r\x00<\x00P\x00\x00\x00\x00\x00\x00'
                 b'\x00d\x00\xce')
    BODY_FRAME = b'\x03\x00\x01\x00\x00\x00\x14I like it that sound\xce'
    BODY_FRAME_VALUE = b'I like it that sound'
    CONTENT_HEADER = (b'\x02\x00\x01\x00\x00\x00\x0f\x00<\x00\x00\x00'
                      b'\x00\x00\x00\x00\x00\x00d\x10\x00\x02\xce')
    HEARTBEAT = b'\x08\x00\x00\x00\x00\x00\x00\xce'
    PROTOCOL_HEADER = b'AMQP\x00\x00\t\x01'

    def test_frame_marshal_not_implemented(self):
        frame_obj = frame.Frame(0x000A000B, 1)
        self.assertRaises(NotImplementedError, frame_obj.marshal)

    def test_frame_underscore_marshal(self):
        basic_ack = frame.Method(1, spec.Basic.Ack(100))
        self.assertEqual(basic_ack.marshal(), self.BASIC_ACK)

    def test_headers_marshal(self):
        header = frame.Header(
            1, 100, spec.BasicProperties(delivery_mode=DeliveryMode.Persistent.value))
        self.assertEqual(header.marshal(), self.CONTENT_HEADER)

    def test_body_marshal(self):
        body = frame.Body(1, b'I like it that sound')
        self.assertEqual(body.marshal(), self.BODY_FRAME)

    def test_heartbeat_marshal(self):
        heartbeat = frame.Heartbeat()
        self.assertEqual(heartbeat.marshal(), self.HEARTBEAT)

    def test_protocol_header_marshal(self):
        protocol_header = frame.ProtocolHeader()
        self.assertEqual(protocol_header.marshal(), self.PROTOCOL_HEADER)

    def test_decode_protocol_header_instance(self):
        self.assertIsInstance(
            frame.decode_frame(self.PROTOCOL_HEADER)[1], frame.ProtocolHeader)

    def test_decode_protocol_header_bytes(self):
        self.assertEqual(frame.decode_frame(self.PROTOCOL_HEADER)[0], 8)

    def test_decode_method_frame_instance(self):
        self.assertIsInstance(
            frame.decode_frame(self.BASIC_ACK)[1], frame.Method)

    def test_decode_protocol_header_failure(self):
        self.assertEqual(frame.decode_frame(b'AMQPa'), (0, None))

    def test_decode_method_frame_bytes(self):
        self.assertEqual(frame.decode_frame(self.BASIC_ACK)[0], 21)

    def test_decode_method_frame_method(self):
        self.assertIsInstance(
            frame.decode_frame(self.BASIC_ACK)[1].method, spec.Basic.Ack)

    def test_decode_header_frame_instance(self):
        self.assertIsInstance(
            frame.decode_frame(self.CONTENT_HEADER)[1], frame.Header)

    def test_decode_header_frame_bytes(self):
        self.assertEqual(frame.decode_frame(self.CONTENT_HEADER)[0], 23)

    def test_decode_header_frame_properties(self):
        frame_value = frame.decode_frame(self.CONTENT_HEADER)[1]
        self.assertIsInstance(frame_value.properties, spec.BasicProperties)

    def test_decode_frame_decoding_failure(self):
        self.assertEqual(frame.decode_frame(b'\x01\x00\x01\x00\x00\xce'),
                         (0, None))

    def test_decode_frame_decoding_no_end_byte(self):
        self.assertEqual(frame.decode_frame(self.BASIC_ACK[:-1]), (0, None))

    def test_decode_frame_decoding_wrong_end_byte(self):
        self.assertRaises(exceptions.InvalidFrameError, frame.decode_frame,
                          self.BASIC_ACK[:-1] + b'A')

    def test_decode_body_frame_instance(self):
        self.assertIsInstance(
            frame.decode_frame(self.BODY_FRAME)[1], frame.Body)

    def test_decode_body_frame_fragment(self):
        self.assertEqual(
            frame.decode_frame(self.BODY_FRAME)[1].fragment,
            self.BODY_FRAME_VALUE)

    def test_decode_body_frame_fragment_consumed_bytes(self):
        self.assertEqual(frame.decode_frame(self.BODY_FRAME)[0], 28)

    def test_decode_heartbeat_frame(self):
        self.assertIsInstance(
            frame.decode_frame(self.HEARTBEAT)[1], frame.Heartbeat)

    def test_decode_heartbeat_frame_bytes_consumed(self):
        self.assertEqual(frame.decode_frame(self.HEARTBEAT)[0], 8)

    def test_decode_frame_invalid_frame_type(self):
        self.assertRaises(exceptions.InvalidFrameError, frame.decode_frame,
                          b'\x09\x00\x00\x00\x00\x00\x00\xce')
