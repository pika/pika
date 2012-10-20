"""
Tests for pika.channel.ContentFrameDispatcher

"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import channel
from pika import exceptions
from pika import frame
from pika import spec


class ContentFrameDispatcherTests(unittest.TestCase):

    def test_init_method_frame(self):
        obj = channel.ContentFrameDispatcher()
        self.assertEqual(obj._method_frame, None)

    def test_init_header_frame(self):
        obj = channel.ContentFrameDispatcher()
        self.assertEqual(obj._header_frame, None)

    def test_init_seen_so_far(self):
        obj = channel.ContentFrameDispatcher()
        self.assertEqual(obj._seen_so_far, 0)

    def test_init_body_fragments(self):
        obj = channel.ContentFrameDispatcher()
        self.assertEqual(obj._body_fragments, list())

    def test_process_with_basic_deliver(self):
        value = frame.Method(1, spec.Basic.Deliver())
        obj = channel.ContentFrameDispatcher()
        obj.process(value)
        self.assertEqual(obj._method_frame, value)

    def test_process_with_content_header(self):
        value = frame.Header(1, 100, spec.BasicProperties)
        obj = channel.ContentFrameDispatcher()
        obj.process(value)
        self.assertEqual(obj._header_frame, value)

    def test_process_with_body_frame_partial(self):
        obj = channel.ContentFrameDispatcher()
        value = frame.Header(1, 100, spec.BasicProperties)
        obj.process(value)
        value = frame.Method(1, spec.Basic.Deliver())
        obj.process(value)
        value = frame.Body(1, 'abc123')
        obj.process(value)
        self.assertEqual(obj._body_fragments, [value.fragment])

    def test_process_with_full_message(self):
        obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        obj.process(method_frame)
        header_frame = frame.Header(1, 6, spec.BasicProperties)
        obj.process(header_frame)
        body_frame = frame.Body(1, 'abc123')
        response = obj.process(body_frame)
        self.assertEqual(response, (method_frame, header_frame, 'abc123'))

    def test_process_with_body_frame_six_bytes(self):
        obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties)
        obj.process(header_frame)
        body_frame = frame.Body(1, 'abc123')
        obj.process(body_frame)
        self.assertEqual(obj._seen_so_far, 6)

    def test_process_with_body_frame_too_big(self):
        obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        obj.process(method_frame)
        header_frame = frame.Header(1, 6, spec.BasicProperties)
        obj.process(header_frame)
        body_frame = frame.Body(1, 'abcd1234')
        self.assertRaises(exceptions.BodyTooLongError,
                          obj.process, body_frame)

    def test_process_with_unexpected_frame_type(self):
        obj = channel.ContentFrameDispatcher()
        value = frame.Method(1, spec.Basic.Qos())
        self.assertRaises(exceptions.UnexpectedFrameError,
                          obj.process, value)

    def test_reset_method_frame(self):
        obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties)
        obj.process(header_frame)
        body_frame = frame.Body(1, 'abc123')
        obj.process(body_frame)
        obj._reset()
        self.assertEqual(obj._method_frame, None)

    def test_reset_header_frame(self):
        obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties)
        obj.process(header_frame)
        body_frame = frame.Body(1, 'abc123')
        obj.process(body_frame)
        obj._reset()
        self.assertEqual(obj._header_frame, None)

    def test_reset_seen_so_far(self):
        obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties)
        obj.process(header_frame)
        body_frame = frame.Body(1, 'abc123')
        obj.process(body_frame)
        obj._reset()
        self.assertEqual(obj._seen_so_far, 0)

    def test_reset_body_fragments(self):
        obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties)
        obj.process(header_frame)
        body_frame = frame.Body(1, 'abc123')
        obj.process(body_frame)
        obj._reset()
        self.assertEqual(obj._body_fragments, list())
