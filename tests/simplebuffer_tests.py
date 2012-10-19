"""
Tests for pika.simplebuffer

"""
import cStringIO
import mock
import os
import socket
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import simplebuffer



class SimpleBuffer(unittest.TestCase):

    def test_simple_buffer_init_buf_is_stringio(self):
        obj = simplebuffer.SimpleBuffer()
        self.assertIsInstance(obj.buf, cStringIO.OutputType)

    def test_simple_buffer_init_buf_position(self):
        # Position should be set to the end
        obj = simplebuffer.SimpleBuffer('Foo Bar')
        self.assertEqual(obj.buf.tell(), 7)

    def test_simple_buffer_init_write(self):
        # Position should be set to the end
        obj = simplebuffer.SimpleBuffer('Foo Bar')
        obj.buf.seek(0)
        self.assertEqual(obj.buf.read(), 'Foo Bar')

    def test_simple_buffer_write(self):
        # Position should be set to the end
        obj = simplebuffer.SimpleBuffer('Foo Bar')
        obj.write(' Baz')
        obj.buf.seek(0)
        self.assertEqual(obj.buf.read(), 'Foo Bar Baz')

    def test_simple_buffer_write_empty(self):
        obj = simplebuffer.SimpleBuffer('Foo Bar')
        obj.write('')
        obj.buf.seek(0)
        self.assertEqual(obj.buf.read(), 'Foo Bar')

    def test_simple_buffer_write_empty_size(self):
        obj = simplebuffer.SimpleBuffer('Foo Bar')
        obj.write('')
        self.assertEqual(obj.size, 7)

    def test_simple_buffer_read_none(self):
        obj = simplebuffer.SimpleBuffer('Foo Bar')
        self.assertEqual(obj.read(), 'Foo Bar')

    def test_simple_buffer_read_no_bytes(self):
        obj = simplebuffer.SimpleBuffer()
        self.assertEqual(obj.read(0), '')

    def test_simple_buffer_read_offset(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        obj.offset = 5
        self.assertEqual(obj.read(), '56789')

    def test_simple_buffer_read_offset_sized(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        obj.offset = 5
        self.assertEqual(obj.read(3), '567')

    def test_simple_buffer_consume(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        obj.consume(5)
        self.assertEqual(obj.size, 5)

    def test_simple_buffer_consume_read(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        obj.consume(5)
        self.assertEqual(obj.read(5), '56789')

    def test_simple_buffer_consume_cleanup(self):
        obj = simplebuffer.SimpleBuffer()
        obj.offset = 65538
        obj.size = 0
        obj.consume(0)
        self.assertEqual(obj.offset, 0)

    def test_read_and_consume_content(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        self.assertEqual(obj.read_and_consume(5), '01234')

    def test_read_and_consume_offset(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        obj.read_and_consume(5)
        self.assertEqual(obj.offset, 5)

    def test_send_to_socket_content(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        with mock.patch('socket.socket') as mock_socket:
            obj.send_to_socket(mock_socket)
            mock_socket.send.called_once_with('0123456789')

    def test_send_to_socket_offset(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        with mock.patch('socket.socket') as mock_socket:
            mock_socket.send = mock.Mock(return_value=5)
            obj.send_to_socket(mock_socket)
            self.assertEqual(obj.offset, 5)

    def test_send_to_socket_offset_reset(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        with mock.patch('socket.socket') as mock_socket:
            mock_socket.send = mock.Mock(return_value=10)
            obj.offset = 1000000
            with mock.patch.object(obj, 'consume') as consume:
                obj.send_to_socket(mock_socket)
                consume.assert_called_once_with(0)

    def test_flush(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        obj.flush()
        self.assertEqual(obj.size, 0)

    def test_flush_offset(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        obj.flush()
        self.assertEqual(obj.offset, 10)

    def test_nonzero(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        self.assertFalse(not obj)

    def test_len(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        self.assertEqual(len(obj), 10)

    def test_repr(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        self.assertEqual(repr(obj),
                         "<SimpleBuffer of 10 bytes, 10 total size, "
                         "'0123456789'>")

    def test_str(self):
        obj = simplebuffer.SimpleBuffer('0123456789')
        self.assertEqual(str(obj),
                         "<SimpleBuffer of 10 bytes, 10 total size, "
                         "'0123456789'>")
