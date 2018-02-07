import unittest

from mock import MagicMock
from pika.frame import Method, Header
from pika.exceptions import DuplicateGetOkCallback
from pika.channel import Channel
from pika.connection import Connection


class OnlyOneBasicGetTestCase(unittest.TestCase):
    def setUp(self):
        self.channel = Channel(MagicMock(Connection)(), 0, None)
        self.channel._state = Channel.OPEN
        self.callback = MagicMock()

    def test_two_basic_get_with_callback(self):
        self.channel.basic_get('test-queue', self.callback)
        self.channel._on_getok(MagicMock(Method)(), MagicMock(Header)(), '')
        self.channel.basic_get('test-queue', self.callback)
        self.channel._on_getok(MagicMock(Method)(), MagicMock(Header)(), '')
        self.assertEqual(self.callback.call_count, 2)

    def test_two_basic_get_without_callback(self):
        self.channel.basic_get('test-queue', self.callback)
        with self.assertRaises(DuplicateGetOkCallback):
            self.channel.basic_get('test-queue', self.callback)

if __name__ == '__main__':
    unittest.main()
