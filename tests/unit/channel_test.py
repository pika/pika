# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import unittest
import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))
import pika.channel as channel

class TestChannelTransport(unittest.TestCase):
    def setUp(self):
        self.transport = channel.ChannelTransport('dummy_connection', 42)

    def test_init(self):
        pass


if __name__ == '__main__':
    unittest.main()
