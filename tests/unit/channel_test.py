# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import support
import unittest


class TestChannelTransport(unittest.TestCase):
    def setUp(self):
        # Disabling this as we need a real mock connection for this with
        # the callbackmanager change
        #self.transport = channel.ChannelTransport('dummy_connection', 42)
        pass

    def test_init(self):
        pass

if __name__ == "__main__":
    unittest.main()
