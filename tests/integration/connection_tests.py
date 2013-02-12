from test_base import TestCase


class TestConnection(TestCase):

    def test_close_with_channels(self):
        '''regression test ensuring connection.close doesn't raise an AttributeError (see #279)'''
        def on_channel_open(channel):
            # no real good way to handle the exception that's raised, so this
            # test will either pass or blow up
            self.stop()

        def on_connected(conn):
            self.connection.channel(on_channel_open)

        self.start(on_connected)
