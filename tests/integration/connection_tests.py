import test_base


class TestSelectConnection(test_base.SelectConnectionTestCase):

    def test_close_with_channels(self):
        """Regression test ensuring connection.close doesn't raise an
        AttributeError (see #279)

        """
        def on_channel_open(channel):
            # no real good way to handle the exception that's raised, so this
            # test will either pass or blow up
            self.stop()

        def on_connected(conn):
            conn.channel(on_channel_open)

        self.start(on_connected)
