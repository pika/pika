# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Send n messages and confirm you can retrieve them with Basic.Consume
"""
import nose
import support
import support.tools
from pika.adapters import SelectConnection
from pika.spec import Basic


class TestConsumeCancel(support.tools.AsyncPattern):

    def __init__(self):
        support.tools.AsyncPattern.__init__(self)
        self._sent = list()
        self._received = list()

    @nose.tools.timed(3)
    def test_consume_and_cancel(self):
        self.confirmed = False
        self.connection = self._connect(SelectConnection, support.PARAMETERS)
        self.connection.ioloop.start()
        if not self.confirmed:
            assert False, "Did not receive Basic.CancelOk"
        pass

    def _on_channel(self, channel):
        self.channel = channel
        self._queue_declare()

    @support.tools.timeout
    def _on_queue_declared(self, frame):
        #self.connection.add_timeout(10, self._on_timeout)
        self.channel.add_callback(self.on_consume_ok, [Basic.ConsumeOk])
        self.channel.basic_consume(self._on_message, queue=self._queue,
                                   consumer_tag=self.__class__.__name__)

    def _on_message(self, channel, method, header, body):
        assert False, "Unexpected content message received."

    def on_consume_ok(self, frame):
        # Wait 1 second until we call basic_cancel so that it can catch up with
        # events since we're fired before anything internally is
        self.connection.add_timeout(.25, self._on_cancel_ready)

    def _on_cancel_ready(self):
        self.channel.basic_cancel(consumer_tag=self.__class__.__name__,
                                  callback=self.on_cancel_ok)

    def on_cancel_ok(self, frame):
        if frame.method.NAME == 'Basic.CancelOk':
            self.confirmed = True
        # Wait 1 second until we call basic_cancel so that it can catch up with
        # events since we're fired before anything internally is
        self.connection.add_timeout(.25, self._close)

    def _close(self):
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.close()
