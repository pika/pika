# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
"""
Declare a queue, consume from it and delete it without cancelling.
"""
import nose
import support
import support.tools
from pika.adapters import SelectConnection


class TestQueueConsumeAndDelete(support.tools.AsyncPattern):

    @nose.tools.timed(2)
    def test_consume_and_delete(self):
        self.confirmed = False
        self.connection = self._connect(SelectConnection, support.PARAMETERS)
        self.connection.ioloop.start()
        if not self.confirmed:
            assert False, 'Did not receive the remote queue delete callback.'
        pass

    def _on_channel(self, channel):
        self.channel = channel
        self.channel.exchange_declare(exchange='queue_delete_exchange',
                                      type="direct", auto_delete=True,
                                      callback=self._on_exchange_declare)

    def _on_exchange_declare(self, frame):
        self._queue_declare()

    def _on_queue_declared(self, frame):
        self.channel.queue_bind(queue=self._queue,
                                exchange='queue_delete_exchange',
                                callback=self._on_queue_bound)

    def _on_queue_bound(self, frame):
        self.channel.basic_consume(consumer_callback=lambda *args: None,
                                   queue=self._queue)
        self.channel.queue_delete(callback=self._on_queue_deleted,
                                  queue=self._queue)

    def _on_queue_deleted(self, frame):
        self.confirmed = True
        self.connection.close()
