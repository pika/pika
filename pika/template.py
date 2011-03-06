# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Pika Impementation Templates

Template classes to extend that make building asynchronous Pika clients easier
"""

from pika.object import object_
import pika.spec as spec


class AsyncConsumer(object_):

    def __init__(self, reconnect=False):

        pass

    def connect(self, host='localhost', port=spec.PORT, vhost='/',
                username='guest', password='guest'):
        pass

    def on_connected(self, connection):
        pass

    def on_disconnected(self):
        pass

    def declare_exchange(self, name, type='direct',
                         auto_delete=False, durable=True):
        pass

    def declare_queue(self, queue):
        pass
