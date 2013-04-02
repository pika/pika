import os
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import logging

import pika
import yaml

CONFIG = yaml.load(open(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     'broker.conf')))

LOGGING_LEVEL = logging.critical
TIMEOUT = 4


class TestCase(unittest.TestCase):

    def __call__(self, result=None):
        self._result = result
        test_method = getattr(self, self._testMethodName)
        skipped = (getattr(self.__class__, '__unittest_skip__', False) or
                   getattr(test_method, '__unittest_skip__', False))
        if not skipped:
            try:
                self._pre_setup()
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                result.addError(self, sys.exc_info())
                return
        super(TestCase, self).__call__(result)

        if not skipped:
            try:
                self._post_teardown()
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                result.addError(self, sys.exc_info())
                return

    def _get_parameters(self):
        self.credentials = pika.PlainCredentials(CONFIG['username'],
                                                 CONFIG['password'])
        return pika.ConnectionParameters(host=CONFIG['host'],
                                         port=CONFIG['port'],
                                         virtual_host=CONFIG['virtual_host'],
                                         credentials=self.credentials)

    def _pre_setup(self):
        self._timed_out = False
        logging.getLogger('pika').setLevel(LOGGING_LEVEL)
        self.parameters = self._get_parameters()
        self._timeout_id = None
        self.connection = None
        self.config = CONFIG

    def _post_teardown(self):
        del self.credentials
        del self.parameters
        del self.connection
        del self.config
        del self._timeout_id
        del self._timed_out

    def start(self):
        pass

    def stop(self):
        """close the connection and stop the ioloop"""
        self.connection.remove_timeout(self._timeout_id)
        self.connection.add_timeout(4, self._on_close_timeout)
        self.connection.add_on_close_callback(self._on_closed)
        self.connection.close()

    def _on_closed(self, connection, reply_code, reply_text):
        """called when the connection has finished closing"""
        self.connection.ioloop.stop()
        if self._timed_out:
            raise AssertionError('Timed out. Did you call `stop`?')

    def _on_close_timeout(self):
        """called when stuck waiting for connection to close"""
        # force the ioloop to stop
        self.connection.ioloop.stop()
        raise AssertionError('Timed out waiting for connection to close')

    def _on_timeout(self):
        self._timed_out = True
        self.stop()


def SelectConnectionTestCase(TestCase):

    def start(self, on_connected, on_error, on_closed):
        """Connect to rabbitmq and start the ioloop

        """
        self.connection = pika.SelectConnection(self.parameters,
                                                on_connected,
                                                on_error,
                                                on_closed,
                                                False)
        self.connection.ioloop.start()
