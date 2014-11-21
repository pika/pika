"""
Tests for pika.base_connection.BaseConnection

"""
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import exceptions
from pika import connection
from pika.adapters import base_connection


class BaseConnectionTests(unittest.TestCase):

    def test_should_raise_value_exception_with_no_params_func_instead(self):
      def foo():
          return True
      self.assertRaises(ValueError, base_connection.BaseConnection, foo)

    class TestBaseConnection(base_connection.BaseConnection):
        def __init__(self):
            pass

    def test_check_state_on_disconnect_skip_false_should_raise_incompatible_protocol_error(self):
        base_conn = self.TestBaseConnection()
        base_conn.connection_state = base_conn.CONNECTION_PROTOCOL
        base_conn.params = mock.Mock(spec=connection.ConnectionParameters)
        base_conn.params.skip_incompatible_protocol_error = False
        self.assertRaises(exceptions.IncompatibleProtocolError,
                          base_conn._check_state_on_disconnect)

    def test_check_state_on_disconnect_skip_true_should_return_normally(self):
        base_conn = self.TestBaseConnection()
        base_conn.connection_state = base_conn.CONNECTION_PROTOCOL
        base_conn.params = mock.Mock(spec=connection.ConnectionParameters)
        base_conn.params.skip_incompatible_protocol_error = True
        try:
            base_conn._check_state_on_disconnect()
        except exceptions.IncompatibleProtocolError:
            self.fail("IncompatibleProtocolError should not be raised")