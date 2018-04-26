"""
Tests for pika.adapters.twisted_connection
"""
import unittest

import mock

from pika.adapters import twisted_connection


# missing-docstring
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103


class TwistedProtocolConnectionTests(unittest.TestCase):

    @mock.patch('pika.connection.Connection.add_on_close_callback')
    def test_twisted_protocol_connection_call_parent(self, mock_add):
        twisted_connection.TwistedProtocolConnection(
            None,
            on_close_callback=self._on_close)
        mock_add.assert_called_once_with(self._on_close)

    @staticmethod
    def _on_close(connection, error):
        pass
