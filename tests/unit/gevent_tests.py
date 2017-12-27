# -*- coding:utf-8 -*-

# author: cainbit

"""
Tests for pika.adapters.tornado_connection

"""

try:
    import gevent
except ImportError:
    gevent = None

try:
    import mock
except ImportError:
    from unittest import mock

try:
    import unittest2 as unittest
except ImportError:
    import unittest

try:
    from pika.adapters import gevent_connection
except ImportError:
    gevent_connection = None


class GeventConnectionTests(unittest.TestCase):
    @unittest.skipIf(gevent is None, 'requires gevent')
    @mock.patch('pika.adapters.base_connection.BaseConnection.__init__')
    def test_gevent_connection_call_parent(self, mock_init):
        from gevent import monkey
        monkey.patch_all()
        obj = gevent_connection.GeventConnection()
        mock_init.assert_called_once_with(
            None, None, None, None,
            gevent_connection.fakeIOLoop)
